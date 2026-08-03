#!/usr/bin/env python3
"""
smartess_poller.py — active Voltronic (PI30) poller for Eybond / SmartESS Wi-Fi dongles.

Most Eybond/SmartESS Wi-Fi dongles bundled with Axpert / Voltronic / MPP Solar inverters
only push a full data frame to the cloud every ~5-10 minutes. This tool replaces the
official cloud (and the passive SmartESS-proxy relay): it impersonates the Eybond cloud,
then actively polls the inverter with Voltronic PI30 commands (QPIGS, QMOD, ...) as often
as you like and publishes the parsed values to MQTT.

How it works
------------
The dongle connects out to `ess.eybond.com:502`. Redirect that hostname to this machine
(e.g. via Pi-hole / your router's local DNS). The dongle then opens a TCP session here.
We speak the real Eybond framing:

    5E <msg-id> 00 01 <len:2>  FF 04  <ASCII Voltronic command> <CRC16> 0x0D

`FF 04` is a transparent pass-through: the dongle forwards the ASCII command over RS485 to
the inverter and returns the inverter's reply wrapped in the same frame. `QPIGS` returns
live telemetry; we parse and publish it. See docs/PROTOCOL.md for the full reverse-eng notes.

Config
------
Reads config.ini next to this file if present; otherwise uses the defaults below.
Environment variables (SMARTESS_*) override both. No code editing required.
"""

import calendar
import configparser
import http.server
import json
import os
import queue
import socket
import sys
import threading
import time
import urllib.request
import urllib.parse

try:
    import paho.mqtt.client as mqtt
except ImportError:
    sys.exit("Missing dependency. Install with:\n"
             "  sudo apt install -y python3-paho-mqtt   # Debian/Raspberry Pi OS\n"
             "  pip3 install paho-mqtt                  # or via pip")

# --------------------------------------------------------------------------- config
DEFAULTS = {
    "listen_host":   "0.0.0.0",
    "listen_port":   "502",
    "mqtt_host":     "127.0.0.1",
    "mqtt_port":     "1883",
    "mqtt_user":     "",
    "mqtt_pass":     "",
    "topic":         "smartess/",
    "poll_interval": "10",   # seconds between polls; 5-10 is safe, do not go below 3
    "sock_timeout":  "8",
    # Passthrough mode: if cloud_host is set, relay the dongle <-> real Eybond cloud
    # transparently (the SmartESS app keeps working) AND poll in parallel for MQTT.
    # If empty, we act as the sole (fake) cloud. Use the cloud's real IP here, since
    # the hostname is DNS-redirected to us:  dig +short ess.eybond.com @8.8.8.8
    "mode":          "mirror",  # "mirror" (app works, transparent relay) or "local" (fast 10s, no app)
    "cloud_host":    "",
    "cloud_port":    "502",
    "log_cloud":     "false",   # in mirror, print every cloud request/reply (for analysis)
    "control_port":  "8899",    # HTTP endpoint to toggle mode from a dashboard button (0 = off)
}


def load_config():
    cfg = dict(DEFAULTS)
    ini_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.ini")
    if os.path.exists(ini_path):
        parser = configparser.ConfigParser()
        parser.read(ini_path)
        if parser.has_section("smartess"):
            for key in cfg:
                if parser.has_option("smartess", key):
                    cfg[key] = parser.get("smartess", key)
    # env overrides, e.g. SMARTESS_POLL_INTERVAL=5
    for key in cfg:
        env = os.environ.get("SMARTESS_" + key.upper())
        if env is not None:
            cfg[key] = env
    cfg["listen_port"] = int(cfg["listen_port"])
    cfg["mqtt_port"] = int(cfg["mqtt_port"])
    cfg["cloud_port"] = int(cfg["cloud_port"])
    cfg["control_port"] = int(cfg["control_port"])
    cfg["poll_interval"] = float(cfg["poll_interval"])
    cfg["sock_timeout"] = float(cfg["sock_timeout"])
    return cfg


# ------------------------------------------------------------------ Voltronic commands
# Exact command payloads captured from the real Eybond cloud.
# Frame payload layout: FF 04 <ASCII cmd> <CRC16> 0x0D
QPIGS = bytes.fromhex("ff045150494753b7a90d")   # general status (live telemetry)
QMOD  = bytes.fromhex("ff04514d4f4449c10d")     # current working mode
QPIWS = bytes.fromhex("ff045150495753b4da0d")   # warning / fault status
QET   = bytes.fromhex("ff0451455481b60d")       # total generated energy
QID   = bytes.fromhex("ff04514944d6ea0d")       # serial number (static)
QPIRI = bytes.fromhex("ff045150495249f8540d")   # rated information (static)
QMN   = bytes.fromhex("ff04514d4ebb640d")       # model name (static)
QPI   = bytes.fromhex("ff04515049beac0d")       # protocol id (static)
QVFW  = bytes.fromhex("ff045156465762990d")     # main CPU firmware (static)
QVFW3 = bytes.fromhex("ff045156465733d3d40d")   # secondary firmware (static)
QFLAG = bytes.fromhex("ff0451464c414798740d")   # device on/off feature flags

# ---- battery float-voltage presets (Eco/Backup dashboard toggle) ----------
# Two safe presets for the 16S LiFePO4 pack. The dashboard toggle writes one of
# these to the inverter's FLOAT voltage (PI30 PBFT): Eco keeps the pack ~80% for
# longevity when the grid is stable; Backup tops it to ~95% before outages.
# Requires "local" mode (we own the RS485 bus); a no-op in "mirror".
BATT_PRESETS = {"eco": 54.2, "backup": 55.6}
_cmd_q = queue.Queue()           # (ascii PI30 SET command, Event, result_box); drained by the local poll loop


def _pi30_crc(data):
    """Voltronic PI30 CRC-16 (CCITT/XMODEM) with 0x28/0x0d/0x0a byte-stuffing."""
    crc = 0
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) if (crc & 0x8000) else (crc << 1)
            crc &= 0xFFFF
    out = bytearray([(crc >> 8) & 0xFF, crc & 0xFF])
    for i in (0, 1):
        if out[i] in (0x28, 0x0d, 0x0a):     # avoid '(' / CR / LF in the CRC bytes
            out[i] += 1
    return bytes(out)


def pi30_cmd(cmd):
    """Build an FF04 command frame (FF 04 <ASCII> <CRC16> 0D) from an ASCII command."""
    a = cmd.encode("ascii")
    return b"\xff\x04" + a + _pi30_crc(a) + b"\x0d"


def battery_mode_for(volts):
    """Map a float voltage to the toggle's mode label (or None if unknown)."""
    if not isinstance(volts, (int, float)):
        return None
    return "backup" if volts >= 54.9 else "eco"


def queue_cmd(ascii_cmd):
    """Thread-safe: ask the local poll loop to send a PI30 SET command over RS485 and
    block briefly for the ACK/NAK. Returns {ok, reply, cmd}. Local mode only (the loop
    owns the bus). The loop re-reads QPIRI+QFLAG after a successful write."""
    ev = threading.Event()
    box = {"ok": None, "reply": "", "cmd": ascii_cmd}
    _cmd_q.put((ascii_cmd, ev, box))
    ev.wait(timeout=20)                                 # loop drains once per poll (~10 s)
    return box


def queue_set_float(volts):
    """Set the battery FLOAT voltage (Eco/Backup toggle). Hard-clamped."""
    volts = max(50.0, min(55.6, float(volts)))
    box = queue_cmd("PBFT%.1f" % volts)
    box["volts"] = volts
    return box

# device identity, filled as the static commands come in; served at /info
INFO = {}
# rated charge targets from QPIRI, surfaced as metrics for the dashboard
RATED = {}
# full parsed QPIRI snapshot (all fields + enum *_name), served read-only at /rated
RATED_ALL = {}
# latest decoded QPIWS warnings/faults, served at /warnings
WARN = {"active": [], "raw": "", "level": "ok"}
# dashboard settings shared across clients (served/saved at /settings)
SETTINGS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".dashboard_settings.json")

# QPIGS fields, in order, per the Voltronic PI30 spec.
QPIGS_FIELDS = [
    "grid_voltage", "grid_frequency",
    "ac_output_voltage", "ac_output_frequency",
    "ac_output_apparent_power", "ac_output_active_power",
    "output_load_percent", "bus_voltage",
    "battery_voltage", "battery_charge_current", "battery_capacity",
    "heatsink_temperature", "pv_input_current", "pv_input_voltage",
    "scc_voltage", "battery_discharge_current",
    "device_status", "fan_offset", "eeprom_version",
    "pv_charging_power", "device_status_2",
]

MODE_NAMES = {
    "P": "PowerOn", "S": "Standby", "L": "Line", "B": "Battery",
    "F": "Fault", "H": "PowerSaving", "D": "Shutdown", "Y": "Bypass",
}
MODE_CODES = {"P": 0, "S": 1, "L": 2, "B": 3, "F": 4, "H": 5, "D": 6, "Y": 7}

# QFLAG device feature flags (PI30). Reply is "(E<enabled letters>D<disabled letters>".
# Each letter is a toggleable function; we surface them as flag_<name> = 1/0.
QFLAG_NAMES = {
    "a": "silence_buzzer", "b": "overload_bypass", "j": "power_saving",
    "k": "lcd_home_timeout", "u": "overload_restart", "v": "overtemp_restart",
    "x": "backlight", "y": "alarm_on_source_interrupt", "z": "fault_record",
}


# --------------------------------------------------------------- weather (solar)
# Fetch tilted-plane irradiance for the array from Open-Meteo (free, no key) and
# publish it to MQTT so it lands in VictoriaMetrics. Storing the *modelled* GTI
# next to the *actual* PV lets us build real history and calibrate the forecast.
# Runs in its own thread — it never touches the RS485 bus, so it works in any mode.
WEATHER_INTERVAL = 300     # seconds between weather writes (~5 min; GTI interpolated from the 15-min source)
_WEATHER_URL = ("https://api.open-meteo.com/v1/forecast?latitude=%s&longitude=%s"
                "&current=cloud_cover,temperature_2m"
                "&minutely_15=global_tilted_irradiance"
                "&tilt=%d&azimuth=%d&forecast_days=1&timezone=GMT")

# PV system derate (PVWatts-style): a temperature-independent base (wiring, inverter,
# soiling, mismatch) times a cell-temperature factor. Cell temp from the NOCT model.
# Keep these in sync with the dashboard (pvDerate in web/index.html).
PV_NOCT = 45.0           # nominal operating cell temperature (°C)
PV_TEMP_COEFF = 0.004    # power loss per °C above 25 (crystalline Si ~0.4%/°C)
PV_BASE_DERATE = 0.90    # non-temperature losses


def pv_derate(gti, t_air):
    """System derate incl. cell-temperature effect. Falls back to the base
    derate when air temperature is unknown."""
    if t_air is None:
        return PV_BASE_DERATE
    t_cell = t_air + (gti / 800.0) * (PV_NOCT - 20.0)
    return max(0.5, min(1.05, PV_BASE_DERATE * (1.0 - PV_TEMP_COEFF * (t_cell - 25.0))))


def _load_pv_geo():
    """PV location/geometry from the dashboard settings file: (lat, lon, kWp, tilt, az)."""
    try:
        with open(SETTINGS_FILE) as f:
            s = json.load(f)
        lat = float(s["pvlat"]); lon = float(s["pvlon"])
    except Exception:
        return None

    def num(key, default):
        v = s.get(key)
        try:
            return float(v) if v not in (None, "") else default
        except (TypeError, ValueError):
            return default

    return lat, lon, num("pvkwp", 0.0), num("pvtilt", 30.0), num("pvaz", 180.0)


def fetch_weather_once(mc, topic):
    """Pull current cloud/temp + 15-min tilted irradiance, publish weather_json."""
    geo = _load_pv_geo()
    if not geo:
        return None
    lat, lon, kwp, tilt, az = geo
    url = _WEATHER_URL % (lat, lon, round(tilt), round(az - 180))   # Open-Meteo az: 0=S,+W
    with urllib.request.urlopen(url, timeout=20) as r:
        j = json.loads(r.read().decode("utf-8"))
    cur = j.get("current") or {}
    out = {}
    if cur.get("cloud_cover") is not None:
        out["cloud"] = cur["cloud_cover"]
    if cur.get("temperature_2m") is not None:
        out["temp"] = cur["temperature_2m"]
    m = j.get("minutely_15") or {}
    times, gtis = m.get("time") or [], m.get("global_tilted_irradiance") or []
    now = time.time()
    # linearly interpolate the 15-min GTI to *now* (source is 15-min; this gives a
    # smooth value so a 5-min write cadence produces a smooth curve, not a staircase)
    epochs = [calendar.timegm(time.strptime(t, "%Y-%m-%dT%H:%M")) for t in times]
    gti = None
    if epochs and gtis:
        if now <= epochs[0]:
            gti = gtis[0]
        elif now >= epochs[-1]:
            gti = gtis[-1]
        else:
            for i in range(1, len(epochs)):
                if epochs[i] >= now:
                    a0, a1 = gtis[i - 1], gtis[i]
                    if a0 is None or a1 is None:
                        gti = a1 if a0 is None else a0
                    else:
                        f = (now - epochs[i - 1]) / ((epochs[i] - epochs[i - 1]) or 1)
                        gti = a0 + (a1 - a0) * f
                    break
    if gti is not None:
        out["gti"] = round(gti, 1)
        if kwp > 0:
            out["pv_potential_w"] = round(kwp * out["gti"] * pv_derate(out["gti"], out.get("temp")))
    if out:
        mc.publish(topic + "weather_json", json.dumps(out), retain=True)
    return out


def weather_loop(mc, topic):
    while True:
        try:
            fetch_weather_once(mc, topic)
        except Exception as e:
            print(time.strftime("%H:%M:%S"), "weather fetch failed:", e)
        time.sleep(WEATHER_INTERVAL)


# ------------------------------------------------------- settings change-history
# Inverter settings (QPIRI + QFLAG) change rarely, so we log only *changes*: an
# append-only JSONL audit trail (old -> new) plus a retained rated_json snapshot
# so VictoriaMetrics keeps sparse change-points. State is persisted across
# restarts so a poller restart alone doesn't fabricate a "change".
RATED_HISTORY_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".rated_history.jsonl")
WARN_HISTORY_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".warnings_history.jsonl")
RATED_STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".rated_state.json")
_rated_state = {}
_rated_lock = threading.Lock()


def _load_rated_state():
    global _rated_state
    try:
        with open(RATED_STATE_FILE) as f:
            _rated_state = json.load(f)
    except Exception:
        _rated_state = {}


def note_rated_changes(mc, topic, updates, names=None):
    """Merge a partial {key: value} settings update into the running snapshot.
    On any real change (or a first-seen value), append a JSONL event and publish
    the full numeric snapshot to rated_json. `names` maps key -> readable value."""
    names = names or {}
    with _rated_lock:
        changes = []
        for k, v in updates.items():
            old = _rated_state.get(k)
            if old != v:
                changes.append({"key": k, "old": old, "new": v, "label": names.get(k)})
                _rated_state[k] = v
        if not changes:
            return
        event = {"ts": int(time.time()),
                 "iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                 "changes": changes}
        try:
            with open(RATED_HISTORY_FILE, "a") as f:
                f.write(json.dumps(event) + "\n")
            with open(RATED_STATE_FILE, "w") as f:
                json.dump(_rated_state, f)
        except Exception as e:
            print(time.strftime("%H:%M:%S"), "rated history write failed:", e)
        snapshot = {k: v for k, v in _rated_state.items() if isinstance(v, (int, float))}
        mc.publish(topic + "rated_json", json.dumps(snapshot), retain=True)
        for c in changes:
            print(time.strftime("%H:%M:%S"), "setting changed:",
                  c["key"], c["old"], "->", c["new"])


def parse_qflag(text):
    """Parse a QFLAG body ('E<enabled letters>D<disabled letters>') -> {flag_<name>: 1/0}."""
    if not text or text[0] not in "ED":
        return {}
    out, enabled = {}, True
    for ch in text:
        if ch == "E":
            enabled = True
        elif ch == "D":
            enabled = False
        elif ch in QFLAG_NAMES:
            out["flag_" + QFLAG_NAMES[ch]] = 1 if enabled else 0
    return out


def publish_qflag(mc, topic, text):
    """Publish device feature flags and feed them into the settings change-log."""
    flags = parse_qflag(text)
    if not flags:
        return
    for k, v in flags.items():
        mc.publish(topic + "rated/" + k, v, retain=True)
    RATED_ALL.update(flags)
    note_rated_changes(mc, topic, flags)


def publish_mode(mc, topic, letter):
    mc.publish(topic + "mode", letter, retain=True)
    mc.publish(topic + "mode_name", MODE_NAMES.get(letter, letter), retain=True)
    mc.publish(topic + "mode_code", MODE_CODES.get(letter, -1), retain=True)

# QPIRI (rated info) fields, in order, per PI30.
QPIRI_FIELDS = [
    "grid_rating_voltage", "grid_rating_current",
    "ac_output_rating_voltage", "ac_output_rating_frequency",
    "ac_output_rating_current", "ac_output_rating_apparent_power",
    "ac_output_rating_active_power", "battery_rating_voltage",
    "battery_recharge_voltage", "battery_under_voltage",
    "battery_bulk_voltage", "battery_float_voltage", "battery_type",
    "max_ac_charging_current", "max_charging_current", "input_voltage_range",
    "output_source_priority", "charger_source_priority", "parallel_max_number",
    "machine_type", "topology", "output_mode", "battery_redischarge_voltage",
    "pv_ok_condition", "pv_power_balance",
]
# Human-readable names for the enum fields in QPIRI.
QPIRI_ENUMS = {
    "battery_type": {"0": "AGM", "1": "Flooded", "2": "User", "3": "Pylontech"},
    "input_voltage_range": {"0": "Appliance", "1": "UPS"},
    "output_source_priority": {"0": "Utility", "1": "Solar", "2": "SBU"},
    "charger_source_priority": {"0": "UtilityFirst", "1": "SolarFirst",
                                "2": "Solar+Utility", "3": "OnlySolar"},
    "output_mode": {"0": "SingleMachine", "1": "Parallel", "2": "Phase1",
                    "3": "Phase2", "4": "Phase3"},
}

# --------------------------------------------------------- settings CONTROL (write)
# Controllable settings via PI30 SET commands. Every value is validated and hard-clamped
# to a safe LiFePO4 range; writes are LOCAL-mode only and confirmed by a QPIRI re-read.
# Charge-current setters (MNCHGC/MUCHGC) are intentionally omitted until their exact
# format is confirmed on this firmware.
def _vset(prefix, lo, hi):
    return {"type": "float", "min": lo, "max": hi, "step": 0.1,
            "cmd": lambda v: "%s%.1f" % (prefix, max(lo, min(hi, float(v))))}

SET_CATALOG = {
    "battery_float_voltage":       _vset("PBFT", 50.0, 56.4),
    "battery_bulk_voltage":        _vset("PCVV", 52.0, 57.6),
    "battery_recharge_voltage":    _vset("PBCV", 44.0, 51.0),
    "battery_redischarge_voltage": {"type": "float", "min": 0.0, "max": 58.0, "step": 0.1,
                                    "cmd": lambda v: "PBDV%.1f" % max(0.0, min(58.0, float(v)))},
    "battery_under_voltage":       _vset("PSDV", 42.0, 48.0),
    "output_source_priority":      {"type": "enum", "options": {0: "Utility", 1: "Solar", 2: "SBU"},
                                    "cmd": lambda v: "POP%02d" % int(v)},
    "charger_source_priority":     {"type": "enum", "options": {0: "UtilityFirst", 1: "SolarFirst",
                                                                2: "Solar+Utility", 3: "OnlySolar"},
                                    "cmd": lambda v: "PCP%02d" % int(v)},
    "battery_type":                {"type": "enum", "options": {0: "AGM", 1: "Flooded", 2: "User"},
                                    "cmd": lambda v: "PBT%02d" % int(v)},
}
FLAG_LETTERS = {"flag_" + name: letter for letter, name in QFLAG_NAMES.items()}


def build_set_cmd(key, value):
    """Return the ASCII PI30 SET command for (key, value), validated/clamped, or None."""
    spec = SET_CATALOG.get(key)
    if spec:
        try:
            if spec["type"] == "enum":
                return spec["cmd"](int(value)) if int(value) in spec["options"] else None
            return spec["cmd"](float(value))
        except (ValueError, TypeError):
            return None
    if key in FLAG_LETTERS:
        on = str(value).lower() in ("1", "true", "on")
        return "P" + ("E" if on else "D") + FLAG_LETTERS[key]
    return None


def catalog_json():
    """Serializable description of the controllable settings, for the dashboard UI."""
    out = {}
    for key, spec in SET_CATALOG.items():
        d = {"type": spec["type"]}
        if "options" in spec:
            d["options"] = spec["options"]
        if "min" in spec:
            d.update(min=spec["min"], max=spec["max"], step=spec.get("step", 0.1))
        out[key] = d
    for key in FLAG_LETTERS:
        out[key] = {"type": "flag"}
    return out

# QPIGS "device status" byte (b7..b0), left-to-right in the 8-char string.
QPIGS_STATUS_BITS = {  # topic_name: string index
    "config_changed": 1,   # b6
    "load_on":        3,   # b4
    "charging":       5,   # b2
    "charging_scc":   6,   # b1 (from solar controller)
    "charging_ac":    7,   # b0 (from grid)
}
# QPIGS "device status 2" (b10 b9 b8), left-to-right in the 3-char string.
QPIGS_STATUS2_BITS = {
    "charging_to_float": 0,  # b10
    "switch_on":         1,  # b9
}
# QPIWS warning/fault bit positions (index in the returned bit string) -> name.
QPIWS_BITS = {
    0: "pv_loss",               # bit 0 = PV Loss (no solar input) — confirmed via the DessMonitor alarm log
    1: "inverter_fault", 2: "bus_over", 3: "bus_under", 4: "bus_soft_fail",
    5: "line_fail", 6: "opv_short", 7: "inverter_voltage_low",
    8: "inverter_voltage_high", 9: "over_temperature", 10: "fan_locked",
    11: "battery_voltage_high", 12: "battery_low_alarm",
    14: "battery_under_shutdown", 16: "over_load", 17: "eeprom_fault",
    18: "inverter_over_current", 19: "inverter_soft_fail", 20: "self_test_fail",
    21: "op_dc_over_voltage", 22: "battery_open", 23: "current_sensor_fail",
    24: "battery_short", 25: "power_limit", 26: "pv_voltage_high",
    27: "mppt_overload_fault", 28: "mppt_overload_warning",
    29: "battery_too_low_to_charge",
}

# Severity per QPIWS bit. "fault" = a protection tripped / inverter has (or is about to) stop —
# needs attention now. "warning" = advisory or derating condition. Everything not listed is a warning.
QPIWS_FAULTS = {
    "inverter_fault", "bus_over", "bus_under", "bus_soft_fail", "opv_short",
    "inverter_voltage_low", "inverter_voltage_high", "inverter_over_current",
    "inverter_soft_fail", "self_test_fail", "op_dc_over_voltage", "battery_open",
    "current_sensor_fail", "battery_short", "mppt_overload_fault",
    "battery_under_shutdown", "over_temperature",
}


def qpiws_severity(name):
    return "fault" if name in QPIWS_FAULTS else "warning"

# ------------------------------------------------------------------------- framing
_mid = [0x53]  # rolling message id; the dongle echoes it back in its reply


def _next_id():
    _mid[0] = (_mid[0] + 1) & 0xFF
    return _mid[0]


def _build(mid, payload):
    return bytes([0x5E, mid, 0x00, 0x01]) + len(payload).to_bytes(2, "big") + payload


class FrameReader:
    """Buffered reader that yields (msg_id, payload) frames from the TCP stream."""

    def __init__(self, sock):
        self.sock = sock
        self.buf = b""

    def read_raw(self):
        """Return the next complete frame as raw bytes, or None on EOF."""
        while True:
            # resync to the 0x5E start byte
            while self.buf and self.buf[0] != 0x5E:
                self.buf = self.buf[1:]
            if len(self.buf) >= 6:
                length = int.from_bytes(self.buf[4:6], "big")
                total = 6 + length
                if len(self.buf) >= total:
                    frame = self.buf[:total]
                    self.buf = self.buf[total:]
                    return frame
            data = self.sock.recv(4096)
            if not data:
                return None
            self.buf += data

    def read(self):
        raw = self.read_raw()
        return None if raw is None else (raw[1], raw[6:])


def request(sock, reader, payload):
    """Send a command and return the payload of the matching reply (matched by msg-id)."""
    mid = _next_id()
    sock.sendall(_build(mid, payload))
    for _ in range(30):
        r = reader.read()
        if r is None:
            raise ConnectionError("dongle closed the connection")
        if r[0] == mid:
            return r[1]
    return None


def ff01_timesync():
    """Cloud-style time-sync / keepalive frame (also sets the dongle clock, UTC)."""
    t = time.gmtime()
    return bytes([0xFF, 0x01, t.tm_year % 100, t.tm_mon, t.tm_mday,
                  t.tm_hour, t.tm_min, t.tm_sec, 0x00, 0x23])


def voltronic_text(payload):
    """Extract the ASCII inverter reply from an FF04 frame payload, sans CRC and CR."""
    if payload is None or len(payload) < 3 or payload[0:2] != b"\xff\x04":
        return None
    body = payload[2:]
    if not body or body[0] != 0x28:          # must start with '('
        return None
    if body.endswith(b"\r"):
        body = body[:-1]
    body = body[1:-2] if len(body) >= 3 else body[1:]   # drop '(' and 2 CRC bytes
    return body.decode("ascii", "ignore").strip()


# ------------------------------------------------------------------------- publishing
class EnergyMeter:
    """Integrates power (W) into energy (Wh) counters per source, with a daily
    'today' reset (local midnight) and a lifetime total. State is persisted to
    disk so it survives restarts. Gaps > 10 min are skipped (inverter offline)."""

    KEYS = ("consumed", "pv", "battery_out", "battery_in", "grid")

    def __init__(self, path):
        self.path = path
        self.last_t = None
        self.day = None
        self.total = {k: 0.0 for k in self.KEYS}
        self.today = {k: 0.0 for k in self.KEYS}
        self.peak = {}                 # all-time high-water marks (e.g. peak PV power)
        self._last_save = 0.0
        try:
            with open(self.path) as f:
                d = json.load(f)
            self.day = d.get("day")
            self.total.update(d.get("total", {}))
            self.today.update(d.get("today", {}))
            self.peak.update(d.get("peak", {}))
        except Exception:
            pass

    def _save(self):
        try:
            tmp = self.path + ".tmp"
            with open(tmp, "w") as f:
                json.dump({"day": self.day, "total": self.total,
                           "today": self.today, "peak": self.peak}, f)
            os.replace(tmp, self.path)
        except Exception:
            pass

    def update(self, powers, now):
        localday = time.strftime("%Y-%m-%d", time.localtime(now))
        if self.day != localday:
            self.day = localday
            self.today = {k: 0.0 for k in self.KEYS}
        if self.last_t is not None:
            dt = now - self.last_t
            if 0 < dt < 600:                       # skip large gaps (offline)
                for k in self.KEYS:
                    wh = powers.get(k, 0.0) * dt / 3600.0
                    self.total[k] += wh
                    self.today[k] += wh
        self.last_t = now
        if now - self._last_save > 60:             # throttle SD writes to ~1/min
            self._save()
            self._last_save = now

    def bump(self, key, val):
        """Track an all-time high-water mark for a metric (e.g. peak PV power)."""
        try:
            v = float(val)
        except (TypeError, ValueError):
            return
        if v > self.peak.get(key, 0.0):
            self.peak[key] = v

    def stats_kwh(self):
        out = {}
        for k in self.KEYS:
            out[k + "_today"] = round(self.today[k] / 1000.0, 3)
            out[k + "_total"] = round(self.total[k] / 1000.0, 3)
        return out


_meter = EnergyMeter(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  ".energy_state.json"))


def publish_qpigs(mc, topic, text):
    tokens = text.split()
    data = {}
    for name, val in zip(QPIGS_FIELDS, tokens):
        try:
            data[name] = float(val) if "." in val else int(val)
        except ValueError:
            data[name] = val
        mc.publish(topic + name, data[name], retain=True)
    # decode the two packed status fields into individual boolean topics
    status = str(data.get("device_status", ""))
    for name, idx in QPIGS_STATUS_BITS.items():
        if len(status) > idx:
            mc.publish(topic + "status/" + name, "1" if status[idx] == "1" else "0", retain=True)
    status2 = str(data.get("device_status_2", ""))
    for name, idx in QPIGS_STATUS2_BITS.items():
        if len(status2) > idx:
            mc.publish(topic + "status/" + name, "1" if status2[idx] == "1" else "0", retain=True)
    mc.publish(topic + "qpigs_json", json.dumps(data), retain=True)

    # energy accounting: integrate the per-source power into Wh counters
    try:
        bv = float(data.get("battery_voltage", 0) or 0)
        p_load = float(data.get("ac_output_active_power", 0) or 0)
        p_pv = float(data.get("pv_input_voltage", 0) or 0) * float(data.get("pv_input_current", 0) or 0)
        p_bout = bv * float(data.get("battery_discharge_current", 0) or 0)
        p_bin = bv * float(data.get("battery_charge_current", 0) or 0)
        p_grid = max(p_load + p_bin - p_pv - p_bout, 0.0)
        _meter.bump("pv_power", p_pv)
        _meter.update({"consumed": p_load, "pv": p_pv, "battery_out": p_bout,
                       "battery_in": p_bin, "grid": p_grid}, time.time())
        stats = _meter.stats_kwh()
        stats["pv_peak_power"] = round(_meter.peak.get("pv_power", 0.0))
        for k, v in RATED.items():
            stats["rated_" + k] = v
        stats["charge_from_solar"] = 1 if (len(status) > 6 and status[6] == "1") else 0
        stats["charge_from_grid"] = 1 if (len(status) > 7 and status[7] == "1") else 0
        mc.publish(topic + "energy_stats", json.dumps(stats), retain=True)
    except Exception:
        pass

    print(time.strftime("%H:%M:%S"),
          "AC %.1fV  LOAD %sW  BAT %.2fV %s%%  PV %.1fV %.1fA" % (
              data.get("ac_output_voltage", 0),
              data.get("ac_output_active_power", "?"),
              data.get("battery_voltage", 0),
              data.get("battery_capacity", "?"),
              data.get("pv_input_voltage", 0),
              data.get("pv_input_current", 0)))
    return data


def publish_qpiri(mc, topic, text):
    """Parse QPIRI rated info and publish per-field topics (+ enum names)."""
    tokens = text.split()
    vals = {}
    for name, val in zip(QPIRI_FIELDS, tokens):
        try:
            out = float(val) if "." in val else int(val)
        except ValueError:
            out = val
        vals[name] = out
        mc.publish(topic + "rated/" + name, out, retain=True)
        if name in QPIRI_ENUMS:
            mc.publish(topic + "rated/" + name + "_name",
                       QPIRI_ENUMS[name].get(val, val), retain=True)
    for src, dst in (("battery_bulk_voltage", "bulk_v"),
                     ("battery_float_voltage", "float_v"),
                     ("max_charging_current", "max_charge_a")):
        if isinstance(vals.get(src), (int, float)):
            RATED[dst] = vals[src]
    # full snapshot (with resolved enum names) for the read-only settings page
    snap = dict(vals)
    for name in QPIRI_ENUMS:
        if name in vals:
            snap[name + "_name"] = QPIRI_ENUMS[name].get(str(vals[name]), vals[name])
    RATED_ALL.clear()
    RATED_ALL.update(snap)
    # settings change-history: numeric fields, with readable names for enums
    numeric = {k: v for k, v in vals.items() if isinstance(v, (int, float))}
    enum_labels = {k: QPIRI_ENUMS[k].get(str(vals[k])) for k in QPIRI_ENUMS if k in vals}
    note_rated_changes(mc, topic, numeric, names=enum_labels)
    mc.publish(topic + "rated_info", text, retain=True)


_warn_last = [None]   # last active name-set; None until the first reading (avoids logging startup all-clear)


def publish_qpiws(mc, topic, text):
    """Parse QPIWS warning/fault bit string, publish active warnings, and log changes with severity."""
    active = [{"name": name, "severity": qpiws_severity(name)}
              for idx, name in sorted(QPIWS_BITS.items())
              if len(text) > idx and text[idx] == "1"]
    level = "fault" if any(a["severity"] == "fault" for a in active) else ("warning" if active else "ok")
    names = [a["name"] for a in active]
    WARN["active"], WARN["raw"], WARN["level"] = active, text, level
    mc.publish(topic + "warnings_active", ",".join(names) if names else "none", retain=True)
    mc.publish(topic + "fault", "1" if active else "0", retain=True)
    mc.publish(topic + "warning_status_raw", text, retain=True)
    # append a history event whenever the active set changes (but not the all-clear state at startup)
    key = sorted(names)
    if _warn_last[0] != key:
        first = _warn_last[0] is None
        _warn_last[0] = key
        if not (first and not key):
            event = {"ts": int(time.time()),
                     "iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                     "active": active, "level": level, "raw": text}
            try:
                with open(WARN_HISTORY_FILE, "a") as f:
                    f.write(json.dumps(event) + "\n")
            except Exception as e:
                print(time.strftime("%H:%M:%S"), "warning history write failed:", e)


def publish_qet(mc, topic, text):
    """Parse QET total generated energy (Wh) and publish Wh + kWh."""
    tok = text.split()
    if not tok:
        return
    try:
        wh = int(tok[0])
    except ValueError:
        return
    mc.publish(topic + "energy_total_wh", wh, retain=True)
    mc.publish(topic + "energy_total_kwh", round(wh / 1000.0, 3), retain=True)


def cmd_word(payload):
    """Extract the Voltronic command word (e.g. 'QPIGS') from an FF04 payload."""
    if len(payload) < 3 or payload[0:2] != b"\xff\x04":
        return None
    out = []
    for b in payload[2:]:
        if 65 <= b <= 90 or 48 <= b <= 57:   # A-Z 0-9
            out.append(chr(b))
        else:
            break
    return "".join(out) or None


def looks_like_qpigs(text):
    """Heuristic to recognise an unsolicited QPIGS reply: 21 space-separated
    tokens, first is numeric, and token[16] is the 8-bit device-status field."""
    toks = text.split()
    return (len(toks) >= 21 and toks[0][:1].isdigit()
            and len(toks[16]) == 8 and set(toks[16]) <= {"0", "1"})


def dispatch_reply(mc, topic, word, payload):
    """Parse a Voltronic reply for a known command word and publish to MQTT."""
    txt = voltronic_text(payload)
    if txt is None:
        return
    if word == "QPIGS" and txt[:1].isdigit():
        publish_qpigs(mc, topic, txt)
    elif word == "QMOD":
        publish_mode(mc, topic, txt)
    elif word == "QET":
        publish_qet(mc, topic, txt)
    elif word == "QPIRI" and txt[:1].isdigit():
        publish_qpiri(mc, topic, txt)
    elif word == "QPIWS":
        publish_qpiws(mc, topic, txt)
    elif word == "QID":
        INFO["serial"] = txt
        mc.publish(topic + "inverter_serial", txt, retain=True)
    elif word == "QMN":
        INFO["model"] = txt
        mc.publish(topic + "inverter_model", txt, retain=True)
    elif word == "QPI":
        INFO["protocol"] = txt
        mc.publish(topic + "inverter_protocol", txt, retain=True)
    elif word == "QVFW":
        INFO["firmware"] = txt.replace("VERFW:", "")
        mc.publish(topic + "inverter_firmware", INFO["firmware"], retain=True)
    elif word == "QVFW3":
        INFO["firmware2"] = txt.replace("VERFW:", "")


# ------------------------------------------------------------------------- session
def handle_fakeclient(sock, mc, cfg):
    """Act as the sole (fake) cloud: we drive the whole conversation ourselves."""
    topic = cfg["topic"]
    sock.settimeout(cfg["sock_timeout"])
    reader = FrameReader(sock)

    # optional handshake: set the dongle clock (also acts as first keepalive)
    try:
        request(sock, reader, ff01_timesync())
    except Exception:
        pass

    # static info, published once per connection
    for word, cmd in (("QID", QID), ("QMN", QMN), ("QPI", QPI),
                      ("QVFW", QVFW), ("QVFW3", QVFW3), ("QPIRI", QPIRI)):
        try:
            dispatch_reply(mc, topic, word, request(sock, reader, cmd))
        except Exception:
            pass
    try:
        publish_qflag(mc, topic, voltronic_text(request(sock, reader, QFLAG)))
    except Exception:
        pass

    last_keepalive = time.time()
    last_energy = 0.0
    last_rated = 0.0
    while True:
        # apply any queued SET command (battery toggle + settings-control page)
        while True:
            try:
                _ascii, _ev, _box = _cmd_q.get_nowait()
            except queue.Empty:
                break
            try:
                _rep = voltronic_text(request(sock, reader, pi30_cmd(_ascii)))
                _box["ok"] = (_rep == "ACK")
                _box["reply"] = _rep if _rep is not None else "(no reply)"
                if _box["ok"]:                                   # confirm by re-reading the rated info
                    dispatch_reply(mc, topic, "QPIRI", request(sock, reader, QPIRI))
                    try:
                        publish_qflag(mc, topic, voltronic_text(request(sock, reader, QFLAG)))
                    except Exception:
                        pass
                    if _ascii.startswith("PBFT"):
                        try:
                            mc.publish(topic + "battery_mode",
                                       battery_mode_for(RATED.get("float_v")), retain=True)
                        except Exception:
                            pass
                print(time.strftime("%H:%M:%S"), "SET", _ascii, "->", _box["reply"])
            except Exception as _e:
                _box["ok"], _box["reply"] = False, str(_e)
            finally:
                _ev.set()

        txt = voltronic_text(request(sock, reader, QPIGS))
        if txt and txt[:1].isdigit():
            publish_qpigs(mc, topic, txt)

        mode = voltronic_text(request(sock, reader, QMOD))
        if mode:
            publish_mode(mc, topic, mode)

        warn = voltronic_text(request(sock, reader, QPIWS))
        if warn:
            publish_qpiws(mc, topic, warn)

        # total energy changes slowly; poll it at most once a minute
        if time.time() - last_energy > 60:
            energy = voltronic_text(request(sock, reader, QET))
            if energy:
                publish_qet(mc, topic, energy)
            last_energy = time.time()

        # settings (QPIRI + QFLAG) change rarely; refresh every few minutes for the
        # config page and log any change to the audit trail
        if time.time() - last_rated > 300:
            dispatch_reply(mc, topic, "QPIRI", request(sock, reader, QPIRI))
            try:
                publish_qflag(mc, topic, voltronic_text(request(sock, reader, QFLAG)))
            except Exception:
                pass
            last_rated = time.time()

        if time.time() - last_keepalive > 30:
            try:
                request(sock, reader, ff01_timesync())
            except Exception:
                pass
            last_keepalive = time.time()

        time.sleep(cfg["poll_interval"])


def handle_passthrough(dongle, mc, cfg, cloud):
    """Byte-exact transparent relay dongle <-> real Eybond cloud so the SmartESS
    app keeps working (viewing AND settings changes). A copy of each direction is
    parsed for MQTT: replies to the cloud's requests plus the dongle's unsolicited
    QPIGS uploads. No injection — the cloud fully owns the RS485 bus."""
    topic = cfg["topic"]
    LOG_CLOUD = cfg["log_cloud"].strip().lower() == "true"
    stop = threading.Event()
    state_lock = threading.Lock()
    cloud_cmd = {}    # msg-id -> (command_word, timestamp) for the cloud's own requests

    def extract_frames(holder, data):
        """Append to a rolling buffer and yield complete 5E frames (tap only —
        never used to rebuild the forwarded stream)."""
        holder[0] += data
        buf = holder[0]
        i, n = 0, len(buf)
        while True:
            while i < n and buf[i] != 0x5E:
                i += 1
            if n - i < 6:
                break
            total = 6 + int.from_bytes(buf[i + 4:i + 6], "big")
            if n - i < total:
                break
            yield buf[i:i + total]
            i += total
        holder[0] = buf[i:]

    def on_cloud_frame(raw):          # cloud -> dongle (requests)
        p = raw[6:]
        word = cmd_word(p)
        label = word or ("FF%02X" % p[1] if len(p) >= 2 else "?")
        with state_lock:
            cloud_cmd[raw[1]] = (label, time.time())
            if len(cloud_cmd) > 512:
                cloud_cmd.clear()
        if LOG_CLOUD:
            print(time.strftime("%H:%M:%S"), "cloud->dongle", label)

    def on_dongle_frame(raw):         # dongle -> cloud (replies / unsolicited uploads)
        p = raw[6:]
        with state_lock:
            w = cloud_cmd.pop(raw[1], None)
        t = voltronic_text(p)
        if LOG_CLOUD:
            print(time.strftime("%H:%M:%S"), "dongle->cloud",
                  (w[0] if w else "unsolicited"), t if t is not None else p.hex())
        if w:
            dispatch_reply(mc, topic, w[0], p)
        elif t and looks_like_qpigs(t):
            publish_qpigs(mc, topic, t)

    def relay(src, dst, on_frame):
        """Forward bytes src->dst VERBATIM (byte-exact, like socat); feed a copy
        to on_frame for MQTT parsing. Parsing can never corrupt the relay."""
        holder = [b""]
        try:
            while not stop.is_set():
                data = src.recv(4096)
                if not data:
                    break
                dst.sendall(data)
                for raw in extract_frames(holder, data):
                    try:
                        on_frame(raw)
                    except Exception:
                        pass
        except OSError:
            pass
        finally:
            stop.set()

    threads = [
        threading.Thread(target=relay, args=(cloud, dongle, on_cloud_frame), daemon=True),
        threading.Thread(target=relay, args=(dongle, cloud, on_dongle_frame), daemon=True),
    ]
    for t in threads:
        t.start()
    stop.wait()
    for s in (dongle, cloud):
        try:
            s.close()
        except OSError:
            pass


# ---- period energy (kWh), aggregated server-side ----
# The dashboard asks for a whole period in ONE request instead of firing hundreds of VM queries.
# Uses the lifetime counter wherever it exists (robust to sparse sampling); pre-counter (imported)
# days fall back to a time-weighted integral — per-day point queries for short spans, one month-chunked
# range query per calendar month for long ones. Whole immutable past days/months are cached in-process.
# Never trust arbitrary MetricsQL from the client: only the 4 keys below map to fixed expressions.
_EN_VM = os.environ.get("SMARTESS_VM_URL", "http://127.0.0.1:8428")
_EN_KEYS = {
    "pv":   ("energymeter_pv_total", "inverter_pv_input_voltage*inverter_pv_input_current"),
    "cons": ("energymeter_consumed_total", "inverter_ac_output_active_power"),
    "bat":  ("energymeter_battery_out_total", "inverter_battery_voltage*inverter_battery_discharge_current"),
    "grid": ("energymeter_grid_total",
             "clamp_min(inverter_ac_output_active_power"
             " + (inverter_battery_voltage*inverter_battery_charge_current)"
             " - (inverter_pv_input_voltage*inverter_pv_input_current)"
             " - (inverter_battery_voltage*inverter_battery_discharge_current),0)"),
}
_en_cstart, _en_seg, _en_month = {}, {}, {}

def _en_get(params):
    with urllib.request.urlopen(_EN_VM + "/api/v1/" + params, timeout=25) as r:
        return json.loads(r.read())

def _en_inst(expr, t_s):
    try:
        j = _en_get("query?query=%s&time=%d" % (urllib.parse.quote(expr), int(t_s)))
        res = j.get("data", {}).get("result") or []
        return float(res[0]["value"][1])
    except Exception:
        return None

def _en_range(expr, a_s, b_s, step):
    try:
        j = _en_get("query_range?query=%s&start=%d&end=%d&step=%d"
                    % (urllib.parse.quote(expr), int(a_s), int(b_s), int(step)))
        res = j.get("data", {}).get("result") or []
        if not res:
            return []
        best = max(res, key=lambda s: len(s.get("values", [])))
        out = []
        for t, v in best["values"]:
            try:
                out.append((float(t), float(v)))
            except Exception:
                pass
        return out
    except Exception:
        return []

def _en_daystart(ms):
    lt = time.localtime(ms / 1000.0)
    return int(time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, 0, 0, 0, 0, 0, -1))) * 1000

def _en_counter_start(counter):
    if counter not in _en_cstart:
        ts = _en_inst("min_over_time(timestamp(%s)[420d:3h])" % counter, time.time())
        _en_cstart[counter] = ts * 1000 if ts is not None else None
    return _en_cstart[counter]

def _en_seg_kwh(power, s, e):
    if e <= s:
        return 0.0
    ds = _en_daystart(s)
    full = (s == ds and e == ds + 86400000)
    key = (power, ds) if full else None
    if key is not None and key in _en_seg:
        return _en_seg[key]
    secs = int(round((e - s) / 1000.0))
    v = _en_inst("integrate((%s)[%ds])/3.6e6" % (power, secs), e / 1000.0)
    r = v if (v and v > 0) else 0.0
    if key is not None and e <= time.time() * 1000:
        _en_seg[key] = r
    return r

def _en_month_bf(power, m_start, m_end, frm, bf_end):
    key = (power, _en_daystart(m_start), m_end)
    if key in _en_month:
        pts = _en_month[key]
    else:
        pts = _en_range("sum_over_time((%s)[86400s:300s]) * 300/3600000" % power,
                        _en_daystart(m_start) / 1000.0, m_end / 1000.0, 86400)
        if m_end <= time.time() * 1000:
            _en_month[key] = pts
    lo = _en_daystart(frm)
    s = 0.0
    for t, v in pts:                                    # map each daily bucket to its local day, keep those in range
        day = _en_daystart(t * 1000 - 12 * 3600 * 1000)
        if v > 0 and lo <= day < bf_end:
            s += v
    return s

def period_energy(key, frm, to):
    if key not in _EN_KEYS:
        return None
    counter, power = _EN_KEYS[key]
    if frm >= to:
        return 0.0
    cS = _en_counter_start(counter)
    total = 0.0
    if cS is not None and to > cS:                       # counter era → exact lifetime delta
        a = _en_inst("last_over_time(%s[2h])" % counter, max(frm, cS) / 1000.0)
        b = _en_inst("last_over_time(%s[2h])" % counter, to / 1000.0)
        if a is not None and b is not None and b >= a:
            total += b - a
    bf_end = cS if (cS is not None and cS < to) else to  # pre-counter (backfill) portion
    if bf_end <= frm:
        return total
    span_days = (bf_end - _en_daystart(frm)) / 86400000.0
    if span_days <= 2.5:                                 # short → exact per-day point integrals (clipped to the window)
        d = _en_daystart(frm)
        while d < bf_end:
            total += _en_seg_kwh(power, max(d, frm), min(d + 86400000, bf_end))
            d += 86400000
    else:                                                # long → one month-chunked range query per calendar month
        m = frm
        while m < bf_end:
            lt = time.localtime(m / 1000.0)
            nm = int(time.mktime((lt.tm_year, lt.tm_mon + 1, 1, 0, 0, 0, 0, 0, -1))) * 1000
            m_end = min(nm, bf_end)
            total += _en_month_bf(power, m, m_end, frm, bf_end)
            m = m_end
    return total


def start_control_server(port, state, state_lock, set_mode, mc=None, topic=""):
    """Lightweight web server: serves the static dashboard (web/index.html),
    proxies read-only VictoriaMetrics queries (/vm/...), and toggles the mode
    (/control/<mode>, returns JSON). One process, no extra services."""
    web_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "web")
    VM_URL = os.environ.get("SMARTESS_VM_URL", "http://127.0.0.1:8428")

    def vm_allowed(p):
        return (p.startswith("/api/v1/query") or p.startswith("/api/v1/label")
                or p.startswith("/api/v1/series"))

    class Handler(http.server.BaseHTTPRequestHandler):
        # Speak HTTP/1.1 with keep-alive. Every response below sets Content-Length, so this is
        # safe — and it's what lets Caddy pool upstream connections. Under HTTP/1.0 the poller
        # closed the socket after every reply, so Caddy kept grabbing a just-closed pooled
        # connection under load and returning intermittent 502s. HTTP/1.1 fixes that at the root.
        protocol_version = "HTTP/1.1"
        timeout = 65                    # reap idle keep-alive connections so threads don't pile up
        def _reply(self, code, ctype, body):
            if isinstance(body, str):
                body = body.encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            try:
                self.wfile.write(body)
            except Exception:
                pass

        def do_GET(self):
            raw = self.path
            path = raw.split("?", 1)[0]
            query = raw.split("?", 1)[1] if "?" in raw else ""
            p = path.rstrip("/")

            if p.endswith("/control/local") or p.endswith("/control/mirror"):
                set_mode(p.rsplit("/", 1)[1], "http")
            if p.endswith("/control/local") or p.endswith("/control/mirror") or p.endswith("/control"):
                with state_lock:
                    cur = state["mode"]
                self._reply(200, "application/json", json.dumps({"mode": cur}))
                return

            if p.endswith("/bms/on") or p.endswith("/bms/off"):
                st = p.rsplit("/", 1)[1]                # "on" | "off"
                if mc is not None:
                    mc.publish(topic + "bms_control", st, retain=True)
                self._reply(200, "application/json", json.dumps({"bms": st}))
                return

            # battery charge target: GET /battery (state) or /battery/{eco,backup} (set float V)
            if p.endswith("/battery") or p.endswith("/battery/eco") or p.endswith("/battery/backup"):
                fv = RATED.get("float_v")
                cur = battery_mode_for(fv)
                if p.endswith("/battery/eco") or p.endswith("/battery/backup"):
                    want = p.rsplit("/", 1)[1]
                    with state_lock:
                        cur_mode = state["mode"]
                    if cur_mode != "local":            # can't touch the bus while the dongle owns it
                        self._reply(409, "application/json", json.dumps(
                            {"error": "battery setpoint needs local mode",
                             "mode": cur_mode, "battery_mode": cur}))
                        return
                    box = queue_set_float(BATT_PRESETS[want])
                    self._reply(200, "application/json", json.dumps(
                        {"battery_mode": (want if box["ok"] else cur), "float": box["volts"],
                         "ok": bool(box["ok"]), "reply": box.get("reply", "")}))
                    return
                self._reply(200, "application/json", json.dumps({"battery_mode": cur, "float": fv}))
                return

            if p == "/settings/catalog":               # controllable settings + options, for the UI
                self._reply(200, "application/json", json.dumps(catalog_json()))
                return

            if path.startswith("/set/"):               # write ONE setting: /set/<key>?value=<v>
                key = path[len("/set/"):].strip("/")
                value = next((kv[6:] for kv in query.split("&") if kv.startswith("value=")), None)
                cmd = build_set_cmd(key, value)
                if cmd is None:
                    self._reply(400, "application/json", json.dumps(
                        {"ok": False, "error": "unknown/invalid setting or value", "key": key}))
                    return
                with state_lock:
                    cur_mode = state["mode"]
                if cur_mode != "local":                # never touch the bus in mirror mode
                    self._reply(409, "application/json", json.dumps(
                        {"ok": False, "error": "settings write needs local mode", "mode": cur_mode}))
                    return
                box = queue_cmd(cmd)
                self._reply(200, "application/json", json.dumps(
                    {"ok": bool(box["ok"]), "reply": box.get("reply", ""), "key": key, "cmd": cmd,
                     "value": RATED_ALL.get(key), "value_name": RATED_ALL.get(key + "_name")}))
                return

            if path.startswith("/vm/"):
                vmpath = path[3:]                      # strip '/vm', keep leading '/'
                if not vm_allowed(vmpath):
                    self._reply(403, "text/plain", "forbidden")
                    return
                url = VM_URL + vmpath + (("?" + query) if query else "")
                try:
                    with urllib.request.urlopen(url, timeout=25) as r:
                        body = r.read()
                        ctype = r.headers.get("Content-Type", "application/json")
                    self._reply(200, ctype, body)
                except Exception as e:
                    self._reply(502, "application/json", json.dumps({"error": str(e)}))
                return

            if path == "/info":
                self._reply(200, "application/json", json.dumps(INFO))
                return

            if path == "/energy":                       # aggregated period energy (kWh); ?keys=pv,cons,bat,grid&from=<ms>&to=<ms>
                q = urllib.parse.parse_qs(query)
                try:
                    frm = int(q.get("from", ["0"])[0]); to = int(q.get("to", ["0"])[0])
                except Exception:
                    frm, to = 0, 0
                out = {}
                for k in (q.get("keys", ["pv"])[0]).split(","):
                    k = k.strip()
                    if k in _EN_KEYS:
                        try:
                            out[k] = period_energy(k, frm, to)
                        except Exception:
                            out[k] = None
                self._reply(200, "application/json", json.dumps(out))
                return

            if path == "/rated":                       # full QPIRI snapshot (read-only)
                self._reply(200, "application/json", json.dumps(RATED_ALL))
                return

            if path == "/warnings":                     # decoded QPIWS warnings/faults (active + raw)
                self._reply(200, "application/json", json.dumps(WARN))
                return

            if path == "/rated/history":               # settings change-log (JSONL -> array)
                try:
                    with open(RATED_HISTORY_FILE) as f:
                        events = [json.loads(ln) for ln in f if ln.strip()][-200:]
                except Exception:
                    events = []
                self._reply(200, "application/json", json.dumps(events))
                return

            if path == "/warnings/history":            # warning/fault change-log (JSONL -> array)
                try:
                    with open(WARN_HISTORY_FILE) as f:
                        events = [json.loads(ln) for ln in f if ln.strip()][-5000:]
                except Exception:
                    events = []
                self._reply(200, "application/json", json.dumps(events))
                return

            if path.startswith("/query/"):             # read-only PI30 probe: send a Q… query, return raw reply
                cmd = path[len("/query/"):].strip("/").upper()
                if not (cmd.startswith("Q") and cmd.isalnum() and 2 <= len(cmd) <= 16):
                    self._reply(400, "application/json", json.dumps(
                        {"error": "read-only Q… queries only (no SET commands)", "cmd": cmd}))
                    return
                with state_lock:
                    cur_mode = state["mode"]
                if cur_mode != "local":                # the local poll loop owns the RS485 bus
                    self._reply(409, "application/json", json.dumps({"error": "needs local mode", "mode": cur_mode}))
                    return
                box = queue_cmd(cmd)                    # replies with data (not "(ACK") -> ok stays False, no re-read
                self._reply(200, "application/json", json.dumps(
                    {"cmd": cmd, "reply": box.get("reply", ""), "supported": bool(box.get("reply")) and box.get("reply") not in ("NAK", "(no reply)")}))
                return

            if path in ("/inverter", "/inverter.html"):
                try:
                    with open(os.path.join(web_dir, "inverter.html"), "rb") as f:
                        self._reply(200, "text/html; charset=utf-8", f.read())
                except Exception:
                    self._reply(404, "text/plain", "inverter page not found")
                return

            if path in ("/warnings.html", "/faults"):
                try:
                    with open(os.path.join(web_dir, "warnings.html"), "rb") as f:
                        self._reply(200, "text/html; charset=utf-8", f.read())
                except Exception:
                    self._reply(404, "text/plain", "warnings page not found")
                return

            if path == "/settings":
                try:
                    with open(SETTINGS_FILE) as f:
                        body = f.read()
                except Exception:
                    body = "{}"
                self._reply(200, "application/json", body)
                return

            if path in ("/", "/index.html"):
                try:
                    with open(os.path.join(web_dir, "index.html"), "rb") as f:
                        self._reply(200, "text/html; charset=utf-8", f.read())
                except Exception:
                    self._reply(404, "text/plain", "dashboard not found (web/index.html)")
                return

            self._reply(404, "text/plain", "not found")

        def do_POST(self):
            path = self.path.split("?", 1)[0]
            if path == "/settings":
                try:
                    n = int(self.headers.get("Content-Length", 0) or 0)
                    if n <= 0 or n > 8192:
                        raise ValueError("bad length")
                    data = json.loads(self.rfile.read(n).decode("utf-8"))
                    if not isinstance(data, dict):
                        raise ValueError("not an object")
                    tmp = SETTINGS_FILE + ".tmp"
                    with open(tmp, "w") as f:
                        json.dump(data, f)
                    os.replace(tmp, SETTINGS_FILE)
                    self._reply(200, "application/json", json.dumps({"ok": True}))
                except Exception as e:
                    self._reply(400, "application/json", json.dumps({"error": str(e)}))
                return
            self._reply(404, "text/plain", "not found")

        def log_message(self, *args):
            pass

    class Server(http.server.ThreadingHTTPServer):
        request_queue_size = 128        # deeper listen backlog to absorb bursts of new connections
        daemon_threads = True
    httpd = Server(("0.0.0.0", port), Handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    print("Web server on 0.0.0.0:%d  (dashboard /, /vm proxy, /control/<mode>)" % port)


def main():
    cfg = load_config()
    boot_mode = cfg["mode"].strip().lower()
    if boot_mode not in ("local", "mirror"):
        boot_mode = "mirror" if cfg["cloud_host"] else "local"

    state = {"mode": boot_mode}
    state_lock = threading.Lock()
    sockets = {"conn": None, "cloud": None}   # current session's sockets (for live switching)
    topic = cfg["topic"]
    ctl_topic = topic + "control/mode"

    def set_mode(val, source):
        """Switch operating mode and drop the live session so the dongle
        reconnects in the new mode. Shared by the MQTT and HTTP controls."""
        val = (val or "").strip().lower()
        if val not in ("local", "mirror"):
            return None
        with state_lock:
            changed = val != state["mode"]
            state["mode"] = val
            conn, cloud = sockets["conn"], sockets["cloud"]
        mc.publish(topic + "mode_active", val, retain=True)
        if changed:
            print(time.strftime("%H:%M:%S"), "mode ->", val, "via", source, "(restarting session)")
            for s in (conn, cloud):     # drop the live session; the dongle reconnects in the new mode
                if s is not None:
                    try:
                        s.close()
                    except OSError:
                        pass
        return val

    def on_connect(client, userdata, flags, rc):
        client.subscribe(ctl_topic)
        client.publish(topic + "mode_active", state["mode"], retain=True)

    def on_message(client, userdata, msg):
        set_mode(msg.payload.decode("utf-8", "ignore"), "mqtt")

    mc = mqtt.Client()
    if cfg["mqtt_user"]:
        mc.username_pw_set(cfg["mqtt_user"], cfg["mqtt_pass"])
    mc.on_connect = on_connect
    mc.on_message = on_message
    mc.connect(cfg["mqtt_host"], cfg["mqtt_port"], 60)
    mc.loop_start()
    print("MQTT connected -> %s:%d (topic %s)" % (cfg["mqtt_host"], cfg["mqtt_port"], topic))

    if cfg["control_port"]:
        start_control_server(cfg["control_port"], state, state_lock, set_mode, mc, topic)

    _load_rated_state()          # so a restart alone doesn't re-log unchanged settings
    threading.Thread(target=weather_loop, args=(mc, topic), daemon=True).start()

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((cfg["listen_host"], cfg["listen_port"]))
    srv.listen(1)
    print("Listening on %s:%d" % (cfg["listen_host"], cfg["listen_port"]))
    print("Mode: %s   (toggle: MQTT %s = local|mirror, or GET /control/<mode>)"
          % (state["mode"], ctl_topic))

    while True:
        conn, addr = srv.accept()
        with state_lock:
            mode = state["mode"]
            sockets["conn"] = conn
            sockets["cloud"] = None
        print(time.strftime("%H:%M:%S"), "Dongle connected:", addr, "| mode:", mode)
        cloud = None
        try:
            if mode == "mirror" and cfg["cloud_host"]:
                try:
                    cloud = socket.create_connection(
                        (cfg["cloud_host"], cfg["cloud_port"]), timeout=10)
                    with state_lock:
                        sockets["cloud"] = cloud
                    print(time.strftime("%H:%M:%S"), "mirroring to cloud %s:%d"
                          % (cfg["cloud_host"], cfg["cloud_port"]))
                    handle_passthrough(conn, mc, cfg, cloud)
                except OSError as e:
                    print("Cloud connect failed (%s) -> serving locally" % e)
                    handle_fakeclient(conn, mc, cfg)
            else:
                handle_fakeclient(conn, mc, cfg)
        except Exception as e:
            print("Session error:", e)
        finally:
            with state_lock:
                sockets["conn"] = None
                sockets["cloud"] = None
            for s in (conn, cloud):
                if s is not None:
                    try:
                        s.close()
                    except OSError:
                        pass
            print(time.strftime("%H:%M:%S"), "Session ended:", addr)


if __name__ == "__main__":
    main()
