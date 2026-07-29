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

import configparser
import json
import os
import socket
import sys
import threading
import time

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
    "cloud_host":    "",
    "cloud_port":    "502",
    "active_poll":   "true",  # in passthrough, also inject our own QPIGS/QMOD/QET polls
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
    for name, val in zip(QPIRI_FIELDS, tokens):
        try:
            out = float(val) if "." in val else int(val)
        except ValueError:
            out = val
        mc.publish(topic + "rated/" + name, out, retain=True)
        if name in QPIRI_ENUMS:
            mc.publish(topic + "rated/" + name + "_name",
                       QPIRI_ENUMS[name].get(val, val), retain=True)
    mc.publish(topic + "rated_info", text, retain=True)


def publish_qpiws(mc, topic, text):
    """Parse QPIWS warning/fault bit string and publish active warnings."""
    active = [name for idx, name in QPIWS_BITS.items()
              if len(text) > idx and text[idx] == "1"]
    mc.publish(topic + "warnings_active", ",".join(active) if active else "none", retain=True)
    mc.publish(topic + "fault", "1" if active else "0", retain=True)
    mc.publish(topic + "warning_status_raw", text, retain=True)


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


def dispatch_reply(mc, topic, word, payload):
    """Parse a Voltronic reply for a known command word and publish to MQTT."""
    txt = voltronic_text(payload)
    if txt is None:
        return
    if word == "QPIGS" and txt[:1].isdigit():
        publish_qpigs(mc, topic, txt)
    elif word == "QMOD":
        mc.publish(topic + "mode", txt, retain=True)
        mc.publish(topic + "mode_name", MODE_NAMES.get(txt, txt), retain=True)
    elif word == "QET":
        publish_qet(mc, topic, txt)
    elif word == "QPIRI" and txt[:1].isdigit():
        publish_qpiri(mc, topic, txt)
    elif word == "QPIWS":
        publish_qpiws(mc, topic, txt)


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
    try:
        sid = voltronic_text(request(sock, reader, QID))
        if sid:
            mc.publish(topic + "inverter_serial", sid, retain=True)
    except Exception:
        pass
    try:
        rated = voltronic_text(request(sock, reader, QPIRI))
        if rated and rated[:1].isdigit():
            publish_qpiri(mc, topic, rated)
    except Exception:
        pass

    last_keepalive = time.time()
    last_energy = 0.0
    while True:
        txt = voltronic_text(request(sock, reader, QPIGS))
        if txt and txt[:1].isdigit():
            publish_qpigs(mc, topic, txt)

        mode = voltronic_text(request(sock, reader, QMOD))
        if mode:
            mc.publish(topic + "mode", mode, retain=True)
            mc.publish(topic + "mode_name", MODE_NAMES.get(mode, mode), retain=True)

        warn = voltronic_text(request(sock, reader, QPIWS))
        if warn:
            publish_qpiws(mc, topic, warn)

        # total energy changes slowly; poll it at most once a minute
        if time.time() - last_energy > 60:
            energy = voltronic_text(request(sock, reader, QET))
            if energy:
                publish_qet(mc, topic, energy)
            last_energy = time.time()

        if time.time() - last_keepalive > 30:
            try:
                request(sock, reader, ff01_timesync())
            except Exception:
                pass
            last_keepalive = time.time()

        time.sleep(cfg["poll_interval"])


def handle_passthrough(dongle, mc, cfg, cloud):
    """Transparently relay dongle <-> real Eybond cloud (the SmartESS app keeps
    working), while injecting our own polls and parsing telemetry into MQTT."""
    topic = cfg["topic"]
    stop = threading.Event()
    state_lock = threading.Lock()
    dongle_send_lock = threading.Lock()
    our_out = {}      # msg-id -> (command_word, timestamp) for polls WE injected
    cloud_cmd = {}    # msg-id -> (command_word, timestamp) for the cloud's own requests
    last_cloud = [0.0]   # timestamp of the last frame seen from the cloud
    SETTLE = 5.0         # small delay after connect before we start polling
    QUIET_GAP = 2.0      # treat the cloud as idle only after this much silence

    def dongle_send(data):
        with dongle_send_lock:
            dongle.sendall(data)

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

    def on_cloud_frame(raw):
        last_cloud[0] = time.time()
        word = cmd_word(raw[6:])
        if word:
            with state_lock:
                cloud_cmd[raw[1]] = (word, time.time())

    def split_out(buf):
        """Split dongle bytes: forward everything to the cloud EXCEPT frames whose
        msg-id is one of our injected polls (stripped so the cloud never sees
        them). Returns (bytes_to_forward, leftover, our_frames, cloud_frames)."""
        out = bytearray()
        ours, clouds = [], []
        i, n = 0, len(buf)
        while i < n:
            if buf[i] != 0x5E:
                out.append(buf[i])
                i += 1
                continue
            if n - i < 6:
                break
            total = 6 + int.from_bytes(buf[i + 4:i + 6], "big")
            if n - i < total:
                break
            frame = bytes(buf[i:i + total])
            with state_lock:
                mine = frame[1] in our_out
            if mine:
                ours.append(frame)          # strip — do not forward to the cloud
            else:
                out += frame                # cloud's own traffic, byte-exact
                clouds.append(frame)
            i += total
        return bytes(out), buf[i:], ours, clouds

    def relay_dongle_to_cloud():
        buf = b""
        try:
            while not stop.is_set():
                data = dongle.recv(4096)
                if not data:
                    break
                buf += data
                out, buf, ours, clouds = split_out(buf)
                if out:
                    cloud.sendall(out)
                for f in ours:
                    with state_lock:
                        w = our_out.pop(f[1], None)
                    if w:
                        try:
                            dispatch_reply(mc, topic, w[0], f[6:])
                        except Exception:
                            pass
                for f in clouds:
                    with state_lock:
                        w = cloud_cmd.pop(f[1], None)
                    if w:
                        try:
                            dispatch_reply(mc, topic, w[0], f[6:])
                        except Exception:
                            pass
        except OSError:
            pass
        finally:
            stop.set()

    def relay(src, dst, on_frame):
        """Forward bytes src->dst VERBATIM (byte-exact, like socat); feed a copy
        to on_frame for MQTT parsing. Parsing can never corrupt the relay."""
        holder = [b""]
        try:
            while not stop.is_set():
                data = src.recv(4096)
                if not data:
                    break
                if dst is dongle:
                    dongle_send(data)
                else:
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

    def pick_id():
        with state_lock:
            used = set(cloud_cmd) | set(our_out)
        for cand in list(range(0xE0, 0x100)) + list(range(0x00, 0xE0)):
            if cand not in used:
                return cand
        return None

    def expire():
        now = time.time()
        with state_lock:
            for store in (our_out, cloud_cmd):
                for k in [k for k, v in store.items() if now - v[1] > 15]:
                    store.pop(k, None)

    def inject(word, payload):
        mid = pick_id()
        if mid is None:
            return
        with state_lock:
            our_out[mid] = (word, time.time())
        dongle_send(_build(mid, payload))

    def cloud_busy():
        with state_lock:
            pending = bool(cloud_cmd)
        return pending or (time.time() - last_cloud[0] < QUIET_GAP)

    def injector():
        if cfg["active_poll"].strip().lower() != "true":
            return
        if stop.wait(SETTLE):
            return
        print(time.strftime("%H:%M:%S"), "injector active (fills cloud-idle gaps)")
        did_rated = False
        last_energy = 0.0
        try:
            while not stop.wait(cfg["poll_interval"]):
                expire()
                if cloud_busy():        # cloud has priority; poll only when it is idle
                    continue
                if not did_rated:
                    inject("QPIRI", QPIRI)
                    did_rated = True
                inject("QPIGS", QPIGS)
                inject("QMOD", QMOD)
                if time.time() - last_energy > 60:
                    inject("QET", QET)
                    last_energy = time.time()
        except OSError:
            stop.set()

    threads = [
        threading.Thread(target=relay, args=(cloud, dongle, on_cloud_frame), daemon=True),
        threading.Thread(target=relay_dongle_to_cloud, daemon=True),
        threading.Thread(target=injector, daemon=True),
    ]
    for t in threads:
        t.start()
    stop.wait()
    for s in (dongle, cloud):
        try:
            s.close()
        except OSError:
            pass


def main():
    cfg = load_config()

    mc = mqtt.Client()
    if cfg["mqtt_user"]:
        mc.username_pw_set(cfg["mqtt_user"], cfg["mqtt_pass"])
    mc.connect(cfg["mqtt_host"], cfg["mqtt_port"], 60)
    mc.loop_start()
    print("MQTT connected -> %s:%d (topic %s)" % (cfg["mqtt_host"], cfg["mqtt_port"], cfg["topic"]))

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((cfg["listen_host"], cfg["listen_port"]))
    srv.listen(1)
    print("Listening on %s:%d — waiting for the dongle..." % (cfg["listen_host"], cfg["listen_port"]))

    mode = ("passthrough -> %s:%d" % (cfg["cloud_host"], cfg["cloud_port"])
            if cfg["cloud_host"] else "fakeclient (local only)")
    print("Mode:", mode)

    while True:
        conn, addr = srv.accept()
        print(time.strftime("%H:%M:%S"), "Dongle connected:", addr)
        cloud = None
        try:
            if cfg["cloud_host"]:
                try:
                    cloud = socket.create_connection(
                        (cfg["cloud_host"], cfg["cloud_port"]), timeout=10)
                    print("Relaying to cloud %s:%d" % (cfg["cloud_host"], cfg["cloud_port"]))
                    handle_passthrough(conn, mc, cfg, cloud)
                except OSError as e:
                    print("Cloud connect failed (%s) -> serving locally" % e)
                    handle_fakeclient(conn, mc, cfg)
            else:
                handle_fakeclient(conn, mc, cfg)
        except Exception as e:
            print("Session ended:", e)
        finally:
            for s in (conn, cloud):
                if s is not None:
                    try:
                        s.close()
                    except OSError:
                        pass
            print(time.strftime("%H:%M:%S"), "Session ended:", addr)


if __name__ == "__main__":
    main()
