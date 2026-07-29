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
    cfg["poll_interval"] = float(cfg["poll_interval"])
    cfg["sock_timeout"] = float(cfg["sock_timeout"])
    return cfg


# ------------------------------------------------------------------ Voltronic commands
# Exact command payloads captured from the real Eybond cloud.
# Frame payload layout: FF 04 <ASCII cmd> <CRC16> 0x0D
QPIGS = bytes.fromhex("ff045150494753b7a90d")   # general status (live telemetry)
QMOD  = bytes.fromhex("ff04514d4f4449c10d")     # current working mode
QPIWS = bytes.fromhex("ff045150495753b4da0d")   # warning / fault status
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

    def read(self):
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
                    return frame[1], frame[6:]
            data = self.sock.recv(4096)
            if not data:
                return None
            self.buf += data


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


# ------------------------------------------------------------------------- session
def handle(sock, mc, cfg):
    topic = cfg["topic"]
    sock.settimeout(cfg["sock_timeout"])
    reader = FrameReader(sock)

    # optional handshake: set the dongle clock (also acts as first keepalive)
    try:
        request(sock, reader, ff01_timesync())
    except Exception:
        pass

    # static info, published once per connection
    for cmd, key in ((QID, "inverter_serial"), (QPIRI, "rated_info")):
        try:
            txt = voltronic_text(request(sock, reader, cmd))
            if txt:
                mc.publish(topic + key, txt, retain=True)
        except Exception:
            pass

    last_keepalive = time.time()
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
            mc.publish(topic + "warning_status", warn, retain=True)

        if time.time() - last_keepalive > 30:
            try:
                request(sock, reader, ff01_timesync())
            except Exception:
                pass
            last_keepalive = time.time()

        time.sleep(cfg["poll_interval"])


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

    while True:
        conn, addr = srv.accept()
        print("Dongle connected:", addr)
        try:
            handle(conn, mc, cfg)
        except Exception as e:
            print("Session ended:", e)
        finally:
            try:
                conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
