#!/usr/bin/env python3
"""JK BMS (BLE) reader -> MQTT, for the SmartESS solar dashboard.

Connects to a JK-BD BMS over Bluetooth LE, parses the JK02 cell-info frame,
and publishes a flat JSON blob to smartess/bms_json. Telegraf turns each field
into a bms_<field> metric in VictoriaMetrics, exactly like the inverter pipeline
(smartess/qpigs_json -> inverter_<field>).

Runtime on/off: publish "on"/"off" (retained) to smartess/bms_control to
connect/disconnect BLE -- lets you free the BMS for the phone app. The current
state is echoed to smartess/bms_active.

  pip3 install bleak paho-mqtt
  python3 jk_bms.py

Field offsets were reverse-engineered from a JK-BD6A17S6P (16S) and verified
against the frame CRC. See docs/PROTOCOL.md.
"""
import asyncio
import json
import time

from bleak import BleakClient, BleakScanner
import paho.mqtt.client as mqtt

# ---- config -----------------------------------------------------------
MAC       = "C8:47:8C:E9:23:D0"
CELLS     = 16                                   # populated cells (16S)
CHAR      = "0000ffe1-0000-1000-8000-00805f9b34fb"
MQTT_HOST = "127.0.0.1"
MQTT_PORT = 1883
MQTT_USER = ""
MQTT_PASS = ""
TOPIC     = "smartess/"
PERIOD    = 5                                    # seconds between publishes

CMD_DEVICE_INFO = 0x97
CMD_CELL_INFO   = 0x96


def _cmd(reg):
    """Build a 20-byte JK command frame (AA 55 90 EB <reg> ... <crc>)."""
    f = bytearray(20)
    f[0], f[1], f[2], f[3], f[4] = 0xAA, 0x55, 0x90, 0xEB, reg
    f[19] = sum(f[:19]) & 0xFF
    return bytes(f)


def parse_cell_info(f):
    """Decode a JK02 type-0x02 frame (300 bytes). Returns a flat dict or None."""
    if len(f) < 300 or f[4] != 0x02:
        return None
    if sum(f[:299]) & 0xFF != f[299]:            # CRC = low byte of the sum
        return None
    u16 = lambda o: int.from_bytes(f[o:o + 2], "little")
    i16 = lambda o: int.from_bytes(f[o:o + 2], "little", signed=True)
    u32 = lambda o: int.from_bytes(f[o:o + 4], "little")
    i32 = lambda o: int.from_bytes(f[o:o + 4], "little", signed=True)

    cells = [u16(6 + i * 2) / 1000 for i in range(CELLS)]
    pack  = u32(118) / 1000
    cur   = i32(126) / 1000                       # >0 charge, <0 discharge (verify under load)

    d = {"cell_%d" % (i + 1): v for i, v in enumerate(cells)}
    d.update({
        "cell_min":          min(cells),
        "cell_max":          max(cells),
        "cell_avg":          u16(58) / 1000,
        "cell_delta":        u16(60) / 1000,
        "pack_voltage":      pack,
        "current":           cur,
        "power":             round(pack * cur, 1),
        "soc":               f[141],
        "soh":               f[158],
        "remaining_ah":      u32(142) / 1000,
        "nominal_ah":        u32(146) / 1000,
        "cycles":            u32(150),
        "cycle_capacity_ah": round(u32(154) / 1000, 1),
        "temp_1":            i16(130) / 10,
        "temp_2":            i16(132) / 10,
        "temp_mos":          i16(134) / 10,
        "charge_mos":        f[166],
        "discharge_mos":     f[167],
    })
    return d


# ---- MQTT -------------------------------------------------------------
enabled = True


def _on_msg(_c, _u, m):
    global enabled
    if m.topic == TOPIC + "bms_control":
        want = m.payload.decode(errors="ignore").strip().lower()
        enabled = want not in ("off", "0", "false", "")
        print("control ->", "ON" if enabled else "OFF")


def _make_client():
    # paho-mqtt 2.x needs an explicit callback API version; 1.x doesn't have it.
    try:
        return mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id="jk-bms-ble")
    except (AttributeError, TypeError):
        return mqtt.Client(client_id="jk-bms-ble")


mc = _make_client()
if MQTT_USER:
    mc.username_pw_set(MQTT_USER, MQTT_PASS)
mc.on_message = _on_msg
mc.connect(MQTT_HOST, MQTT_PORT, 60)
mc.subscribe(TOPIC + "bms_control")
mc.loop_start()


# ---- BLE frame reassembly + publish -----------------------------------
buf = bytearray()
last_pub = [0.0]
last_rx = [0.0]


def _on_notify(_char, data):
    buf.extend(data)
    while True:
        i = buf.find(b"\x55\xaa\xeb\x90")        # response frame header
        if i < 0:
            if len(buf) > 8192:
                del buf[:-4]
            return
        if i:
            del buf[:i]
        if len(buf) < 5:
            return
        length = 300 if buf[4] == 0x02 else 320  # cell-info=300, settings/device=320
        if len(buf) < length:
            return
        frame = bytes(buf[:length])
        del buf[:length]
        d = parse_cell_info(frame)
        if d:
            last_rx[0] = time.time()
            if time.time() - last_pub[0] >= PERIOD:
                last_pub[0] = time.time()
                mc.publish(TOPIC + "bms_json", json.dumps(d), retain=True)
                print("SoC %d%%  %.2fV  %+.1fA  cells %.3f-%.3f d%dmV  T %.1f/%.1f/%.1f  cyc %d  SoH %d%%"
                      % (d["soc"], d["pack_voltage"], d["current"], d["cell_min"], d["cell_max"],
                         round(d["cell_delta"] * 1000), d["temp_1"], d["temp_2"], d["temp_mos"],
                         d["cycles"], d["soh"]))


async def run():
    while True:
        if not enabled:
            mc.publish(TOPIC + "bms_active", "off", retain=True)
            await asyncio.sleep(3)
            continue
        try:
            dev = await BleakScanner.find_device_by_address(MAC, timeout=15)
            if not dev:
                print("BMS not found (in range? phone app closed?) - retrying")
                await asyncio.sleep(5)
                continue
            async with BleakClient(dev, timeout=20) as c:
                print("connected", MAC)
                mc.publish(TOPIC + "bms_active", "on", retain=True)
                await c.start_notify(CHAR, _on_notify)
                # Handshake like the app: device-info request first, then cell-info.
                # The BMS then streams cell-info frames on its own -- it beeps on
                # every command, so we DON'T re-poll; we only nudge it again if the
                # stream goes quiet for a while.
                await c.write_gatt_char(CHAR, _cmd(CMD_DEVICE_INFO), response=False)
                await asyncio.sleep(1.0)
                await c.write_gatt_char(CHAR, _cmd(CMD_CELL_INFO), response=False)
                last_rx[0] = time.time()
                while enabled and c.is_connected:
                    await asyncio.sleep(3)
                    if time.time() - last_rx[0] > 20:      # stream stalled -> nudge once
                        await c.write_gatt_char(CHAR, _cmd(CMD_CELL_INFO), response=False)
                        last_rx[0] = time.time()
                await c.stop_notify(CHAR)
            mc.publish(TOPIC + "bms_active", "off", retain=True)
        except Exception as e:                    # noqa: BLE stack raises many types
            print("BLE error:", e)
            mc.publish(TOPIC + "bms_active", "off", retain=True)
            await asyncio.sleep(5)


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
