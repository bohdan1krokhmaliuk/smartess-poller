# JK BMS → MQTT (Bluetooth LE)

Reads a **JK-BD series BMS** (JiKong) over **Bluetooth LE** and publishes the
battery telemetry to MQTT, so it flows into VictoriaMetrics and the dashboard
through the same pipeline as the inverter
(`smartess/bms_json` → Telegraf `name_override="bms"` → `bms_<field>` metrics).

No extra hardware: the Raspberry Pi's built-in Bluetooth talks straight to the
BMS. Verified on a **JK-BD6A17S6P** (16S LiFePO4); other JK02-protocol JK BMS
should work too — adjust `MAC` and `CELLS` at the top of `jk_bms.py`.

## What you get

Per-cell voltages, min/max/avg/delta, pack voltage, current & power, **true SoC**
(reported by the BMS, not estimated from voltage like the inverter's), **SoH**,
remaining/nominal capacity, cycle count, total throughput, three temperatures
(2 probes + MOS), and the charge/discharge MOSFET states.

## Install (on the Pi)

```bash
pip3 install -r bms/requirements.txt      # bleak + paho-mqtt
```

## Configure

Find your BMS address (close the JK phone app first — BLE allows one connection):

```bash
bluetoothctl --timeout 15 scan on | grep -i jk
```

Edit the top of `jk_bms.py`: set `MAC` (and `CELLS` if your pack isn't 16S), plus
the MQTT host/port if you don't use the local broker.

## Run

```bash
python3 bms/jk_bms.py
```

One line every few seconds means it works:

```
SoC 78%  53.78V  +0.0A  cells 3.359-3.366 d7mV  T 24.3/24.7/29.9  cyc 111  SoH 100%
```

Run it 24/7 with the provided unit (`systemd/jk-bms.service`).

## Feed it into VictoriaMetrics

Add this block to `dashboard/telegraf.conf` and restart Telegraf — each JSON
field becomes a `bms_<field>` metric (`bms_soc`, `bms_pack_voltage`,
`bms_cell_1`, …):

```toml
[[inputs.mqtt_consumer]]
  servers = ["tcp://127.0.0.1:1883"]
  topics = ["smartess/bms_json"]
  data_format = "json"
  name_override = "bms"
  client_id = "telegraf-smartess-bms"
```

## MQTT topics

| Topic | Direction | Meaning |
|---|---|---|
| `smartess/bms_json`    | published (retained) | flat JSON of all fields (Telegraf reads this) |
| `smartess/bms_active`  | published (retained) | `on` / `off` — is the reader currently connected |
| `smartess/bms_control` | **subscribed**       | send `off` to release BLE (use the phone app), `on` to reconnect |

Hand the BLE link to the phone app and take it back:

```bash
mosquitto_pub -t smartess/bms_control -m off -r
mosquitto_pub -t smartess/bms_control -m on  -r
```

## Caveats

- **One BLE connection at a time.** While this runs, the JK phone app can't
  connect (and vice-versa). Use `bms_control off` to hand it over.
- **Range.** The Pi must be within Bluetooth range of the battery.

## Protocol (JK02)

The BMS streams frames on characteristic `ffe1` after a `AA 55 90 EB 96 …`
request. Response frames start with `55 AA EB 90`; type `0x02` is cell-info
(300 bytes, last byte = `sum(bytes[:-1]) & 0xFF`). Field offsets, reverse-
engineered and CRC-verified on a JK-BD6A17S6P:

| Field | Offset | Type | Scale |
|---|---|---|---|
| cell N voltage               | 6 + 2·(N-1) | u16 LE | mV |
| avg / delta cell voltage     | 58 / 60     | u16 LE | mV |
| balance current              | 138         | i16 LE | mA (>0 balancing) |
| balance action               | 140         | u8     | 0 off / 1 charge / 2 discharge |
| pack voltage                 | 118         | u32 LE | mV |
| current                      | 126         | i32 LE | mA (>0 charge) |
| temperature probe 1 / 2      | 130 / 132   | i16 LE | 0.1 °C |
| MOS temperature              | 134         | i16 LE | 0.1 °C |
| SoC                          | 141         | u8     | % |
| remaining / nominal capacity | 142 / 146   | u32 LE | mAh |
| cycle count                  | 150         | u32 LE | — |
| total cycle capacity         | 154         | u32 LE | mAh |
| SoH                          | 158         | u8     | % |
| charge / discharge MOSFET    | 166 / 167   | u8     | 0/1 |
