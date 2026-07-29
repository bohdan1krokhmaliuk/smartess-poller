# Eybond / SmartESS dongle protocol (reverse-engineered)

Notes captured by MITM-logging a live session between an Eybond Wi-Fi dongle
(serial `W0821…`, firmware `3.1.0.1`) and the real cloud `ess.eybond.com:502`,
using a transparent `socat` relay. Inverter: Axpert VMIII-5600, protocol PI30.

## Transport

- Dongle makes an outbound **TCP** connection to `ess.eybond.com:502`.
- The **server drives** the conversation; the dongle mostly responds.

## Frame format

```
5E <msg-id> <seq:2> <len:2> <payload...>
```

| Field    | Size | Notes |
|----------|------|-------|
| `5E`     | 1    | Start byte (constant). `Paxy/SmartESS-proxy` uses `3D` instead; the dongle accepts either and echoes it back — it's effectively opaque. |
| `msg-id` | 1    | Rolling counter, incremented by the server per message. **The dongle echoes it in its reply**, so requests/replies can be matched by id. |
| `seq`    | 2    | `00 01` for server requests. The dongle uses `01 02` for the multi-record device-info burst, `00 01` otherwise. |
| `len`    | 2    | Big-endian length of `payload`. |
| payload  | len  | Starts with a function code (`FF 01`, `FF 02`, `FF 04`, …). |

## Function codes seen

### `FF 01` — time sync / keepalive

Server → dongle. Sets the dongle's clock and keeps the socket alive (~every 36 s when idle).

```
FF 01 <YY> <MM> <DD> <hh> <mm> <ss> 00 23
```

`YY` = year mod 100, time in **UTC**. Trailer `00 23` is constant. The dongle replies with a
frame echoing the msg-id whose payload is `FF 01` + its serial number in ASCII.

Example: `ff 01 1a 07 1d 12 1a 28 00 23` → 2026-07-29 18:26:40 UTC.

### `FF 02` — read Eybond config/registers (binary)

Server → dongle. Payload `FF 02` followed by a list of register indices. The dongle replies
with one sub-record per index carrying ASCII values (serial, firmware versions, config
string, …). This is the "device info" channel. On Axpert units it is **not** used for live
telemetry.

Example request: `ff 02 01 02 03 04 05 08 0c 0e 19 1a 20 41`.

### `FF 04` — Voltronic PI30 pass-through (the important one)

Server → dongle. Payload is `FF 04` + a **raw Voltronic ASCII command** + its 2-byte CRC16 +
`0x0D`. The dongle forwards the command over RS485 to the inverter and returns the inverter's
raw ASCII reply wrapped in the same framing:

```
request : 5E <id> 00 01 <len>  FF 04  <CMD> <crc-hi> <crc-lo> 0D
reply   : 5E <id> 00 01 <len>  FF 04  ( <ASCII response> <crc-hi> <crc-lo> 0D
```

The reply body starts with `(` and ends with a 2-byte CRC + `0x0D`, exactly like a direct
serial connection to the inverter.

## Captured `FF 04` command payloads

These are copy-paste exact (CRC included), so no CRC computation is needed to reuse them:

| Command  | Payload (hex)                    | Purpose |
|----------|----------------------------------|---------|
| `QPIGS`  | `ff 04 51 50 49 47 53 b7 a9 0d`  | general status (live telemetry) |
| `QMOD`   | `ff 04 51 4d 4f 44 49 c1 0d`     | working mode |
| `QPIWS`  | `ff 04 51 50 49 57 53 b4 da 0d`  | warning/fault status |
| `QPIRI`  | `ff 04 51 50 49 52 49 f8 54 0d`  | rated information |
| `QID`    | `ff 04 51 49 44 d6 ea 0d`        | serial number |
| `QMN`    | `ff 04 51 4d 4e bb 64 0d`        | model name |
| `QPI`    | `ff 04 51 50 49 be ac 0d`        | protocol id (`PI30`) |
| `QDI`    | `ff 04 51 44 49 71 1b 0d`        | default settings |
| `QFLAG`  | `ff 04 51 46 4c 41 47 98 74 0d`  | enabled/disabled flags |
| `QET`    | `ff 04 51 45 54 81 b6 0d`        | total generated energy |

(The Voltronic CRC is CRC16-CCITT/XMODEM, with any resulting `0x28`/`0x0D`/`0x0A` byte bumped
by 1 — but you only need this if you build new commands rather than reusing the table above.)

## `QPIGS` response fields (PI30)

Space-separated, in order:

```
( grid_V grid_Hz acOut_V acOut_Hz acOut_VA acOut_W load_% bus_V \
  bat_V bat_chg_A bat_% heatsink_C pv_A pv_V scc_V bat_dischg_A \
  status8 fan_offset eeprom_ver pv_charge_W status3 <crc>
```

Real example:
`(239.5 49.9 239.5 49.9 0382 0189 006 409 54.20 000 100 0029 00.0 116.7 00.00 00000 00010110 00 00 00000 111`

→ grid 239.5 V/49.9 Hz, output 239.5 V/49.9 Hz 382 VA/189 W load 6 %, bus 409 V,
battery 54.20 V charge 0 A 100 %, heatsink 29 °C, PV 0.0 A/116.7 V, charge 0 W.

## Key takeaways

1. Live data is **pull-based**: the server must send `FF 04 QPIGS…`. Nothing arrives on its
   own between the dongle's slow (~10 min) auto-uploads.
2. `updateFrequency` in `Paxy/SmartESS-proxy` only changes how often its `3D0C…` keepalive is
   sent — it never triggers `QPIGS`, so it doesn't speed up Axpert telemetry.
3. Replacing the passive relay with an **active poller** (this project) gives per-second-class
   updates from the stock dongle, no extra hardware required.
