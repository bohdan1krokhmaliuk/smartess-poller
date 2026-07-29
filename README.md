# smartess-poller

Real-time local monitoring for **Axpert / Voltronic / MPP Solar** inverters that ship with an
**Eybond / SmartESS** Wi-Fi dongle — **without extra hardware** (no ESP32, no RS485 adapter)
and **without the cloud**.

The stock dongle only uploads a full data frame to the SmartESS cloud every ~5–10 minutes,
which is far too slow for live monitoring. This tool impersonates the Eybond cloud on your
LAN and **actively polls** the inverter with Voltronic PI30 commands (`QPIGS`, `QMOD`, …) as
often as you like (e.g. every 10 s), publishing the parsed values to MQTT for Home Assistant,
Node-RED, Grafana, etc.

## Why this exists

The popular [`Paxy/SmartESS-proxy`](https://github.com/Paxy/SmartESS-proxy) is a **passive
relay** that waits for the dongle to push a binary register frame. That works for some PowMr
units but **not** for Axpert/Voltronic inverters, which the Eybond cloud drives with
**text-based Voltronic PI30 commands** (`FF 04` pass-through), not binary registers. So the
passive relay just sees keepalives and never publishes anything.

`smartess-poller` speaks that real protocol and takes the active role the cloud normally
plays. See [`docs/PROTOCOL.md`](docs/PROTOCOL.md) for the full reverse-engineering notes.

## Requirements

- A Linux box on your LAN that runs 24/7 (Raspberry Pi is perfect).
- Python 3.7+ and `paho-mqtt`.
- An MQTT broker (e.g. Mosquitto), local or remote.
- A way to redirect the dongle's cloud hostname to this box (Pi-hole "Local DNS", your
  router's DNS/hosts, or NAT redirect).

## Install

```bash
git clone https://github.com/<you>/smartess-poller.git
cd smartess-poller
sudo apt install -y python3-paho-mqtt        # or: pip3 install -r requirements.txt
cp config.example.ini config.ini            # then edit config.ini
```

## Configure

Edit `config.ini` (all keys optional; sane defaults are built in):

```ini
[smartess]
listen_port   = 502          # port the dongle connects to (Eybond uses 502)
mqtt_host     = 127.0.0.1
mqtt_port     = 1883
mqtt_user     =
mqtt_pass     =
topic         = smartess/
poll_interval = 10           # seconds between polls (5-10 safe; do not go below 3)
```

Any key can also be overridden by an env var, e.g. `SMARTESS_POLL_INTERVAL=5`.

## Two modes

**Local-only (default, `cloud_host` empty).** We act as the sole (fake) cloud. The
dongle is fully cut off from the internet; we drive the whole conversation. Most private,
simplest, and the SmartESS phone app will **not** work (the dongle no longer reaches the
real cloud).

**Passthrough (`cloud_host` set).** We become a transparent proxy between the dongle and
the real Eybond cloud, so the **SmartESS app keeps working**, while we poll in parallel and
publish to MQTT. Set `cloud_host` to the cloud's numeric IP — the hostname is
DNS-redirected to us, so resolve it externally:

```bash
dig +short ess.eybond.com @8.8.8.8
```

```ini
[smartess]
cloud_host  = 47.83.160.214   # example — verify yours with the dig command above
cloud_port  = 502
active_poll = true            # also inject our own QPIGS/QMOD/QET polls
```

With `active_poll = true` we inject our own read commands (using reserved message ids that
avoid the cloud's) for a steady rate even when the app is closed, and additionally parse the
replies the cloud/app requests. With `active_poll = false` we only passively parse whatever
the cloud/app asks for (zero extra load on the inverter, but the data rate then depends on
the app being open). If the cloud is unreachable, the poller falls back to local-only mode
so data keeps flowing.

> Note: passthrough adds a little extra RS485 traffic and shares one TCP link with the
> cloud. It's designed to be safe (injected replies are stripped before reaching the cloud),
> but if you don't need the app, local-only is the simplest choice.

## Redirect the dongle to this machine

The dongle connects out to **`ess.eybond.com:502`**. Point that hostname at this box.

**Pi-hole v6:** *Settings → Local DNS Records* → add
`ess.eybond.com` → `<this-machine-IP>`.

Then power-cycle / restart the dongle's Wi-Fi (or reboot your router) so it re-resolves the
hostname and reconnects here.

## Run

```bash
sudo python3 smartess_poller.py     # sudo needed only because 502 is a privileged port
```

You should see, every `poll_interval` seconds:

```
MQTT connected -> 127.0.0.1:1883 (topic smartess/)
Listening on 0.0.0.0:502 — waiting for the dongle...
Dongle connected: ('192.168.x.x', 54329)
19:31:41 AC 233.5V  LOAD 177W  BAT 54.20V 100%  PV 112.6V 0.0A
```

Verify MQTT in another terminal:

```bash
mosquitto_sub -h 127.0.0.1 -t "smartess/#" -v
```

### Avoiding `sudo` (optional)

Grant Python the capability to bind low ports once, then run without `sudo`:

```bash
sudo setcap 'cap_net_bind_service=+ep' "$(readlink -f "$(which python3)")"
```

Or set `listen_port` to something ≥ 1024 and NAT-redirect 502 → that port for the dongle's IP.

## Run 24/7 (systemd)

```bash
sudo cp systemd/smartess-poller.service /etc/systemd/system/
sudo sed -i "s|/path/to|$(pwd)|" /etc/systemd/system/smartess-poller.service
sudo systemctl daemon-reload
sudo systemctl enable --now smartess-poller
sudo systemctl status smartess-poller
journalctl -u smartess-poller -f
```

## Published topics

| Topic | Meaning |
|---|---|
| `smartess/grid_voltage`, `grid_frequency` | grid input |
| `smartess/ac_output_voltage`, `ac_output_frequency` | AC output |
| `smartess/ac_output_apparent_power` (VA), `ac_output_active_power` (W), `output_load_percent` | output load |
| `smartess/bus_voltage` | DC bus |
| `smartess/battery_voltage`, `battery_charge_current`, `battery_discharge_current`, `battery_capacity` | battery |
| `smartess/pv_input_voltage`, `pv_input_current`, `pv_charging_power` | solar |
| `smartess/heatsink_temperature` | temperature |
| `smartess/energy_total_wh`, `energy_total_kwh` | total generated energy (`QET`) |
| `smartess/mode`, `mode_name` | working mode (L=Line, B=Battery, …) |
| `smartess/status/load_on`, `charging`, `charging_scc`, `charging_ac`, `charging_to_float`, `switch_on`, `config_changed` | decoded device-status bits (0/1) |
| `smartess/warnings_active` | comma-list of active warnings (or `none`) |
| `smartess/fault` | `1` if any warning/fault bit is set, else `0` |
| `smartess/warning_status_raw` | raw `QPIWS` bit string |
| `smartess/qpigs_json` | all `QPIGS` fields as one JSON object |
| `smartess/rated/*` | rated info from `QPIRI` (e.g. `rated/battery_float_voltage`, `rated/output_source_priority_name`, `rated/max_charging_current`) |
| `smartess/inverter_serial` | serial number (`QID`) |

Static topics (`rated/*`, `inverter_serial`) are published once per connection; everything
else refreshes every `poll_interval` (energy at most once a minute).

All values are published with the MQTT `retain` flag, so new subscribers get the last value
immediately.

## Home Assistant

MQTT sensors example:

```yaml
mqtt:
  sensor:
    - name: "Inverter Battery Voltage"
      state_topic: "smartess/battery_voltage"
      unit_of_measurement: "V"
      device_class: voltage
    - name: "Inverter Load"
      state_topic: "smartess/ac_output_active_power"
      unit_of_measurement: "W"
      device_class: power
    - name: "Inverter PV Voltage"
      state_topic: "smartess/pv_input_voltage"
      unit_of_measurement: "V"
      device_class: voltage
```

## Notes & safety

- Keep `poll_interval` at 5–10 s. The dongle's microcontroller and the inverter's RS485 bus
  are slow; polling too aggressively can cause dropped replies or TCP resets.
- This talks only to your own inverter on your own LAN. The dongle is fully cut off from the
  Chinese cloud (which is also a privacy win).
- Tested with an Axpert VMIII-5600 (PI30) + Eybond Wi-Fi dongle. Other Voltronic/MPP Solar
  models using PI30 should work; PI18 models would need different command bytes.

## Credits

Protocol framing insights build on the community work around `Paxy/SmartESS-proxy` and the
well-documented Voltronic PI30 / MPP Solar command set. Command payloads here were captured
from a live Eybond cloud session (see `docs/PROTOCOL.md`).

## License

MIT — see [LICENSE](LICENSE).
