# Dashboard (Grafana + VictoriaMetrics + Telegraf)

A lightweight, self-hosted visualization stack for `smartess-poller`, sized for a
Raspberry Pi 3B+ (1 GB RAM, armv7) running alongside Pi-hole / Vaultwarden / Caddy.

```
inverter → smartess-poller → Mosquitto → Telegraf → VictoriaMetrics → Grafana
```

- **Telegraf** reads `smartess/qpigs_json` + `smartess/energy_total_kwh` from MQTT.
- **VictoriaMetrics** stores the time-series (tiny, efficient, 24-month retention, exportable).
- **Grafana** serves the dashboard, including a **LOCAL / MIRROR** mode-switch panel that
  calls the poller's `/control/<mode>` endpoint.

Approx. RAM: VictoriaMetrics ~80 MB, Telegraf ~50 MB, Grafana ~200 MB.

## Prerequisites

- `smartess-poller` running with MQTT enabled (default) and `control_port = 8899`.
- Mosquitto on the Pi (`127.0.0.1:1883`).
- Docker + Docker Compose.
- Your existing Caddy (optional but recommended, to serve Grafana over TLS on a subdomain).

## Deploy

```bash
cd smartess-poller/dashboard
cp .env.example .env          # set GF_ADMIN_PASSWORD and GF_ROOT_URL
docker compose up -d
docker compose logs -f        # watch startup
```

Telegraf writes to VictoriaMetrics every 10 s; within a minute VM has data. Metrics are
named `inverter_<field>` (e.g. `inverter_battery_voltage`) plus `energy_value`.

## Expose Grafana via Caddy

Grafana is published on the Pi at `<pi-ip>:3001`, and the poller's control endpoint at
`<pi-ip>:8899`. Add a site to your `Caddyfile` (adjust host/IP):

```caddy
solar.bodka {
    tls internal
    handle /control/* {
        reverse_proxy 192.168.68.68:8899
    }
    handle {
        reverse_proxy 192.168.68.68:3001
    }
}
```

Reload Caddy (`docker exec -it caddy caddy reload` or your usual method). Set
`GF_ROOT_URL=https://solar.bodka` in `.env` to match. Now:

- Dashboard: `https://solar.bodka`
- The mode buttons link to `/control/local` and `/control/mirror`, which Caddy routes to the
  poller — so switching works straight from the dashboard.

Anonymous viewing is enabled (Viewer role) so the dashboard opens without login; the admin
account (from `.env`) is still required to edit.

## The dashboard

Auto-provisioned into folder **SmartESS**, dashboard **Solar / Inverter**:

- Stat row: battery SoC, battery voltage, output load, PV voltage.
- Output power (active + apparent), battery (voltage + SoC), solar (V/A/W),
  grid vs output voltage + heatsink temperature, total energy.
- **Operating mode** panel with LOCAL / MIRROR buttons.

Edit freely in the UI (`allowUiUpdates` is on). To version a change, export the dashboard
JSON and replace `grafana/dashboards/solar.json`.

## Data for later analysis

VictoriaMetrics keeps everything and is queryable/exportable:

```bash
# export a metric over a range to JSON
curl 'http://127.0.0.1:8428/api/v1/export?match[]=inverter_battery_voltage' > bat.jsonl

# instant query
curl 'http://127.0.0.1:8428/api/v1/query?query=inverter_ac_output_active_power'
```

Grafana panels also export to CSV from the panel menu.

## Notes

- On a 3B+, Grafana's first load can take a few seconds; steady use is fine. If it feels too
  heavy, move **only Grafana** to another machine and point its VictoriaMetrics datasource at
  `http://<pi-ip>:8428` — the data (VM + Telegraf) stays on the Pi.
- Retention is 24 months (`-retentionPeriod=24` in `docker-compose.yml`); adjust as needed.
- All images are multi-arch and include `linux/arm/v7`.
