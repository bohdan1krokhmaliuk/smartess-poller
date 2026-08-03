#!/usr/bin/env python3
"""dess_backfill.py — turn the pulled DessMonitor XLSX (+ archive weather) into an
InfluxDB line-protocol file that VictoriaMetrics can ingest with historical
timestamps. Runs on any machine with internet; the resulting .gz is POSTed to VM
on the Pi:

    curl --data-binary @backfill.lp.gz -H 'Content-Encoding: gzip' \
         'http://127.0.0.1:8428/write'

It emits three measurements matching the LIVE schema, so history merges seamlessly:
  inverter_*  (battery_voltage, pv_input_voltage/current, pv_charging_power,
               ac_output_active_power, battery_charge/discharge_current, ...)
  rated_*     (bulk/float/recharge/redischarge voltage, max charging current,
               output/charger source priority, battery type)  -- only on change
  weather_*   (gti, cloud, temp, pv_potential_w)  -- from the Open-Meteo ERA5 archive

XLSX timestamps are Kyiv local (verified: PV-peak median hour 13) -> converted to UTC.
Timestamps are written in nanoseconds so VM ingests them at the default precision.
"""
import argparse
import calendar
import glob
import gzip
import json
import os
import time
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime
from zoneinfo import ZoneInfo

NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
KYIV = ZoneInfo("Europe/Kyiv")

# --- PV derate (mirror of smartess_poller.py / index.html) -------------------
PV_NOCT, PV_TEMP_COEFF, PV_BASE_DERATE = 45.0, 0.004, 0.90


def pv_derate(gti, t_air):
    if t_air is None:
        return PV_BASE_DERATE
    t_cell = t_air + (gti / 800.0) * (PV_NOCT - 20.0)
    return max(0.5, min(1.05, PV_BASE_DERATE * (1.0 - PV_TEMP_COEFF * (t_cell - 25.0))))


# --- XLSX column -> line-protocol field --------------------------------------
INV_MAP = {                     # col index -> inverter_<field>
    5: "grid_voltage", 6: "grid_frequency", 7: "pv_input_voltage", 40: "pv_input_current",
    9: "pv_charging_power", 11: "battery_voltage", 12: "battery_capacity",
    13: "battery_charge_current", 14: "battery_discharge_current",
    15: "ac_output_voltage", 16: "ac_output_frequency", 17: "ac_output_apparent_power",
    18: "ac_output_active_power", 19: "output_load_percent", 39: "bus_voltage", 41: "scc_voltage",
}
RATED_NUM = {                   # col index -> rated_<field> (plain numbers)
    28: "battery_recharge_voltage", 29: "battery_bulk_voltage", 30: "battery_float_voltage",
    32: "max_ac_charging_current", 33: "max_charging_current", 38: "battery_redischarge_voltage",
}
RATED_ENUM = {                  # col index -> (rated_<field>, {text: code})
    35: ("output_source_priority", {"Utility Solar Bat": 0, "Solar Utility Bat": 1,
                                    "Solar Bat Utility": 2, "Only Solar Charging": 3}),
    36: ("charger_source_priority", {"Utility First": 0, "Solar First": 1,
                                     "Solar + Utility": 2, "Only Solar": 3, "Solar And Utility": 2}),
    31: ("battery_type", {"AGM": 0, "Flooded": 1, "User": 2, "Pylontech": 3, "Pyl": 3}),
}
FAULT_COL = 48                  # "Inverter Fault" — numeric Axpert fault code (0 = OK)
FAULT_CODE_NAME = {             # code -> canonical name (reuses the poller's QPIWS names where they align)
    "01": "fan_locked", "02": "over_temperature", "03": "battery_voltage_high",
    "04": "battery_low_alarm", "05": "opv_short", "06": "inverter_voltage_high",
    "07": "over_load", "08": "bus_over", "09": "bus_soft_fail", "10": "pv_voltage_high",
    "11": "main_relay_fault", "51": "inverter_over_current", "52": "bus_under",
    "53": "inverter_soft_fail", "55": "op_dc_over_voltage", "57": "current_sensor_fail",
    "58": "inverter_voltage_low", "60": "power_feedback_fault", "71": "firmware_mismatch",
    "72": "current_share_fault", "80": "can_fault", "81": "host_loss", "82": "sync_loss",
}


def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def load_rows(path):
    z = zipfile.ZipFile(path)
    shared = ["".join(t.text or "" for t in si.iter(NS + "t"))
              for si in ET.fromstring(z.read("xl/sharedStrings.xml"))] if "xl/sharedStrings.xml" in z.namelist() else []
    sheet = ET.fromstring(z.read("xl/worksheets/sheet1.xml")).find(NS + "sheetData")

    def cidx(ref):
        s = "".join(c for c in ref if c.isalpha()); n = 0
        for c in s:
            n = n * 26 + (ord(c) - 64)
        return n - 1

    rows = []
    for row in sheet.findall(NS + "row"):
        v = {}
        for c in row.findall(NS + "c"):
            e = c.find(NS + "v")
            v[cidx(c.get("r"))] = shared[int(e.text)] if c.get("t") == "s" else (e.text if e is not None else None)
        rows.append([v.get(i) for i in range(max(v) + 1)] if v else [])
    return rows


def to_utc_ns(local_str):
    dt = datetime.strptime(local_str[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=KYIV)
    return int(dt.timestamp()) * 1_000_000_000


def fetch_weather(lat, lon, tilt, az, start, end):
    """Open-Meteo ERA5 archive: hourly GTI + cloud + temp (UTC). Yearly chunks."""
    out = []
    y0, y1 = int(start[:4]), int(end[:4])
    for yr in range(y0, y1 + 1):
        s = max(start, "%d-01-01" % yr)
        e = min(end, "%d-12-31" % yr)
        url = ("https://archive-api.open-meteo.com/v1/archive?latitude=%s&longitude=%s"
               "&start_date=%s&end_date=%s"
               "&hourly=global_tilted_irradiance,cloud_cover,temperature_2m"
               "&tilt=%d&azimuth=%d&timezone=GMT" % (lat, lon, s, e, round(tilt), round(az - 180)))
        j = json.loads(urllib.request.urlopen(url, timeout=120).read().decode())
        h = j.get("hourly", {})
        T = h.get("time", []); G = h.get("global_tilted_irradiance", [])
        C = h.get("cloud_cover", []); TM = h.get("temperature_2m", [])
        added = 0
        for i, t in enumerate(T):
            g = G[i] if i < len(G) else None
            if g is None:
                continue
            ts = calendar.timegm(time.strptime(t, "%Y-%m-%dT%H:%M")) * 1_000_000_000
            out.append((ts, g, C[i] if i < len(C) else None, TM[i] if i < len(TM) else None))
            added += 1
        print("  weather %d: %d hourly points" % (yr, added))
    return out


def fnum(x):
    return ("%.4f" % x).rstrip("0").rstrip(".")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx-dir", required=True)
    ap.add_argument("--out", default="backfill.lp.gz")
    ap.add_argument("--lat", type=float, default=49.9153)
    ap.add_argument("--lon", type=float, default=23.949)
    ap.add_argument("--kwp", type=float, default=3.6)
    ap.add_argument("--tilt", type=float, default=11.0)
    ap.add_argument("--az", type=float, default=235.0)
    ap.add_argument("--no-weather", action="store_true")
    ap.add_argument("--warn-out", default="warnings_backfill.jsonl",
                    help="fault change-log to merge into the Pi's .warnings_history.jsonl")
    args = ap.parse_args()

    files = sorted(glob.glob(os.path.join(args.xlsx_dir, "*.xlsx")))
    if not files:
        raise SystemExit("no XLSX in " + args.xlsx_dir)

    n_inv = n_rated = n_wx = n_warn = 0
    days = set()
    rated_obs = []
    fault_obs = []
    gz = gzip.open(args.out, "wt")

    for f in files:
        for r in load_rows(f)[1:]:
            if not r or not r[0] or not str(r[0])[:4].isdigit():
                continue
            try:
                ts = to_utc_ns(str(r[0]))
            except Exception:
                continue
            days.add(str(r[0])[:10])
            # inverter telemetry
            fields = []
            for col, key in INV_MAP.items():
                if col < len(r):
                    v = num(r[col])
                    if v is not None:
                        fields.append("%s=%s" % (key, fnum(v)))
            if fields:
                gz.write("inverter " + ",".join(fields) + " %d\n" % ts)
                n_inv += 1
            # settings snapshot for this row (emitted on-change, in time order, below)
            rated = {}
            for col, key in RATED_NUM.items():
                if col < len(r):
                    v = num(r[col])
                    if v is not None:
                        rated[key] = v
            for col, (key, m) in RATED_ENUM.items():
                if col < len(r) and r[col] in m:
                    rated[key] = m[r[col]]
            if rated:
                rated_obs.append((ts, tuple(sorted(rated.items()))))
            # fault code for this row (0 = OK); collected here, emitted on-change below
            if FAULT_COL < len(r) and r[FAULT_COL] is not None:
                base = str(r[FAULT_COL]).split(".")[0].strip()
                fault_obs.append((ts, "0" if base in ("", "0", "None") else base.zfill(2)))

    # rated: emit only real changes, in chronological order (rows arrive newest-first)
    rated_obs.sort(key=lambda x: x[0])
    prev = None
    for ts, items in rated_obs:
        if items != prev:
            gz.write("rated " + ",".join("%s=%s" % (k, fnum(v)) for k, v in items) + " %d\n" % ts)
            prev = items
            n_rated += 1

    # faults: emit onset/clear events as a change-log in the poller's warnings-history JSONL shape
    # (append to .warnings_history.jsonl on the Pi so history merges with the live log)
    fault_obs.sort(key=lambda x: x[0])
    prev = None
    with open(args.warn_out, "w") as wf:
        for ts, code in fault_obs:
            if code == prev:
                continue
            if prev is None and code == "0":     # skip the initial all-clear baseline
                prev = code
                continue
            prev = code
            active = [] if code == "0" else [
                {"name": FAULT_CODE_NAME.get(code, "fault_code_%s" % code), "severity": "fault"}]
            secs = ts // 1_000_000_000
            wf.write(json.dumps({"ts": secs,
                                 "iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(secs)),
                                 "active": active, "level": "fault" if active else "ok",
                                 "raw": code}) + "\n")
            n_warn += 1

    # weather from the archive
    if not args.no_weather and days:
        start, end = min(days), max(days)
        print("fetching archive weather %s .. %s ..." % (start, end))
        for ts, g, c, t in fetch_weather(args.lat, args.lon, args.tilt, args.az, start, end):
            fields = ["gti=%s" % fnum(g)]
            if c is not None:
                fields.append("cloud=%s" % fnum(c))
            if t is not None:
                fields.append("temp=%s" % fnum(t))
            fields.append("pv_potential_w=%s" % fnum(round(args.kwp * g * pv_derate(g, t))))
            gz.write("weather " + ",".join(fields) + " %d\n" % ts)
            n_wx += 1

    gz.close()
    size = os.path.getsize(args.out)
    print("\nwrote %s  (%.1f MB gzip)" % (args.out, size / 1e6))
    print("  inverter points: %d" % n_inv)
    print("  rated points   : %d (on change)" % n_rated)
    print("  weather points : %d" % n_wx)
    print("  fault events   : %d (on change) -> %s" % (n_warn, args.warn_out))
    print("\nMerge fault history on the Pi (then it shows on /warnings.html):")
    print("  cat %s >> ~/smartess-poller/.warnings_history.jsonl" % os.path.basename(args.warn_out))
    print("  day span       : %s .. %s (%d days)" % (min(days), max(days), len(days)))
    print("\nIngest on the Pi (where VM is 127.0.0.1:8428):")
    print("  curl --data-binary @%s -H 'Content-Encoding: gzip' 'http://127.0.0.1:8428/write'"
          % os.path.basename(args.out))


if __name__ == "__main__":
    main()
