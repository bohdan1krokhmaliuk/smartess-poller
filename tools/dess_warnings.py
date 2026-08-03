#!/usr/bin/env python3
"""dess_warnings.py — pull the DessMonitor *alarm/warning log* (separate from the telemetry
export, which only carries a hard-fault code per 5-min sample).

Confirmed action: queryDeviceWarning. Response shape:
    dat = {total, page, pagesize, warning:[{gts, code, desc, title, level, status, handle, ...}]}
    gts = onset time (Kyiv local); level = severity (0 = warning-class in practice).

NOTE: this device's DessMonitor account keeps only a SHORT rolling alarm log (the date
params are ignored; `total` reflects the whole log). So this pulls whatever is currently
retained — it is not a long historical archive. The poller records the full history from
its own start onward (.warnings_history.jsonl).

Auth is identical to dess_pull.py:  sign = sha1(salt + secret + token + params_from_&action).

    python3 tools/dess_warnings.py            # -> warnings_backfill.jsonl (+ prints the events)

Needs DESS_TOKEN (+ DESS_SECRET) in .env (a fresh logged-in DessMonitor session), or passed
as environment variables.
"""
import argparse
import hashlib
import json
import os
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime
from zoneinfo import ZoneInfo

BASE = "https://web.dessmonitor.com/public/"
DEV = {"pn": "W0821424844211", "devcode": "2449", "devaddr": "1", "sn": "92932106103282"}
KYIV = ZoneInfo("Europe/Kyiv")
HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.dessmonitor.com",
    "Referer": "https://www.dessmonitor.com/",
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36"),
}

# DessMonitor alarm description -> our canonical name (so /warnings.html shows a nice label);
# anything unmatched keeps a slug of the description and its raw text is preserved too.
DESC_NAME = {
    "line_fail": "line_fail", "line fail": "line_fail", "grid loss": "line_fail",
    "pv loss": "pv_loss", "pv_loss": "pv_loss",
    "over load": "over_load", "overload": "over_load", "over_load": "over_load",
    "over temperature": "over_temperature", "over_temp": "over_temperature",
    "battery low": "battery_low_alarm", "low battery": "battery_low_alarm",
    "battery high": "battery_voltage_high", "fan": "fan_locked", "bus": "bus_over",
    "short": "opv_short", "over current": "inverter_over_current",
    "eeprom": "eeprom_fault", "mppt": "mppt_overload_warning",
}


def load_env(path=".env"):
    env = {}
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip().strip('"').strip("'")
    except FileNotFoundError:
        pass
    env.update(os.environ)
    return env


def signed_url(secret, token, ap):
    salt = str(int(time.time() * 1000))
    sign = hashlib.sha1((salt + (secret or "") + token + ap).encode()).hexdigest()
    return "%s?sign=%s&salt=%s&token=%s%s" % (BASE, sign, salt, token, ap)


def call(secret, token, ap, tries=6):
    for i in range(tries):
        req = urllib.request.Request(signed_url(secret, token, ap), headers=HEADERS)
        try:
            with urllib.request.urlopen(req, timeout=25) as r:
                return json.loads(r.read().decode("utf-8", "ignore"))
        except Exception as e:                       # DessMonitor throttles bursts -> back off
            if i < tries - 1:
                print("  retry %d (%s)" % (i, str(e)[:40]))
                time.sleep(12)
    return {"err": -1, "desc": "gave up (network/throttle)"}


def kyiv_to_epoch(s):
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return int(datetime.strptime(s[:19], fmt).replace(tzinfo=KYIV).timestamp())
        except Exception:
            pass
    return None


def to_name(desc):
    t = (desc or "").strip().lower()
    for kw, name in DESC_NAME.items():
        if kw in t:
            return name
    return "".join(c if c.isalnum() else "_" for c in t)[:40] or "warning"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="warnings_backfill.jsonl")
    ap.add_argument("--size", type=int, default=100)
    args = ap.parse_args()

    env = load_env()
    token, secret = env.get("DESS_TOKEN"), env.get("DESS_SECRET", "")
    if not token:
        sys.exit("Set DESS_TOKEN in .env (a fresh logged-in DessMonitor session token).")

    dev = "&pn=%s&devcode=%s&devaddr=%s&sn=%s" % (DEV["pn"], DEV["devcode"], DEV["devaddr"], DEV["sn"])
    records, page = [], 0
    while True:
        aps = ("&action=queryDeviceWarning&source=1&i18n=en_US" + dev +
               "&page=%d&pagesize=%d" % (page, args.size))
        j = call(secret, token, aps)
        if j.get("err") != 0:
            print("stop: err=%s desc=%s" % (j.get("err"), str(j.get("desc"))[:70]))
            if page == 0:
                print("\nIf this is an auth error, refresh DESS_TOKEN in .env and retry.")
            break
        dat = j.get("dat", {})
        lst = dat.get("warning", []) if isinstance(dat, dict) else []
        if page == 0:
            print("alarm log: total=%s (DessMonitor keeps only a short rolling log)" % dat.get("total"))
        if not lst:
            break
        records += lst
        if len(lst) < args.size:
            break
        page += 1
        time.sleep(3)

    events = []
    for r in records:
        if not isinstance(r, dict):
            continue
        ts = kyiv_to_epoch(str(r.get("gts") or r.get("cts") or ""))
        if ts is None:
            continue
        desc = r.get("desc") or r.get("title") or "warning"
        level = r.get("level")
        sev = "fault" if isinstance(level, (int, float)) and level >= 2 else "warning"
        events.append({"ts": ts,
                       "iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts)),
                       "active": [{"name": to_name(desc), "severity": sev, "text": desc}],
                       "level": sev, "raw": str(r.get("code") or "")})

    events.sort(key=lambda e: e["ts"])
    with open(args.out, "w") as f:
        for e in events:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")

    print("\nwrote %d alarm events -> %s" % (len(events), args.out))
    for e in events:
        print("  %s  %-9s  %s" % (e["iso"], e["level"], e["active"][0]["text"]))
    print("\nMerge on the Pi (then it shows on /warnings.html):")
    print("  scp %s pi:~/smartess-poller/  &&  ssh pi 'cat ~/smartess-poller/%s >> ~/smartess-poller/.warnings_history.jsonl'"
          % (os.path.basename(args.out), os.path.basename(args.out)))


if __name__ == "__main__":
    main()
