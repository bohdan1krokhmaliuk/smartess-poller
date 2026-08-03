#!/usr/bin/env python3
"""dess_warnings.py — pull the DessMonitor *alarm/warning log* (separate from the telemetry
export, which only carries a hard-fault code per 5-min sample).

The alarm log is its own signed action on the same endpoint. We don't know the exact action
name a priori, so this script PROBES a list of known candidates with your token and reports
which one answers, then paginates the full range and writes a change-log JSONL in the poller's
warnings-history shape (merge it into .warnings_history.jsonl on the Pi).

Auth is identical to dess_pull.py:  sign = sha1(salt + secret + token + params_from_&action)

    python3 tools/dess_warnings.py --start 2024-01-01 --end 2026-12-31
    # -> prints the working action, writes warnings_backfill.jsonl

Needs DESS_TOKEN (+ optional DESS_SECRET) in .env — the same fresh, logged-in session token
you used for the export. If every candidate returns an auth error, your token expired: log in
to dessmonitor.com again and refresh it.
"""
import argparse
import hashlib
import json
import os
import sys
import time
import urllib.parse
import urllib.request

BASE = "https://web.dessmonitor.com/public/"
DEV = {"pn": "W0821424844211", "devcode": "2449", "devaddr": "1", "sn": "92932106103282"}
HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.dessmonitor.com",
    "Referer": "https://www.dessmonitor.com/",
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36"),
}

# Axpert fault keywords -> our canonical names (for severity + nicer labels). Matched loosely
# against whatever text the alarm log returns.
FAULT_KEYWORDS = [
    ("over temp", "over_temperature"), ("temperature", "over_temperature"),
    ("over load", "over_load"), ("overload", "over_load"),
    ("short", "opv_short"), ("over current", "inverter_over_current"),
    ("bus", "bus_over"), ("battery low", "battery_low_alarm"),
    ("low battery", "battery_low_alarm"), ("battery high", "battery_voltage_high"),
    ("fan", "fan_locked"), ("line fail", "line_fail"), ("grid", "line_fail"),
    ("self test", "self_test_fail"), ("sensor", "current_sensor_fail"),
    ("eeprom", "eeprom_fault"), ("mppt", "mppt_overload_warning"),
]
FAULT_LEVEL_HINTS = ("fault", "fail", "short", "over current", "over temp", "shutdown", "over load")


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


def call(secret, token, ap):
    """Return parsed JSON (or {'_http': code}) for a params string starting at &action=."""
    req = urllib.request.Request(signed_url(secret, token, ap), headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            body = r.read().decode("utf-8", "ignore")
    except urllib.error.HTTPError as e:
        return {"_http": e.code, "_body": e.read().decode("utf-8", "ignore")[:200]}
    except Exception as e:
        return {"_err": str(e)}
    try:
        return json.loads(body)
    except Exception:
        return {"_raw": body[:200]}


def dev_params():
    return "&pn=%s&devcode=%s&devaddr=%s&sn=%s" % (DEV["pn"], DEV["devcode"], DEV["devaddr"], DEV["sn"])


def candidates(start, end, page, size):
    """(label, params) pairs — known DessMonitor/Shinemonitor alarm actions and param shapes."""
    i = "&i18n=en_US"
    dev = dev_params()
    pg = "&page=%d&pagesize=%d" % (page, size)
    dr1 = "&start=%s&end=%s" % (start, end)                 # plain date range
    dr2 = "&startDate=%s&endDate=%s" % (start, end)
    dr3 = "&begindate=%s&enddate=%s" % (start, end)
    out = []
    for act in ("queryDeviceWarn", "webQueryDeviceWarn", "queryDeviceWarnList",
                "querySPDeviceWarn", "queryDeviceEsWarn", "webQueryDeviceEsWarn",
                "queryDeviceWarnAllList", "queryDeviceCtrlWarn"):
        out.append((act + " [start/end]", "&action=%s%s%s%s%s" % (act, i, dev, dr1, pg)))
    # a couple of date-key variants on the most likely action
    out.append(("queryDeviceWarn [startDate]", "&action=queryDeviceWarn%s%s%s%s" % (i, dev, dr2, pg)))
    out.append(("queryDeviceWarn [begindate]", "&action=queryDeviceWarn%s%s%s%s" % (i, dev, dr3, pg)))
    return out


def is_ok(j):
    return isinstance(j, dict) and j.get("err") == 0 and isinstance(j.get("dat"), (dict, list))


def find_list(dat):
    """Locate the list of warning records inside dat (dict or list)."""
    if isinstance(dat, list):
        return dat
    if isinstance(dat, dict):
        for k in ("warn", "warns", "warning", "warnings", "list", "records", "rows", "data", "page"):
            v = dat.get(k)
            if isinstance(v, list):
                return v
        for v in dat.values():                              # fallback: first list value
            if isinstance(v, list):
                return v
    return []


def classify(text):
    t = (text or "").lower()
    name = next((n for kw, n in FAULT_KEYWORDS if kw in t), None)
    if not name:
        name = "".join(c if c.isalnum() else "_" for c in t.strip())[:40] or "warning"
    sev = "fault" if any(h in t for h in FAULT_LEVEL_HINTS) else "warning"
    return name, sev


def rec_to_event(rec):
    """Best-effort map one alarm record -> (ts_seconds, text, code)."""
    if not isinstance(rec, dict):
        return None
    ts = None
    for k, v in rec.items():
        kl = k.lower()
        if any(s in kl for s in ("start", "gts", "begin", "time", "date")) and v:
            s = str(v)
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
                try:
                    ts = int(time.mktime(time.strptime(s[:19], fmt)))
                    break
                except Exception:
                    pass
            if ts is None and s.isdigit():
                ts = int(s) // (1000 if len(s) > 10 else 1)
        if ts is not None:
            break
    text = ""
    for k, v in rec.items():
        kl = k.lower()
        if any(s in kl for s in ("desc", "content", "name", "warn", "text", "msg")) and isinstance(v, str) and v:
            text = v
            break
    code = next((str(rec[k]) for k in rec if k.lower() in ("code", "warncode", "warntype")), "")
    return (ts, text or code or "warning", code)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2024-01-01")
    ap.add_argument("--end", default=time.strftime("%Y-%m-%d"))
    ap.add_argument("--out", default="warnings_backfill.jsonl")
    ap.add_argument("--size", type=int, default=100)
    args = ap.parse_args()

    env = load_env()
    token, secret = env.get("DESS_TOKEN"), env.get("DESS_SECRET", "")
    if not token:
        sys.exit("Set DESS_TOKEN in .env (a fresh logged-in DessMonitor session token).")

    print("probing alarm-log actions for %s .. %s ...\n" % (args.start, args.end))
    working = None
    for label, ap_str in candidates(args.start, args.end, 0, args.size):
        j = call(secret, token, ap_str)
        if is_ok(j):
            lst = find_list(j["dat"])
            print("  ✓ %-34s err=0  records=%d" % (label, len(lst)))
            if working is None and lst:
                working = (label, ap_str.split("&action=")[1].split("&")[0])
        else:
            msg = j.get("desc") or j.get("_http") or j.get("_err") or j.get("_raw") or j.get("err")
            print("  ✗ %-34s %s" % (label, str(msg)[:60]))

    if working is None:
        print("\nNo candidate returned alarm records. Two likely reasons:")
        print("  1) token expired -> log in to dessmonitor.com, refresh DESS_TOKEN in .env, retry.")
        print("  2) the action name differs -> open the DessMonitor 'Alarm/Warning' page in the")
        print("     browser for a period with known errors, copy that request from the Network tab,")
        print("     and share it — the exact &action= + params are all that's missing.")
        return

    label, action = working
    print("\nusing action: %s — paginating full range ...\n" % action)
    events, page = [], 0
    while True:
        ap_str = "&action=%s&i18n=en_US%s&start=%s&end=%s&page=%d&pagesize=%d" % (
            action, dev_params(), args.start, args.end, page, args.size)
        j = call(secret, token, ap_str)
        lst = find_list(j.get("dat", {})) if is_ok(j) else []
        if not lst:
            break
        for rec in lst:
            ev = rec_to_event(rec)
            if ev and ev[0]:
                events.append(ev)
        print("  page %d: +%d (total %d)" % (page, len(lst), len(events)))
        if len(lst) < args.size:
            break
        page += 1
        if page > 500:
            print("  (stopping at 500 pages)")
            break

    events.sort(key=lambda e: e[0])
    with open(args.out, "w") as f:
        for ts, text, code in events:
            name, sev = classify(text)
            f.write(json.dumps({
                "ts": ts, "iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts)),
                "active": [{"name": name, "severity": sev, "text": text}],
                "level": sev, "raw": code or text}) + "\n")
    print("\nwrote %d alarm events -> %s" % (len(events), args.out))
    if events:
        print("first record shape (verify the mapping looks right):")
        print("  ", events[0])
    print("\nMerge on the Pi (then it shows on /warnings.html):")
    print("  cat %s >> ~/smartess-poller/.warnings_history.jsonl" % os.path.basename(args.out))


if __name__ == "__main__":
    main()
