#!/usr/bin/env python3
"""dess_pull.py — bulk-pull historical detail from DessMonitor (Eybond).

One-time backfill helper. It hits the same signed endpoint the website fires on
"export":
    GET https://web.dessmonitor.com/public/?sign=..&salt=..&token=..
        &action=exportDeviceDataDetail&source=1&i18n=en_US
        &pn=..&devcode=..&devaddr=..&sn=..&startDate=..&endDate=..&customizePeriod=1
which returns an XLSX workbook with ALL fields for the whole range — far fewer
requests than the per-field chart endpoint, and (verified) NOT capped for long ranges;
the data is just intermittent (the device only reports on days it was online).

sign = sha1(salt + secret + token + <params starting at &action=>)   [verified]

Uses a *session* token+secret captured from a logged-in browser (login `authSource`
response -> dat.token / dat.secret; valid ~5 days). Put them in a .env:
    DESS_TOKEN=CN....
    DESS_SECRET=....
then:  python3 tools/dess_pull.py --start 2024-01-01 --end 2026-07-30
Device ids default to the captured ones; override with --pn/--devcode/--devaddr/--sn.
"""
import argparse
import csv
import datetime
import hashlib
import io
import os
import sys
import time
import urllib.request
import xml.etree.ElementTree as ET
import zipfile

BASE = "https://web.dessmonitor.com/public/"
DEV = {"pn": "W0821424844211", "devcode": "2449", "devaddr": "1", "sn": "92932106103282"}
HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.dessmonitor.com",
    "Referer": "https://www.dessmonitor.com/",
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36"),
}
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"


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


def action_params(start, end):
    return ("&action=exportDeviceDataDetail&source=1&i18n=en_US"
            "&pn=%s&devcode=%s&devaddr=%s&sn=%s"
            "&startDate=%s&endDate=%s&customizePeriod=1"
            % (DEV["pn"], DEV["devcode"], DEV["devaddr"], DEV["sn"], start, end))


def signed_url(secret, token, ap):
    salt = str(int(time.time() * 1000))
    sign = hashlib.sha1((salt + (secret or "") + token + ap).encode()).hexdigest()
    return "%s?sign=%s&salt=%s&token=%s%s" % (BASE, sign, salt, token, ap)


def fetch(secret, token, start, end):
    url = signed_url(secret, token, action_params(start, end))
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=300) as r:
        return r.read()


def is_data(body):
    """Accept an XLSX (zip 'PK') or CSV payload; reject a JSON error blob."""
    head = body[:256].lstrip()
    return bool(head) and head[:1] not in (b"{", b"[")


def _rows_xlsx(body):
    z = zipfile.ZipFile(io.BytesIO(body))
    shared = []
    if "xl/sharedStrings.xml" in z.namelist():
        shared = ["".join(t.text or "" for t in si.iter(NS + "t"))
                  for si in ET.fromstring(z.read("xl/sharedStrings.xml"))]
    sheet = ET.fromstring(z.read("xl/worksheets/sheet1.xml")).find(NS + "sheetData")

    def cidx(ref):
        s = "".join(c for c in ref if c.isalpha())
        n = 0
        for c in s:
            n = n * 26 + (ord(c) - 64)
        return n - 1

    out = []
    for row in sheet.findall(NS + "row"):
        v = {}
        for c in row.findall(NS + "c"):
            e = c.find(NS + "v")
            v[cidx(c.get("r"))] = shared[int(e.text)] if c.get("t") == "s" else (e.text if e is not None else None)
        out.append([v.get(i) for i in range(max(v) + 1)] if v else [])
    return out


def inspect(body):
    """(n_rows, first_ts, last_ts) — parses XLSX or CSV; never raises."""
    try:
        rows = _rows_xlsx(body) if body[:2] == b"PK" else \
            [r for r in csv.reader(body.decode("utf-8-sig", "ignore").splitlines()) if r]
    except Exception:
        return (0, None, None)
    rows = [r for r in rows if r]
    if len(rows) < 2:
        return (0, None, None)
    hdr = rows[0]
    tcol = next((i for i, h in enumerate(hdr) if h and ("time" in str(h).lower() or "date" in str(h).lower())), 0)
    stamps = sorted(str(r[tcol])[:19] for r in rows[1:]
                    if tcol < len(r) and r[tcol] and str(r[tcol])[:4].isdigit())
    return (len(rows) - 1, stamps[0] if stamps else None, stamps[-1] if stamps else None)


def save(out, s, e, body):
    ext = "xlsx" if body[:2] == b"PK" else "csv"
    path = os.path.join(out, "dess_%s_%s.%s" % (s, e, ext))
    with open(path, "wb") as f:
        f.write(body)
    n, f0, f1 = inspect(body)
    print("  saved %s  (%d rows, %s .. %s)" % (path, n, f0, f1))
    return n, f0, f1


def chunks(start, end, days):
    d0, d1 = datetime.date.fromisoformat(start), datetime.date.fromisoformat(end)
    cur = d0
    while cur <= d1:
        ce = min(d1, cur + datetime.timedelta(days=days - 1))
        yield cur.isoformat(), ce.isoformat()
        cur = ce + datetime.timedelta(days=1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD (inclusive)")
    # The export keeps only the most-recent ~8000 rows of any range, so we always
    # chunk. A month peaks near ~3300 rows, so 30-day windows stay well under the cap.
    ap.add_argument("--chunk-days", type=int, default=30, help="window size (keep < ~cap; 30 is safe)")
    ap.add_argument("--out", default="dess_xlsx")
    for k in DEV:
        ap.add_argument("--" + k, default=DEV[k])
    args = ap.parse_args()
    for k in DEV:
        DEV[k] = getattr(args, k)

    env = load_env()
    token, secret = env.get("DESS_TOKEN"), env.get("DESS_SECRET", "")
    if not token:
        sys.exit("Set DESS_TOKEN in .env (from a logged-in DessMonitor session).")
    os.makedirs(args.out, exist_ok=True)

    windows = list(chunks(args.start, args.end, args.chunk_days))
    print("pulling %s .. %s in %d windows of %d days..." % (args.start, args.end, len(windows), args.chunk_days))
    ok = rows = 0
    for s, e in windows:
        try:
            body = fetch(secret, token, s, e)
        except Exception as ex:
            print("FAIL %s..%s : %s" % (s, e, ex))
            continue
        if not is_data(body):
            msg = body[:160].decode("utf-8", "ignore")
            print("server error %s..%s : %s" % (s, e, msg))
            if "sign" in msg.lower() or "token" in msg.lower():
                sys.exit("auth rejected — token/secret expired; re-capture a fresh login.")
            continue
        n, f0, f1 = save(args.out, s, e, body)
        rows += n
        ok += 1
        time.sleep(0.8)
    print("done: %d/%d windows, %d rows total, saved to %s/" % (ok, len(windows), rows, args.out))


if __name__ == "__main__":
    main()
