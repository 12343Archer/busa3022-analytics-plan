#!/usr/bin/env python3
"""
Proof of concept: ingest the RBA Interbank Overnight Cash Rate end to end.

    fetch (retry/backoff) -> raw landing (immutable + manifest)
        -> header contract check -> row validation
        -> staging (good rows) + quarantine (bad rows) -> DQ + run log

Standard library only (no pip install needed).

Usage:
    python rba_pipeline.py                       # live download from the RBA
    python rba_pipeline.py --file fixtures/x.csv # replay a local/raw file
    python rba_pipeline.py --report              # print the run + DQ log
"""
import argparse
import csv
import hashlib
import io
import json
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------- config
SOURCE = "rba_f1"
SOURCE_URL = "https://www.rba.gov.au/statistics/tables/csv/f1-data.csv"
TARGET_TITLE = "Interbank Overnight Cash Rate"   # column located by title, not position
BASE = Path(__file__).parent
RAW_DIR = BASE / "data" / "raw" / SOURCE
DB_PATH = BASE / "data" / "pipeline.db"

TIMEOUT_S = 20
MAX_RETRIES = 3
BACKOFF_S = 2            # 2s, 4s, 8s
RATE_MIN, RATE_MAX = -1.0, 25.0
MAX_REJECT_PCT = 5.0     # hold the batch if more than this % of rows are hard rejects
FRESH_WARN_BDAYS = 3     # warn if the latest observation is older than this
DATE_FORMATS = ("%d-%b-%Y", "%d/%m/%Y", "%Y-%m-%d")


class IngestError(Exception):
    """Source could not be fetched (timeout, HTTP error, connection error)."""


class SchemaChangedError(Exception):
    """The file no longer matches the expected header contract."""


def log(event, **fields):
    """One structured line per event to stdout (scheduler logs keep it)."""
    stamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(json.dumps({"ts": stamp, "event": event, **fields}), flush=True)


# ---------------------------------------------------------------- database
SCHEMA = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT, started_at TEXT, finished_at TEXT, status TEXT,
    rows_read INTEGER, rows_loaded INTEGER, rows_quarantined INTEGER,
    raw_file TEXT, error TEXT);
CREATE TABLE IF NOT EXISTS stg_cash_rate (
    obs_date TEXT PRIMARY KEY, rate_pct REAL NOT NULL,
    run_id INTEGER NOT NULL, raw_file TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS quarantine (
    quarantine_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER, source TEXT, source_line_no INTEGER, raw_line TEXT,
    reason_code TEXT, detail TEXT, quarantined_at TEXT);
CREATE TABLE IF NOT EXISTS dq_results (
    dq_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER, check_name TEXT, severity TEXT, status TEXT,
    failed_count INTEGER, detail TEXT);
"""


def connect(db_path=DB_PATH):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path)
    con.executescript(SCHEMA)
    return con


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------------------------------------------------------------- 1. fetch
def fetch(url, retries=MAX_RETRIES, timeout=TIMEOUT_S, backoff=BACKOFF_S):
    """GET with timeout and exponential backoff. Raises IngestError at the end."""
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (student-pipeline-poc)"})
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = resp.read()
                log("fetch_ok", attempt=attempt, bytes=len(body))
                return body
        except urllib.error.HTTPError as e:
            last_err = f"HTTP {e.code}"
            if e.code < 500 and e.code != 429:      # 4xx (except rate limit) won't fix itself
                break
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            last_err = f"{type(e).__name__}: {getattr(e, 'reason', e)}"
        wait = backoff * 2 ** (attempt - 1)
        log("fetch_retry", attempt=attempt, error=last_err,
            next_wait_s=wait if attempt < retries else None)
        if attempt < retries:
            time.sleep(wait)
    raise IngestError(f"gave up after {attempt} attempt(s): {last_err}")


# ---------------------------------------------------------------- 2. land raw
def land_raw(body, run_id, origin):
    """Write bytes unchanged + a manifest. Same content already landed -> reuse it."""
    sha = hashlib.sha256(body).hexdigest()
    for manifest in RAW_DIR.glob("ingest_date=*/*.manifest.json"):
        m = json.loads(manifest.read_text())
        if m["sha256"] == sha:
            log("raw_duplicate", sha256=sha[:12], existing=m["file"])
            return Path(m["file"]), False
    part = RAW_DIR / f"ingest_date={date.today().isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    path = part / f"{SOURCE}_run{run_id}_{sha[:8]}.csv"
    path.write_bytes(body)
    manifest = {"file": str(path), "origin": origin, "sha256": sha, "bytes": len(body),
                "run_id": run_id, "fetched_at": now()}
    path.with_suffix(".manifest.json").write_text(json.dumps(manifest, indent=2))
    log("raw_landed", file=str(path), sha256=sha[:12])
    return path, True


# ---------------------------------------------------------------- 3. contract
def decode(body):
    for enc in ("utf-8-sig", "cp1252"):   # RBA files have used Windows encoding
        try:
            return body.decode(enc)
        except UnicodeDecodeError:
            continue
    return body.decode("latin-1")


def parse(body):
    """Check the header contract and return (target_col, data_rows).

    RBA CSVs start with ~10 metadata rows (Title, Description, Units, ...,
    Series ID); data follows the 'Series ID' row. We find the target column
    by its Title, so a column moving is fine but a rename/removal is not.
    """
    rows = list(csv.reader(io.StringIO(decode(body))))
    title_row = next((r for r in rows if r and r[0].strip() == "Title"), None)
    sid_idx = next((i for i, r in enumerate(rows) if r and r[0].strip() == "Series ID"), None)
    if title_row is None or sid_idx is None:
        raise SchemaChangedError("metadata rows 'Title' / 'Series ID' not found")
    titles = [t.strip() for t in title_row]
    if TARGET_TITLE not in titles:
        raise SchemaChangedError(
            f"expected column '{TARGET_TITLE}' not found; found {titles[1:]}")
    col = titles.index(TARGET_TITLE)
    data = [(sid_idx + 2 + i, r) for i, r in enumerate(rows[sid_idx + 1:]) if any(c.strip() for c in r)]
    return col, data


# ---------------------------------------------------------------- 4. validate
def parse_date(s):
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(s.strip(), fmt).date()
        except ValueError:
            pass
    return None


def validate(col, data):
    """Split rows into good [(date, rate)] and bad [(line, raw, code, detail)]."""
    good, bad, seen = [], [], set()
    series_started = False
    for line_no, r in data:
        raw = ",".join(r)
        d = parse_date(r[0]) if r else None
        val = r[col].strip() if len(r) > col else ""
        if d is None:
            bad.append((line_no, raw, "BAD_DATE", f"unparseable date '{r[0] if r else ''}'"))
            continue
        if d > date.today():
            bad.append((line_no, raw, "FUTURE_DATE", d.isoformat()))
            continue
        if val == "":
            code = "MISSING_VALUE" if series_started else "BEFORE_SERIES_START"
            bad.append((line_no, raw, code, "blank value"))
            continue
        try:
            rate = float(val)
        except ValueError:
            bad.append((line_no, raw, "NON_NUMERIC", f"value '{val}'"))
            continue
        if not RATE_MIN <= rate <= RATE_MAX:
            bad.append((line_no, raw, "OUT_OF_RANGE", f"{rate} not in [{RATE_MIN}, {RATE_MAX}]"))
            continue
        if d in seen:
            bad.append((line_no, raw, "DUPLICATE_DATE", d.isoformat()))
            continue
        seen.add(d)
        series_started = True
        good.append((d.isoformat(), rate))
    return good, bad


# ---------------------------------------------------------------- 5. run
def run(con, body_source, origin):
    cur = con.execute("INSERT INTO pipeline_runs (source, started_at, status) VALUES (?, ?, 'RUNNING')",
                      (SOURCE, now()))
    run_id = cur.lastrowid
    con.commit()
    log("run_start", run_id=run_id, origin=origin)
    raw_path, stats = None, {}

    def finish(status, error=None):
        con.execute("""UPDATE pipeline_runs SET finished_at=?, status=?, rows_read=?, rows_loaded=?,
                       rows_quarantined=?, raw_file=?, error=? WHERE run_id=?""",
                    (now(), status, stats.get("read"), stats.get("loaded"), stats.get("quarantined"),
                     str(raw_path) if raw_path else None, error, run_id))
        con.commit()
        log("run_end", run_id=run_id, status=status, error=error, **stats)
        return status

    def dq(check, severity, ok, count=0, detail=""):
        con.execute("""INSERT INTO dq_results (run_id, check_name, severity, status, failed_count, detail)
                       VALUES (?,?,?,?,?,?)""", (run_id, check, severity, "PASS" if ok else "FAIL", count, detail))

    # fetch -> land
    try:
        body = body_source()
    except IngestError as e:
        dq("source_reachable", "ERROR", False, detail=str(e))
        return finish("FAILED", f"INGEST_FAILED: {e}")
    dq("source_reachable", "ERROR", True)
    raw_path, _ = land_raw(body, run_id, origin)

    # contract
    try:
        col, data = parse(body)
    except SchemaChangedError as e:
        dq("header_contract", "ERROR", False, detail=str(e))
        return finish("FAILED", f"SCHEMA_CHANGED: {e}")
    dq("header_contract", "ERROR", True, detail=f"'{TARGET_TITLE}' at column {col}")

    # validate
    good, bad = validate(col, data)
    stats = {"read": len(data), "loaded": len(good), "quarantined": len(bad)}
    by_reason = {}
    for _, _, code, _ in bad:
        by_reason[code] = by_reason.get(code, 0) + 1
    for code, n in sorted(by_reason.items()):
        dq(f"row_{code.lower()}", "INFO" if code == "BEFORE_SERIES_START" else "WARN", False, n)

    # quarantine is always written, even if the batch is held below
    con.executemany("""INSERT INTO quarantine (run_id, source, source_line_no, raw_line, reason_code,
                       detail, quarantined_at) VALUES (?,?,?,?,?,?,?)""",
                    [(run_id, SOURCE, ln, raw, code, det, now()) for ln, raw, code, det in bad])

    # reconciliation: nothing may disappear
    ok = stats["read"] == stats["loaded"] + stats["quarantined"]
    dq("reconciliation", "ERROR", ok, detail=f"read={stats['read']} loaded={stats['loaded']} "
                                             f"quarantined={stats['quarantined']}")
    if not ok:
        con.commit()
        return finish("FAILED", "RECONCILIATION_FAILED")

    # reject threshold (expected pre-series blanks don't count)
    hard = sum(n for c, n in by_reason.items() if c != "BEFORE_SERIES_START")
    pct = 100.0 * hard / max(stats["read"], 1)
    held = pct > MAX_REJECT_PCT
    dq("reject_rate", "ERROR", not held, hard, f"{pct:.1f}% hard rejects (limit {MAX_REJECT_PCT}%)")
    if held:
        stats["loaded"] = 0
        con.commit()
        return finish("FAILED", f"BATCH_HELD: {pct:.1f}% rejects")

    # load (idempotent upsert) + freshness
    con.executemany("""INSERT INTO stg_cash_rate (obs_date, rate_pct, run_id, raw_file) VALUES (?,?,?,?)
                       ON CONFLICT(obs_date) DO UPDATE SET rate_pct=excluded.rate_pct,
                       run_id=excluded.run_id, raw_file=excluded.raw_file""",
                    [(d, r, run_id, str(raw_path)) for d, r in good])
    if good:
        latest = max(d for d, _ in good)
        age = business_days_between(date.fromisoformat(latest), date.today())
        dq("freshness", "WARN", age <= FRESH_WARN_BDAYS,
           detail=f"latest obs {latest}, {age} business days old (limit {FRESH_WARN_BDAYS})")
    con.commit()
    return finish("SUCCESS")


def business_days_between(start, end):
    """Weekdays after `start` up to and including `end` (public holidays ignored)."""
    days, d = 0, start
    while d < end:
        d = date.fromordinal(d.toordinal() + 1)
        days += d.weekday() < 5
    return days


def report(con):
    print("\n== pipeline_runs ==")
    for r in con.execute("""SELECT run_id, status, rows_read, rows_loaded, rows_quarantined, error
                            FROM pipeline_runs ORDER BY run_id"""):
        print(r)
    print("\n== dq_results (FAIL only) ==")
    for r in con.execute("""SELECT run_id, check_name, severity, failed_count, detail FROM dq_results
                            WHERE status='FAIL' ORDER BY dq_id"""):
        print(r)
    print("\n== quarantine by reason ==")
    for r in con.execute("""SELECT run_id, reason_code, COUNT(*) FROM quarantine
                            GROUP BY run_id, reason_code ORDER BY run_id"""):
        print(r)
    n, lo, hi = con.execute("SELECT COUNT(*), MIN(obs_date), MAX(obs_date) FROM stg_cash_rate").fetchone()
    print(f"\n== stg_cash_rate == {n} rows, {lo} .. {hi}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--file", help="replay a local CSV instead of downloading")
    ap.add_argument("--url", default=SOURCE_URL)
    ap.add_argument("--report", action="store_true", help="print the run/DQ log and exit")
    args = ap.parse_args()
    con = connect()
    if args.report:
        report(con)
        return 0
    if args.file:
        status = run(con, lambda: Path(args.file).read_bytes(), origin=f"file:{args.file}")
    else:
        status = run(con, lambda: fetch(args.url), origin=args.url)
    return 0 if status == "SUCCESS" else 1      # non-zero exit = scheduler alert


if __name__ == "__main__":
    sys.exit(main())
