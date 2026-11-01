# RBA cash-rate ingest — proof of concept

One source (RBA table F1, Interbank Overnight Cash Rate) end to end:
fetch with retries -> immutable raw landing -> header contract check ->
row validation -> staging + quarantine -> run and DQ log (SQLite).
Python 3.9+, standard library only.

## Run
    ./demo.sh                                   # all 6 scenarios on a fresh DB
    python3 rba_pipeline.py                     # live download from rba.gov.au
    python3 rba_pipeline.py --file some.csv     # replay a raw/local file
    python3 rba_pipeline.py --report            # print runs, DQ failures, quarantine

Exit code is 0 on SUCCESS and 1 on FAILED, so cron / Airflow can alert on it.

## Schedule (cron, 18:30 Sydney, Mon-Fri)
    CRON_TZ=Australia/Sydney
    30 18 * * 1-5  cd /path/to/poc && python3 rba_pipeline.py >> logs/rba.log 2>&1

## Layout
    data/raw/rba_f1/ingest_date=YYYY-MM-DD/   raw CSV + .manifest.json (never modified)
    data/pipeline.db                          pipeline_runs, dq_results, quarantine, stg_cash_rate

## Useful queries (sqlite3 data/pipeline.db)
    SELECT reason_code, COUNT(*) FROM quarantine GROUP BY reason_code;
    SELECT * FROM quarantine WHERE run_id = 3;
    SELECT run_id, status, rows_read, rows_loaded, rows_quarantined FROM pipeline_runs;

## Before the first live run
Open the live F1 CSV and confirm (a) the URL in SOURCE_URL and (b) the exact
column title in TARGET_TITLE. If either differs, the run fails with
INGEST_FAILED or SCHEMA_CHANGED instead of loading the wrong column — that is
the contract check doing its job. If F1 holds a different series than expected,
the table list is at rba.gov.au/statistics/tables/.
