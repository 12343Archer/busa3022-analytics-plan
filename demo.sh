#!/usr/bin/env bash
# Runs every scenario against a fresh database and prints the audit trail.
cd "$(dirname "$0")"
rm -rf data && python3 make_fixtures.py
scenario() {
  echo; echo "### $1"
  python3 rba_pipeline.py "${@:2}" | grep -E '"(raw_landed|raw_duplicate|fetch_retry|run_end)"'
  echo "exit code: ${PIPESTATUS[0]}"
}
scenario "1. clean file"                       --file fixtures/f1_good.csv
scenario "2. same file again (idempotent)"     --file fixtures/f1_good.csv
scenario "3. dirty rows -> quarantine"         --file fixtures/f1_dirty.csv
scenario "4. schema change -> fail fast"       --file fixtures/f1_schema_changed.csv
scenario "5. format drift -> batch held"       --file fixtures/f1_format_drift.csv
scenario "6. live RBA download"
python3 rba_pipeline.py --report
