import os
import pandas as pd
from datetime import datetime, timezone

DQ_COLS = ["run_id","timestamp_utc","source","record_key",
           "check_name","severity","action","detail"]

def _e(run_id, source, key, check, sev, action, detail):
    return dict(run_id=run_id, timestamp_utc=datetime.now(timezone.utc).isoformat(),
                source=source, record_key=str(key), check_name=check,
                severity=sev, action=action, detail=detail)

def write_log(entries, path="logs/dq_log.csv"):
    df = pd.DataFrame(entries, columns=DQ_COLS)
    df.to_csv(path, mode="a", header=not os.path.exists(path), index=False)
    return df

def _apply(df, hard, soft, run_id, source):
    entries, bad = [], pd.Series(False, index=df.index)
    for name, mask in hard.items():
        for i in df.index[mask]:
            entries.append(_e(run_id, source, df.loc[i, "date"].date(), name,
                              "error", "quarantined", "removed from curated; saved to quarantine"))
        bad |= mask
    for name, mask in soft.items():
        for i in df.index[mask]:
            entries.append(_e(run_id, source, df.loc[i, "date"].date(), name,
                              "warning", "flagged", "kept in curated"))
    return df[~bad], df[bad], entries

def validate_asx(df, run_id):
    df = df.reset_index()
    hard = {
        "duplicate_date": df["date"].duplicated(),
        "missing_close": df["Close"].isna(),
        "non_positive_close": df["Close"] <= 0,
        "high_below_low": df["High"] < df["Low"],
    }
    soft = {
        "zero_or_missing_volume": df["Volume"].fillna(0) == 0,
        "big_daily_move_over_7pct": df["Close"].pct_change().abs() > 0.07,
        "calendar_gap_over_5d": df["date"].diff().dt.days > 5,
        "repeated_volume": (df["Volume"] == df["Volume"].shift(1)) & (df["Volume"] > 0),
    }
    return _apply(df, hard, soft, run_id, "asx200")

def validate_vix(path, run_id):
    df = pd.read_csv(path)
    if list(df.columns) not in (["observation_date","VIXCLS"], ["DATE","VIXCLS"]):
        write_log([_e(run_id, "fred_vixcls", "(file)", "schema_change", "error",
                      "halted", f"unexpected columns {list(df.columns)}")])
        raise ValueError(f"FRED schema changed: {list(df.columns)}")
    df.columns = ["date", "vix_raw"]
    df["date"] = pd.to_datetime(df["date"])
    df["vix"] = pd.to_numeric(df["vix_raw"], errors="coerce")   # '.' or blank becomes NaN
    df = df[df["date"] >= pd.Timestamp.today() - pd.Timedelta(days=400)].reset_index(drop=True)
    hard = {"duplicate_date": df["date"].duplicated(),
            "non_positive_vix": df["vix"] <= 0}
    soft = {"missing_value_fred": df["vix"].isna()}
    clean, bad, entries = _apply(df, hard, soft, run_id, "fred_vixcls")
    if (pd.Timestamp.today() - df["date"].max()).days > 5:
        entries.append(_e(run_id, "fred_vixcls", df["date"].max().date(),
                          "stale_data", "warning", "flagged", "latest obs older than 5 days"))
    return clean.drop(columns="vix_raw"), bad, entries
