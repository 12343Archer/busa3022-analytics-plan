import numpy as np, pandas as pd

def align(asx, vix):
    """Pair each ASX session with the latest US VIX close strictly BEFORE it."""
    a = asx[["date","Close","Volume"]].sort_values("date")
    v = (vix.dropna(subset=["vix"])[["date","vix"]]
            .rename(columns={"date": "vix_source_date"})
            .sort_values("vix_source_date"))
    out = pd.merge_asof(a, v, left_on="date", right_on="vix_source_date",
                        direction="backward", allow_exact_matches=False)
    expected = out["date"] - pd.offsets.BDay(1)      # previous weekday
    out["vix_filled"] = out["vix_source_date"] != expected   # True = gap carried forward
    return out

def rolling_avg(s, window=20, min_periods=None):
    return s.rolling(window, min_periods=min_periods or window).mean()   # trailing: no look-ahead

def realised_vol(close, window=14):
    r = np.log(close).diff()
    return r.rolling(window).std() * np.sqrt(252)    # annualised
