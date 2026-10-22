import sys; sys.path.append("src")
import numpy as np, pandas as pd
from curate import rolling_avg, realised_vol

def test_rolling_avg():
    out = rolling_avg(pd.Series([10,20,30,40,50]), 3)
    assert out.isna().sum() == 2
    assert list(out.dropna()) == [20, 30, 40]

def test_rolling_avg_tolerates_missing():
    out = rolling_avg(pd.Series([10, np.nan, 30, 40]), 3, min_periods=2)
    assert out.iloc[2] == 20          # mean of 10 and 30, NaN ignored

def test_realised_vol():
    close = pd.Series([100, 110, 99, 108.9])
    got = realised_vol(close, 3).iloc[-1]
    exp = np.std([np.log(1.1), np.log(0.9), np.log(1.1)], ddof=1) * np.sqrt(252)
    assert abs(got - exp) < 1e-9
