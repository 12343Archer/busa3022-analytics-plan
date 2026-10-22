import time, io, glob, logging
from datetime import datetime, timezone
import pandas as pd, requests, yfinance as yf

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("logs/pipeline.log")])
log = logging.getLogger("ingest")

FRED_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=VIXCLS"

def with_retry(fn, attempts=3, base_wait=2):
    """Retry with exponential backoff: waits 2s, 4s, ..."""
    for i in range(1, attempts + 1):
        try:
            return fn()
        except Exception as e:
            log.warning("attempt %d/%d failed: %s", i, attempts, e)
            if i == attempts:
                raise
            time.sleep(base_wait * 2 ** (i - 1))

def fetch_asx():
    def _get():
        df = yf.download("^AXJO", period="1y", interval="1d",
                         auto_adjust=False, progress=False)
        if df is None or df.empty:
            raise ValueError("Yahoo returned no data")
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        idx = pd.DatetimeIndex(df.index)
        if idx.tz is not None:              # keep the local (Sydney) trading date
            idx = idx.tz_localize(None)
        df.index = idx.normalize()
        df.index.name = "date"
        return df
    return with_retry(_get)

def fetch_vix(url=FRED_URL):
    def _get():
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        return r.text                       # keep raw text untouched
    return with_retry(_get)

def latest_raw(prefix):
    files = sorted(glob.glob(f"data/raw/{prefix}_*.csv"))
    return files[-1] if files else None

def run(vix_url=FRED_URL):
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    paths = {}
    try:
        fetch_asx().to_csv(f"data/raw/asx200_{ts}.csv")
        paths["asx"] = f"data/raw/asx200_{ts}.csv"
        log.info("ASX200 ingested OK")
    except Exception as e:
        paths["asx"] = latest_raw("asx200")
        log.error("ASX200 FAILED (%s). Falling back to %s", e, paths["asx"])
    try:
        with open(f"data/raw/vix_{ts}.csv", "w") as f:
            f.write(fetch_vix(vix_url))
        paths["vix"] = f"data/raw/vix_{ts}.csv"
        log.info("VIX ingested OK")
    except Exception as e:
        paths["vix"] = latest_raw("vix")
        log.error("VIX FAILED (%s). Falling back to %s", e, paths["vix"])
    return ts, paths
