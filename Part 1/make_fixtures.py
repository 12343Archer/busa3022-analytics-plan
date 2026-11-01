"""Build test CSVs shaped like the RBA F1 table (metadata header + data rows)."""
from datetime import date, timedelta
from pathlib import Path

HEADER = """F1 INTEREST RATES AND YIELDS – MONEY MARKET,,
Title,Cash Rate Target,{title}
Description,Cash Rate Target,Interbank Overnight Cash Rate
Frequency,Daily,Daily
Type,Original,Original
Units,Per cent,Per cent
,,
,,
Source,RBA,RBA
Publication date,{pub},{pub}
Series ID,FIRMMCRTD,FIRMMCRID
"""

def bdays(start, n):
    d, out = start, []
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out

def write(name, rows, title="Interbank Overnight Cash Rate"):
    pub = date.today().strftime("%d-%b-%Y")
    body = HEADER.format(title=title, pub=pub) + "\n".join(rows) + "\n"
    Path("fixtures", name).write_bytes(body.encode("cp1252"))   # RBA-style encoding

# 120 business days ending yesterday (or the last weekday before it)
end, days = date.today() - timedelta(days=1), []
while len(days) < 120:
    if end.weekday() < 5:
        days.insert(0, end)
    end -= timedelta(days=1)
fmt = lambda d: d.strftime("%d-%b-%Y")
clean = [f"{fmt(d)},3.60,{3.60 if i < 60 else 3.85:.2f}" for i, d in enumerate(days)]

# 1. clean file, with 5 leading blanks before the series starts
good = [f"{fmt(d)},3.60," for d in bdays(days[0] - timedelta(days=10), 5)] + clean
write("f1_good.csv", good)

# 2. dirty file: a few bad rows mixed into otherwise good data
dirty = list(clean)
dirty[10] = f"{fmt(days[10])},3.60,"            # MISSING_VALUE
dirty[20] = f"{fmt(days[20])},3.60,n/a"         # NON_NUMERIC
dirty[30] = f"{fmt(days[30])},3.60,385"         # OUT_OF_RANGE (missing decimal point)
dirty[40] = "31-Feb-2026,3.60,3.60"             # BAD_DATE
dirty.append(dirty[50])                         # DUPLICATE_DATE
write("f1_dirty.csv", dirty)

# 3. schema change: the RBA renames the column
write("f1_schema_changed.csv", clean, title="Interbank Overnight Cash Rate (%)")

# 4. format drift: values suddenly published as "3.60%" -> most rows reject
write("f1_format_drift.csv", [r + "%" for r in clean])
print("fixtures written:", sorted(p.name for p in Path("fixtures").iterdir()))
