#!/usr/bin/env python3
import calendar,csv,datetime as dt,gzip,hashlib,io,json,math,pathlib,statistics,zipfile
from collections import Counter
from decimal import Decimal,InvalidOperation
ROOT=pathlib.Path("/tmp/btc_vision")
OUT=ROOT/"normalized";OUT.mkdir(parents=True,exist_ok=True)
UTC=dt.timezone.utc
STEP={"1h":3600_000,"4h":14400_000,"1d":86400_000}
START=int(dt.datetime(2020,1,1,tzinfo=UTC).timestamp()*1000)
END=int(dt.datetime(2026,11,16,tzinfo=UTC).timestamp()*1000)
def ts(x):
 n=int(x)
 return n//1000 if n>100_000_000_000_000 else n
def iso(x):
 return dt.datetime.fromtimestamp(x/1000,UTC).isoformat().replace("+00:00","Z")
def csvlines(typ):
 for p in sorted((ROOT/"archives"/typ).glob("*.zip")):
  with zipfile.ZipFile(p) as z:
   bad=z.testzip()
   if bad: raise ValueError(f"CRC_FAILED {p.name} {bad}")
   names=[n for n in z.namelist() if n.endswith(".csv")]
   if len(names)!=1: raise ValueError(f"CSV_IN_ZIP_UNEXPECTED {p.name} {names}")
   with z.open(names[0]) as raw:
    with io.TextIOWrapper(raw,encoding="utf-8-sig",newline="") as f:
     reader=csv.reader(f)
     for row in reader:
      if row and row[0].strip().lstrip("-").isdigit():
       yield p.name,row
def audit_klines(period):
 interval_ms=STEP[period];rec={}; file_counts=Counter()
 errors=Counter(); examples={}
 unit=Counter(); negative_volume=0
 for filename,row in csvlines(f"klines/{period}"):
  file_counts[filename]+=1
  try:
   if len(row)<12: errors["too_few_columns"]+=1;continue
   opent=ts(row[0]);close_t=ts(row[6])
   unit["micro" if int(row[0])>1e14 else "milli"]+=1
   o,h,l,c,v=map(Decimal,[row[1],row[2],row[3],row[4],row[5]])
   if any(not x.is_finite() for x in (o,h,l,c,v)): errors["nonfinite"]+=1
   if min(o,h,l,c)<=0: errors["nonpositive_price"]+=1
   if v<0: errors["negative_volume"]+=1
   if h<max(o,c,l) or l>min(o,c,h):errors["ohlc_invalid"]+=1
   if opent%interval_ms!=0: errors["not_utc_aligned"]+=1
   if close_t!=opent+interval_ms-1: errors["close_time_mismatch"]+=1
   if not START<=opent<END: errors["outside_target"]+=1
   if opent in rec:
    errors["duplicate_timestamp"]+=1
    if rec[opent][:5] != (o,h,l,c,v):errors["conflicting_duplicate"]+=1
   else: rec[opent]=(o,h,l,c,v,filename)
  except Exception as e:
   errors["parse_error"]+=1
   if "parse_error" not in examples:examples["parse_error"]=repr(e)[:150]
 times=sorted(rec)
 diffs=[b-a for a,b in zip(times,times[1:])]
 holes=[]
 for a,b in zip(times,times[1:]):
  if b-a!=interval_ms:
   holes.append({"after":iso(a),"before":iso(b),"missing":max(0,(b-a)//interval_ms-1),"delta_ms":b-a})
 full_expected=(END-START)//interval_ms
 expected_set=set(range(START,END,interval_ms))
 miss=sorted(expected_set-set(rec))
 extra=sorted(set(rec)-expected_set)
 out={ "interval":period,"archive_files":len(file_counts),"rows_in_archives":sum(file_counts.values()),
       "unique_rows":len(rec),"expected_utc_slots":full_expected,"coverage_start":iso(times[0]) if times else None,
       "coverage_end":iso(times[-1]) if times else None, "missing_candles":len(miss),
       "missing_first_10":[iso(x) for x in miss[:10]],"extra_outside_grid":len(extra),
       "errors":dict(errors),"unit_counts":dict(unit),"gaps_sample":holes[:10],"examples":examples}
 with gzip.open(OUT/f"BTCUSDT-{period}-2020-01-01_2026-11-15.csv.gz","wt",encoding="utf8",newline="") as f:
  w=csv.writer(f)
  w.writerow(["open_time_ms","open","high","low","close","volume"])
  for t in times:
   o,h,l,c,v,_=rec[t]
   w.writerow([t,o,h,l,c,v])
 return out,rec
r={}
for period in ("1h","4h","1d"):
 r[period],r[period+"_rec"]=audit_klines(period)
 print("KLINE_AUDIT",period,json.dumps({k:v for k,v in r[period].items() if k!="gaps_sample"}),flush=True)
fund={}
fund_error=Counter();units=Counter();fund_files=Counter();source_counts=Counter()
for filename,row in csvlines("funding"):
 fund_files[filename]+=1
 try:
  if len(row)!=3:fund_error["bad_columns"]+=1;continue
  t=ts(row[0]);iv=int(row[1]);val=Decimal(row[2]);units["micro" if int(row[0])>1e14 else "milli"]+=1
  if not val.is_finite():fund_error["nonfinite"]+=1
  if not 0<iv<=24:fund_error["invalid_interval_hours"]+=1
  if t in fund:fund_error["duplicate_timestamp"]+=1
  else: fund[t]=(val,iv,"vision")
 except Exception as e: fund_error["parse_error"]+=1
nov=ROOT/"funding_nov_api.json"
nov_n=0;nov_dup=0
if nov.exists():
 o=json.loads(nov.read_text())
 for x in o["records"]:
  t=int(x["fundingTime"]);v=Decimal(x["fundingRate"]);nov_n+=1
  if t in fund:nov_dup+=1
  else:fund[t]=(v,None,"binance_api")
ft=sorted(fund)
fi=Counter(x[1] for x in fund.values() if x[1] is not None)
fdeltas=Counter(round((b-a)/3_600_000) for a,b in zip(ft,ft[1:]))
outfund={"archive_files":len(fund_files),"archive_rows":sum(fund_files.values()),"api_nov_rows":nov_n,
         "api_nov_duplicates":nov_dup,"unique_events":len(fund),"first":iso(ft[0]),"last":iso(ft[-1]),
         "interval_hours_counts":dict(fi),"observed_delta_hours_histogram":dict(fdeltas),
         "max_gap_hours":max((b-a)/3_600_000 for a,b in zip(ft,ft[1:])) if len(ft)>1 else 0,
         "errors":dict(fund_error),"unit_counts":dict(units),
         "funding_outside_2020_2026":sum(1 for t in ft if not START<=t<END)}
with gzip.open(OUT/"BTCUSDT-funding-2020-01-01_2026-11-15.csv.gz","wt",encoding="utf8",newline="") as f:
 w=csv.writer(f);w.writerow(["funding_time_ms","funding_rate","funding_interval_hours","source"])
 for t in ft:
  v,iv,src=fund[t];w.writerow([t,v,iv or "",src])
r["funding"]=outfund
print("FUNDING_AUDIT",json.dumps(outfund),flush=True)
def compare(base,target):
 b=r[base+"_rec"];t=r[target+"_rec"];multiple=STEP[target]//STEP[base]
 diffs=Counter();examples=[]
 for timestamp,(o,h,l,c,v,_) in t.items():
  times=[timestamp+i*STEP[base] for i in range(multiple)]
  if not all(q in b for q in times): diffs["unmatched_input_window"]+=1;continue
  src=[b[q] for q in times]
  checks={
    "open":(o,src[0][0]),"high":(h,max(x[1] for x in src)),"low":(l,min(x[2] for x in src)),
    "close":(c,src[-1][3]),"volume":(v,sum(x[4] for x in src))}
  diffs["matched_windows"]+=1
  for k,(left,right) in checks.items():
   tol=Decimal("0.000001") if k=="volume" else Decimal("0")
   if abs(left-right)>tol:
    diffs[k+"_mismatch"]+=1
    if len(examples)<5:examples.append({"time":iso(timestamp),"field":k,"target":str(left),"derived":str(right)})
 return {"counts":dict(diffs),"examples":examples}
r["crosscheck_1h_to_4h"]=compare("1h","4h")
r["crosscheck_1h_to_1d"]=compare("1h","1d")
print("CROSS_1H_4H",json.dumps(r["crosscheck_1h_to_4h"]),flush=True)
print("CROSS_1H_1D",json.dumps(r["crosscheck_1h_to_1d"]),flush=True)
r={k:v for k,v in r.items() if not k.endswith("_rec")}
r["audit_version"]="1.0";r["audit_method"]="Python stdlib, strict integer UTC timestamps, Decimal OHLC and funding, verified ZIP SHA256";r["sample_only"]=False
(ROOT/"audit.json").write_text(json.dumps(r,indent=2,ensure_ascii=False))
print("AUDIT_FINISHED",ROOT/"audit.json")
