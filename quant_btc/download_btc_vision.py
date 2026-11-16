#!/usr/bin/env python3
import calendar, concurrent.futures, datetime as dt, hashlib, json, os, pathlib, urllib.request, urllib.error, time, random
from collections import Counter
ROOT=pathlib.Path("/tmp/btc_vision")
ARCH=ROOT/"archives"
ARCH.mkdir(parents=True,exist_ok=True)
BASE="https://data.binance.vision/data/futures/um"
BEGIN=(2020,1)
END=(2026,10)
UTC=dt.timezone.utc
jobs=[]
for yy in range(2020,2027):
 for mm in range(1,13):
  if (yy,mm)<BEGIN or (yy,mm)>END: continue
  ymo=f"{yy}-{mm:02d}"
  for intv in ("1h","4h","1d"):
   stem=f"BTCUSDT-{intv}-{ymo}.zip"
   u=f"{BASE}/monthly/klines/BTCUSDT/{intv}/{stem}"
   jobs.append((f"klines/{intv}",stem,u))
  stem=f"BTCUSDT-fundingRate-{ymo}.zip"
  jobs.append(("funding",stem,f"{BASE}/monthly/fundingRate/BTCUSDT/{stem}"))
for dd in range(1,16):
 for intv in ("1h","4h","1d"):
  stem=f"BTCUSDT-{intv}-2026-11-{dd:02d}.zip"
  jobs.append((f"klines/{intv}",stem,f"{BASE}/daily/klines/BTCUSDT/{intv}/{stem}"))
def fetch(u,timeout=20):
 for attempt in range(3):
  try:
   with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"BTC-Vision-Quant-Research/1.0"}),timeout=timeout) as r:
    return r.read()
  except urllib.error.HTTPError as exc:
   if exc.code in (403,404,451): raise
   if attempt==2: raise
   time.sleep(.3*(attempt+1))
  except Exception:
   if attempt==2: raise
   time.sleep(.3*(attempt+1))
def handle(job):
 typ,stem,url=job
 dest=ARCH/typ/stem
 dest.parent.mkdir(parents=True,exist_ok=True)
 try:
  checksum_bytes=fetch(url+".CHECKSUM")
  checksum=checksum_bytes.decode().split()[0].lower()
  if not (len(checksum)==64 and all(c in "0123456789abcdef" for c in checksum)): raise ValueError("bad checksum record")
  if dest.exists():
   contents=dest.read_bytes()
  else:
   contents=fetch(url)
  got=hashlib.sha256(contents).hexdigest()
  if got!=checksum: raise ValueError("sha256 mismatch")
  if not dest.exists(): dest.write_bytes(contents)
  return {"type":typ,"file":stem,"status":"verified","bytes":len(contents),"sha256":got,"url":url}
 except urllib.error.HTTPError as e: return {"type":typ,"file":stem,"status":"missing" if e.code==404 else "failed","error":f"HTTP {e.code}","url":url}
 except Exception as e: return {"type":typ,"file":stem,"status":"failed","error":str(e)[:200],"url":url}
start=time.time()
results=[]
with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
 for k,item in enumerate(pool.map(handle,jobs),1):
  results.append(item)
  if k%70==0: print(f"PROGRESS {k}/{len(jobs)} elapsed={round(time.time()-start,1)}s",flush=True)
( ROOT/"manifest.json").write_text(json.dumps({"source":"Binance Vision official","target":"BTCUSDT USD-M Futures","utc_downloaded":dt.datetime.now(UTC).isoformat(),"files":results},indent=2))
status=Counter(r["status"] for r in results)
bytype={t:Counter(r["status"] for r in results if r["type"]==t) for t in sorted(set(r["type"] for r in results))}
print(json.dumps({"total":len(jobs),"status":dict(status),"by_type":{k:dict(v) for k,v in bytype.items()},"failed": [{k:x[k] for k in ["file","error"] if k in x} for x in results if x["status"]!="verified"][:35],"seconds":round(time.time()-start,2)},ensure_ascii=False))
