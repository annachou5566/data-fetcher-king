#!/usr/bin/env python3
"""Package previously downloaded and audited data only; never fetch or trade."""
from pathlib import Path
import zipfile
ROOT=Path('/tmp/btc_vision')
OUT=ROOT/'BTCUSDT_BINANCE_VISION_AUDITED_20261116.zip'
FILES=[ROOT/'audit.json',ROOT/'manifest.json',ROOT/'funding_nov_api.json']+sorted((ROOT/'normalized').glob('*.gz'))
for p in FILES:
    if not p.is_file():raise SystemExit(f'missing verified data: {p}')
with zipfile.ZipFile(OUT,'w',compression=zipfile.ZIP_DEFLATED,compresslevel=6) as z:
    for p in FILES:z.write(p,p.relative_to(ROOT))
print(OUT)
