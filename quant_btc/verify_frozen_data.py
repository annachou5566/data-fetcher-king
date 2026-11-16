#!/usr/bin/env python3
"""Fail closed if re-fetched archive SHA256 differs from frozen research data."""
import json,sys
from pathlib import Path
f=json.loads(Path('quant_btc/FROZEN_MANIFEST_SHA256.json').read_text())
c=json.loads(Path('/tmp/btc_vision/manifest.json').read_text())
b={x['type']+'/'+x['file']:x.get('sha256') for x in c['files'] if x['status']=='verified'}
errors=[k for k,v in f.items() if b.get(k)!=v]
if set(f)!=set(b):errors.append('file keys changed')
if errors:print('FROZEN_SOURCE_CHANGED',errors[:20],file=sys.stderr);sys.exit(24)
print('FROZEN_DATA_HASHES_MATCH',len(f))
