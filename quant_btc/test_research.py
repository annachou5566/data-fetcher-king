#!/usr/bin/env python3
"""No blind access: unit & train-only event-ordering invariants."""
import os,sys
from pathlib import Path
import numpy as np
from research import load_data,specs,signals,run,metrics,segment_idx,SLIP,STRESS,EQUITY

def main():
 p=os.environ.get('DATASET_ZIP','/tmp/btc_vision/BTCUSDT_BINANCE_VISION_AUDITED_20261116.zip')
 d=load_data(p)
 left,right=segment_idx(d,'train')
 for spec in specs():
  signal=signals(d,spec)
  # A future suffix alteration cannot change signal decisions from the past.
  cut=left+1200
  m={k:v.copy() if hasattr(v,'copy') else v for k,v in d.items()}
  m['close'][cut+1:]+=123.456
  m['high'][cut+1:]+=123.456
  m['low'][cut+1:]+=123.456
  m['funding'][cut+1:]+=.0004
  assert np.array_equal(signal[:cut+1],signals(m,spec)[:cut+1]),('lookahead',spec['id'])
  result=run(d,spec,'train')
  assert not result['risk_violations'],spec['id']
  assert all(left<=t['entry']<right and left<=t['exit']<right for t in result['trades'])
  assert all(t['exit']>=t['entry'] for t in result['trades'])
  assert all(abs(t['entry_fill']-d['open'][t['entry']]*(1+t['side']*SLIP))<1e-7 for t in result['trades'])
  if spec['family']=='H2':
   assert all(t['entry']%24==0 for t in result['trades']),spec['id']
  print('PASS',spec['id'],'train_trades',len(result['trades']))
 print('PASS_NO_BLIND_ACCESS',len(specs()),'configs')
if __name__=='__main__':main()
