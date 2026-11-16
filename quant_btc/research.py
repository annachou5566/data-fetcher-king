#!/usr/bin/env python3
"""Preregistered offline BTCUSDT perpetual hypotheses, deterministic 1h next-open event simulator.
No network, no orders, no side effects beyond output files. Only Python standard + numpy + pandas.
"""
from __future__ import annotations
import argparse, csv, gzip, hashlib, io, json, math, os, random, statistics, sys, zipfile
from dataclasses import dataclass
from pathlib import Path
from collections import defaultdict
import datetime as dt
import numpy as np
import pandas as pd

PREREG_SHA='52733a19faf42b6296808f77fe976642b34d30a1d84db06204f4a5193272268c'
FEE=.0005
SLIP=.0003
STRESS=.0006
EQUITY=10000.
RISK=.005
CAP=3.
BAR_MS=3600_000
SPLITS={
 'train':('2020-01-01T00:00:00Z','2024-01-01T00:00:00Z'),
 'validation':('2024-01-01T00:00:00Z','2025-01-01T00:00:00Z'),
 'blind':('2025-01-01T00:00:00Z','2026-11-16T00:00:00Z')}
MASK_WINDOWS=[
 ('2022-07-16T00:00:00Z','2022-07-16T08:00:00Z'),
 ('2023-08-16T00:00:00Z','2023-08-17T00:00:00Z'),
 ('2023-11-10T12:00:00Z','2023-11-10T16:00:00Z'),
 ('2024-10-28T20:00:00Z','2024-10-29T00:00:00Z')]

def tm(s):return int(pd.Timestamp(s).timestamp()*1000)
def utc(t):return dt.datetime.fromtimestamp(t/1000,dt.timezone.utc).isoformat().replace('+00:00','Z')

def load_data(zip_path):
 with zipfile.ZipFile(zip_path) as z:
  names=z.namelist()
  def rawcsv(fragment):
   paths=[x for x in names if x.startswith('normalized/') and fragment in x and x.endswith('.gz')]
   if len(paths)!=1:raise RuntimeError('Need exactly one '+fragment+': '+repr(paths))
   with gzip.GzipFile(fileobj=io.BytesIO(z.read(paths[0]))) as f:
    return pd.read_csv(f)
  k=rawcsv('BTCUSDT-1h-');f=rawcsv('BTCUSDT-funding-')
  audit=json.loads(z.read('audit.json'))
  if audit['1h']['missing_candles'] or audit['1h']['errors'] or audit['1h']['unique_rows']!=len(k):raise RuntimeError('Kline QA failed')
  if audit['funding']['errors'] or audit['funding']['unique_events']!=len(f):raise RuntimeError('Funding QA failed')
  t=k.open_time_ms.to_numpy(np.int64)
  if len(np.unique(t))!=len(t) or not np.all(np.diff(t)==BAR_MS):raise RuntimeError('Unordered/duplicate/discontinuous data')
  dat={'t':t,'open':k.open.to_numpy(float),'high':k.high.to_numpy(float),'low':k.low.to_numpy(float),
       'close':k.close.to_numpy(float),'volume':k.volume.to_numpy(float)}
  n=len(t)
  funding=np.zeros(n)
  fidx=((f.funding_time_ms.to_numpy(np.int64)-t[0])//BAR_MS).astype(int)
  for i,v in zip(fidx,f.funding_rate.to_numpy(float)):
   if i<0 or i>=n:raise RuntimeError('funding outside candles')
   funding[i]+=v
  dat['funding']=funding
  masks=np.zeros(n,bool)
  for lo,hi in MASK_WINDOWS:masks|=(t>=tm(lo))&(t<tm(hi))
  # Prohibit entries one bar before and within suspect intervals so existing positions can flatten at an earlier valid OPEN.
  dat['mask']=masks
  s=pd.Series(dat['close']); h=pd.Series(dat['high']);l=pd.Series(dat['low'])
  prev=s.shift(1)
  tr=pd.concat([h-l,(h-prev).abs(),(l-prev).abs()],axis=1).max(axis=1)
  dat['atr']=tr.rolling(20,min_periods=20).mean().to_numpy(float)
  return dat

def specs():
 out=[]
 for N in (24,48,96):
  for A in (2.,3.):out.append({'id':f'H1_N{N}_A{int(A)}','family':'H1','lookback':N,'atr_mult':A})
 for L in (48,96,168):
  for A in (2.,3.):out.append({'id':f'H2_L{L}_A{int(A)}','family':'H2','lookback':L,'atr_mult':A})
 for threshold_bp in (1,2,3):
  for horizon in (8,16):out.append({'id':f'H3_F{threshold_bp}_H{horizon}','family':'H3','threshold':threshold_bp*.0001,'horizon':horizon,'atr_mult':2.5})
 return out

def signals(dat,spec):
 close=dat['close'];high=dat['high'];low=dat['low'];n=len(close)
 sig=np.zeros(n,dtype=np.int8)
 fam=spec['family']
 if fam=='H1':
  N=spec['lookback']
  up=pd.Series(high).shift(1).rolling(N,min_periods=N).max().to_numpy(float)
  dn=pd.Series(low).shift(1).rolling(N,min_periods=N).min().to_numpy(float)
  upnow=close>up;dnnow=close<dn
  sig[1:][upnow[1:]&~upnow[:-1]]=1
  sig[1:][dnnow[1:]&~dnnow[:-1]]=-1
 elif fam=='H2':
  L=spec['lookback']
  for i in range(L,n):
   if i%24==23:
    sig[i]=1 if close[i]>close[i-L] else -1 if close[i]<close[i-L] else 0
 elif fam=='H3':
  events=np.flatnonzero(dat['funding'])
  # Event at start of hour j (possibly few milliseconds after); signal at close j+1; execute open j+2.
  for j in events:
   i=j+1
   if i>=n:continue
   rate=dat['funding'][j]
   if rate>=spec['threshold']:sig[i]=-1
   if rate<=-spec['threshold']:sig[i]=1
 else:raise ValueError(fam)
 return sig

@dataclass
class Pos:
 side:int
 qty:float
 entry_idx:int
 entry_fill:float
 entry_fee:float
 entry_equity:float
 stop:float
 risk_amt:float
 atr_entry:float
 funding_pnl:float=0.

def worst_unit_loss(mid,stop,side,slip=STRESS):
 e=mid*(1+side*slip);x=stop*(1-side*slip)
 return max(0.,side*(e-x))+FEE*(e+x)

def exit_trade(pos,mid,at,slippage,cash,trades,reason):
 x=mid*(1-pos.side*slippage)
 fee=abs(x*pos.qty)*FEE
 price_pnl=pos.side*pos.qty*(x-pos.entry_fill)
 cash+=price_pnl-fee
 pnl=price_pnl-pos.entry_fee-fee+pos.funding_pnl
 trades.append({'entry':pos.entry_idx,'exit':at,'side':pos.side,'qty':pos.qty,
  'entry_fill':pos.entry_fill,'exit_fill':x,'fees':pos.entry_fee+fee,'funding':pos.funding_pnl,
  'pnl':pnl,'r':pnl/pos.risk_amt if pos.risk_amt>0 else math.nan,'reason':reason,
  'duration_h':at-pos.entry_idx,'initial_risk':pos.risk_amt})
 return cash

def segment_idx(dat,name):
 lo,hi=SPLITS[name];ts=dat['t'];return int(np.searchsorted(ts,tm(lo))),int(np.searchsorted(ts,tm(hi)))

def run(dat,spec,seg,slip=SLIP,sig=None,extra_mask_hours=0):
 if sig is None:sig=signals(dat,spec)
 t=dat['t'];op=dat['open'];hi=dat['high'];lo=dat['low'];cl=dat['close'];atr=dat['atr'];fund=dat['funding'];mask=dat['mask']
 left,right=segment_idx(dat,seg)
 cash=EQUITY;pos=None;trades=[];equity=[];fund_paid=0.;fees_paid=0.;risk_violations=0;blocks=0
 skip=mask.copy()
 if extra_mask_hours:
  for k in np.flatnonzero(mask):skip[k:min(len(skip),k+extra_mask_hours+1)]=True
 for i in range(left,right):
  if not math.isfinite(atr[i-1]) if i>0 else True:
   equity.append(cash);continue
  at_risk= bool(skip[i] or (i+1<len(skip) and skip[i+1]))
  # If last good bar directly precedes suspect data, flatten at its open, avoiding the suspect candle price.
  if at_risk:blocks+=1
  action=int(sig[i-1]) if i>0 else 0
  at_open_exit=False;why=''
  if pos:
   if at_risk:at_open_exit=True;why='data_mask'
   elif spec['family']=='H3' and (i-pos.entry_idx)>=spec['horizon']:
    at_open_exit=True;why='time_exit'
   elif spec['family']!='H3' and action and action!=pos.side:
    at_open_exit=True;why='opposite_signal'
  if at_open_exit:
   cash=exit_trade(pos,op[i],i,slip,cash,trades,why)
   fees_paid+=trades[-1]['fees']-pos.entry_fee
   pos=None
  if cash<=0:
   equity.append(max(0.,cash));continue
  if not at_risk and pos is None and action and math.isfinite(atr[i-1]) and atr[i-1]>0:
   # Only H1/H2 opposite signal can start replacement on same open. H3 settlements after previous hour known.
   side=action
   stop=op[i]-side*atr[i-1]*spec['atr_mult']
   if stop>0:
    unit_risk=worst_unit_loss(op[i],stop,side)
    qty=min(RISK*cash/unit_risk,CAP*cash/op[i]) if unit_risk>0 else 0.
    if qty>0:
     entry_fill=op[i]*(1+side*slip)
     fee=qty*entry_fill*FEE
     risk_amt=qty*unit_risk
     if risk_amt>cash*RISK*(1+1e-8) or qty*op[i]>CAP*cash*(1+1e-8):risk_violations+=1
     pos=Pos(side=side,qty=qty,entry_idx=i,entry_fill=entry_fill,entry_fee=fee,
              entry_equity=cash,stop=stop,risk_amt=risk_amt,atr_entry=atr[i-1])
     cash-=fee;fees_paid+=fee
  if pos:
   # Stop checks occur only after opening transaction; same-bar stop is allowed, no bar-close decisions.
   hit=(lo[i]<=pos.stop) if pos.side==1 else (hi[i]>=pos.stop)
   if hit:
    # Favorable funding cannot be received if possible intrabar stop came before settlement;
    # for conservatism, still charge adverse funding at this hour.
    x=-pos.side*pos.qty*op[i]*fund[i]
    if x<0:
     cash+=x;pos.funding_pnl+=x;fund_paid+=x
    stop_mid=min(op[i],pos.stop) if pos.side==1 else max(op[i],pos.stop)
    cash=exit_trade(pos,stop_mid,i,slip,cash,trades,'stop')
    fees_paid+=trades[-1]['fees']-pos.entry_fee
    pos=None
   else:
    x=-pos.side*pos.qty*op[i]*fund[i]
    cash+=x;fund_paid+=x;pos.funding_pnl+=x
    if spec['family']!='H3':
     # New trailing stop from *completed* bar i can only execute from i+1 onward.
     if pos.side==1:pos.stop=max(pos.stop,cl[i]-atr[i]*spec['atr_mult'])
     else:pos.stop=min(pos.stop,cl[i]+atr[i]*spec['atr_mult'])
  value=cash+(pos.side*pos.qty*(cl[i]-pos.entry_fill) if pos else 0.)
  equity.append(value)
 if pos:
  cash=exit_trade(pos,cl[right-1],right-1,slip,cash,trades,'segment_end')
  fees_paid+=trades[-1]['fees']-pos.entry_fee
  equity[-1]=cash
 return {'trades':trades,'equity':np.asarray(equity),'first':left,'last':right,
         'risk_violations':risk_violations,'fees_paid':fees_paid,'funding_paid':fund_paid,'masked_hours':blocks}

def metrics(result):
 trades=result['trades'];n=len(trades);eq=result['equity'];pnl=np.array([x['pnl'] for x in trades]);R=np.array([x['r'] for x in trades]);
 wins=float(pnl[pnl>0].sum());losses=float(-pnl[pnl<0].sum());
 rets=eq[1:]/np.maximum(eq[:-1],1e-9)-1 if len(eq)>1 else np.array([])
 dd=eq/np.maximum.accumulate(np.r_[EQUITY,eq])[1:]-1 if len(eq) else np.array([0.])
 sd=rets.std(ddof=1) if len(rets)>1 else 0.
 pf=(wins/losses if losses>0 else (math.inf if wins>0 else math.nan))
 sharpe=(float(rets.mean()/sd*math.sqrt(24*365)) if sd>0 else math.nan)
 out={'trades':n,'win_rate':float(np.mean(pnl>0)) if n else math.nan,
      'expectancy_R':float(np.mean(R)) if n else math.nan,
      'PF':pf,'sharpe':sharpe,'max_drawdown':float(-np.min(dd)),
      'total_return':float(eq[-1]/EQUITY-1) if len(eq) else math.nan,
      'net_pnl':float(pnl.sum()) if n else 0.,'fees':result['fees_paid'],
      'funding':result['funding_paid'],'masked_hours':result['masked_hours'],
      'risk_violations':result['risk_violations']}
 return out

def finite_json(val):
 if isinstance(val,(np.float64,np.float32,np.int64,np.int32)):return val.item()
 if isinstance(val,float) and not math.isfinite(val):return None
 if isinstance(val,dict):return {k:finite_json(v) for k,v in val.items()}
 if isinstance(val,list):return [finite_json(v) for v in val]
 return val

def qualifies_train(m):return m['trades']>=120 and m['expectancy_R']>0 and m['PF']>=1.05 and m['max_drawdown']<=.25

def qualifies_val(m):return m['trades']>=20 and m['expectancy_R']>0 and m['PF']>=1.10 and m['max_drawdown']<=.20

def perturb(spec,ratio):
 x=spec.copy()
 if spec['family']=='H3':x['threshold']=spec['threshold']*ratio
 else:x['lookback']=int(math.floor(spec['lookback']*ratio+.5))
 x['id']=spec['id']+('_minus20pct' if ratio<1 else '_plus20pct')
 return x

def audit_lookahead(dat,spec):
 sig=signals(dat,spec)
 i=1200
 manipulated={k:(v.copy() if isinstance(v,np.ndarray) else v) for k,v in dat.items()}
 manipulated['high'][i+10:]+=10000
 manipulated['close'][i+10:]*=.25
 manipulated['low'][i+10:]*=.5
 altered=signals(manipulated,spec)
 assert np.array_equal(sig[:i+10],altered[:i+10]),'LOOKAHEAD signal changed before modified future'
 # All events only use funding at prior hour; injecting funding in the future may not impact earlier time.
 if spec['family']=='H3':
  manipulated['funding'][i+10:]+=0.001
  altered=signals(manipulated,spec)
  assert np.array_equal(sig[:i+10],altered[:i+10]),'LOOKAHEAD funding'
 return True

def bootstrap_ci(r_list,B=1000,seed=20261116):
 rng=np.random.default_rng(seed);arr=np.asarray(r_list,float)
 if len(arr)<2:return [None,None]
 means=np.empty(B)
 for i in range(B):means[i]=rng.choice(arr,size=len(arr),replace=True).mean()
 return [float(np.quantile(means,.025)),float(np.quantile(means,.975))]

def buy_hold(dat,period,slip=SLIP):
 left,right=segment_idx(dat,period);a=dat['open'][left];b=dat['close'][right-1];v=1.0
 # 1x perpetual long open to last close, funding from official settled records, estimated settlement mark at hour open.
 feein=v*(1+slip)*FEE
 feeout=v*b/a*(1-slip)*FEE
 funding=-sum(dat['open'][left:right]/a*dat['funding'][left:right]);
 # Dedicated 1x notional, not a rebalanced investment.
 p=(b*(1-slip)-a*(1+slip))/a+funding-feein-feeout
 vals=EQUITY*(1+(dat['close'][left:right]-a*(1+slip))/a
               -feein-np.cumsum(dat['open'][left:right]/a*dat['funding'][left:right]))
 vals[-1]=EQUITY*(1+p)
 e={'trades':[{'pnl':p*EQUITY,'r':p*EQUITY/(EQUITY*.005)}],'equity':vals,'fees_paid':EQUITY*(feein+feeout),'funding_paid':EQUITY*funding,'masked_hours':0,'risk_violations':0}
 m=metrics(e);m['trades']=1;return m,vals

def rand_metrics_from_fixed_schedule(dat, period, source_trades, champion, num=1000, seed=20261116, shift_only=False, base_slip=SLIP):
 """Two cost/financing/ATR-stop matched statistical null models.
 Schedules precommit original max trade durations (stop can exit early). No hindsight
 signal selection. Nulls are diagnostic controls, not automatically executable strategies.
 """
 rng=np.random.default_rng(seed)
 left,right=segment_idx(dat,period);n=right-left
 durations=[max(1,int(x['duration_h'])) for x in source_trades]
 sides=[int(x['side']) for x in source_trades]
 count=len(durations)
 a=dat['open'];h=dat['high'];l=dat['low'];cl=dat['close'];f=dat['funding'];atr=dat['atr'];mask=dat['mask']
 out=[];failed=0
 for k in range(num):
  if shift_only:
   # Fixed realised trade schedule, rotated by an exogenous strictly nonzero hourly offset.
   offset=int(rng.integers(1,n))
   sched=sorted([((int(x['entry'])-left+offset)%n, max(1,int(x['duration_h'])),int(x['side'])) for x in source_trades])
  else:
   # Random composition of free hours; exact planned entry count/durations and random sides.
   order=rng.permutation(count)
   dur=[durations[j] for j in order]
   direction=rng.choice([-1,1],size=count)
   available=n-sum(dur)-count
   if available<=0:failed+=1;continue
   cuts=np.sort(rng.choice(available+count,count,replace=False))
   gaps=np.diff(np.r_[-1,cuts,available+count])-1
   sched=[];q=int(gaps[0])
   for j,d in enumerate(dur):
    sched.append((q,d,int(direction[j])));q+=d+int(gaps[j+1])
  cash=EQUITY;curve=np.zeros(n);pnl=[];last=0;executed=0;overlaps=0;masked=0
  for start,duration,side in sched:
   if start<last:
    overlaps+=1;continue
   if start>=n or start+duration>=n:
    overlaps+=1;continue
   j=left+start
   if mask[j] or not math.isfinite(atr[j-1]) or atr[j-1]<=0:
    masked+=1;continue
   curve[last:start]=cash
   stop=a[j]-side*atr[j-1]*champion['atr_mult']
   if stop<=0:continue
   loss=worst_unit_loss(a[j],stop,side)
   if loss<=0:continue
   qty=min(RISK*cash/loss,CAP*cash/a[j]);entry=a[j]*(1+side*base_slip);entryfee=qty*entry*FEE
   cash-=entryfee;fundpnl=0.;xmid=None;out_idx=j+duration
   for idx in range(j,j+duration):
    if mask[idx] or (idx+1<len(mask) and mask[idx+1]):
     out_idx=idx;xmid=a[idx];break
    fr=-side*qty*a[idx]*f[idx]
    hit=l[idx]<=stop if side>0 else h[idx]>=stop
    if hit:
     if fr<0:cash+=fr;fundpnl+=fr
     xmid=min(stop,a[idx]) if side>0 else max(stop,a[idx])
     out_idx=idx;break
    cash+=fr;fundpnl+=fr
    curve[idx-left]=cash+side*qty*(cl[idx]-entry)
    if champion['family']!='H3':
     if side>0:stop=max(stop,cl[idx]-atr[idx]*champion['atr_mult'])
     else:stop=min(stop,cl[idx]+atr[idx]*champion['atr_mult'])
   if xmid is None:xmid=a[j+duration]
   x=xmid*(1-side*base_slip);exitfee=qty*x*FEE;gross=side*qty*(x-entry)
   cash+=gross-exitfee
   pnl.append(gross-entryfee-exitfee+fundpnl)
   executed+=1
   # Only exit-hour equity is settled; do not overwrite historical unrealised MTM.
   endj=min(n,out_idx-left+1)
   curve[out_idx-left]=cash
   last=endj
  curve[last:]=cash
  # Mark-to-market equity of the null portfolio; no unrealised intra-hour risk interpolation.
  rr=curve[1:]/np.maximum(curve[:-1],1e-10)-1
  sd=rr.std(ddof=1) if len(rr)>1 else 0
  sharpe=rr.mean()/sd*math.sqrt(8760) if sd>0 else math.nan
  prof=sum(x for x in pnl if x>0);loss=-sum(x for x in pnl if x<0)
  out.append({'total_return':cash/EQUITY-1,'sharpe':sharpe,
              'PF':prof/loss if loss>0 else math.nan,'trades':executed,
              'overlaps':overlaps,'masked':masked})
 if not out:return {'repetitions':0,'failed_schedules':failed}
 return {'repetitions':len(out),'failed_schedules':failed,
         'planned_trades':count,'trades_min':min(x['trades'] for x in out),
         'trades_max':max(x['trades'] for x in out),
         'overlaps_max':max(x['overlaps'] for x in out),
         'sharpe_p95':float(np.nanquantile([x['sharpe'] for x in out],.95)),
         'sharpe_median':float(np.nanmedian([x['sharpe'] for x in out])),
         'return_p95':float(np.quantile([x['total_return'] for x in out],.95)),
         'return_median':float(np.median([x['total_return'] for x in out])),
         'PF_p95':float(np.nanquantile([x['PF'] for x in out],.95))}

def main():
 p=argparse.ArgumentParser();p.add_argument('--zip',required=True);p.add_argument('--output',required=True);p.add_argument('--prereg',required=True);p.add_argument('--allow-blind',action='store_true');args=p.parse_args()
 out=Path(args.output);out.mkdir(parents=True,exist_ok=True)
 digest=hashlib.sha256(Path(args.prereg).read_bytes()).hexdigest()
 if digest!=PREREG_SHA:raise RuntimeError('PREREG HASH DOES NOT MATCH FROZEN SOURCE '+digest)
 dat=load_data(args.zip)
 spec_list=specs();assert len(spec_list)==18
 checks={sp['id']:audit_lookahead(dat,sp) for sp in spec_list}
 all_results={};ledger=[]
 for sp in spec_list:
  rr={}
  for period in ('train','validation'):
   z=run(dat,sp,period,SLIP)
   rr[period]=metrics(z)
   if z['risk_violations']:raise RuntimeError('Risk sizing violation '+sp['id'])
  all_results[sp['id']]={'spec':sp,**rr}
  ledger.append({'id':sp['id'],'family':sp['family'],'kind':'primary','splits_tested':['train','validation']})
 # Family winner training, then validation filter; never peek at OOS.
 family_winners={}
 for fam in ('H1','H2','H3'):
  pool=[v for v in all_results.values() if v['spec']['family']==fam and qualifies_train(v['train'])]
  pool.sort(key=lambda x:(-x['train']['sharpe'],-x['train']['PF'],x['spec']['id']))
  if pool:family_winners[fam]=pool[0]['spec']['id']
 eligible=[all_results[x] for x in family_winners.values() if qualifies_val(all_results[x]['validation'])]
 eligible.sort(key=lambda x:(-x['validation']['sharpe'],-x['validation']['PF'],x['spec']['id']))
 champion=eligible[0]['spec'] if eligible else None
 robust={};robust_pass=None
 if champion:
  robust_pass=True
  for ratio in (.8,1.2):
   sp=perturb(champion,ratio)
   checks[sp['id']]=audit_lookahead(dat,sp)
   z=run(dat,sp,'train',SLIP);v=run(dat,sp,'validation',SLIP)
   robust[sp['id']]={'spec':sp,'train':metrics(z),'validation':metrics(v)}
   ledger.append({'id':sp['id'],'family':sp['family'],'kind':'robustness','splits_tested':['train','validation']})
   if robust[sp['id']]['validation']['PF']<1. or robust[sp['id']]['validation']['expectancy_R']<=0:robust_pass=False
 result={'prereg_sha256':digest,'data_zip_sha256':hashlib.sha256(Path(args.zip).read_bytes()).hexdigest(),
         'lookahead_unit_checks':checks,'n_unique_parameter_configurations':len(ledger),
         'ledger':ledger,'primary_results':all_results,'family_winners_train':family_winners,
         'validation_eligible_ids':[x['spec']['id'] for x in eligible],
         'champion_id':champion['id'] if champion else None,'robustness':robust,
         'robustness_pass':robust_pass,'blind_executed':False,
         'friction':{'taker_fee_per_side':FEE,'slippage_per_side':SLIP,'stress_slippage_per_side':STRESS,
                     'funding':'actual historic settled rates/times times 1h bar OPEN mark-price proxy'},
         'windows':SPLITS}
 # Write validation-only evidence FIRST. Without --allow-blind this executable never evaluates OOS.
 (out/'training_validation_results.json').write_text(json.dumps(finite_json(result),indent=2,ensure_ascii=False,allow_nan=False))
 print('TRAIN_VALIDATION_COMPLETE',json.dumps({'configs':len(ledger),'family_winners':family_winners,'validation_eligible':result['validation_eligible_ids'],'champion':result['champion_id'],'robustness_pass':robust_pass}),flush=True)
 if not args.allow_blind or not champion or not robust_pass:
  result['decision']='HOLD_BLIND_NOT_RUN' if not args.allow_blind else 'NO_GO_NO_VALIDATED_CHAMPION'
  (out/'FINAL_REPORT.json').write_text(json.dumps(finite_json(result),indent=2,ensure_ascii=False,allow_nan=False))
  return
 # BLIND IS EVALUATED EXACTLY HERE. Only one selected primary signal; no parameter adjustment after results.
 sig=signals(dat,champion)
 z=run(dat,champion,'blind',SLIP,sig)
 zz=run(dat,champion,'blind',STRESS,sig)
 oos=metrics(z);stress=metrics(zz)
 left,right=segment_idx(dat,'blind')
 # Per-year reporting: fresh equity each year, same locked parameters and history warmup.
 old=SPLITS['blind'];per_year={}
 for year in (2025,2026):
  lab=f'y{year}';SPLITS[lab]=(f'{year}-01-01T00:00:00Z',old[1] if year==2026 else f'{year+1}-01-01T00:00:00Z')
  per_year[str(year)]=metrics(run(dat,champion,lab,SLIP,sig))
  del SPLITS[lab]
 ci=bootstrap_ci([x['r'] for x in z['trades']])
 bh,eqbh=buy_hold(dat,'blind',SLIP)
 # Extra simple alternative: exposure-scaled buy and hold to annualized vol of champion;
 # no re-leveraging of price path (stated comparator limitation), report realized Sharpe of its scaled equity trajectory.
 bret=np.diff(eqbh)/eqbh[:-1];sret=np.diff(z['equity'])/z['equity'][:-1]
 factor=float(min(1.,sret.std(ddof=1)/bret.std(ddof=1))) if bret.std(ddof=1)>0 else 0.
 volbh=EQUITY+factor*(eqbh-EQUITY)
 scaled_rets=np.diff(volbh)/volbh[:-1]
 scaled_sharpe=float(scaled_rets.mean()/scaled_rets.std(ddof=1)*math.sqrt(8760)) if scaled_rets.std(ddof=1)>0 else None
 # Controls: entry/duration matched, not exact daily equity Sharpe control. Stats are return percentiles only.
 controls={}
 if z['trades']:
  controls['random_entry_1000']=rand_metrics_from_fixed_schedule(dat,'blind',z['trades'],champion,1000,20261116,False)
  controls['shift_signal_1000']=rand_metrics_from_fixed_schedule(dat,'blind',z['trades'],champion,1000,20261117,True)
 else:controls={'random_entry_1000':None,'shift_signal_1000':None}
 # Compare null Sharpe as preregistered, not just gross return.
 reasons=[]
 if oos['trades']<200:reasons.append('LESS_THAN_200_CLOSED_OOS_TRADES')
 if not (oos['PF']>=1.3):reasons.append('PF_BELOW_1_3')
 if not (oos['expectancy_R']>0):reasons.append('NONPOSITIVE_EXPECTANCY')
 if not (oos['max_drawdown']<=.15):reasons.append('MAX_DD_ABOVE_15_PERCENT')
 if not (oos['sharpe']>0):reasons.append('NONPOSITIVE_SHARPE')
 if any(per_year[y]['net_pnl']<=0 for y in ('2025','2026')):reasons.append('NOT_POSITIVE_EACH_OOS_YEAR')
 if not (ci[0] is not None and ci[0]>0):reasons.append('BOOTSTRAP_95CI_CROSSES_ZERO')
 if not (stress['PF']>=1.1 and stress['expectancy_R']>0):reasons.append('STRESS_SLIPPAGE_FAIL')
 if not (oos['sharpe']>bh['sharpe']+.2 and oos['sharpe']>(scaled_sharpe or 0)+.2):reasons.append('BUY_HOLD_RISK_ADJUSTED_FAIL')
 if not (controls.get('random_entry_1000') and oos['sharpe']>controls['random_entry_1000']['sharpe_p95']):reasons.append('RANDOM_BASELINE_FAIL')
 if not (controls.get('shift_signal_1000') and oos['sharpe']>controls['shift_signal_1000']['sharpe_p95']):reasons.append('SHIFT_BASELINE_FAIL')
 if controls.get('random_entry_1000',{}).get('trades_min',0) < .95*len(z['trades']): reasons.append('RANDOM_CONTROL_TRADE_COUNT_MISMATCH')
 if controls.get('shift_signal_1000',{}).get('trades_min',0) < .95*len(z['trades']): reasons.append('SHIFT_CONTROL_TRADE_COUNT_MISMATCH')
 result.update({'blind_executed':True,'blind_metrics':oos,'blind_stress_metrics':stress,'blind_years':per_year,
                'blind_expectancy_bootstrap_95CI':ci,'buy_hold_perpetual':bh,
                'vol_matched_buy_hold':{'annualized_sharpe':scaled_sharpe,'exposure_factor':factor,'total_return':float(volbh[-1]/EQUITY-1)},
                'controls':controls,'decision':'GO' if not reasons else 'NO_GO','failure_reasons':reasons,
                'blind_trade_count':len(z['trades'])})
 # Trade data OOS for independent review, no account data.
 with open(out/'blind_trades.csv','w',newline='') as f:
  w=csv.DictWriter(f,fieldnames=list(z['trades'][0].keys()) if z['trades'] else ['entry','exit','side','pnl'])
  w.writeheader();w.writerows(z['trades'])
 (out/'FINAL_REPORT.json').write_text(json.dumps(finite_json(result),indent=2,ensure_ascii=False,allow_nan=False))
 print('BLIND_COMPLETE',json.dumps(finite_json({'decision':result['decision'],'champion':champion['id'],'metrics':oos,'stress':stress,'CI':ci,'failure_reasons':reasons})),flush=True)

if __name__=='__main__':main()
