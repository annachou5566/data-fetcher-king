"""Lite auto-archive from event_time, in the EXISTING All/History writer.
Past schedule != confirmed completion. No I/O, new service, cron or Active writes.
"""
import copy
import re
from datetime import datetime, timezone
from alpha_events_completion_only_guard import ensure_all_contains_existing_history

CID = re.compile(r'ae:[0-9a-f]{24}\Z')
TIME = re.compile(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z\Z')
ACTIVE_KEYS = ('alpha-events/pending.json', 'alpha-events/upcoming.json', 'alpha-events/live.json')
IDENTITY = ('id', 'symbol', 'event_time', 'event_type', 'contract_address', 'chain_id', 'project_name')

class ScheduledArchiveHold(RuntimeError):
    pass

def hold(code):
    raise ScheduledArchiveHold('WA_AE_LITE_SCHEDULE_HOLD_' + code)

def _rows(value, name):
    if not isinstance(value, list) or len(value)>20000 or any(not isinstance(x,dict) for x in value):
        hold(name+'_INVALID')
    return value

def _index(rows, name):
    out={}
    for row in rows:
        cid=row.get('id')
        if not isinstance(cid,str) or not CID.fullmatch(cid):
            if name=='ACTIVE': hold('ACTIVE_ID_INVALID')
            continue
        if cid in out: hold(name+'_DUPLICATE_ID')
        out[cid]=row
    return out

def _time(value):
    if not isinstance(value,str) or not TIME.fullmatch(value): return None
    try: dt=datetime.strptime(value,'%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
    except ValueError: return None
    return dt if dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]+'Z'==value else None

def _identity(current, prior):
    for k in IDENTITY:
        if current.get(k)!=prior.get(k): hold('IMMUTABLE_IDENTITY_CONFLICT')

def plan_scheduled_archive(all_rows, history_rows, active_versions, public_rows, now):
    """Pure pair candidate. Only existing writer may CAS, only Pages may retire."""
    if not isinstance(now,datetime) or now.tzinfo is None or now.utcoffset() is None:
        hold('INVALID_CLOCK')
    now=now.astimezone(timezone.utc)
    before_all=_rows(all_rows,'ALL')
    before_hist=_rows(history_rows,'HISTORY')
    ensure_all_contains_existing_history(before_all,before_hist)
    if (not isinstance(active_versions,list) or len(active_versions)!=3 or
        [v.get('key') for v in active_versions if isinstance(v,dict)]!=list(ACTIVE_KEYS)):
        hold('ACTIVE_COVERAGE')
    current=[]
    for v in active_versions:
        current+=_rows(v.get('rows'),'ACTIVE')
    active=_index(current,'ACTIVE')
    published=_index(_rows(public_rows,'PUBLIC'),'ACTIVE')
    if active.keys()!=published.keys(): hold('PUBLIC_ACTIVE_DIVERGENCE')
    for cid,raw in active.items():
        visible=published[cid]
        for k in IDENTITY+('firstActiveAt',):
            if k in visible and raw.get(k)!=visible[k]: hold('PUBLIC_IDENTITY_CONFLICT')
    new_all=copy.deepcopy(before_all)
    new_hist=copy.deepcopy(before_hist)
    by_all=_index(new_all,'ALL')
    by_hist=_index(new_hist,'HISTORY')
    promoted=[]
    for cid,raw in active.items():
        scheduled=_time(raw.get('event_time'))
        if scheduled is None or scheduled>now: continue
        prior_a=by_all.get(cid)
        prior_h=by_hist.get(cid)
        if prior_a is not None: _identity(raw,prior_a)
        if prior_h is not None: _identity(raw,prior_h)
        if prior_h is not None:
            if (prior_h.get('status')!='expired_unverified' or
                prior_h.get('archive_reason')!='scheduled_time_elapsed_unverified'):
                continue
            if prior_a is None or prior_a!=prior_h: hold('ALREADY_ARCHIVED_PAIR_CONFLICT')
            promoted.append(cid)
            continue
        record=copy.deepcopy(prior_a if prior_a is not None else raw)
        record.update({'status':'expired_unverified','lifecycle_state':'history',
                       'archive_reason':'scheduled_time_elapsed_unverified',
                       'completion_verified':False})
        if prior_a is not None:
            for i,row in enumerate(new_all):
                if row.get('id')==cid:
                    new_all[i]=copy.deepcopy(record)
                    break
        else: new_all.append(copy.deepcopy(record))
        new_hist.append(copy.deepcopy(record))
        promoted.append(cid)
    if new_hist[:len(before_hist)]!=before_hist: hold('LEGACY_HISTORY_MUTATED')
    ensure_all_contains_existing_history(new_all,new_hist)
    return {'all':new_all,'history':new_hist,'promoted':promoted,
            'retire_live':False,'independent_writer':False}
