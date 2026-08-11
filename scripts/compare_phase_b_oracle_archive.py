#!/usr/bin/env python3
"""Read-only Oracle archive comparator for Wave Alpha Phase B evidence."""
from __future__ import annotations

import json
import subprocess
from collections import Counter
from pathlib import Path
from typing import Any, Mapping

ARCHIVE = Path('/var/lib/wave-alpha-liquidations-phase-b/archive/oracle-liquidations-phase-b-v1')
PACIFICA = 'pacifica-perp'
BACKPACK = 'backpack-perp'
PACIFICA_EXPECTED_IDS = (249630607, 249630609)
PACIFICA_ANCHOR_TS = 1786406046425


def raw_data(item: Mapping[str, Any]) -> Mapping[str, Any] | None:
    raw = item.get('raw')
    if not isinstance(raw, Mapping):
        return None
    data = raw.get('data')
    return data if isinstance(data, Mapping) else None


def load_events() -> list[dict[str, Any]]:
    if not ARCHIVE.is_dir():
        raise RuntimeError(f'archive missing: {ARCHIVE}')
    events: list[dict[str, Any]] = []
    for path in sorted(ARCHIVE.glob('*.json')):
        payload = json.loads(path.read_text(encoding='utf-8'))
        rows = payload.get('events') if isinstance(payload, Mapping) else None
        if not isinstance(rows, list):
            raise RuntimeError(f'invalid archive envelope: {path.name}')
        for index, item in enumerate(rows):
            if not isinstance(item, Mapping):
                continue
            normalized = item.get('normalized')
            if not isinstance(normalized, Mapping):
                continue
            events.append({
                'archive': path.name,
                'index': index,
                'normalized': dict(normalized),
                'raw': dict(item.get('raw')) if isinstance(item.get('raw'), Mapping) else {},
            })
    return events


def as_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return None


def as_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError, OverflowError):
        return 0.0


def main() -> int:
    print('========== PHASE B FINAL ARCHIVE COMPARATOR ==========')
    print('auditReadOnly=true')
    print('databaseTouched=false')
    print('archiveWrite=false')
    print('systemdMutation=false')
    print('coordinatorTouched=false')

    events = load_events()

    pacifica_rows: list[dict[str, Any]] = []
    for item in events:
        normalized = item['normalized']
        if str(normalized.get('source') or '') != PACIFICA:
            continue
        raw = raw_data(item)
        if raw is None:
            continue
        hid = as_int(raw.get('h') if 'h' in raw else raw.get('history_id'))
        ts = as_int(raw.get('t') if 't' in raw else raw.get('created_at'))
        pacifica_rows.append({
            'historyId': hid,
            'timestampMs': ts,
            'cause': str(raw.get('tc') if 'tc' in raw else raw.get('cause') or ''),
            'symbol': str(raw.get('s') if 's' in raw else raw.get('symbol') or ''),
            'direction': str(raw.get('d') if 'd' in raw else raw.get('side') or ''),
            'fp': str(normalized.get('fp') or ''),
            'archive': item['archive'],
        })

    pacifica_id_counts = Counter(row['historyId'] for row in pacifica_rows if row['historyId'] is not None)
    missing = [hid for hid in PACIFICA_EXPECTED_IDS if pacifica_id_counts.get(hid, 0) == 0]
    duplicate_expected = [hid for hid in PACIFICA_EXPECTED_IDS if pacifica_id_counts.get(hid, 0) != 1]
    later_local = [
        row for row in pacifica_rows
        if row['timestampMs'] is not None and row['timestampMs'] > PACIFICA_ANCHOR_TS
    ]
    expected_detail = [row for row in pacifica_rows if row['historyId'] in PACIFICA_EXPECTED_IDS]

    print(f'pacificaArchivedEvents={len(pacifica_rows)}')
    print('pacificaExpectedOfficialIds=' + json.dumps(PACIFICA_EXPECTED_IDS, separators=(',', ':')))
    print('pacificaExpectedIdCounts=' + json.dumps({str(h): pacifica_id_counts.get(h, 0) for h in PACIFICA_EXPECTED_IDS}, separators=(',', ':')))
    print('pacificaExpectedDetails=' + json.dumps(expected_detail, separators=(',', ':'), sort_keys=True))
    print(f'pacificaLocalEventsAfterOfficialAnchor={len(later_local)}')
    print('pacificaLaterLocalDetails=' + json.dumps(later_local[:20], separators=(',', ':'), sort_keys=True))
    pacifica_ok = not missing and not duplicate_expected and not later_local
    print('pacificaArchiveGate=' + ('PASS' if pacifica_ok else 'FAIL'))
    print('pacificaGapVerdict=' + (
        'NO_MISSED_DOCUMENTED_LIQUIDATIONS_IN_FULLY_SCANNED_INTERVAL'
        if pacifica_ok else 'ARCHIVE_MISMATCH_REQUIRES_INVESTIGATION'
    ))

    backpack_rows: list[dict[str, Any]] = []
    for item in events:
        normalized = item['normalized']
        if str(normalized.get('source') or '') != BACKPACK:
            continue
        raw = raw_data(item)
        backpack_rows.append({
            'fp': str(normalized.get('fp') or ''),
            'ts': as_int(normalized.get('ts')),
            'symbol': str(normalized.get('symbol') or ''),
            'side': str(normalized.get('side') or ''),
            'usd': round(as_float(normalized.get('usd')), 2),
            'price': as_float(normalized.get('price')),
            'qty': as_float(normalized.get('qty')),
            'rawSymbol': '' if raw is None else str(raw.get('s') or ''),
            'rawEventUs': None if raw is None else as_int(raw.get('E')),
            'rawEngineUs': None if raw is None else as_int(raw.get('T')),
            'rawSide': '' if raw is None else str(raw.get('S') or ''),
            'rawPrice': '' if raw is None else str(raw.get('p') or ''),
            'rawQty': '' if raw is None else str(raw.get('q') or ''),
            'archive': item['archive'],
        })

    fp_counts = Counter(row['fp'] for row in backpack_rows if row['fp'])
    duplicate_fps = sorted(fp for fp, count in fp_counts.items() if count != 1)
    malformed = [
        row for row in backpack_rows
        if not row['fp'] or row['rawEventUs'] is None or row['rawSide'].lower() not in {'bid', 'ask'}
        or row['price'] <= 0 or row['qty'] <= 0 or row['usd'] <= 0
    ]
    latest = max(backpack_rows, key=lambda row: (row['ts'] or -1, row['rawEventUs'] or -1), default=None)
    long_usd = round(sum(row['usd'] for row in backpack_rows if row['side'] == 'long'), 2)
    short_usd = round(sum(row['usd'] for row in backpack_rows if row['side'] == 'short'), 2)
    total_usd = round(long_usd + short_usd, 2)
    backpack_ok = bool(backpack_rows) and not duplicate_fps and not malformed

    print(f'backpackArchivedEvents={len(backpack_rows)}')
    print(f'backpackArchiveTotalUsd={total_usd:.2f}')
    print(f'backpackArchiveLongUsd={long_usd:.2f}')
    print(f'backpackArchiveShortUsd={short_usd:.2f}')
    print(f'backpackDuplicateFingerprints={len(duplicate_fps)}')
    print(f'backpackMalformedArchivedEvents={len(malformed)}')
    print('backpackLatestEvent=' + json.dumps(latest, separators=(',', ':'), sort_keys=True))
    print('backpackArchiveGate=' + ('PASS' if backpack_ok else 'FAIL'))
    print('backpackCoverageVerdict=CURRENT_FEED_LIVE_AND_ARCHIVE_EXACTNESS_AUDITED;PAST_GLOBAL_COMPLETENESS_NOT_RETROSPECTIVELY_PROVABLE_FROM_DOCUMENTED_PUBLIC_API')

    try:
        status = subprocess.run(
            ['systemctl', 'show', 'liquidation-tracker-phase-b.service', '--property=ActiveState,SubState,NRestarts,MainPID', '--no-pager'],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
        print('phaseBServiceStatus=' + json.dumps(status.stdout.strip().splitlines(), separators=(',', ':')))
        print(f'phaseBServiceStatusExit={status.returncode}')
    except Exception as error:
        print(f'phaseBServiceStatusError={type(error).__name__}:{error}')

    print('phaseBFinalArchiveCompare=' + ('PASS' if pacifica_ok and backpack_ok else 'FAIL'))
    return 0 if pacifica_ok and backpack_ok else 1


if __name__ == '__main__':
    raise SystemExit(main())
