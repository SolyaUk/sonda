#!/usr/bin/env python3
"""
diagnose_geo_cache.py — діагностика кешу геолокації SONDA.

Перевіряє:
1. Загальна статистика кешу (total / valid / expired).
2. Розподіл TTL по записах.
3. Розподіл cached_at по днях (виявити синхронні батчі експайрів).
4. Для кожного запису пробуємо розпарсити в GeolocationData.
   Якщо парсинг падає — IP буде в uncached щоциклу. Це ключова перевірка.
5. Беремо останній mainnet snapshot, виявляємо які IPs з нього:
   - не в кеші
   - в кеші valid
   - в кеші expired
6. Для проблемних IPs показуємо primary_source, щоб зрозуміти чи DB-IP
   взагалі повертав дані для них.

Запуск на сервері:
    python3 diagnose_geo_cache.py

Опціонально:
    --db PATH                шлях до geolocation_v3.db
    --snapshot PATH          конкретний snapshot файл
    --snapshot-dir PATH      директорія снапшотів (для пошуку останнього)
    --analyzer-path PATH     шлях до solana_analyzer.py
    --show-uncached N        показати перші N проблемних IPs (default 15)
    --check-all-parse        парсити всі записи (повільно при великому кеші)
"""
import argparse
import importlib.util
import json
import os
import sqlite3
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_DB = os.path.expanduser("~/.solana_network_cache/geolocation_v3.db")
DEFAULT_SNAPSHOT_DIR = "/home/solya/sonda_data/snapshots"
DEFAULT_ANALYZER = "/home/solya/sonda/analyzer/solana_analyzer.py"


def hr(title=""):
    if title:
        print(f"\n{'=' * 70}\n  {title}\n{'=' * 70}")
    else:
        print("=" * 70)


def find_latest_snapshot(snapshot_dir, prefix="sonda-mainnet-"):
    paths = sorted(Path(snapshot_dir).glob(f"{prefix}*.json"))
    return paths[-1] if paths else None


def load_geolocation_dataclass(analyzer_path):
    try:
        spec = importlib.util.spec_from_file_location("solana_analyzer_diag", analyzer_path)
        if spec is None or spec.loader is None:
            return None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.GeolocationData
    except Exception as e:
        print(f"  WARN: failed to import GeolocationData: {e}")
        return None


def extract_ips_from_snapshot(path):
    with open(path) as f:
        data = json.load(f)
    ips = set()
    for rec in data.get("records", []):
        ip = rec.get("ip_address")
        if ip:
            ips.add(ip)
    return ips


def chunked(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def try_parse(ds, valid_fields, GeolocationData):
    """Повторює логіку get_batch. Повертає (ok, error_str_or_None, raw_keys)."""
    raw_keys = []
    try:
        d = json.loads(ds)
        raw_keys = list(d.keys())
        if 'org' in d and 'asn_name' not in d:
            d['asn_name'] = d.pop('org')
        if 'asn_number' not in d:
            d['asn_number'] = None
        if 'discrepancy_alternatives' not in d:
            d['discrepancy_alternatives'] = None
        d = {k: v for k, v in d.items() if k in valid_fields}
        GeolocationData(**d)
        return True, None, raw_keys
    except Exception as e:
        return False, f"{type(e).__name__}: {e}", raw_keys


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--snapshot", default=None)
    ap.add_argument("--snapshot-dir", default=DEFAULT_SNAPSHOT_DIR)
    ap.add_argument("--analyzer-path", default=DEFAULT_ANALYZER)
    ap.add_argument("--show-uncached", type=int, default=15)
    ap.add_argument("--check-all-parse", action="store_true")
    args = ap.parse_args()

    hr("SONDA Geolocation Cache Diagnostics")
    print(f"DB:       {args.db}")
    print(f"Analyzer: {args.analyzer_path}")

    if not os.path.exists(args.db):
        print(f"\nERROR: DB not found at {args.db}")
        sys.exit(1)

    # ---- 1. General stats ----
    hr("1. General stats")
    with sqlite3.connect(args.db) as c:
        total = c.execute("SELECT COUNT(*) FROM geolocation").fetchone()[0]
        valid = c.execute("""
            SELECT COUNT(*) FROM geolocation
            WHERE (strftime('%s','now') - cached_at) / 86400.0 <= ttl_days
        """).fetchone()[0]
    expired = total - valid
    print(f"  Total entries:   {total}")
    print(f"  Valid (TTL ok):  {valid}")
    print(f"  Expired:         {expired}")

    # ---- 2. TTL distribution ----
    hr("2. TTL distribution")
    with sqlite3.connect(args.db) as c:
        for ttl, cnt in c.execute(
            "SELECT ttl_days, COUNT(*) FROM geolocation GROUP BY ttl_days ORDER BY ttl_days"
        ):
            print(f"  ttl_days={ttl:>3}  count={cnt}")

    # ---- 3. Cached_at distribution by day ----
    hr("3. Cached_at distribution (last 30 days, then aggregated)")
    with sqlite3.connect(args.db) as c:
        rows = c.execute("""
            SELECT date(cached_at, 'unixepoch') as day, COUNT(*)
            FROM geolocation
            GROUP BY day
            ORDER BY day DESC
        """).fetchall()
    print(f"  Total distinct days: {len(rows)}")
    for day, cnt in rows[:30]:
        print(f"    {day}: {cnt}")
    if len(rows) > 30:
        older = sum(cnt for _, cnt in rows[30:])
        print(f"    [older days combined]: {older}")

    # ---- 4. Parse check ----
    hr("4. Parse check (replicates get_batch logic)")
    GeolocationData = load_geolocation_dataclass(args.analyzer_path)
    parse_failures = []  # (ip, error, raw_keys)
    parse_ok = 0
    parse_err = 0
    if GeolocationData is None:
        print("  SKIPPED: cannot import GeolocationData")
    else:
        from dataclasses import fields as dc_fields
        valid_fields = {f.name for f in dc_fields(GeolocationData)}
        print(f"  GeolocationData fields ({len(valid_fields)}): {sorted(valid_fields)}")
        with sqlite3.connect(args.db) as c:
            cur = c.execute("SELECT ip, data FROM geolocation")
            for ip, ds in cur:
                ok, err, raw_keys = try_parse(ds, valid_fields, GeolocationData)
                if ok:
                    parse_ok += 1
                else:
                    parse_err += 1
                    if len(parse_failures) < 10:
                        parse_failures.append((ip, err, raw_keys))
        print(f"  Parsed OK:    {parse_ok}")
        print(f"  Parse errors: {parse_err}")
        if parse_failures:
            print("  Sample errors:")
            for ip, err, keys in parse_failures:
                print(f"    {ip}")
                print(f"      error: {err}")
                print(f"      keys:  {keys}")

    # ---- 5. Snapshot check ----
    snapshot_path = args.snapshot
    if not snapshot_path:
        snapshot_path = find_latest_snapshot(args.snapshot_dir)
    if not snapshot_path:
        print("\n  WARN: no snapshot found, skipping snapshot check")
        hr("Done.")
        return

    snapshot_path = str(snapshot_path)
    hr(f"5. Snapshot vs cache: {os.path.basename(snapshot_path)}")
    snap_ips = extract_ips_from_snapshot(snapshot_path)
    print(f"  IPs in snapshot: {len(snap_ips)}")

    # Fetch cache rows for snapshot IPs (chunked for SQLite limit)
    cached = {}  # ip -> (cached_at, ttl_days, data_json)
    snap_list = list(snap_ips)
    with sqlite3.connect(args.db) as c:
        for chunk in chunked(snap_list, 500):
            ph = ",".join("?" * len(chunk))
            for ip, cat, ttl, data in c.execute(
                f"SELECT ip, cached_at, ttl_days, data FROM geolocation WHERE ip IN ({ph})",
                chunk,
            ):
                cached[ip] = (cat, ttl, data)

    not_in_cache = sorted(snap_ips - set(cached.keys()))
    now = time.time()

    valid_ips = []     # (ip, cat, ttl, primary, age_days)
    expired_ips = []   # (ip, cat, ttl, primary, age_days)
    parse_fail_ips = []  # entries that have data in cache but cannot be parsed
    for ip, (cat, ttl, ds) in cached.items():
        age_days = (now - cat) / 86400.0
        primary = "?"
        try:
            d = json.loads(ds)
            primary = d.get("primary_source", "?")
        except Exception:
            primary = "json-broken"
        if GeolocationData is not None:
            ok, _, _ = try_parse(ds, valid_fields, GeolocationData)
            if not ok:
                parse_fail_ips.append((ip, cat, ttl, primary, age_days))
                continue
        if age_days <= ttl:
            valid_ips.append((ip, cat, ttl, primary, age_days))
        else:
            expired_ips.append((ip, cat, ttl, primary, age_days))

    print(f"  In cache valid:        {len(valid_ips)}")
    print(f"  In cache expired:      {len(expired_ips)}")
    print(f"  In cache parse-fail:   {len(parse_fail_ips)}")
    print(f"  Not in cache at all:   {len(not_in_cache)}")
    print(f"  ---")
    print(f"  Total problematic:     {len(expired_ips) + len(parse_fail_ips) + len(not_in_cache)}")

    # primary_source distribution for valid entries
    if valid_ips:
        ps_counter = Counter(p for _, _, _, p, _ in valid_ips)
        print(f"\n  Primary source for VALID cached entries:")
        for ps, cnt in ps_counter.most_common():
            print(f"    {ps:15s} {cnt}")
    if expired_ips:
        ps_counter = Counter(p for _, _, _, p, _ in expired_ips)
        print(f"\n  Primary source for EXPIRED cached entries:")
        for ps, cnt in ps_counter.most_common():
            print(f"    {ps:15s} {cnt}")

    N = args.show_uncached
    if not_in_cache:
        print(f"\n  Sample 'not in cache' IPs (first {N}):")
        for ip in not_in_cache[:N]:
            print(f"    {ip}")
    if expired_ips:
        print(f"\n  Sample 'expired' IPs (first {N}):")
        for ip, cat, ttl, primary, age in expired_ips[:N]:
            dt = datetime.fromtimestamp(cat, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
            print(f"    {ip:20s}  primary={primary:8s}  cached={dt}  ttl={ttl}d  age={age:.1f}d")
    if parse_fail_ips:
        print(f"\n  Sample 'parse-fail' IPs (first {N}):")
        for ip, cat, ttl, primary, age in parse_fail_ips[:N]:
            dt = datetime.fromtimestamp(cat, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
            print(f"    {ip:20s}  primary={primary:8s}  cached={dt}  ttl={ttl}d  age={age:.1f}d")

    # Cached_at distribution for problematic entries
    if expired_ips:
        cat_dist = Counter()
        for _, cat, _, _, _ in expired_ips:
            day = datetime.fromtimestamp(cat, tz=timezone.utc).strftime("%Y-%m-%d")
            cat_dist[day] += 1
        print(f"\n  Cached_at distribution for EXPIRED snapshot IPs:")
        for day, cnt in sorted(cat_dist.items()):
            print(f"    {day}: {cnt}")

    hr("Done.")


if __name__ == "__main__":
    main()
