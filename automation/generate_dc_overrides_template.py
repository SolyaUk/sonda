#!/usr/bin/env python3
"""
generate_dc_overrides_template.py

Extracts all unique (ASN, city, country) combinations from the latest SONDA
snapshot JSON files on disk and produces a YAML template for
analyzer/dc_overrides.yaml.

Output is sorted by total validator stake (most important DCs first).

Usage:
    python3 automation/generate_dc_overrides_template.py \\
        --snapshots-dir /home/solya/sonda_data/snapshots \\
        --output /tmp/dc_overrides_template.yaml

Then review the output and use it as starting point for analyzer/dc_overrides.yaml.

Logic:
    1. For each cluster, find the most recent snapshot JSON file
    2. Read records[] — full per-node state including geolocation
    3. For each validator: extract asn, asn_name, city, country, stake_pct
    4. Normalize city names (strip parentheses, unicode-fold)
    5. Aggregate by ASN (ASN-level entries) and by ASN_city (specific entries)
    6. Emit YAML with stake-sorted entries

Why snapshots not SQLite:
    current_state in timeseries.db only stores tracked-changes fields
    (delta state), not full record. Snapshots have full geolocation including
    city, asn_name, region — exactly what we need for DC aggregation.
"""

import argparse
import json
import logging
import re
from collections import defaultdict
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def normalize_city(city):
    """Basic normalization matching solana_analyzer.py (extended in v6.6).

    Strip parentheses, normalize unicode, normalize separators, title case.
    """
    if not city:
        return ""
    city = city.split('(')[0].strip()
    city = city.replace('_', ' ').replace('-', ' ')
    try:
        from unidecode import unidecode
        city = unidecode(city)
    except ImportError:
        replacements = {'ü': 'u', 'ö': 'o', 'ä': 'a', 'ß': 'ss',
                        'é': 'e', 'è': 'e', 'ê': 'e',
                        'à': 'a', 'á': 'a', 'â': 'a',
                        'í': 'i', 'î': 'i',
                        'ó': 'o', 'ô': 'o', 'õ': 'o',
                        'ú': 'u', 'û': 'u',
                        'ç': 'c', 'ñ': 'n'}
        for k, v in replacements.items():
            city = city.replace(k, v).replace(k.upper(), v.upper())
    city = ' '.join(w.capitalize() for w in city.split() if w)
    return city


def find_latest_snapshots(snapshots_dir):
    """For each cluster, find the most recent snapshot file.

    Filenames look like: sonda-mainnet-beta-2026-05-27T14-15-01.json

    Returns dict {cluster: path} for each cluster found.
    """
    snapshots_dir = Path(snapshots_dir)
    if not snapshots_dir.is_dir():
        raise RuntimeError(f"Snapshots dir not found: {snapshots_dir}")
    pattern = re.compile(r"^sonda-(.+?)-\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}\.json$")
    by_cluster = defaultdict(list)
    for f in snapshots_dir.glob("sonda-*.json"):
        m = pattern.match(f.name)
        if not m:
            continue
        cluster = m.group(1)
        by_cluster[cluster].append(f)
    latest = {}
    for cluster, files in by_cluster.items():
        files.sort()  # filename has ISO timestamp — sort = latest last
        latest[cluster] = files[-1]
    return latest


def fetch_validator_records(snapshots_dir):
    """Read latest snapshot for each cluster and extract per-validator DC info."""
    latest = find_latest_snapshots(snapshots_dir)
    if not latest:
        raise RuntimeError(f"No snapshot files found in {snapshots_dir}")

    records = []
    for cluster, path in sorted(latest.items()):
        logger.info(f"  reading {cluster}: {path.name}")
        try:
            with open(path) as f:
                data = json.load(f)
        except Exception as e:
            logger.warning(f"  failed to read {path}: {e}")
            continue
        for rec in data.get("records", []):
            role = rec.get("role")
            # We only count validators (variants); other roles don't matter for DC sizing
            if role not in ("validator", "validator-hidden"):
                continue
            geo = rec.get("geolocation") or {}
            asn = geo.get("asn") or ""
            asn_name = geo.get("asn_name") or ""
            city = geo.get("city") or ""
            country = geo.get("country_code") or geo.get("country") or ""
            stake_pct = rec.get("stake_percentage") or 0.0
            if not asn:
                continue
            records.append({
                "cluster": cluster,
                "identity": rec.get("identity_pubkey") or rec.get("identity") or "",
                "asn": asn,
                "asn_name": asn_name,
                "city_raw": city,
                "city": normalize_city(city),
                "country": country,
                "stake_pct": stake_pct,
            })
    return records


def aggregate(records):
    """Group records by ASN and by ASN_city. Return two sorted lists.

    For each group: total stake%, validator count, cities (for ASN-level),
    representative country, asn_name.
    """
    by_asn = defaultdict(lambda: {
        "asn": "", "asn_name": "",
        "validator_count": 0,
        "stake_pct_total": 0.0,
        "cities": set(),
        "countries": set(),
        "clusters": set(),
    })

    by_dc = defaultdict(lambda: {
        "asn": "", "asn_name": "",
        "city": "", "country": "",
        "validator_count": 0,
        "stake_pct_total": 0.0,
        "clusters": set(),
    })

    for r in records:
        asn_key = r["asn"]
        dc_key = f"{r['asn']}_{r['city']}" if r["city"] else r["asn"]

        a = by_asn[asn_key]
        a["asn"] = r["asn"]
        if not a["asn_name"]:
            a["asn_name"] = r["asn_name"]
        a["validator_count"] += 1
        a["stake_pct_total"] += r["stake_pct"]
        if r["city"]:
            a["cities"].add(r["city"])
        if r["country"]:
            a["countries"].add(r["country"])
        a["clusters"].add(r["cluster"])

        d = by_dc[dc_key]
        d["asn"] = r["asn"]
        d["asn_name"] = r["asn_name"]
        d["city"] = r["city"]
        d["country"] = r["country"]
        d["validator_count"] += 1
        d["stake_pct_total"] += r["stake_pct"]
        d["clusters"].add(r["cluster"])

    asn_sorted = sorted(by_asn.values(), key=lambda x: x["stake_pct_total"], reverse=True)
    dc_sorted = sorted(by_dc.values(), key=lambda x: x["stake_pct_total"], reverse=True)
    return asn_sorted, dc_sorted


def render_yaml(asn_entries, dc_entries, output_path):
    lines = []
    lines.append("# =============================================================================")
    lines.append("# Datacenter and ASN Overrides (auto-generated template)")
    lines.append("# =============================================================================")
    lines.append("#")
    lines.append("# Two-level lookup in derive_views.py:")
    lines.append("#   1. \"{asn}_{city}\" — overrides for specific DC location (most specific wins)")
    lines.append("#   2. \"{asn}\"        — overrides for entire ASN")
    lines.append("#   3. Fallback to raw asn_name from geolocation")
    lines.append("#")
    lines.append("# Optional fields (all may be omitted or empty):")
    lines.append("#   display_name   — human-readable label (e.g. 'TeraSwitch', 'Edgevana Singapore')")
    lines.append("#   website        — provider's homepage")
    lines.append("#   icon_url       — logo URL (PNG/SVG preferred), used in light theme")
    lines.append("#   icon_url_dark  — alternative logo for dark theme (optional)")
    lines.append("#   country        — ISO country code (only if you want to override geo data)")
    lines.append("#   city           — explicit city (rarely needed; geo data usually correct)")
    lines.append("#   notes          — internal notes, NOT shown on UI")
    lines.append("#   tags           — list of strings, e.g. ['solana-focused', 'bare-metal']")
    lines.append("#   affiliate:     — for future monetization, currently inert")
    lines.append("#     code:        — referral code")
    lines.append("#     url:         — full referral URL")
    lines.append("#     notes:       — internal notes about partnership")
    lines.append("#")
    lines.append("# Suggested tag conventions (use sparingly, only if confirmed):")
    lines.append("#   solana-focused   — provider has explicit Solana packages (Edgevana, Cherry, Mevspace)")
    lines.append("#   bare-metal       — physical hardware (typical for Solana validators)")
    lines.append("#   cloud-only       — VPS/cloud only; warning, atypical for Solana")
    lines.append("#   banned           — provider doesn't allow Solana validators or community avoids them")
    lines.append("#   resold           — reselling another provider's infrastructure (e.g. Edgevana on Vultr)")
    lines.append("#   deprecated       — provider no longer recommended")
    lines.append("#   validator-funding — provider invests in Solana ecosystem")
    lines.append("#   DZ-partner       — integrated with DoubleZero")
    lines.append("#")
    lines.append("# WORKFLOW:")
    lines.append("#   1. Review entries sorted by total validator stake (most impactful first)")
    lines.append("#   2. For ASN-level entries: fill in display_name, website, icon_url")
    lines.append("#   3. For ASN_city entries: only override if specific location has different brand")
    lines.append("#      (e.g. Edgevana resells Vultr in Singapore but not in Frankfurt)")
    lines.append("#   4. Delete entries you don't want to override")
    lines.append("#   5. Save as analyzer/dc_overrides.yaml")
    lines.append("# =============================================================================")
    lines.append("")
    lines.append("# ============================================================================")
    lines.append("# ASN-LEVEL OVERRIDES (apply across all cities of this ASN)")
    lines.append("# Sorted by total stake % across all clusters, descending")
    lines.append("# ============================================================================")
    lines.append("")

    for e in asn_entries:
        cities_list = sorted(e["cities"])
        countries_list = sorted(e["countries"])
        clusters_list = sorted(e["clusters"])
        cities_str = ", ".join(cities_list[:5])
        if len(cities_list) > 5:
            cities_str += f", ... (+{len(cities_list) - 5} more)"
        asn_name_safe = (e['asn_name'] or '(unknown)').replace('"', "'")
        lines.append(f"# {asn_name_safe}")
        lines.append(f"#   Validators: {e['validator_count']}  |  Stake total: {e['stake_pct_total']:.2f}%  |  Clusters: {', '.join(clusters_list)}")
        lines.append(f"#   Countries: {', '.join(countries_list)}")
        lines.append(f"#   Cities: {cities_str}")
        lines.append(f"\"{e['asn']}\":")
        lines.append(f"  display_name: \"{asn_name_safe}\"   # TODO: replace with friendly name if needed")
        lines.append(f"  # website: \"\"")
        lines.append(f"  # icon_url: \"\"")
        lines.append(f"  # tags: []")
        lines.append("")

    lines.append("")
    lines.append("# ============================================================================")
    lines.append("# ASN_CITY-LEVEL OVERRIDES (apply to specific DC location only)")
    lines.append("# Use ONLY when a specific city has a different brand from the ASN owner")
    lines.append("# (e.g. Edgevana resells Vultr AS20473 in Singapore)")
    lines.append("# ============================================================================")
    lines.append("")

    # Only show DC entries that have meaningful stake (skip tiny ones to keep file manageable)
    significant_dc = [e for e in dc_entries if e["stake_pct_total"] >= 0.1 or e["validator_count"] >= 3]
    skipped_dc = len(dc_entries) - len(significant_dc)

    for e in significant_dc:
        key = f"{e['asn']}_{e['city']}" if e["city"] else e["asn"]
        clusters_list = sorted(e["clusters"])
        asn_name_safe = (e['asn_name'] or '(unknown)').replace('"', "'")
        lines.append(f"# {asn_name_safe} — {e['city']}, {e['country']}")
        lines.append(f"#   Validators: {e['validator_count']}  |  Stake: {e['stake_pct_total']:.2f}%  |  Clusters: {', '.join(clusters_list)}")
        lines.append(f"# \"{key}\":")
        lines.append(f"#   display_name: \"\"")
        lines.append(f"#   website: \"\"")
        lines.append(f"#   icon_url: \"\"")
        lines.append(f"#   notes: \"\"")
        lines.append("")

    if skipped_dc > 0:
        lines.append(f"# (skipped {skipped_dc} DC entries with <0.1% stake and <3 validators for brevity)")
        lines.append("")

    summary = [
        "",
        "# ============================================================================",
        "# SUMMARY",
        f"# Total unique ASNs: {len(asn_entries)}",
        f"# Total unique DCs (ASN_city): {len(dc_entries)}",
        f"# Significant DCs (>=0.1% stake or >=3 validators): {len(significant_dc)}",
        "# ============================================================================",
        "",
    ]

    with open(output_path, "w") as f:
        f.write("\n".join(summary + lines))


def main():
    parser = argparse.ArgumentParser(description="Generate dc_overrides.yaml template")
    parser.add_argument("--snapshots-dir", default="/home/solya/sonda_data/snapshots",
                        help="Path to SONDA snapshots directory")
    parser.add_argument("--output", default="/tmp/dc_overrides_template.yaml",
                        help="Output YAML path")
    args = parser.parse_args()

    if not Path(args.snapshots_dir).is_dir():
        logger.error(f"Snapshots dir not found: {args.snapshots_dir}")
        return 1

    logger.info(f"Reading latest snapshots from {args.snapshots_dir}...")
    try:
        records = fetch_validator_records(args.snapshots_dir)
    except Exception as e:
        logger.error(f"Failed: {e}")
        return 1
    logger.info(f"Found {len(records)} validator records across all clusters")
    if not records:
        logger.warning("No records found — check snapshot files")
        return 1

    asn_entries, dc_entries = aggregate(records)
    logger.info(f"Unique ASNs: {len(asn_entries)}, unique DCs: {len(dc_entries)}")

    render_yaml(asn_entries, dc_entries, args.output)
    logger.info(f"Template written to {args.output}")
    logger.info(f"Top 10 ASNs by total stake:")
    for e in asn_entries[:10]:
        logger.info(f"  {e['asn']:12} {(e['asn_name'] or '(unknown)')[:35]:37} "
                    f"validators={e['validator_count']:3}  stake={e['stake_pct_total']:6.2f}%")
    return 0


if __name__ == "__main__":
    exit(main())
