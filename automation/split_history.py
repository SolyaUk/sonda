#!/usr/bin/env python3
"""
split_history.py
================
One-time script: strips _progress from history files and splits
into per-validator files ready for R2 upload.

Input:  validator_history_{cluster}.json (from solana_history.py)
Output: {data_dir}/history/{cluster}/
          _index.json              — list of all identities with metadata
          {identity}.json          — per-validator location_changes (~5KB each)

Usage:
  python split_history.py --config /path/to/config.yaml
  python split_history.py --config /path/to/config.yaml --cluster mainnet-beta
  python split_history.py --config /path/to/config.yaml --dry-run

Input file locations (read from config paths.data_dir or defaults):
  ~/sonda_data/imports/validator_history_mainnet-beta.json
  ~/sonda_data/imports/validator_history_testnet.json
  ~/sonda_data/imports/validator_history_devnet.json
"""

import argparse
import json
import logging
import time
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

CLUSTER_INPUT_FILES = {
    "mainnet-beta": "/home/solya/sonda_data/imports/validator_history_mainnet-beta.json",
    "testnet":      "/home/solya/sonda_data/imports/validator_history_testnet.json",
    "devnet":       "/home/solya/sonda_data/imports/validator_history_devnet.json",
}

CLUSTERS = ["mainnet-beta", "testnet", "devnet"]


def load_config(config_path: str) -> dict:
    import yaml
    with open(config_path) as f:
        return yaml.safe_load(f)


def split_history(input_path: str, output_dir: Path, dry_run: bool = False) -> dict:
    """
    Strips _progress, splits into per-validator files and _index.json.
    Returns stats dict.
    """
    logger.info(f"📂 Loading: {input_path}")

    with open(input_path) as f:
        data = json.load(f)

    meta = data.get("meta", {})
    cluster = meta.get("cluster", "unknown")
    epoch_dates = data.get("epoch_dates", {})
    validators = data.get("validators", {})

    # _progress contains geo_lookup (private IPs) — strip it
    had_progress = "_progress" in data
    geo_lookup_count = len((data.get("_progress") or {}).get("geo_lookup", {}))
    if had_progress:
        logger.info(f"  🗑️  Stripped _progress (geo_lookup: {geo_lookup_count} IPs)")

    logger.info(f"  cluster={cluster}, validators={len(validators)}, epoch_dates={len(epoch_dates)}")

    if dry_run:
        logger.info(f"  [DRY RUN] Would write {len(validators)} validator files + _index.json")
        return {"cluster": cluster, "total": len(validators), "with_lc": 0, "empty": 0}

    output_dir.mkdir(parents=True, exist_ok=True)

    index = []
    with_lc = empty = 0

    for vote_account, vd in validators.items():
        identity = vd.get("identity", "")
        location_changes = vd.get("location_changes", [])

        # Index entry
        index_entry = {
            "identity": identity,
            "vote_account": vote_account,
            "location_changes_count": len(location_changes),
        }

        # Add first/last country if available
        if location_changes:
            first = location_changes[0]
            last = location_changes[-1]
            index_entry["first_epoch"] = first.get("from_epoch")
            index_entry["last_epoch"] = last.get("to_epoch")
            index_entry["current_country"] = last.get("country_code", "")
            index_entry["current_asn"] = last.get("asn", "")
            with_lc += 1
        else:
            empty += 1

        index.append(index_entry)

        # Per-validator file — only write if identity exists
        if not identity:
            continue

        validator_data = {
            "identity": identity,
            "vote_account": vote_account,
            "cluster": cluster,
            "location_changes": location_changes,
        }

        out_path = output_dir / f"{identity}.json"
        with open(out_path, "w") as f:
            json.dump(validator_data, f, separators=(",", ":"), ensure_ascii=False)

    # Write _index.json
    index_data = {
        "cluster": cluster,
        "meta": meta,
        "epoch_dates": epoch_dates,
        "validators": index,
    }
    index_path = output_dir / "_index.json"
    with open(index_path, "w") as f:
        json.dump(index_data, f, separators=(",", ":"), ensure_ascii=False)

    index_size = index_path.stat().st_size // 1024
    total_files = len(list(output_dir.glob("*.json")))

    logger.info(f"  ✅ _index.json: {index_size}KB")
    logger.info(f"  ✅ {with_lc} validator files with location data")
    logger.info(f"  ℹ️  {empty} validators with empty location_changes (written to index only)")
    logger.info(f"  📁 Total files: {total_files} in {output_dir}")

    return {
        "cluster": cluster,
        "total": len(validators),
        "with_lc": with_lc,
        "empty": empty,
        "output_dir": str(output_dir),
    }


def upload_history_to_r2(cfg: dict, cluster: str, output_dir: Path) -> dict:
    """Upload all history files for a cluster to R2."""
    import boto3

    r2 = cfg["r2"]
    endpoint = f"https://{r2['account_id']}.r2.cloudflarestorage.com"
    bucket = r2["bucket"]

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=r2["access_key_id"],
        aws_secret_access_key=r2["secret_access_key"],
        region_name="auto",
    )

    files = list(output_dir.glob("*.json"))
    logger.info(f"📤 Uploading {len(files)} files for {cluster} to R2...")

    ok = failed = 0
    t0 = time.time()

    for i, local_path in enumerate(files):
        r2_key = f"history/{cluster}/{local_path.name}"
        try:
            s3.upload_file(
                str(local_path), bucket, r2_key,
                ExtraArgs={"ContentType": "application/json"},
            )
            ok += 1
        except Exception as e:
            logger.error(f"  ❌ Failed: {local_path.name}: {e}")
            failed += 1

        # Progress every 100 files
        if (i + 1) % 100 == 0:
            pct = (i + 1) / len(files) * 100
            elapsed = time.time() - t0
            logger.info(f"  📦 {i+1}/{len(files)} ({pct:.0f}%) — {elapsed:.0f}s elapsed")

    elapsed = time.time() - t0
    logger.info(f"  ✅ {ok} uploaded, {failed} failed in {elapsed:.1f}s")
    return {"ok": ok, "failed": failed}


def main():
    parser = argparse.ArgumentParser(description="Split and upload SONDA validator history")
    parser.add_argument("--config", required=True, help="Path to config.yaml")
    parser.add_argument("--cluster", default="all",
                        help="Cluster: mainnet-beta, testnet, devnet, or all (default: all)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be done without writing files")
    parser.add_argument("--split-only", action="store_true",
                        help="Split files but do not upload to R2")
    parser.add_argument("--upload-only", action="store_true",
                        help="Upload already-split files (skip splitting)")
    args = parser.parse_args()

    cfg = load_config(args.config)
    data_dir = Path(cfg["paths"]["data_dir"])

    clusters = CLUSTERS if args.cluster == "all" else [args.cluster]

    for cluster in clusters:
        logger.info(f"\n{'='*50}")
        logger.info(f"Processing: {cluster}")
        logger.info(f"{'='*50}")

        output_dir = data_dir / "history" / cluster
        input_file = CLUSTER_INPUT_FILES[cluster]

        if not args.upload_only:
            if not Path(input_file).exists():
                logger.warning(f"  ⚠️  Input file not found: {input_file} — skipping")
                continue

            stats = split_history(input_file, output_dir, dry_run=args.dry_run)

            print(f"\n  {cluster}: {stats['total']} total, "
                  f"{stats['with_lc']} with history, {stats['empty']} empty")

        if not args.dry_run and not args.split_only:
            if not output_dir.exists():
                logger.warning(f"  ⚠️  Output dir not found: {output_dir} — run without --upload-only first")
                continue

            upload_stats = upload_history_to_r2(cfg, cluster, output_dir)
            print(f"  Upload: {upload_stats['ok']} OK, {upload_stats['failed']} failed")


if __name__ == "__main__":
    main()