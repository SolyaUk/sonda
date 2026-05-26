#!/usr/bin/env python3
"""
r2_upload.py
============
Uploads SONDA data files to Cloudflare R2.

Three modes:

1. Cluster mode — uploads split files from current/{cluster}/*.json:
     python r2_upload.py --config config.yaml --cluster mainnet-beta
     python r2_upload.py --config config.yaml --cluster all
     python r2_upload.py --config config.yaml --cluster mainnet-beta --dry-run

2. Single-file mode — uploads one arbitrary file to a given R2 key
   (used by run_sonda.py for SQLite backup uploads):
     python r2_upload.py --config config.yaml --file /path/file --key backups/x.db.gz

3. Programmatic use — upload_epoch_snapshot() can be imported by other
   scripts to push gzipped epoch snapshots to epochs/{cluster}/epoch-N.json.gz.

Exit code: 0 on success, 1 on any failure.
"""

import argparse
import logging
import mimetypes
import sys
import time
from pathlib import Path

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Known clusters. Kept as a constant so --cluster all works even if config
# doesn't list them or uses different casing.
CLUSTERS = ["mainnet-beta", "testnet", "devnet", "alpenglow-community"]

# Files produced by split_snapshot.py that we upload in cluster mode
CURRENT_FILES = [
    "network_summary.json",
    "validators.json",
    "rpc.json",
    "infrastructure.json",
]


def load_config(config_path: str) -> dict:
    import yaml
    with open(config_path) as f:
        return yaml.safe_load(f)


def make_s3_client(cfg: dict):
    """Create a boto3 S3-compatible client for Cloudflare R2."""
    r2 = cfg["r2"]
    endpoint = f"https://{r2['account_id']}.r2.cloudflarestorage.com"
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=r2["access_key_id"],
        aws_secret_access_key=r2["secret_access_key"],
        region_name="auto",
        config=BotoConfig(
            signature_version="s3v4",
            retries={"max_attempts": 3, "mode": "standard"},
        ),
    )


def guess_content_type(path: Path) -> str:
    """Pick a sensible Content-Type. JSON and gzip get explicit types so that
    browsers/CDN always recognize them regardless of system mimetypes quirks."""
    name = path.name.lower()
    if name.endswith(".json"):
        return "application/json"
    if name.endswith(".gz"):
        return "application/gzip"
    ct, _ = mimetypes.guess_type(str(path))
    return ct or "application/octet-stream"


def upload_file(s3, bucket: str, local_path: Path, r2_key: str,
                dry_run: bool = False) -> bool:
    """Upload a single file to R2. Returns True on success, False on failure.

    Files get a short CacheControl (60s) so freshly-uploaded data is visible
    through the public CDN within a minute — clients don't need cache-busting.
    """
    if not local_path.exists():
        logger.warning(f"  ⚠️  File not found, skipping: {local_path}")
        return False

    size_kb = local_path.stat().st_size // 1024
    content_type = guess_content_type(local_path)

    if dry_run:
        logger.info(f"  [DRY RUN] Would upload: {local_path.name} ({size_kb}KB) → "
                    f"{r2_key} [{content_type}]")
        return True

    try:
        t0 = time.time()
        s3.upload_file(
            str(local_path),
            bucket,
            r2_key,
            ExtraArgs={
                "ContentType": content_type,
                "CacheControl": "public, max-age=60",
            },
        )
        elapsed = time.time() - t0
        logger.info(f"  ✅ {local_path.name} ({size_kb}KB) → {r2_key} [{elapsed:.1f}s]")
        return True
    except ClientError as e:
        logger.error(f"  ❌ Failed to upload {local_path.name}: {e}")
        return False
    except Exception as e:
        logger.error(f"  ❌ Unexpected error uploading {local_path.name}: {e}")
        return False


def upload_cluster(s3, cfg: dict, cluster: str, dry_run: bool = False) -> dict:
    """Upload all current/{cluster}/*.json files. Returns stats dict."""
    data_dir = Path(cfg["paths"]["data_dir"])
    bucket = cfg["r2"]["bucket"]
    local_dir = data_dir / "current" / cluster

    logger.info(f"📤 Uploading {cluster}...")

    if not local_dir.exists():
        logger.warning(f"  ⚠️  Directory not found: {local_dir}")
        return {"ok": 0, "failed": 0, "skipped": len(CURRENT_FILES)}

    ok = failed = skipped = 0

    for filename in CURRENT_FILES:
        local_path = local_dir / filename
        r2_key = f"current/{cluster}/{filename}"
        result = upload_file(s3, bucket, local_path, r2_key, dry_run)
        if result:
            ok += 1
        elif local_path.exists():
            failed += 1
        else:
            skipped += 1

    logger.info(f"  📊 {cluster}: {ok} uploaded, {failed} failed, {skipped} skipped")
    return {"ok": ok, "failed": failed, "skipped": skipped}


def upload_epoch_snapshot(s3, cfg: dict, cluster: str, local_path: str,
                          epoch: int, dry_run: bool = False) -> bool:
    """Upload a gzipped epoch snapshot to R2 at epochs/{cluster}/epoch-N.json.gz.
    Programmatic entry point for scripts that want to push epoch archives."""
    bucket = cfg["r2"]["bucket"]
    r2_key = f"epochs/{cluster}/epoch-{epoch}.json.gz"
    path = Path(local_path)
    return upload_file(s3, bucket, path, r2_key, dry_run=dry_run)


def main():
    parser = argparse.ArgumentParser(description="Upload SONDA files to Cloudflare R2")
    parser.add_argument("--config", required=True, help="Path to config.yaml")
    parser.add_argument("--cluster", default=None,
                        help="Cluster to upload: mainnet-beta, testnet, devnet, alpenglow-community, or all")
    parser.add_argument("--file", default=None,
                        help="Single file to upload (use with --key)")
    parser.add_argument("--key", default=None,
                        help="R2 key for --file upload "
                             "(e.g. backups/timeseries-2026-04-24.db.gz)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be uploaded without actually uploading")
    args = parser.parse_args()

    # Validate mutually exclusive modes
    if args.file or args.key:
        if not (args.file and args.key):
            parser.error("--file and --key must be used together")
        if args.cluster:
            parser.error("--cluster can't be combined with --file/--key")
    elif not args.cluster:
        parser.error("either --cluster or --file+--key is required")

    cfg = load_config(args.config)
    s3 = make_s3_client(cfg)

    if args.dry_run:
        logger.info("🔍 DRY RUN mode — no files will be uploaded")

    # --- Single-file mode ---
    if args.file:
        src = Path(args.file)
        if not src.exists():
            logger.error(f"File not found: {src}")
            sys.exit(2)
        bucket = cfg["r2"]["bucket"]
        success = upload_file(s3, bucket, src, args.key, dry_run=args.dry_run)
        if success:
            logger.info(f"Upload complete: {args.key}")
            sys.exit(0)
        sys.exit(1)

    # --- Cluster mode ---
    clusters = CLUSTERS if args.cluster == "all" else [args.cluster]

    total_ok = total_failed = 0
    for cluster in clusters:
        stats = upload_cluster(s3, cfg, cluster, dry_run=args.dry_run)
        total_ok += stats["ok"]
        total_failed += stats["failed"]

    print(f"\nUpload complete: {total_ok} OK, {total_failed} failed")
    sys.exit(1 if total_failed > 0 else 0)


if __name__ == "__main__":
    main()