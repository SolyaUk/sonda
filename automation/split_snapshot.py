#!/usr/bin/env python3
"""
split_snapshot.py
=================
Takes a full solana_analyzer.py snapshot and splits it into
separate files for R2 upload, organized by cluster and category.

Output structure (mirrors R2 bucket):
  {data_dir}/current/{cluster}/
    network_summary.json   — meta + metrics (no records)
    validators.json        — validator/validator-hidden/validator-inactive + co-hosted
    rpc.json               — rpc nodes
    infrastructure.json    — jito-*/harmonic-*/solana-*/dz-device

Usage:
  python split_snapshot.py --snapshot /path/to/snapshot.json --data-dir /path/to/sonda_data
  python split_snapshot.py --config /path/to/config.yaml --snapshot /path/to/snapshot.json
"""

import json
import argparse
import logging
from pathlib import Path
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Role groupings
VALIDATOR_ROLES = {"validator", "validator-hidden", "validator-inactive", "co-hosted"}
RPC_ROLES = {"rpc"}
INFRASTRUCTURE_ROLES = {
    "jito-block-engine", "jito-shred-receiver", "jito-ntp", "jito-bam",
    "harmonic-auction", "harmonic-tpu-relayer", "harmonic-shred-receiver", "harmonic-bundles",
    "solana-rpc-official", "solana-entrypoint",
    "dz-device",
}
# unknown-node and backup-node stay server-side only, not uploaded to R2


def split_snapshot(snapshot_path: str, data_dir: str) -> dict:
    """
    Split a full snapshot into category files.
    Returns dict with file paths and record counts.
    """
    logger.info(f"📂 Loading snapshot: {snapshot_path}")
    with open(snapshot_path) as f:
        snap = json.load(f)

    cluster = snap.get("cluster", "mainnet-beta")
    epoch = snap.get("epoch")
    timestamp = snap.get("timestamp", datetime.now(timezone.utc).isoformat())
    records = snap.get("records", [])

    logger.info(f"  cluster={cluster}, epoch={epoch}, records={len(records)}")

    # Prepare output directory
    out_dir = Path(data_dir) / "current" / cluster
    out_dir.mkdir(parents=True, exist_ok=True)

    # --- network_summary.json: everything except records ---
    summary = {k: v for k, v in snap.items() if k != "records"}
    summary["split_at"] = datetime.now(timezone.utc).isoformat()

    # --- Split records by role ---
    validators = []
    rpc_nodes = []
    infrastructure = []
    skipped = 0

    for rec in records:
        role = rec.get("role", "unknown-node")
        if role in VALIDATOR_ROLES:
            validators.append(rec)
        elif role in RPC_ROLES:
            rpc_nodes.append(rec)
        elif role in INFRASTRUCTURE_ROLES:
            infrastructure.append(rec)
        else:
            # unknown-node, backup-node — server-side only
            skipped += 1

    # --- Write files ---
    files = {
        "network_summary.json": summary,
        "validators.json": {
            "meta": {"cluster": cluster, "epoch": epoch, "timestamp": timestamp,
                     "count": len(validators)},
            "records": validators,
        },
        "rpc.json": {
            "meta": {"cluster": cluster, "epoch": epoch, "timestamp": timestamp,
                     "count": len(rpc_nodes)},
            "records": rpc_nodes,
        },
        "infrastructure.json": {
            "meta": {"cluster": cluster, "epoch": epoch, "timestamp": timestamp,
                     "count": len(infrastructure)},
            "records": infrastructure,
        },
    }

    written = {}
    for filename, data in files.items():
        path = out_dir / filename
        with open(path, "w") as f:
            json.dump(data, f, separators=(",", ":"), ensure_ascii=False)
        size_kb = path.stat().st_size // 1024
        written[filename] = str(path)
        logger.info(f"  ✅ {filename}: {size_kb}KB")

    logger.info(f"  ℹ️  Skipped {skipped} unknown/backup nodes (server-side only)")
    logger.info(f"  📁 Output: {out_dir}")

    return {
        "cluster": cluster,
        "epoch": epoch,
        "out_dir": str(out_dir),
        "files": written,
        "counts": {
            "validators": len(validators),
            "rpc": len(rpc_nodes),
            "infrastructure": len(infrastructure),
            "skipped": skipped,
        },
    }


def main():
    parser = argparse.ArgumentParser(description="Split SONDA snapshot into R2-ready files")
    parser.add_argument("--snapshot", required=True, help="Path to full snapshot JSON")
    parser.add_argument("--data-dir", default="/home/solya/sonda_data",
                        help="Base data directory (default: /home/solya/sonda_data)")
    parser.add_argument("--config", help="Path to config.yaml (to read data_dir from)")
    args = parser.parse_args()

    data_dir = args.data_dir
    if args.config:
        try:
            import yaml
            with open(args.config) as f:
                cfg = yaml.safe_load(f)
            data_dir = cfg.get("paths", {}).get("data_dir", data_dir)
        except Exception as e:
            logger.warning(f"Could not read config: {e}, using default data_dir")

    result = split_snapshot(args.snapshot, data_dir)

    print(f"\nSplit complete:")
    print(f"  validators:     {result['counts']['validators']}")
    print(f"  rpc:            {result['counts']['rpc']}")
    print(f"  infrastructure: {result['counts']['infrastructure']}")
    print(f"  skipped:        {result['counts']['skipped']}")
    print(f"  output:         {result['out_dir']}")


if __name__ == "__main__":
    main()
