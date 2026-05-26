#!/usr/bin/env python3
"""
run_once.py
===========
Runs one full cycle for a given cluster:
  1. solana_analyzer.py  → snapshot JSON
  2. split_snapshot.py   → current/{cluster}/ files
  3. r2_upload.py        → upload to R2

Usage:
  python run_once.py --config /path/to/config.yaml --cluster mainnet-beta
  python run_once.py --config /path/to/config.yaml --cluster testnet
"""

import argparse
import json
import logging
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

AUTOMATION_DIR = Path(__file__).parent


def load_config(config_path: str) -> dict:
    import yaml
    with open(config_path) as f:
        return yaml.safe_load(f)


def run_analyzer(cfg: dict, cluster: str, snapshot_path: Path) -> bool:
    """Run solana_analyzer.py and save snapshot. Returns True on success."""
    sonda_dir = Path(cfg["paths"]["sonda_scripts"])
    analyzer = sonda_dir / "solana_analyzer.py"
    endpoints = sonda_dir / "endpoints.yaml"
    geo_overrides = sonda_dir / "geo_overrides.yaml"

    cluster_cfg = cfg["clusters"][cluster]
    rpc_url = cluster_cfg.get("rpc_url", "")

    cmd = [
        sys.executable, str(analyzer),
        "--dbip-key", cfg["api_keys"]["dbip_key"],
        "--ipinfo-token", cfg["api_keys"]["ipinfo_token"],
        "--cluster", cluster,
        "--export",
        "--output", str(snapshot_path),
    ]

    if rpc_url:
        cmd += ["--rpc-url", rpc_url]
    if endpoints.exists():
        cmd += ["--endpoints", str(endpoints)]
    if geo_overrides.exists():
        cmd += ["--geo-overrides", str(geo_overrides)]

    logger.info(f"🔬 Running analyzer for {cluster}...")
    t0 = time.time()

    try:
        result = subprocess.run(
            cmd,
            capture_output=False,   # let logs flow to stdout
            timeout=300,            # 5 min max
        )
        elapsed = time.time() - t0

        if result.returncode == 0:
            size_kb = snapshot_path.stat().st_size // 1024 if snapshot_path.exists() else 0
            logger.info(f"  ✅ Analyzer done in {elapsed:.1f}s, snapshot: {size_kb}KB")
            return True
        else:
            logger.error(f"  ❌ Analyzer exited with code {result.returncode} after {elapsed:.1f}s")
            return False

    except subprocess.TimeoutExpired:
        logger.error(f"  ❌ Analyzer timed out after 300s")
        return False
    except Exception as e:
        logger.error(f"  ❌ Analyzer error: {e}")
        return False


def run_split(config_path: str, snapshot_path: Path) -> bool:
    """Run split_snapshot.py. Returns True on success."""
    split_script = AUTOMATION_DIR / "split_snapshot.py"

    logger.info(f"✂️  Splitting snapshot...")
    t0 = time.time()

    try:
        result = subprocess.run(
            [sys.executable, str(split_script),
             "--snapshot", str(snapshot_path),
             "--config", config_path],
            capture_output=False,
            timeout=60,
        )
        elapsed = time.time() - t0

        if result.returncode == 0:
            logger.info(f"  ✅ Split done in {elapsed:.1f}s")
            return True
        else:
            logger.error(f"  ❌ Split failed (code {result.returncode})")
            return False

    except Exception as e:
        logger.error(f"  ❌ Split error: {e}")
        return False


def run_upload(config_path: str, cluster: str) -> bool:
    """Run r2_upload.py. Returns True on success."""
    upload_script = AUTOMATION_DIR / "r2_upload.py"

    logger.info(f"📤 Uploading to R2...")
    t0 = time.time()

    try:
        result = subprocess.run(
            [sys.executable, str(upload_script),
             "--config", config_path,
             "--cluster", cluster],
            capture_output=False,
            timeout=120,
        )
        elapsed = time.time() - t0

        if result.returncode == 0:
            logger.info(f"  ✅ Upload done in {elapsed:.1f}s")
            return True
        else:
            logger.error(f"  ❌ Upload failed (code {result.returncode})")
            return False

    except Exception as e:
        logger.error(f"  ❌ Upload error: {e}")
        return False


def run_once(config_path: str, cluster: str) -> dict:
    """
    Full cycle: analyze → split → upload.
    Returns result dict with success flag and timing.
    """
    cfg = load_config(config_path)
    data_dir = Path(cfg["paths"]["data_dir"])

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
    cluster_short = cluster.replace("-beta", "")
    snapshot_path = data_dir / "snapshots" / f"sonda-{cluster_short}-{ts}.json"
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)

    t_start = time.time()
    logger.info(f"\n{'='*55}")
    logger.info(f"  run_once: {cluster} — {ts}")
    logger.info(f"{'='*55}")

    # Step 1: Analyzer
    ok_analyze = run_analyzer(cfg, cluster, snapshot_path)
    if not ok_analyze:
        return {"success": False, "stage": "analyze", "cluster": cluster,
                "elapsed": time.time() - t_start}

    # Step 2: Split
    ok_split = run_split(config_path, snapshot_path)
    if not ok_split:
        return {"success": False, "stage": "split", "cluster": cluster,
                "elapsed": time.time() - t_start}

    # Step 3: Upload
    ok_upload = run_upload(config_path, cluster)
    if not ok_upload:
        return {"success": False, "stage": "upload", "cluster": cluster,
                "elapsed": time.time() - t_start}

    elapsed = time.time() - t_start
    logger.info(f"✅ run_once complete: {cluster} in {elapsed:.1f}s")

    # Clean up old snapshots — keep only last 3 per cluster
    snapshots = sorted(data_dir.glob(f"snapshots/sonda-{cluster_short}-*.json"))
    for old in snapshots[:-3]:
        try:
            old.unlink()
            logger.info(f"  🗑️  Removed old snapshot: {old.name}")
        except Exception:
            pass

    return {"success": True, "cluster": cluster, "elapsed": elapsed,
            "snapshot": str(snapshot_path)}


def main():
    parser = argparse.ArgumentParser(description="Run one full SONDA cycle: analyze → split → upload")
    parser.add_argument("--config", required=True, help="Path to config.yaml")
    parser.add_argument("--cluster", default="mainnet-beta",
                        choices=["mainnet-beta", "testnet", "devnet", "alpenglow-community"],
                        help="Cluster to run (default: mainnet-beta)")
    args = parser.parse_args()

    result = run_once(args.config, args.cluster)

    if result["success"]:
        print(f"\n✅ Success: {result['cluster']} in {result['elapsed']:.1f}s")
        sys.exit(0)
    else:
        print(f"\n❌ Failed at stage: {result['stage']} ({result['elapsed']:.1f}s)")
        sys.exit(1)


if __name__ == "__main__":
    main()