#!/usr/bin/env python3
"""
test_features_rpc.py

Standalone test script that validates fetching feature status via JSON RPC
getProgramAccounts vs the legacy `solana feature status` CLI command.

Purpose: confirm the JSON RPC approach returns accurate feature data for
EVERY cluster, including alpenglow-community where the CLI catalog is
incompatible with the cluster's actual feature set.

Background: `solana feature status` uses the CLI binary's BUILT-IN feature
catalog. On a server running CLI 4.0.0, that catalog reflects testnet's
known features. When we point it at the Alpenglow cluster (different
catalog), 7 catalog entries that don't exist on Alpenglow show up as
"inactive" — these are pure CLI artifacts, not real data.

JSON RPC getProgramAccounts (owner = Feature1111...111) returns the
actual on-chain feature accounts from the cluster itself. This is the
source of truth.

Usage:
    python3 test_features_rpc.py --rpc-url http://84.32.71.43:8899 --cluster alpenglow-community
    python3 test_features_rpc.py --rpc-url https://api.mainnet-beta.solana.com --cluster mainnet-beta
    python3 test_features_rpc.py --rpc-url https://api.testnet.solana.com --cluster testnet
    python3 test_features_rpc.py --rpc-url https://api.devnet.solana.com --cluster devnet

Output:
    - Total accounts found via JSON RPC
    - Count of active / pending / inactive
    - For inactive: shows pubkey + (best-effort description from CLI if --compare-cli)
    - With --compare-cli: cross-checks count against `solana feature status`
"""

import argparse
import base64
import json
import logging
import struct
import subprocess
import sys
import urllib.request
import urllib.error

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

FEATURE_PROGRAM_ID = "Feature111111111111111111111111111111111111"


def rpc_call(url, method, params=None, timeout=30):
    """Generic JSON RPC POST helper."""
    payload = {"jsonrpc": "2.0", "id": 1, "method": method}
    if params is not None:
        payload["params"] = params
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            data = json.loads(r.read())
        if "error" in data:
            raise RuntimeError(f"RPC error: {data['error']}")
        return data.get("result")
    except urllib.error.URLError as e:
        raise RuntimeError(f"RPC URL error: {e}")


def parse_feature_account(account_data_b64):
    """Parse a Feature program account data.

    Layout (9 bytes total):
      - bytes 0-7: activation_slot as Option<u64>:
          * If first byte == 0: None (feature not yet activated)
          * If first byte != 0: Some(u64), but in Rust serde the
            discriminant is the first byte and the slot is bytes 1-9 (8 bytes LE).
        Actually the Solana feature program uses bincode which writes Option as:
          - 0u8 (None) → 1 byte total
          - 1u8 (Some) followed by the u64 value → 9 bytes total

    So:
      - If account data is 9 bytes and starts with 0x01 → activation_slot = u64 from bytes 1-8
      - If account data is 1 byte and equals 0x00 → not activated
      - Otherwise we just say "unknown format"
    """
    raw = base64.b64decode(account_data_b64)
    if not raw:
        return {"status": "empty", "activation_slot": None}
    # bincode Option<u64>: 0x00 = None, 0x01 + u64_LE = Some
    discriminator = raw[0]
    if discriminator == 0:
        return {"status": "pending", "activation_slot": None}
    if discriminator == 1 and len(raw) >= 9:
        slot = struct.unpack("<Q", raw[1:9])[0]
        return {"status": "active", "activation_slot": slot}
    return {"status": "unknown", "activation_slot": None, "raw_len": len(raw)}


def fetch_features_via_rpc(rpc_url):
    """Fetch all feature accounts from cluster via JSON RPC."""
    logger.info(f"📡 Calling getProgramAccounts(owner={FEATURE_PROGRAM_ID})...")
    result = rpc_call(
        rpc_url,
        "getProgramAccounts",
        [FEATURE_PROGRAM_ID, {"encoding": "base64"}],
    )
    if not isinstance(result, list):
        raise RuntimeError(f"Unexpected response shape: {type(result)}")
    logger.info(f"✅ Got {len(result)} feature accounts")

    features = []
    for entry in result:
        pubkey = entry.get("pubkey")
        account = entry.get("account") or {}
        data_field = account.get("data")
        if isinstance(data_field, list) and len(data_field) >= 1:
            data_b64 = data_field[0]
        elif isinstance(data_field, str):
            data_b64 = data_field
        else:
            data_b64 = ""
        parsed = parse_feature_account(data_b64)
        features.append({
            "pubkey": pubkey,
            "status": parsed["status"],
            "activation_slot": parsed["activation_slot"],
        })
    return features


def fetch_descriptions_via_cli(rpc_url, timeout=30):
    """Best-effort: call `solana feature status --display-all` to get descriptions per pubkey.

    The --display-all flag is critical: without it, the CLI hides "old" active
    features (those activated long ago), returning only recent activations.
    With --display-all, we get description for every feature the CLI binary
    knows about, which is what we need for full coverage.

    For cluster-specific features unknown to the CLI binary (e.g. Alpenglow-
    only features when running CLI 4.0.0), no description is available — the
    pubkey will be in our RPC list but absent from CLI output.
    """
    logger.info("📝 Calling `solana feature status --display-all` for descriptions...")
    try:
        r = subprocess.run(
            ["solana", "--url", rpc_url, "feature", "status", "--display-all"],
            capture_output=True, text=True, timeout=timeout,
        )
        if r.returncode != 0:
            logger.warning(f"CLI returned {r.returncode}: {r.stderr[:200]}")
            return {}
    except Exception as e:
        logger.warning(f"CLI call failed: {e}")
        return {}

    descriptions = {}
    for line in r.stdout.splitlines():
        line = line.rstrip()
        if not line:
            continue
        if line.startswith("Feature ") or line.startswith("Software ") \
           or line.startswith("Tool ") or line.startswith("---") \
           or line.startswith("To "):
            continue
        parts = [p.strip() for p in line.split("|")]
        if len(parts) < 4:
            continue
        pubkey = parts[0]
        if not pubkey or len(pubkey) < 32:
            continue
        description = "|".join(parts[3:]).strip()
        if description:
            descriptions[pubkey] = description
    logger.info(f"✅ Got descriptions for {len(descriptions)} features (CLI catalog)")
    return descriptions


def slot_to_epoch(slot, slots_in_epoch):
    """Convert absolute slot to epoch number."""
    if slot is None or slots_in_epoch is None or slots_in_epoch == 0:
        return None
    return slot // slots_in_epoch


def main():
    parser = argparse.ArgumentParser(description="Test feature status via JSON RPC")
    parser.add_argument("--rpc-url", required=True, help="Cluster RPC URL")
    parser.add_argument("--cluster", default="unknown", help="Cluster name (for log clarity)")
    parser.add_argument(
        "--compare-cli", action="store_true",
        help="Also call `solana feature status` for cross-validation",
    )
    parser.add_argument(
        "--show-inactive", action="store_true",
        help="List all inactive/pending features with pubkey + description",
    )
    parser.add_argument(
        "--show-all", action="store_true",
        help="List ALL features (including active)",
    )
    args = parser.parse_args()

    print(f"\n{'='*70}")
    print(f"  Feature RPC test — cluster: {args.cluster}")
    print(f"  RPC URL: {args.rpc_url}")
    print(f"{'='*70}\n")

    # Step 1: get epoch info (for slot → epoch conversion)
    try:
        epoch_info = rpc_call(args.rpc_url, "getEpochInfo")
        slots_in_epoch = epoch_info.get("slotsInEpoch", 432000)
        current_epoch = epoch_info.get("epoch")
        logger.info(f"📅 Current epoch: {current_epoch}, slotsInEpoch: {slots_in_epoch}")
    except Exception as e:
        logger.error(f"❌ getEpochInfo failed: {e}")
        sys.exit(1)

    # Step 2: get all features via JSON RPC
    try:
        features = fetch_features_via_rpc(args.rpc_url)
    except Exception as e:
        logger.error(f"❌ getProgramAccounts failed: {e}")
        sys.exit(1)

    # Step 3: get descriptions via CLI (best effort)
    descriptions = {}
    if args.compare_cli or args.show_inactive or args.show_all:
        descriptions = fetch_descriptions_via_cli(args.rpc_url)

    # Step 4: enrich + categorize
    active = []
    pending = []
    unknown = []
    for f in features:
        f["description"] = descriptions.get(f["pubkey"])  # may be None
        if f["status"] == "active":
            f["activation_epoch"] = slot_to_epoch(f["activation_slot"], slots_in_epoch)
            active.append(f)
        elif f["status"] == "pending":
            pending.append(f)
        else:
            unknown.append(f)

    # Step 5: summary
    print(f"\n📊 Summary for {args.cluster}:")
    print(f"  Total feature accounts on-chain: {len(features)}")
    print(f"  Active:  {len(active)}")
    print(f"  Pending: {len(pending)}")
    print(f"  Unknown format: {len(unknown)}")

    # Features without descriptions = unknown to CLI catalog
    without_descriptions = [f for f in features if not f["description"]]
    if descriptions:
        print(f"\n📝 Description coverage:")
        print(f"  With description (CLI knew): {len(features) - len(without_descriptions)}")
        print(f"  WITHOUT description (CLI catalog doesn't know): {len(without_descriptions)}")
        if without_descriptions:
            print(f"\n  Pubkeys CLI doesn't recognize (likely cluster-specific):")
            for f in without_descriptions[:15]:
                slot = f["activation_slot"]
                epoch = slot_to_epoch(slot, slots_in_epoch) if slot else "n/a"
                print(f"    {f['pubkey'][:44]:46} status={f['status']:8} slot={slot or 'n/a':>10} epoch={epoch}")
            if len(without_descriptions) > 15:
                print(f"    ... and {len(without_descriptions) - 15} more")

    # Step 6: optional detail
    if args.show_inactive:
        print(f"\n⏳ Pending features ({len(pending)}):")
        for f in pending:
            desc = f["description"] or "(no description in CLI catalog)"
            print(f"  {f['pubkey'][:44]:46} {desc[:80]}")

    if args.show_all:
        print(f"\n✅ Active features ({len(active)}):")
        for f in active:
            desc = f["description"] or "(no description)"
            print(f"  {f['pubkey'][:44]:46} epoch={f['activation_epoch']:>5} {desc[:60]}")

    # Step 7: compare CLI count if requested
    if args.compare_cli:
        print(f"\n🔍 CLI cross-check:")
        try:
            r = subprocess.run(
                ["solana", "--url", args.rpc_url, "feature", "status", "--display-all"],
                capture_output=True, text=True, timeout=30,
            )
            cli_active = 0
            cli_pending = 0
            cli_inactive = 0
            for line in r.stdout.splitlines():
                parts = [p.strip() for p in line.split("|")]
                if len(parts) < 4: continue
                pubkey = parts[0]
                status = parts[1]
                if not pubkey or len(pubkey) < 32: continue
                if status.startswith("active "): cli_active += 1
                elif status.startswith("pending"): cli_pending += 1
                elif status == "inactive": cli_inactive += 1
            print(f"  CLI reports: active={cli_active}, pending={cli_pending}, inactive={cli_inactive}")
            print(f"  CLI total:   {cli_active + cli_pending + cli_inactive}")
            print(f"  RPC total:   {len(features)}")
            cli_total = cli_active + cli_pending + cli_inactive
            if cli_total != len(features):
                print(f"  ⚠️  MISMATCH: CLI has {cli_total - len(features):+} extra entries")
                print(f"     These are CLI-catalog ghosts: features in the CLI binary's")
                print(f"     hardcoded list that don't exist as on-chain accounts on this cluster.")
            else:
                print(f"  ✅ CLI and RPC counts match")
        except Exception as e:
            print(f"  ⚠️  CLI call failed: {e}")


if __name__ == "__main__":
    main()
