# analyzer/

Core SONDA analysis pipeline — collects validator/infrastructure data
from Solana clusters and produces JSON snapshots.

## Files

- **`solana_analyzer.py`** (~2800 lines) — main analyzer.
  Single-run script that:
  1. Fetches gossip + validators from RPC
  2. Loads endpoints from `endpoints.yaml` and checks reachability
  3. Fetches genesis_hash, features, BLS pubkeys (Alpenglow only)
  4. Geolocates all IPs (DB-IP primary, IPInfo + GeoJS verification)
  5. Fetches DoubleZero, Jito BAM, Rakurai status (not on Alpenglow)
  6. Writes a snapshot JSON to disk

- **`solana_history.py`** (~1250 lines) — historical collector.
  Pulls full validator location history from Jito kobe API (mainnet,
  epoch 500+) and SFDP API (pre-Jito epochs). Initial run completed
  in April 2026 (results in `/home/solya/sonda_data/history/`).
  Re-run anytime to fill gaps between historical data and the moment
  regular SONDA collection started — see "Re-running history" below.

- **`endpoints.yaml`** — endpoint definitions per cluster
  (Solana entrypoints, RPC official, Jito BAM/block-engine, Harmonic, etc.)
  used for reachability checks. Add new clusters here when expanding.

- **`geo_overrides.yaml`** — manual geolocation overrides for IPs where
  automatic providers disagree or are wrong. Mostly DoubleZero devices
  (verified by the DZ team). Safe to commit — no secrets.

- **`geo_overrides.example.yaml`** — template for the above.

## Usage (standalone, not via orchestrator)

```bash
# Run analyzer once for a cluster, write snapshot to file
python3 /home/solya/sonda/analyzer/solana_analyzer.py \
    --cluster mainnet-beta \
    --rpc-url https://mainnet.helius-rpc.com/?api-key=YOUR_KEY \
    --dbip-key YOUR_KEY \
    --ipinfo-token YOUR_TOKEN \
    --endpoints /home/solya/sonda/analyzer/endpoints.yaml \
    --geo-overrides /home/solya/sonda/analyzer/geo_overrides.yaml \
    --export \
    --output /tmp/test-snapshot.json
```

In production, the orchestrator (`automation/run_sonda.py`) calls the
analyzer as a subprocess every cycle (60s mainnet/alpenglow, 300s testnet/
devnet) with the right arguments from `automation/config.yaml`.

## Adding a new cluster

See system prompt section 8.5 "Cluster names hardcoded in many places" —
adding a new cluster requires changes in multiple files:

1. `analyzer/solana_analyzer.py`:
   - `--cluster choices` (argparse)
   - `CLUSTER_URLS` dict
   - `CLUSTER_YAML_MAP` dict
2. `analyzer/endpoints.yaml` — new cluster section
3. `automation/run_once.py` — `--cluster choices`
4. `automation/r2_upload.py` — `CLUSTERS` list
5. `automation/config.yaml` — new cluster block

Frontend (sonda.network, separate repo) needs:
6. Cluster switcher dropdown
7. Cluster-specific routing

## Re-running history (filling gaps)

`solana_history.py` can be re-run anytime to refresh or extend historical
data. Useful when there's a gap between the initial import and when
regular SONDA collection started (typical scenario when bringing up
frontend history features long after backend went live).

The convention: input files for `split_history.py` live in
`/home/solya/sonda_data/imports/`. `solana_history.py` writes
`validator_history_{cluster}.json` to the current working directory,
so `cd` there first:

```bash
# Ensure imports directory exists
mkdir -p /home/solya/sonda_data/imports

# Collect history for one cluster
cd /home/solya/sonda_data/imports
python3 /home/solya/sonda/analyzer/solana_history.py \
    --cluster mainnet-beta \
    --rpc-url https://mainnet.helius-rpc.com/?api-key=YOUR_KEY

# Output: /home/solya/sonda_data/imports/validator_history_mainnet-beta.json
# (file naming follows {suffix}.json where suffix is the cluster name)
```

Then split it into per-validator R2-ready files:

```bash
python3 /home/solya/sonda/automation/split_history.py \
    --config /home/solya/sonda/automation/config.yaml \
    --cluster mainnet-beta
# Writes per-validator files to /home/solya/sonda_data/history/mainnet-beta/
```

Finally upload to R2. `r2_upload.py` in cluster mode handles only
`current/` files, so for `history/` use either:

Option A — recursive aws-cli (if installed, configured with R2 keys):
```bash
aws s3 sync /home/solya/sonda_data/history/mainnet-beta/ \
    s3://sonda-data/history/mainnet-beta/ \
    --endpoint-url https://YOUR_CF_ACCOUNT_ID.r2.cloudflarestorage.com
```

Option B — file-by-file via `r2_upload.py --file --key`:
```bash
cd /home/solya/sonda_data/history/mainnet-beta
for f in *.json; do
    python3 /home/solya/sonda/automation/r2_upload.py \
        --config /home/solya/sonda/automation/config.yaml \
        --file "$f" \
        --key "history/mainnet-beta/$f"
done
```

Option B is slower (~1 sec per file × 780 validators on mainnet) but
requires no extra tools.

**Time estimates:**
- `solana_history.py mainnet-beta`: 2-4 hours (Jito kobe API rate-limited)
- `solana_history.py testnet`: 30-60 min
- `solana_history.py devnet`: ~5 min (less data)
- `split_history.py`: seconds
- `r2_upload.py history`: 1-2 min per cluster

`solana_history.py` writes a `_progress` field that allows resumption if
interrupted. Re-run the same command — it picks up where it left off.
