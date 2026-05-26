# automation/

Orchestration layer — runs analyzer per-cluster on intervals, processes
output, uploads to R2, sends Telegram alerts.

## Files

### Orchestrator
- **`run_sonda.py`** — main process. Reads config, spawns worker threads
  per cluster. Each worker on its own interval runs: analyze → split →
  upload → record → telegram. Started by systemd, NOT meant to be run
  by hand in production.
- **`run_once.py`** — diagnostic. Runs the full pipeline once for one
  cluster and exits. Useful for testing changes before restarting service.

### Pipeline steps (called by orchestrator)
- **`split_snapshot.py`** — splits the full snapshot JSON (5+ MB) into
  4 smaller cluster-specific files (`network_summary.json`, `validators.json`,
  `rpc.json`, `infrastructure.json`) for frontend consumption.
- **`r2_upload.py`** — uploads split files to Cloudflare R2 bucket
  (`sonda-data`, exposed via `data.sonda.network`).
- **`timeseries.py`** — analyzes the snapshot against historical state
  in SQLite (`/home/solya/sonda_data/timeseries.db`). Records changes
  (node_changes, ip_changes), tracks cluster_state (genesis_hash, slot)
  for rollback detection, builds epoch summaries when epoch wraps.
- **`telegram.py`** — sends events to debug (private) and public Telegram
  channels based on `config.yaml` settings.

### Utilities
- **`split_history.py`** — one-time tool that split historical validator
  data (from `solana_history.py` output) into per-validator files. Already
  ran; kept for archival reference.
- **`diagnose_geo_cache.py`** — debugging tool. Inspects geo cache state,
  shows hit/miss patterns, recent failures.

### Config
- **`config.example.yaml`** — template with placeholders.
- **`config.yaml`** — real config (NEVER committed, lives only on server).
  Copy from example and fill in API keys, R2 credentials, Telegram tokens.

## First-time setup

```bash
cd /home/solya/sonda/automation
cp config.example.yaml config.yaml
nano config.yaml
# Fill in:
#   - api_keys.dbip_key (your DB-IP key)
#   - api_keys.ipinfo_token (your IPInfo token)
#   - r2.* (Cloudflare R2 credentials)
#   - telegram.* (bot tokens and chat IDs)
#   - clusters.*.rpc_url (if different from defaults)
```

Then install the systemd unit (see `../systemd/README.md`).

## Configuration reference

See `config.example.yaml` for all available options. Key sections:

- **`clusters`** — per-cluster settings. Each cluster needs `interval`
  (seconds), `rpc_url`, `enabled` (boolean). Disabling a cluster makes
  the orchestrator skip its worker entirely.
- **`telegram.public_clusters`** — which clusters' events reach the
  public channel. Do NOT include `alpenglow-community` here.
- **`endpoint_alerts.mode`** — `state_based` (recommended, quiet) or
  `realtime` (noisy but immediate).

## Critical: secrets management

`config.yaml` contains real API keys, R2 credentials, and Telegram bot
tokens. It MUST NOT be committed to git. Protection layers:

1. `.gitignore` blocks `automation/config.yaml` from being staged
2. `.githooks/pre-commit` checks staged content for obvious secret
   patterns (Telegram tokens, Helius API keys) and blocks commit
3. To install the pre-commit hook (one-time):
   ```bash
   cd /home/solya/sonda
   git config core.hooksPath .githooks
   ```

If you ever accidentally commit secrets:
1. Rotate ALL exposed secrets IMMEDIATELY (Helius, DB-IP, IPInfo, R2, Telegram)
2. Force-push corrected history (or accept it's public and rotate)
3. Github bots scan public repos for tokens within minutes

## Logs

- `/home/solya/sonda/automation/logs/run_sonda.log` — orchestrator log
- `journalctl -u sonda.service -f` — service-level log (same content)
- Per-cluster analyzer output:
  `/home/solya/sonda/automation/logs/analyze-{cluster}.log`
