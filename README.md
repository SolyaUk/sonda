# SONDA

**Solana Observatory for Network Decentralization Analysis**

> Network analytics and tactical infrastructure intelligence for Solana validator operators.

[![Status](https://img.shields.io/badge/status-live-brightgreen)]()
[![Python](https://img.shields.io/badge/python-3.10+-blue)]()
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

**Live dashboard:** [sonda.network](https://sonda.network)
**Public data:** [data.sonda.network](https://data.sonda.network/current/mainnet-beta/)
**Project Twitter:** [@SondaNetwork](https://x.com/SondaNetwork)

> [!NOTE]
> SONDA is built and maintained by [Solya Validator](https://solya.studio) as a public good. The pipeline runs continuously on production, updating every 60 seconds for mainnet and `alpenglow-community`, every 5 minutes for testnet and devnet. Phase 1 dashboard shipped as part of [Colosseum Frontier 2026](https://colosseum.org/frontier). Phase 2 pages (per-validator, per-datacenter) are in active development.

---

## Contents

- [Why SONDA?](#why-sonda)
- [Who is this for?](#who-is-this-for)
- [What SONDA Analyzes](#what-sonda-analyzes)
- [Repository Layout](#repository-layout)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Output Format](#output-format)
- [Production Status](#production-status)
- [Roadmap](#roadmap)
- [Built By](#built-by)
- [Contributing](#contributing)

---

## Why SONDA?

SONDA is built for a specific question: how to make tactical decisions about Solana validator infrastructure. Operators face concrete trade-offs every week, and the data needed to answer them is scattered across half a dozen tools.

**Tactical placement for operators.** Where should the next validator node go? Which cities and ASNs already concentrate too much stake? Which datacenters have a track record of stability versus recurring delinquency? SONDA aggregates the data you would otherwise collect by hand.

**Per-validator and per-datacenter history.** Validators move datacenters. Operators switch to backup nodes. ASNs grow and shrink. SONDA tracks all of this over time, with location history going back to epoch 196 (2021). Per-validator and per-datacenter pages are rolling out in Phase 2.

**Verified data foundation.** Every IP is checked against four independent geolocation providers (DB-IP, IPInfo, GeoJS, ip-api), with DoubleZero device locations used as protocol-verified ground truth. Without this foundation, every map and metric on top of it would inherit the errors of whichever single source was chosen.

**Multi-cluster coverage.** Mainnet, testnet, devnet, and the experimental `alpenglow-community` cluster where Anza is testing the Alpenglow consensus algorithm. Each cluster looks very different. SONDA covers all four with the same pipeline.

**Decentralization metrics as foundation, not headline.** Nakamoto, HHI, Gini, and Shannon entropy across four dimensions (country, ASN, city, validator) are computed every cycle and published as part of the data. They support the tactical pages rather than being the primary product.

---

## Who is this for?

| Audience | What SONDA gives them |
|---|---|
| **Validator operators** (primary) | Tactical decisions on placement, datacenter and ASN selection, backup-node patterns, infrastructure timing |
| **Stake pool operators** | Due diligence on validator candidates, datacenter and provider risk visibility |
| **Researchers and journalists** | Measurable decentralization data across four dimensions, full historical context |
| **SOL stakers** | Where stake actually concentrates, which providers and regions dominate |

---

## What SONDA Analyzes

### Network Nodes (5,000+ per scan on mainnet)

| Category | Description |
|---|---|
| Validators | Active, hidden (no gossip), inactive (delinquent) |
| RPC Nodes | Public RPC endpoints in gossip |
| DoubleZero | DZ devices, connected validators, multicast groups |
| BAM / Jito | Block engines, shred receivers, NTP servers, BAM nodes |
| Harmonic | Auction engines, TPU relayers, bundles |
| Other | Entrypoints, co-hosted nodes, backup nodes |

### Geolocation: 4-source cross-verification

```
DB-IP ───── IPInfo ───── GeoJS ───── ip-api (used only for discrepancies)
                                       │
                       DoubleZero ─────┘  (protocol-verified ground truth)
```

Each IP receives a confidence score (high, medium, low) based on source agreement. Discrepancies are preserved with full alternatives for audit. Geo overrides from DZ devices automatically correct mislocated carrier IPs.

### Decentralization Metrics

| Metric | Dimensions | What it reveals |
|---|---|---|
| **Nakamoto Coefficient** | Country, ASN, City, Validator | Minimum entities to control 33% of stake |
| **Superminority** | Country, ASN, Validator (at 33 / 50 / 66%) | Geographic and organizational concentration thresholds |
| **HHI** | Country, ASN, Validator | Market concentration (competitive vs monopolistic) |
| **Gini Coefficient** | Validators | Stake inequality distribution |
| **Shannon Entropy** | Country, ASN, Validator | Diversity and evenness of distribution |

### Infrastructure Intelligence

| Source | Data |
|---|---|
| **DoubleZero** | Device locations, validator connections, multicast groups |
| **BAM** | Node topology, validator mapping, IBRL performance scores, stake % |
| **Rakurai** | MEV-optimized validator detection, geo distribution |
| **Trillium** | Client types, vote latency, slot duration, SFDP status |
| **Endpoints** | 90+ infrastructure endpoints with per-service reachability checks |
| **Alpenglow** | BLS pubkey adoption, feature activation status, genesis hash, rollback detection |

---

## Repository Layout

```
sonda/
├── analyzer/                       Core network analysis pipeline
│   ├── solana_analyzer.py          Real-time snapshot collector (2,800 lines)
│   ├── solana_history.py           Historical geo data collector (1,250 lines)
│   ├── endpoints.yaml              Infrastructure endpoint configuration
│   ├── geo_overrides.yaml          DZ-verified location overrides
│   └── README.md                   Detailed usage and reference
│
├── automation/                     Orchestration and pipeline glue
│   ├── run_sonda.py                Multi-cluster orchestrator (systemd entry point)
│   ├── run_once.py                 Single-cluster pipeline for testing
│   ├── split_snapshot.py           Splits 5MB snapshot into per-role files
│   ├── split_history.py            Splits history into per-validator files
│   ├── r2_upload.py                Cloudflare R2 upload (multi-mode)
│   ├── timeseries.py               SQLite state tracking, event detection
│   ├── telegram.py                 Public and debug bot notifications
│   ├── config.example.yaml         Configuration template
│   └── README.md                   Configuration and operation guide
│
├── systemd/                        Service definitions
│   ├── sonda.service               systemd unit (multi-worker orchestrator)
│   ├── sonda-logrotate             Log rotation config
│   └── README.md                   Installation and troubleshooting
│
├── .githooks/pre-commit            Blocks accidental secret commits
├── .gitignore                      Protects config.yaml with real keys
├── LICENSE                         MIT
└── README.md                       This file
```

`sonda_data/` (snapshots, SQLite, history, R2 staging) lives separately at `/home/solya/sonda_data/` on the production server. Not part of this repository.

---

## Architecture

### Real-time pipeline

```
run_sonda.py (systemd)
    │
    ├── Worker [mainnet-beta]            60s interval
    ├── Worker [testnet]                 300s interval
    ├── Worker [devnet]                  300s interval
    └── Worker [alpenglow-community]     60s interval
            │
            ▼
        Each worker cycle:
        ┌─────────────────────────────────────────┐
        │  1. analyzer/solana_analyzer.py         │
        │     ├── Gossip + validators (Solana CLI)│
        │     ├── Genesis hash, features          │
        │     ├── BLS pubkeys (Alpenglow only)    │
        │     ├── External APIs (BAM, DZ, etc.)   │
        │     ├── 4-source geolocation engine     │
        │     └── Write snapshot JSON (~5MB)      │
        │                                          │
        │  2. automation/split_snapshot.py        │
        │     └── 4 role-specific JSON files      │
        │                                          │
        │  3. automation/r2_upload.py             │
        │     └── Push to data.sonda.network      │
        │                                          │
        │  4. automation/timeseries.py            │
        │     ├── Diff against previous state     │
        │     ├── Record node_changes, ip_changes │
        │     ├── Detect cluster rollback         │
        │     └── Record epoch snapshot on wrap   │
        │                                          │
        │  5. automation/telegram.py              │
        │     └── Send events to bot channels     │
        └─────────────────────────────────────────┘
```

### Cluster rollback detection

The `alpenglow-community` cluster experiences regenesis events approximately weekly and snapshot-based restarts more frequently. SONDA's timeseries layer distinguishes three rollback types using `genesis_hash`, `epoch`, and `slot`:

| Scenario | Signal | Response |
|---|---|---|
| **Regenesis** | genesis_hash changed | Skip all comparisons, warn |
| **Epoch rollback** | Same genesis, lower epoch | Skip epoch comparison, warn |
| **Slot rollback within epoch** | Same genesis, same epoch, lower slot | Info-level log |

The same logic protects other clusters from incorrect epoch summaries during rare restart events.

### API cache TTL strategy

Not all data changes at the same rate. SONDA caches intelligently.

| Source | TTL | Rationale |
|---|---|---|
| Gossip, Validators, Epoch | Always fresh | Core monitoring, never cached |
| DZ devices, users, multicast | 2 min | Real-time infrastructure awareness |
| BAM validators, nodes | 2 min | Fast-changing connections |
| Trillium | 30 min | Updates several times per epoch |
| Rakurai | 1 hour | Validator list changes slowly |
| BAM IBRL, stake | 1 hour | Calculated per epoch |
| validator-info | 4 hours | On-chain, rarely changes |
| Geolocation | 7 to 30 days | Adaptive TTL with jitter, separate SQLite DB |
| SFDP epoch data | Permanent | Historical data, never changes |

---

## Quick Start

### Prerequisites

- Python 3.10+
- Solana CLI in PATH
- Optional: `doublezero` CLI for DZ data
- For automation: Cloudflare R2 bucket, Telegram bots, DB-IP and IPInfo accounts

### Standalone analyzer (no automation needed)

For a single one-off scan of any cluster:

```bash
git clone https://github.com/SolyaUk/sonda.git
cd sonda
pip install requests pyyaml boto3

# Mainnet scan
python3 analyzer/solana_analyzer.py \
    --cluster mainnet-beta \
    --dbip-key YOUR_DBIP_KEY \
    --ipinfo-token YOUR_IPINFO_TOKEN \
    --endpoints analyzer/endpoints.yaml \
    --geo-overrides analyzer/geo_overrides.yaml \
    --export \
    --output /tmp/snapshot.json

# Alpenglow community cluster
python3 analyzer/solana_analyzer.py \
    --cluster alpenglow-community \
    --rpc-url http://YOUR_ALPENGLOW_NODE:8899 \
    --dbip-key YOUR_DBIP_KEY \
    --ipinfo-token YOUR_IPINFO_TOKEN \
    --endpoints analyzer/endpoints.yaml \
    --export \
    --output /tmp/snapshot-alpenglow.json
```

### Historical data collector

For per-validator location history back to epoch 196 (2021):

```bash
mkdir -p ~/sonda_data/imports
cd ~/sonda_data/imports

# Initial collection for mainnet (takes around 6 hours, resumable)
python3 ~/sonda/analyzer/solana_history.py \
    --cluster mainnet-beta \
    --dbip-key YOUR_DBIP_KEY \
    --ipinfo-token YOUR_IPINFO_TOKEN

# Resume after interruption (Jito phase re-fetches in 40s, SFDP continues from checkpoint)
python3 ~/sonda/analyzer/solana_history.py --resume

# Incremental update later (add new epochs, preserve all SFDP data)
python3 ~/sonda/analyzer/solana_history.py --update --dbip-key YOUR_KEY
```

See [`analyzer/README.md`](analyzer/README.md) for the full collection-to-R2-upload workflow.

### Full production automation

For a continuously-running, multi-cluster, R2-publishing, Telegram-alerting deployment:

```bash
# 1. Set up directories
mkdir -p ~/sonda_data/{snapshots,backups,current,history,imports}
mkdir -p ~/sonda/automation/logs

# 2. Configure
cp automation/config.example.yaml automation/config.yaml
nano automation/config.yaml   # Fill in API keys, R2 credentials, Telegram tokens

# 3. Install systemd service (one-time)
sudo ln -s ~/sonda/systemd/sonda.service /etc/systemd/system/sonda.service
sudo cp ~/sonda/systemd/sonda-logrotate /etc/logrotate.d/sonda
sudo chown root:root /etc/logrotate.d/sonda
sudo systemctl daemon-reload
sudo systemctl enable sonda.service

# 4. Enable pre-commit hook (one-time, after cloning)
git config core.hooksPath .githooks

# 5. Start
sudo systemctl start sonda.service
journalctl -u sonda.service -f
```

Detailed operational instructions: [`systemd/README.md`](systemd/README.md), [`automation/README.md`](automation/README.md).

### API keys

| Service | Required for | Free tier |
|---|---|---|
| [DB-IP](https://db-ip.com/) | Primary geo + ASN lookup | 1K lookups/day on free tier (paid Starter plan: 10K/day with Extended API) |
| [IPInfo](https://ipinfo.io/) | Secondary geo verification | 50K lookups/month |
| GeoJS | Tertiary geo (no key needed) | Unlimited |
| ip-api | Discrepancy resolution (no key needed) | 45 req/min |
| RIPE Stat | ASN name lookup (no key needed) | Unlimited |
| [Cloudflare R2](https://cloudflare.com/products/r2/) | Public data hosting (automation only) | 10 GB storage free |
| [Helius](https://helius.dev/) | Recommended mainnet RPC (automation) | 1M credits/month on free tier (paid Developer plan: 10M/month) |
| [Telegram BotFather](https://t.me/BotFather) | Notification bots (automation only) | Free |

---

## Output Format

### Real-time snapshot (`solana_analyzer.py`)

Structured JSON with predictable field schemas per node role.

```json
{
  "timestamp": "2026-05-26T09:55:00.000000+00:00",
  "cluster": "alpenglow-community",
  "epoch": 30,
  "slot": 1641261,
  "epoch_completed_percent": 39.37,
  "genesis_hash": "3QWCajStkp68qAAgCjofJ3BpCyYfPQFxSVZppkYrSpju",
  "features": {
    "total_count": 278,
    "active_count": 278,
    "pending_count": 0,
    "pending": []
  },
  "record_counts": {
    "validator": 82, "rpc": 4, "infrastructure-node": 2
  },
  "records": [
    {
      "identity_pubkey": "...",
      "role": "validator",
      "ip_address": "1.2.3.4",
      "geolocation": {
        "country_code": "US", "city": "Ashburn",
        "asn": "AS24940", "asn_name": "HETZNER-AS",
        "confidence": "high", "discrepancy": false
      },
      "version": "0.3.2",
      "client_type": "Anza Alpenglow (1)",
      "bls_pubkey": "7B34dCYCh9wkUUmBpxrbNNsuU1gxMjRgLBs6PhDCdzMbJeyUu2tgUPkw7zJLLdobuT",
      "stake_percentage": 1.72,
      "is_rakurai": null,
      "dz_connected": false
    }
  ],
  "metrics": {
    "validators": {
      "decentralization": {
        "nakamoto": { "country": 3, "asn": 4, "city": 5, "validator": 9 },
        "hhi": { "country": 1842, "asn": 1201 },
        "gini": 0.847,
        "shannon_entropy": { "country": 0.71, "asn": 0.68 }
      }
    }
  }
}
```

Three new top-level fields versus older versions: `genesis_hash`, `features`, and per-validator `bls_pubkey`. The first two are populated for every cluster. `bls_pubkey` is currently populated only for `alpenglow-community` (SIMD-0387 active there); other clusters will follow once the VAT feature activates.

### Historical timeline (`solana_history.py`)

Per-validator datacenter history from epoch 196 (2021) to present.

```json
{
  "meta": {
    "cluster": "mainnet-beta",
    "fetched_at": "2026-03-27T12:26:40Z",
    "sources": ["jito_kobe", "sfdp", "stakewiz"],
    "total_validators": 788
  },
  "epoch_dates": { "500": "2023-02-15", "947": "2026-03-25" },
  "validators": {
    "HwcVgFSg...": {
      "identity": "HwN6eoEe...",
      "vote_account": "HwcVgFSg...",
      "location_changes": [
        {
          "from_epoch": 231, "to_epoch": 499,
          "ip": null,
          "country_code": "CA", "city": "Beauharnois",
          "asn": "AS16276", "asn_name": "OVH",
          "source": "sfdp",
          "dc_stake_percent": 1.79
        },
        {
          "from_epoch": 500, "to_epoch": 804,
          "ip": "15.235.13.5",
          "country_code": "CA", "city": "Montreal",
          "region": "Quebec", "latitude": 45.50, "longitude": -73.57,
          "asn": "AS16276", "asn_name": "OVH", "isp": "Ovh Sas",
          "source": "jito"
        }
      ]
    }
  }
}
```

### Public data on Cloudflare R2

The automation pipeline publishes split files to `data.sonda.network` on every cycle.

```
data.sonda.network/
├── current/
│   ├── mainnet-beta/
│   │   ├── network_summary.json     ~100 KB (meta + metrics, no records)
│   │   ├── validators.json          ~1.2 MB (~780 validators)
│   │   ├── rpc.json                 ~130 KB
│   │   └── infrastructure.json      ~160 KB (DZ, BAM, Jito, Harmonic)
│   ├── testnet/
│   ├── devnet/
│   └── alpenglow-community/
├── history/
│   ├── mainnet-beta/
│   │   ├── _index.json
│   │   └── {identity}.json          ~5 KB each, 787 files
│   ├── testnet/                     761 files
│   └── devnet/                      36 files (mostly empty)
└── backups/
    └── timeseries-YYYY-MM-DD.db.gz  Nightly SQLite backups
```

---

## Production Status

| Component | Status | Notes |
|---|---|---|
| Real-time analyzer | Live | Running every 60s (mainnet, alpenglow-community), 300s (testnet, devnet) |
| Historical collector | Complete | Initial pull done April 2026; re-runnable for gap fills |
| Multi-source geolocation | Live | DB-IP Starter plan + IPInfo + GeoJS + ip-api |
| Cluster rollback detection | Live | Three-type detection deployed May 2026 with Alpenglow integration |
| BLS pubkey tracking | Live | Currently `alpenglow-community` only |
| systemd service | Live | 4 workers, auto-restart, daily log rotation |
| Cloudflare R2 publishing | Live | data.sonda.network, public read |
| Telegram notifications | Live | Debug bot (private) + SONDA Network Events (public) |
| Public dashboard Phase 1 | Live | sonda.network, all 4 clusters |
| Per-validator pages | In progress | Phase 2 |
| Per-datacenter pages | In progress | Phase 2 |

---

## Roadmap

**Phase 1 (delivered)**
- Core analyzer with 4-source geolocation
- Infrastructure mapping for BAM, DoubleZero, Rakurai, Jito, Harmonic
- API cache with per-source TTL strategy
- Geo overrides with full audit trail
- Historical collector back to 2021
- Resume and update modes for fault-tolerant collection
- Multi-cluster automation (mainnet, testnet, devnet, `alpenglow-community`)
- Public dashboard at sonda.network

**Phase 2 (in development)**
- Per-validator pages with field-change timeline and location history
- Per-datacenter pages with hosting analytics and incident history
- Epoch snapshot auto-push to R2 with full backfill
- Cluster-wide time series (Nakamoto, HHI, Gini trends)
- Endpoint health uptime API

**Phase 3 (planned)**
- BLS pubkey adoption tracking expanded to all clusters once SIMD-0357 (VAT) activates
- RPC fallback chain for the `alpenglow-community` cluster
- Infrastructure health score (synthetic SONDA metric for validator ranking)
- Public API access with JSON exports
- Attribution program for downstream tools

**Phase 4 and beyond**
- Synthetic Validator Score and Datacenter Score composites
- Embeddable widgets, custom alerts, watchlists
- Internationalization

---

## Built By

Created and maintained by [Solya Validator](https://solya.studio), an independent Solana validator running since September 2021. Currently hosted in Singapore (AS20473 Vultr/Edgevana, same provider as São Paulo). Relocated in May 2026.

Identity: `HwN6eoEe9N3kwHi66hpQDBMFPk6ASQGthWKPX5MZmisp`
Vote: [`HwcVgFSgmfeeF7zGFUBLoVA8Hpx8rtwyfCrJ1npBaSVC`](https://stakewiz.com/validator/HwcVgFSgmfeeF7zGFUBLoVA8Hpx8rtwyfCrJ1npBaSVC)

Project Twitter: [@SondaNetwork](https://x.com/SondaNetwork)
Operator Twitter: [@SolyaOS](https://x.com/SolyaOS)

---

## Contributing

SONDA is open source under MIT. Contributions welcome via pull requests. For larger changes, open an issue first to discuss the approach.

When working with the codebase, note the security model: `automation/config.yaml` contains live API keys and is gitignored. The `.githooks/pre-commit` hook blocks accidental commits of this file and detects obvious secret patterns. Enable hooks after cloning:

```bash
git config core.hooksPath .githooks
```

---

## License

[MIT](LICENSE)
