# SONDA

**Solana Observatory for Network Decentralization Analysis**

> Multi-source geo-verified analysis of validator distribution, infrastructure mapping, and network health metrics for the Solana blockchain.

[![Status](https://img.shields.io/badge/status-work_in_progress-yellow)]()
[![Python](https://img.shields.io/badge/python-3.10+-blue)]()
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

🌐 Dashboard (coming soon): [sonda.network](https://sonda.network)

> [!NOTE]
> SONDA is under active development. The core analyzer is production-ready and tested on mainnet.
> The public dashboard and automation layer are coming next.

---

## Why SONDA?

Solana has dozens of dashboards. Most pull data from a single source and display it as-is. **SONDA takes a fundamentally different approach:**

- **Cross-verified geolocation.** Every IP is checked against 4 independent geo providers (DB-IP, IPInfo, GeoJS, ip-api). Discrepancies are detected, logged, and resolved — not silently averaged. Protocol-verified locations from DoubleZero devices serve as ground truth.

- **Complete infrastructure mapping.** Not just validators. SONDA maps the full network topology: RPC nodes, DoubleZero devices, BAM nodes, Jito block engines, Harmonic auction engines, NTP servers, shred receivers, co-hosted nodes, and more — each with their own role classification and field schema.

- **Multi-dimensional decentralization metrics.** Nakamoto coefficient alone doesn't tell the full story. SONDA computes Nakamoto, HHI, Gini, and Shannon entropy across 4 dimensions simultaneously (country, ASN, city, validator) — revealing concentration patterns that single-metric tools miss entirely.

- **MEV ecosystem visibility.** BAM (Block Auction Marketplace) integration with IBRL performance scores, Rakurai validator detection, Jito infrastructure mapping — showing who builds blocks, how fast, and where.

---

## What SONDA Analyzes

### Network Nodes (~5,000+ per scan)

| Category | Description |
|---|---|
| Validators | Active, hidden (no gossip), inactive (delinquent) |
| RPC Nodes | Public RPC endpoints in gossip |
| DoubleZero | DZ devices, connected validators, multicast groups |
| BAM/Jito | Block engines, shred receivers, NTP servers, BAM nodes |
| Harmonic | Auction engines, TPU relayers, bundles |
| Infrastructure | Entrypoints, co-hosted nodes, backup nodes |

### Geolocation (4-source cross-verification)

```
DB-IP (primary) ──→ IPInfo ──→ GeoJS ──→ ip-api (discrepancies only)
                                              │
                              DoubleZero ─────┘ (protocol-verified ground truth)
```

Each IP receives a confidence score (high/medium/low) based on source agreement. Discrepancies are preserved with full alternatives for audit. Geo overrides from DZ devices automatically correct mislocated carrier IPs.

### Decentralization Metrics

| Metric | Dimensions | What it reveals |
|---|---|---|
| **Nakamoto Coefficient** | Country, ASN, City, Validator | Minimum entities to control 33% of stake |
| **Superminority** | Country, ASN, Validator (at 33/50/66%) | Geographic and organizational concentration thresholds |
| **HHI** | Country, ASN, Validator | Market concentration (competitive vs monopolistic) |
| **Gini Coefficient** | Validators | Stake inequality distribution |
| **Shannon Entropy** | Country, ASN, Validator | Diversity and evenness of distribution |

### Infrastructure Intelligence

| Source | Data |
|---|---|
| **DoubleZero** | Device locations, validator connections, multicast groups (9 groups) |
| **BAM** | Node topology, validator mapping, IBRL performance scores, stake % |
| **Rakurai** | MEV-optimized validator detection, geo distribution |
| **Trillium** | Client types, vote latency, slot duration, SFDP status |
| **Endpoints** | 90+ infrastructure endpoints with differentiated reachability checks |

---

## Architecture

```
solana_analyzer.py          Single-file analyzer (~1,900 lines)
├── Data Collection         Gossip, validators, epoch from Solana CLI
├── External APIs           Trillium, BAM, Rakurai, DoubleZero CLI
├── Geolocation Engine      4-source with adaptive TTL cache (SQLite)
├── Geo Overrides           DZ-verified locations + admin overrides
├── API Cache               Per-source TTL (2min → 4hr) in SQLite
├── Metrics Calculator      Nakamoto, HHI, Gini, Shannon, Superminority
├── Infrastructure Map      BAM nodes, IBRL, DZ multicast, Rakurai
└── JSON Export             Structured output (~5.6 MB per scan)

endpoints.yaml              Infrastructure endpoint configuration
geo_overrides.yaml          Auto-generated DZ overrides + admin entries
```

### API Cache TTL Strategy

Not all data changes at the same rate. SONDA caches intelligently:

| Source | TTL | Rationale |
|---|---|---|
| Gossip, Validators, Epoch | Always fresh | Core monitoring — never cached |
| DZ devices, users, multicast | 2 min | Real-time infrastructure awareness |
| BAM validators, nodes | 2 min | Fast-changing connections |
| Trillium | 30 min | Updates several times per epoch |
| Rakurai | 1 hour | Validator list changes slowly |
| BAM IBRL, stake | 1 hour | Calculated per epoch |
| validator-info | 4 hours | On-chain, rarely changes |
| Geolocation | 7–30 days | Separate SQLite DB, adaptive TTL |

---

## Quick Start

### Prerequisites

- Python 3.10+
- Solana CLI (`solana` in PATH)
- Optional: `doublezero` CLI (for DZ data), `ntplib` (for NTP checks)

### Installation

```bash
git clone https://github.com/SolyaUk/sonda.git
cd sonda

pip install requests pyyaml ntplib
```

### Usage

```bash
# Basic analysis (mainnet)
python solana_analyzer.py \
  --dbip-key YOUR_DBIP_KEY \
  --ipinfo-token YOUR_IPINFO_TOKEN \
  --endpoints endpoints.yaml \
  --geo-overrides geo_overrides.yaml \
  --export --output analysis.json

# Custom RPC endpoint
python solana_analyzer.py \
  --rpc-url https://your-rpc.com \
  --dbip-key YOUR_KEY \
  --export --output analysis.json

# Testnet / Devnet
python solana_analyzer.py --cluster testnet --export
```

### API Keys

| Service | Required | Free Tier |
|---|---|---|
| [DB-IP](https://db-ip.com/) | Yes (primary geo) | 10K lookups/month |
| [IPInfo](https://ipinfo.io/) | Recommended | 50K lookups/month |
| GeoJS | Auto (no key) | Unlimited |
| ip-api | Auto (discrepancies) | 45 req/min |

---

## Output Format

SONDA produces a structured JSON with predictable field schemas per node role:

```json
{
  "timestamp": "2026-03-02T18:21:14.432854+00:00",
  "cluster": "mainnet-beta",
  "epoch": 934,
  "slot": 403773442,
  "epoch_completed_percent": 66.07453703703705,
  "record_counts": {
    "validator": 773, "validator-hidden": 3, "validator-inactive": 27,
    "backup-node": 0, "rpc": 204, "co-hosted": 2, "unknown-node": 3986,
    "dz-device": 95, "jito-block-engine": 36, "jito-shred-receiver": 8,
    "jito-ntp": 8, "jito-bam": 12, "harmonic-auction": 6,
    "harmonic-tpu-relayer": 6, "harmonic-shred-receiver": 6, "harmonic-bundles": 6,
    "solana-rpc-official": 1, "solana-entrypoint": 5
  },
  "records": [
    {
      "identity_pubkey": "...",
      "role": "validator",
      "ip_address": "1.2.3.4",
      .....
      "geolocation": {
        "country_code": "US", "city": "Ashburn",
        "confidence": "high", "discrepancy": false,
        "primary_source": "dbip"
      },
      .....
      "is_rakurai": true,
      "bam_node": "ny-mainnet-bam-1-tee",
      "ibrl": { "ibrl_score": 97.2, "median_block_build_ms": 362 },
      "dz_connected": true, "dz_device_name": "frankry", "dz_location": "Frankfurt"
    }
  ],
  "metrics": {
    "overall": {
      "total": 5154,
      "unique_ips": 5131,
      "country_distribution": { ... },
      "asn_distribution": { ... },
      "city_distribution": { ... },
    },
    "validators": {
      "total": 773,
      "unique_ips": 773,
      "country_distribution": { ... },
      .....
      "decentralization": {
        "methodology": { ... },
        "current": { ... },
      }
    },
    "rpc": { ... },
    "endpoints": { ... },
    "doublezero": { ... },
    "bam": { ... },
    "rakurai": { ... },
    "cluster_health": { ... }
  }
}
```

---

## Roadmap

- [x] **Core Analyzer** — Multi-source geolocation, role classification, metrics engine
- [x] **Infrastructure Mapping** — BAM, DoubleZero, Rakurai, Jito, Harmonic
- [x] **API Cache** — SQLite with per-source TTL strategy
- [x] **Geo Overrides** — DZ-verified + admin overrides with full audit trail
- [ ] **Automation** — Cron scheduling, snapshot history, failure monitoring
- [ ] **Public Dashboard** — Interactive visualization at [sonda.network](https://sonda.network)
- [ ] **Historical Analysis** — Trend tracking, epoch-over-epoch comparisons
- [ ] **Multi-chain** — Extending beyond Solana

---

## Built By

Created by **[Solya Validator](https://solya.studio)** — an independent Solana validator committed to network health, transparency, and decentralization.

Identity: [`HwcVgFSgmfeeF7zGFUBLoVA8Hpx8rtwyfCrJ1npBaSVC`](https://stakewiz.com/validator/HwcVgFSgmfeeF7zGFUBLoVA8Hpx8rtwyfCrJ1npBaSVC)

---

## License

[MIT](LICENSE)
