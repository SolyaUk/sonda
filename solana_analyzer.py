#!/usr/bin/env python3
"""
Solana Network Decentralization Analyzer v3.7
==============================================
Changes from v3.6:
- run_solana_cmd: added critical=True mode for unlimited retry with backoff.
  Now used for gossip, validators, and epoch fetches. When Solana RPC returns
  transient errors (e.g. "invalid type: integer `3`, expected a string"),
  the script retries indefinitely with progressive backoff (max 60s) until
  the cluster responds correctly. This prevents losing whole snapshots to
  temporary RPC issues common on all clusters (including paid RPCs).
  Non-critical calls (Trillium, validator-info, etc.) still use bounded retries.
- Malbec API: retry 3x with progressive backoff per request/page (was 1 attempt).
  Timeout increased 15s -> 20s. Multicast publishers fetch now recovers from
  previous cache if some groups fail mid-cycle (partial data merge), preventing
  "DZ publishers: 4 total" regressions when data.malbeclabs.com is flaky.
- BAM endpoints: dynamic creation from BAM API (mainnet). Previously endpoints.yaml
  held a hardcoded bam: section; Jito rotates BAM node names frequently
  (e.g. '-1-tee' becomes '-2-tee'), causing constant false "missing from API" and
  "new BAM node" warnings. Now each entry from /api/v1/nodes becomes an endpoint
  record with bam_node as primary ID. Reachability: connected_validators > 0
  -> reachable; 0 -> None (probe state, not a hard failure). Disappearance from
  API is detected by timeseries.py via missing_cycles logic. Testnet BAM still
  reads from endpoints.yaml (no public testnet BAM API).

Changes from v3.5.1:
- DoubleZero data migration: CLI -> Malbec HTTP API (data.malbeclabs.com)
  with CLI fallback for graceful degradation
- New: dz_connection_type (ibrl / multicast / ibrl+multicast) per validator
- New: dz_multicast_publisher flag + dz_multicast_groups list per validator
  (bebop, corvus, jito-shredstream publisher detection via node_pubkey + IP)
- New: DZ network health telemetry (device + link status, issues detection)
  via doublezero.xyz/api/network_health/v1
- New: DZ contributors list (infrastructure operator analysis)
- New: dz_metro_code, dz_contributor fields
- Multicast metrics: connection_types breakdown, publisher stake %
- DZ metrics: contributors summary, network_health block
Previous (v3.5.1): Geo override fix, Rakurai, DZ TTL 2min
"""

import json
import os
import sys
import time
import math
import sqlite3
import socket
import requests
import subprocess
import argparse
import re
import signal
import logging
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional, Any, Set
from dataclasses import dataclass, asdict, field
from pathlib import Path
from collections import defaultdict, Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False

try:
    import ntplib
    HAS_NTPLIB = True
except ImportError:
    HAS_NTPLIB = False

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

LAMPORTS_PER_SOL = 1_000_000_000
DEFAULT_CACHE_DAYS = 15
CACHE_TTL_HIGH = 30
CACHE_TTL_LOW = 7
DBIP_BATCH_SIZE = 100
CONCURRENT_WORKERS = 20
RPC_RETRIES = 3
RPC_TIMEOUT = 30
EP_CHECK_TIMEOUT = 4
EP_CHECK_RETRIES = 2
EP_CHECK_COOLDOWN = 0.1  # seconds between batches of checks

BAM_API_BASE = "https://explorer.bam.dev/api/v1"
BAM_API_TIMEOUT = 10
BAM_API_RETRIES = 2

RAKURAI_API_BASE = "https://api.rakurai.io/api/v1"
RAKURAI_API_TIMEOUT = 10

MALBEC_API_BASE = "https://data.malbeclabs.com/api/dz"
MALBEC_API_TIMEOUT = 20
MALBEC_API_RETRIES = 3
MALBEC_RETRY_DELAYS = [0, 1, 3]  # seconds between attempts
MALBEC_PAGE_SIZE = 100   # max items per page for paginated endpoints

DZ_HEALTH_API_BASE = "https://doublezero.xyz/api/network_health/v1"
DZ_HEALTH_API_TIMEOUT = 10

CLUSTER_URLS = {
    "mainnet-beta": "https://api.mainnet-beta.solana.com",
    "testnet": "https://api.testnet.solana.com",
    "devnet": "https://api.devnet.solana.com",
}
CLUSTER_YAML_MAP = {"mainnet-beta": "mainnet", "testnet": "testnet", "devnet": "devnet"}

EXIT_OK = 0
EXIT_CRITICAL = 1
EXIT_DBIP_FAIL = 2

# Endpoint service types and their verification methods
# "https" = HTTPS HEAD, "tcp" = TCP connect on port, "udp" = unverifiable (null)
EP_CHECK_METHOD = {
    "block-engine": "https",
    "bam": "api",              # BAM checked via explorer.bam.dev API, not TCP/HTTP
    "auction": "https",
    "tpu-relayer": "https",
    "bundles": "https",
    "rpc-official": "https",
    "shred-receiver": "udp",   # UDP protocol, can't verify via TCP/HTTP
    "ntp": "ntp",              # NTP query via ntplib (fallback to udp/null if not installed)
    "entrypoint": "tcp:8001",  # gossip port
}

METRIC_METHODOLOGY = {
    "nakamoto": {
        "name": "Nakamoto Coefficient",
        "description": (
            "Minimum number of entities (countries/providers/cities/validators) that together "
            "control ≥33.33% of active stake on Solana. This is the threshold at which these "
            "entities could theoretically halt the network (break liveness). Higher values indicate "
            "greater resilience against coordinated attacks or infrastructure failures. "
            "Measured across 4 dimensions: country, hosting provider (ASN), city, and individual validator."
        ),
        "reference": "https://news.earn.com/quantifying-decentralization-e39db233c28e",
        "scale": "1+ (higher = more decentralized)",
        "ratings": {">=20": "Excellent", ">=15": "Good", ">=10": "Moderate", ">=5": "Poor", "<5": "Critical"},
    },
    "superminority": {
        "name": "Superminority",
        "description": (
            "The smallest set of entities whose combined stake exceeds a given threshold. "
            "In Solana, the superminority (33.33%) can halt block production, the majority (50%) "
            "can influence consensus, and the supermajority (66.67%) has full control over the network. "
            "Measured at three thresholds: 33.33%, 50%, and 66.67%."
        ),
        "scale": "Count of entities at each threshold",
    },
    "hhi": {
        "name": "Herfindahl-Hirschman Index (HHI)",
        "description": (
            "Standard market concentration index used by antitrust authorities worldwide. "
            "Calculated as the sum of squared market shares (stake percentages). Applied to Solana "
            "to measure how concentrated stake is among countries, providers, and validators. "
            "A network dominated by one provider scores near 10000; perfectly distributed scores near 0."
        ),
        "reference": "https://en.wikipedia.org/wiki/Herfindahl%E2%80%93Hirschman_index",
        "scale": "0-10000",
        "ratings": {"<1500": "Competitive", "1500-2500": "Moderately concentrated", ">2500": "Highly concentrated"},
    },
    "gini": {
        "name": "Gini Coefficient",
        "description": (
            "Measures inequality in stake distribution among Solana validators. "
            "A Gini of 0 means every validator has equal stake; 1 means one validator holds everything. "
            "High Gini indicates a few large validators dominate, which increases centralization risk "
            "even if geographic distribution looks healthy."
        ),
        "reference": "https://en.wikipedia.org/wiki/Gini_coefficient",
        "scale": "0-1",
        "ratings": {"<0.3": "Low inequality", "0.3-0.5": "Moderate inequality", ">0.5": "High inequality"},
    },
    "shannon": {
        "name": "Shannon Entropy",
        "description": (
            "Information-theoretic measure of diversity in stake distribution. "
            "Normalized to 0-1 scale where 1 means perfectly uniform distribution across all entities "
            "and 0 means complete concentration in one entity. Applied to Solana stake distribution "
            "by country, provider, and validator to quantify how diverse the network truly is."
        ),
        "reference": "https://en.wikipedia.org/wiki/Entropy_(information_theory)",
        "scale": "0-1 (normalized)",
        "ratings": {">0.8": "High diversity", "0.5-0.8": "Moderate diversity", "<0.5": "Low diversity"},
    },
}


def signal_handler(sig, frame):
    logger.info("\n⚠️  Interrupted (Ctrl+C)")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)


class PrimarySourceError(Exception):
    pass


# ========================================================================
# DATA CLASSES
# ========================================================================

@dataclass
class GeolocationData:
    ip: str
    country: str = "Unknown"
    country_code: str = "??"
    city: str = "Unknown"
    region: str = ""
    latitude: float = 0.0
    longitude: float = 0.0
    asn: str = "N/A"
    asn_name: str = "Unknown"
    asn_number: Optional[int] = None
    isp: str = ""
    continent_code: str = ""
    continent_name: str = ""
    # Internal: sources kept for cache, not exported
    sources: dict = field(default_factory=dict)
    discrepancy: bool = False
    discrepancy_details: str = ""
    discrepancy_alternatives: Optional[dict] = None
    confidence: str = "unknown"
    ttl_days: int = DEFAULT_CACHE_DAYS
    cached_at: Optional[int] = None
    primary_source: str = "dbip"


@dataclass
class NodeInfo:
    identity_pubkey: str
    ip_address: Optional[str] = None
    role: str = "unknown-node"
    name: Optional[str] = None
    gossip_port: Optional[int] = None
    tpu_port: Optional[int] = None
    tpu_quic_port: Optional[int] = None
    rpc_port: Optional[int] = None
    version: Optional[str] = None
    version_status: Optional[str] = None
    feature_set: Optional[int] = None
    # Validator
    vote_account: Optional[str] = None
    commission: Optional[float] = None
    epoch_credits: Optional[int] = None
    activated_stake_lamports: Optional[int] = None
    stake_percentage: Optional[float] = None
    cumulative_stake_percent: Optional[float] = None
    is_superminority: bool = False
    delinquent: bool = False
    skip_rate: Optional[float] = None
    client_type: Optional[str] = None
    mev_commission: Optional[float] = None
    is_sfdp: Optional[bool] = None
    sfdp_state: Optional[str] = None
    slot_duration_median: Optional[float] = None
    median_vote_latency: Optional[float] = None
    testnet_pubkey: Optional[str] = None
    icon_url: Optional[str] = None
    website: Optional[str] = None
    details: Optional[str] = None
    # DoubleZero
    dz_connected: bool = False
    dz_device_account: Optional[str] = None
    dz_device_name: Optional[str] = None
    dz_location: Optional[str] = None
    dz_exchange: Optional[str] = None
    dz_connection_type: Optional[str] = None        # "ibrl", "multicast", "ibrl+multicast"
    dz_multicast_publisher: Optional[bool] = None   # True if publishing shreds
    dz_multicast_groups: Optional[List[str]] = None  # ["bebop", "corvus"]
    dz_metro_code: Optional[str] = None             # "fra", "sao", "nyc"
    dz_contributor: Optional[str] = None            # device contributor (for dz-device role)
    # BAM (Jito Block Auction Marketplace)
    bam_node: Optional[str] = None
    bam_region: Optional[str] = None
    # IBRL scores
    ibrl_score: Optional[float] = None
    ibrl_build_time_score: Optional[float] = None
    ibrl_vote_packing_score: Optional[float] = None
    ibrl_non_vote_packing_score: Optional[float] = None
    ibrl_median_block_build_ms: Optional[int] = None
    # Flags
    is_validator: bool = False
    is_rpc: bool = False
    is_multi_ip: bool = False
    is_dz_device: bool = False
    is_jito: bool = False
    is_rakurai: bool = False
    is_offline: bool = False
    is_co_hosted: bool = False
    co_hosted_with: Optional[List[str]] = None
    multi_ip_count: int = 1
    # Endpoint
    endpoint_provider: Optional[str] = None
    endpoint_service: Optional[str] = None
    endpoint_label: Optional[str] = None
    endpoint_port: Optional[int] = None
    endpoint_reachable: Optional[bool] = None
    endpoint_bam_id: Optional[str] = None  # for BAM API mapping
    _ep_source: str = ""  # transient, not exported


# ========================================================================
# HELPERS
# ========================================================================

def run_solana_cmd(args, timeout=RPC_TIMEOUT, retries=RPC_RETRIES, desc="", critical=False):
    """Execute solana CLI command with retries.

    critical=False (default): tries up to `retries` times, then returns None.
    critical=True: retries forever with progressive backoff capped at 60s.
      Use for operations without which the snapshot is meaningless (gossip,
      validators, epoch). Handles transient Solana RPC issues gracefully —
      occasional "invalid type: integer `3`, expected a string" and similar
      are retried until the cluster responds correctly. May delay the run
      but ensures data integrity. Only returns None on fatal Python errors.
    """
    attempt = 0
    while True:
        attempt += 1
        error_msg = None
        try:
            result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
            if result.returncode == 0:
                try:
                    return json.loads(result.stdout)
                except json.JSONDecodeError as e:
                    if critical:
                        error_msg = f"invalid JSON: {e}"
                    else:
                        logger.error(f"Invalid JSON from {desc}: {e}")
                        return None
            else:
                error_msg = result.stderr.strip()[:150] or f"exit code {result.returncode}"
        except subprocess.TimeoutExpired:
            error_msg = f"timeout after {timeout}s"
        except Exception as e:
            logger.error(f"{desc} error: {e}")
            return None

        # error_msg is set if we need to retry
        if critical:
            # Unlimited retry with progressive backoff: 2, 4, 8, 16, 32, 60, 60, ...
            wait = min(60, 2 ** attempt)
            # Log first 3 attempts, then every 10th to avoid spam
            if attempt <= 3 or attempt % 10 == 0:
                logger.warning(f"⚠️  {desc} fail (attempt {attempt}, retry in {wait}s): {error_msg}")
            time.sleep(wait)
            continue
        # Non-critical: bounded retries
        if attempt < retries:
            w = 2 ** (attempt - 1)
            logger.warning(f"{desc} fail ({attempt}/{retries}), retry {w}s: {error_msg}")
            time.sleep(w)
        else:
            logger.error(f"{desc} failed after {retries}: {error_msg[:200]}")
            return None


def resolve_dns(domain):
    try:
        host = domain.split(":")[0]
        return list(set(r[4][0] for r in socket.getaddrinfo(host, None, socket.AF_INET)))
    except socket.gaierror:
        return []


def _format_bandwidth(val):
    """Format bandwidth: int (bps) → human string, pass through strings."""
    if val is None: return None
    if isinstance(val, str): return val  # Already formatted (CLI data)
    if isinstance(val, (int, float)):
        if val >= 1_000_000_000: return f"{val/1_000_000_000:.0f}Gbps"
        if val >= 1_000_000: return f"{val/1_000_000:.0f}Mbps"
        if val >= 1_000: return f"{val/1_000:.0f}Kbps"
        return f"{val:.0f}bps"
    return str(val)


def parse_ip_port(value):
    if ":" in value:
        parts = value.rsplit(":", 1)
        try: return parts[0], int(parts[1])
        except ValueError: pass
    return value, None


def _fetch_malbec_paginated(path, params=None, max_pages=20):
    """Fetch all pages from Malbec API (data.malbeclabs.com) with per-page retries.
    Returns list of items or None on failure."""
    all_items = []
    offset = 0
    for page in range(max_pages):
        page_ok = False
        last_error = None
        for attempt, delay in enumerate(MALBEC_RETRY_DELAYS):
            if delay:
                time.sleep(delay)
            try:
                p = {"limit": MALBEC_PAGE_SIZE, "offset": offset}
                if params:
                    p.update(params)
                r = requests.get(f"{MALBEC_API_BASE}/{path}", params=p, timeout=MALBEC_API_TIMEOUT)
                if r.status_code == 403:
                    logger.warning(f"  ⚠️  Malbec API blocked (403) for {path}")
                    return None
                if r.status_code == 200:
                    data = r.json()
                    items = data.get("items", data if isinstance(data, list) else [])
                    all_items.extend(items)
                    total = data.get("total", len(items))
                    if offset + MALBEC_PAGE_SIZE >= total:
                        return all_items
                    offset += MALBEC_PAGE_SIZE
                    page_ok = True
                    break
                last_error = f"HTTP {r.status_code}"
            except requests.RequestException as e:
                last_error = str(e)[:150]
            except Exception as e:
                logger.warning(f"  ⚠️  Malbec API parse error for {path}: {e}")
                return None
        if not page_ok:
            logger.warning(f"  ⚠️  Malbec API error for {path} page {page} (after {MALBEC_API_RETRIES} retries): {last_error}")
            return None
    return all_items


def _fetch_malbec_single(path, timeout=MALBEC_API_TIMEOUT):
    """Fetch single non-paginated endpoint from Malbec API with retries.
    Returns parsed JSON or None after all retries exhausted."""
    last_error = None
    for attempt, delay in enumerate(MALBEC_RETRY_DELAYS):
        if delay:
            time.sleep(delay)
        try:
            r = requests.get(f"{MALBEC_API_BASE}/{path}", timeout=timeout)
            if r.status_code == 403:
                logger.warning(f"  ⚠️  Malbec API blocked (403) for {path}")
                return None
            if r.status_code == 200:
                return r.json()
            last_error = f"HTTP {r.status_code}"
        except Exception as e:
            last_error = str(e)[:150]
    logger.warning(f"  ⚠️  Malbec API error for {path} (after {MALBEC_API_RETRIES} retries): {last_error}")
    return None


def _fetch_dz_health_api(endpoint, network="mainnet"):
    """Fetch DZ network health API. Returns data array or None."""
    try:
        r = requests.get(f"{DZ_HEALTH_API_BASE}/{endpoint}", params={"network": network},
                        timeout=DZ_HEALTH_API_TIMEOUT)
        if r.status_code != 200: return None
        data = r.json()
        if data.get("success"): return data.get("data", [])
        return None
    except Exception as e:
        logger.warning(f"  ⚠️  DZ health API error: {e}"); return None


def check_endpoint(ip, port, source, method):
    """Check endpoint reachability based on method type.
    Returns: True (reachable), False (unreachable), None (unverifiable/UDP)
    """
    if method == "udp":
        return None  # UDP services can't be reliably checked

    if method == "ntp":
        if not HAS_NTPLIB:
            return None  # ntplib not installed, can't verify
        # NTP query — use source (domain) if available, fall back to IP
        host = source if source and not source[0].isdigit() else ip
        for attempt in range(EP_CHECK_RETRIES):
            try:
                c = ntplib.NTPClient()
                c.request(host, version=3, timeout=EP_CHECK_TIMEOUT)
                return True
            except Exception:
                if attempt < EP_CHECK_RETRIES - 1:
                    time.sleep(0.5)
        return False

    for attempt in range(EP_CHECK_RETRIES):
        try:
            if method == "https":
                url = f"https://{source}" if not source.startswith("http") else source
                r = requests.head(url, timeout=EP_CHECK_TIMEOUT, allow_redirects=True)
                return r.status_code < 500
            elif method.startswith("tcp:"):
                check_port = int(method.split(":")[1])
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(EP_CHECK_TIMEOUT)
                result = sock.connect_ex((ip, check_port))
                sock.close()
                return result == 0
            elif method == "tcp" and port:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(EP_CHECK_TIMEOUT)
                result = sock.connect_ex((ip, port))
                sock.close()
                return result == 0
        except Exception:
            if attempt < EP_CHECK_RETRIES - 1:
                time.sleep(0.5)
    return False


def version_family(ver):
    if not ver or ver in ("unknown", "-"): return None
    parts = ver.split(".")
    if parts[0] == "0" and len(parts) >= 2: return f"{parts[0]}.{parts[1]}"
    return parts[0]


def version_tuple(ver):
    if not ver or ver in ("unknown", "-"): return (0,)
    try: return tuple(int(p) for p in ver.split("."))
    except ValueError: return (0,)


def truncate_id(s, n=13):
    return s if len(s) <= n else f"{s[:5]}...{s[-5:]}"

def normalize_city(city):
    """Strip district/neighborhood in parentheses for metric aggregation.
    'Frankfurt am Main (Rödelheim)' → 'Frankfurt am Main'
    'Amsterdam (Amsterdam-Zuidoost)' → 'Amsterdam'
    Raw geo data keeps full detail; only metrics use normalized form.
    """
    if not city: return city
    return city.split('(')[0].strip()


# ========================================================================
# METRIC FUNCTIONS
# ========================================================================

def calc_nakamoto(shares, threshold=33.33):
    s = sorted(shares, reverse=True); cum = 0
    for i, v in enumerate(s, 1):
        cum += v
        if cum >= threshold: return i
    return len(s)

def calc_hhi(shares): return sum(s**2 for s in shares)

def calc_gini(values):
    if not values: return 0.0
    sv = sorted(values); n = len(sv); total = sum(sv)
    if total == 0: return 0.0
    return sum((2*(i+1)-n-1)*v for i, v in enumerate(sv)) / (n * total)

def calc_shannon(shares):
    total = sum(shares)
    if total == 0: return 0.0, 0.0
    probs = [s/total for s in shares if s > 0]; n = len(probs)
    if n <= 1: return 0.0, 0.0
    h = -sum(p * math.log2(p) for p in probs); hm = math.log2(n)
    return h, (h/hm if hm > 0 else 0.0)

def rate_nakamoto(n): return "Excellent" if n>=20 else "Good" if n>=15 else "Moderate" if n>=10 else "Poor" if n>=5 else "Critical"
def rate_hhi(h): return "Competitive" if h<1500 else "Moderately concentrated" if h<2500 else "Highly concentrated"
def rate_gini(g): return "Low inequality" if g<0.3 else "Moderate inequality" if g<0.5 else "High inequality"
def rate_shannon(h): return "High diversity" if h>0.8 else "Moderate diversity" if h>0.5 else "Low diversity"


# ========================================================================
# CACHE
# ========================================================================

class GeoCache:
    def __init__(self, cache_dir=None):
        if cache_dir is None: cache_dir = os.path.expanduser("~/.solana_network_cache")
        os.makedirs(cache_dir, exist_ok=True)
        self.cache_dir = Path(cache_dir)
        self.db_path = self.cache_dir / "geolocation_v3.db"
        with sqlite3.connect(self.db_path) as c:
            c.execute("""CREATE TABLE IF NOT EXISTS geolocation (
                ip TEXT PRIMARY KEY, data TEXT NOT NULL, ttl_days INTEGER DEFAULT 15, cached_at INTEGER NOT NULL)""")
            c.execute("CREATE INDEX IF NOT EXISTS idx_cached ON geolocation(cached_at)")

    def get_batch(self, ips):
        result = {}
        if not ips: return result
        try:
            from dataclasses import fields as dc_fields
            valid_fields = {f.name for f in dc_fields(GeolocationData)}
            now = time.time()
            with sqlite3.connect(self.db_path) as c:
                for i in range(0, len(ips), 500):
                    chunk = ips[i:i+500]; ph = ",".join("?"*len(chunk))
                    for ip, ds, ttl, cat in c.execute(f"SELECT ip,data,ttl_days,cached_at FROM geolocation WHERE ip IN ({ph})", chunk):
                        if (now-cat)/86400 <= ttl:
                            try:
                                d = json.loads(ds)
                                # Migrate old field names (v3.2 → v3.3)
                                if 'org' in d and 'asn_name' not in d:
                                    d['asn_name'] = d.pop('org')
                                if 'asn_number' not in d:
                                    d['asn_number'] = None
                                if 'discrepancy_alternatives' not in d:
                                    d['discrepancy_alternatives'] = None
                                # Remove any unexpected fields from older cache versions
                                d = {k: v for k, v in d.items() if k in valid_fields}
                                result[ip] = GeolocationData(**d)
                                result[ip].cached_at = cat
                            except Exception:
                                pass  # Skip corrupted entries, will re-fetch
        except Exception as e: logger.debug(f"Cache read: {e}")
        return result

    def save_batch(self, geos):
        if not geos: return
        try:
            now = int(time.time()); entries = []
            for g in geos:
                d = asdict(g); d.pop('cached_at', None); d.pop('sources', None)
                entries.append((g.ip, json.dumps(d), g.ttl_days, now))
            with sqlite3.connect(self.db_path) as c:
                c.executemany("INSERT OR REPLACE INTO geolocation (ip,data,ttl_days,cached_at) VALUES (?,?,?,?)", entries)
        except Exception as e: logger.debug(f"Cache write: {e}")

    def stats(self):
        try:
            with sqlite3.connect(self.db_path) as c:
                t = c.execute("SELECT COUNT(*) FROM geolocation").fetchone()[0]
                e = c.execute("SELECT COUNT(*) FROM geolocation WHERE (?-cached_at)/86400.0>ttl_days", (time.time(),)).fetchone()[0]
                return {"total": t, "expired": e, "valid": t-e}
        except: return {"total": 0, "expired": 0, "valid": 0}


class APICache:
    """TTL-based cache for external API responses (SQLite)."""
    def __init__(self, cache_dir=None):
        if cache_dir is None: cache_dir = os.path.expanduser("~/.solana_network_cache")
        os.makedirs(cache_dir, exist_ok=True)
        self.db_path = Path(cache_dir) / "api_cache.db"
        with sqlite3.connect(self.db_path) as c:
            c.execute("""CREATE TABLE IF NOT EXISTS api_cache (
                source TEXT, key TEXT, data TEXT NOT NULL, fetched_at REAL NOT NULL,
                PRIMARY KEY (source, key))""")

    def get(self, source, key="default", ttl_seconds=300):
        try:
            with sqlite3.connect(self.db_path) as c:
                row = c.execute("SELECT data, fetched_at FROM api_cache WHERE source=? AND key=?",
                                (source, key)).fetchone()
                if row and (time.time() - row[1]) < ttl_seconds:
                    age = int(time.time() - row[1])
                    logger.debug(f"  📦 Cache hit: {source}/{key} (age={age}s)")
                    return json.loads(row[0])
        except Exception as e: logger.debug(f"API cache read: {e}")
        return None

    def set(self, source, key="default", data=None):
        try:
            with sqlite3.connect(self.db_path) as c:
                c.execute("INSERT OR REPLACE INTO api_cache (source, key, data, fetched_at) VALUES (?,?,?,?)",
                          (source, key, json.dumps(data, ensure_ascii=False), time.time()))
        except Exception as e: logger.debug(f"API cache write: {e}")

    def invalidate(self, source, key=None):
        try:
            with sqlite3.connect(self.db_path) as c:
                if key: c.execute("DELETE FROM api_cache WHERE source=? AND key=?", (source, key))
                else: c.execute("DELETE FROM api_cache WHERE source=?", (source,))
        except: pass

# API cache TTL in seconds
API_TTL = {
    "trillium": 1800,          # 30 min
    "dz_devices": 120,         # 2 min — core monitoring
    "dz_users": 120,           # 2 min — core monitoring
    "dz_multicast": 120,       # 2 min — core monitoring
    "dz_publishers": 120,      # 2 min — multicast publisher lists
    "dz_health_devices": 300,  # 5 min — matches DZ update frequency
    "dz_health_links": 300,    # 5 min — matches DZ update frequency
    "dz_contributors": 3600,   # 1 hour — infrastructure changes slowly
    "bam_validators": 120,     # 2 min
    "bam_nodes": 120,          # 2 min
    "bam_ibrl": 3600,          # 1 hour
    "bam_stake": 3600,         # 1 hour
    "validator_info": 14400,   # 4 hours
    "rakurai": 3600,           # 1 hour — validator list changes slowly
}


# ========================================================================
# GEOLOCATION
# ========================================================================

def _q_dbip(ips, key):
    if not key or not ips: return {}
    try:
        r = requests.get(f"https://api.db-ip.com/v2/{key}/{','.join(ips)}", timeout=15)
        if r.status_code == 429: raise PrimarySourceError("DB-IP rate limit (429)")
        if r.status_code != 200: raise PrimarySourceError(f"DB-IP HTTP {r.status_code}")
        data = r.json()
        if isinstance(data, dict) and "errorCode" in data: raise PrimarySourceError(f"DB-IP: {data.get('error')}")
        return {ip: data[ip] for ip in ips if ip in data and isinstance(data.get(ip), dict) and "errorCode" not in data[ip]}
    except PrimarySourceError: raise
    except requests.exceptions.RequestException as e: raise PrimarySourceError(f"DB-IP net: {e}")
    except Exception as e: raise PrimarySourceError(f"DB-IP: {e}")

def _q_ipinfo(ip, token):
    if not token: return None
    try: r = requests.get(f"https://ipinfo.io/{ip}?token={token}", timeout=5); return r.json() if r.status_code==200 else None
    except: return None

def _q_geojs(ip):
    try: r = requests.get(f"https://get.geojs.io/v1/ip/geo/{ip}.json", timeout=5); return r.json() if r.status_code==200 else None
    except: return None

def _q_ipapi(ip):
    try:
        r = requests.get(f"http://ip-api.com/json/{ip}", timeout=5)
        if r.status_code==200: d = r.json(); return d if d.get("status")=="success" else None
    except: pass
    return None


class GeolocationService:
    def __init__(self, dbip_key="", ipinfo_token=""):
        self.dbip_key = dbip_key; self.ipinfo_token = ipinfo_token
        self.cache = GeoCache(); self.stats = defaultdict(int)

    def process_ips(self, ips):
        all_ips = list(set(ips)); results = {}
        cached = self.cache.get_batch(all_ips)
        self.stats["cache_hits"] = len(cached); results.update(cached)
        uncached = [ip for ip in all_ips if ip not in cached]
        logger.info(f"📍 Cache: {len(cached)} hits, {len(uncached)} to fetch")
        if not uncached: return results

        dbip = self._fetch_dbip(uncached)
        ipinfo, geojs = self._fetch_secondary(uncached)
        disc_ips = [ip for ip in uncached if dbip.get(ip,{}).get("countryCode") and ipinfo.get(ip,{}).get("country") and dbip[ip]["countryCode"]!=ipinfo[ip]["country"]]
        ipapi = self._fetch_ipapi(disc_ips) if disc_ips else {}

        to_cache = []
        for ip in uncached:
            geo = self._build(ip, dbip.get(ip), ipinfo.get(ip), geojs.get(ip), ipapi.get(ip))
            results[ip] = geo; to_cache.append(geo)
        self.cache.save_batch(to_cache)
        logger.info(f"💾 Cached {len(to_cache)} entries")
        logger.info(f"📊 Geo: DB-IP={self.stats['dbip']}, IPInfo={self.stats['ipinfo']}, GeoJS={self.stats['geojs']}, ip-api={self.stats['ipapi']}, disc={self.stats['discrepancies']}")
        return results

    def _fetch_dbip(self, ips):
        if not self.dbip_key: logger.warning("⚠️  No DB-IP key"); return {}
        batches = [ips[i:i+DBIP_BATCH_SIZE] for i in range(0, len(ips), DBIP_BATCH_SIZE)]
        logger.info(f"  📡 DB-IP: {len(ips)} IPs, {len(batches)} batches...")
        all_r = {}
        for i, batch in enumerate(batches):
            if i>0 and i%10==0: logger.info(f"    DB-IP batch {i}/{len(batches)}...")
            all_r.update(_q_dbip(batch, self.dbip_key)); self.stats["dbip"] += len(batch)
            if i < len(batches)-1: time.sleep(0.2)
        logger.info(f"  ✅ DB-IP: {len(all_r)}/{len(ips)}"); return all_r

    def _fetch_secondary(self, ips):
        ir, gr = {}, {}
        logger.info(f"  📡 IPInfo+GeoJS: {len(ips)} IPs, {CONCURRENT_WORKERS} workers...")
        with ThreadPoolExecutor(max_workers=CONCURRENT_WORKERS) as ex:
            iff = {ex.submit(_q_ipinfo, ip, self.ipinfo_token): ip for ip in ips} if self.ipinfo_token else {}
            gf = {ex.submit(_q_geojs, ip): ip for ip in ips}
            for f in as_completed(iff):
                r = f.result()
                if r: ir[iff[f]] = r; self.stats["ipinfo"] += 1
            for f in as_completed(gf):
                r = f.result()
                if r: gr[gf[f]] = r; self.stats["geojs"] += 1
        logger.info(f"  ✅ IPInfo: {len(ir)}, GeoJS: {len(gr)}"); return ir, gr

    def _fetch_ipapi(self, ips):
        logger.info(f"  🔍 ip-api: {len(ips)} discrepancies...")
        results = {}
        for i, ip in enumerate(ips):
            if i>0 and i%40==0: logger.info(f"    ip-api pause ({i}/{len(ips)})..."); time.sleep(60)
            r = _q_ipapi(ip)
            if r: results[ip] = r; self.stats["ipapi"] += 1
        return results

    def _build(self, ip, dbip, ipinfo, geojs, ipapi):
        geo = GeolocationData(ip=ip)
        if dbip: geo.sources["dbip"] = dbip
        if ipinfo: geo.sources["ipinfo"] = ipinfo
        if geojs: geo.sources["geojs"] = geojs
        if ipapi: geo.sources["ipapi"] = ipapi

        # Primary: DB-IP
        if dbip:
            geo.primary_source = "dbip"
            geo.country_code = dbip.get("countryCode", "??")
            geo.country = dbip.get("countryName", geo.country_code)
            geo.city = dbip.get("city", "Unknown"); geo.region = dbip.get("stateProv", "")
            geo.latitude = float(dbip.get("latitude",0) or 0)
            geo.longitude = float(dbip.get("longitude",0) or 0)
            geo.continent_code = dbip.get("continentCode", "")
            geo.continent_name = dbip.get("continentName", "")
            geo.isp = dbip.get("isp", "")
            asn_num = dbip.get("asNumber")
            geo.asn = f"AS{asn_num}" if asn_num else "N/A"
            geo.asn_number = int(asn_num) if asn_num else None
            geo.asn_name = dbip.get("asName", dbip.get("organization", dbip.get("isp", "Unknown")))
        elif ipinfo:
            geo.primary_source = "ipinfo"
            geo.country_code = ipinfo.get("country", "??")
            geo.city = ipinfo.get("city", "Unknown"); geo.region = ipinfo.get("region", "")
            loc = ipinfo.get("loc", "").split(",")
            geo.latitude = float(loc[0]) if len(loc)>0 and loc[0] else 0.0
            geo.longitude = float(loc[1]) if len(loc)>1 and loc[1] else 0.0
            org_info = ipinfo.get("org", "")
            if org_info.startswith("AS"):
                parts = org_info.split(None, 1)
                geo.asn = parts[0] if parts else "N/A"
                geo.asn_name = parts[1] if len(parts)>1 else org_info
                try: geo.asn_number = int(parts[0].replace("AS",""))
                except: pass
            else:
                geo.asn_name = org_info or "Unknown"

        # Discrepancy
        countries = {}
        if dbip: countries["dbip"] = dbip.get("countryCode", "")
        if ipinfo: countries["ipinfo"] = ipinfo.get("country", "")
        if geojs: countries["geojs"] = geojs.get("country_code", "")
        if ipapi: countries["ipapi"] = ipapi.get("countryCode", "")
        countries = {k: v for k, v in countries.items() if v}

        if len(set(countries.values())) > 1:
            geo.discrepancy = True
            geo.discrepancy_details = ", ".join(f"{s}={c}" for s, c in countries.items())
            self.stats["discrepancies"] += 1
            # Build compact alternatives (only non-primary sources)
            alts = {}
            if ipinfo and geo.primary_source != "ipinfo":
                org = ipinfo.get("org", "")
                asn_alt = ""
                if org.startswith("AS"):
                    parts = org.split(None, 1)
                    asn_alt = parts[0] if parts else ""
                loc = ipinfo.get("loc", "").split(",")
                alts["ipinfo"] = {"country": ipinfo.get("country",""), "city": ipinfo.get("city",""),
                                   "region": ipinfo.get("region",""),
                                   "latitude": float(loc[0]) if len(loc)>0 and loc[0] else None,
                                   "longitude": float(loc[1]) if len(loc)>1 and loc[1] else None,
                                   "org": org, "asn": asn_alt}
            if geojs:
                alts["geojs"] = {"country": geojs.get("country_code",""), "city": geojs.get("city",""),
                                  "region": geojs.get("region",""),
                                  "latitude": float(geojs.get("latitude",0) or 0) or None,
                                  "longitude": float(geojs.get("longitude",0) or 0) or None,
                                  "org": geojs.get("organization_name",""), "asn": geojs.get("asn","")}
            if ipapi:
                alts["ipapi"] = {"country": ipapi.get("countryCode",""), "city": ipapi.get("city",""),
                                  "region": ipapi.get("regionName",""),
                                  "latitude": ipapi.get("lat"), "longitude": ipapi.get("lon"),
                                  "org": ipapi.get("org",""), "asn": ipapi.get("as","")}
            geo.discrepancy_alternatives = alts

        # Confidence
        uniq = len(set(countries.values())); nsrc = len(countries)
        if uniq <= 1 and nsrc >= 2:
            geo.confidence, geo.ttl_days = "high", CACHE_TTL_HIGH; self.stats["high"] += 1
        elif uniq >= 3 or nsrc < 2:
            geo.confidence, geo.ttl_days = "low", CACHE_TTL_LOW; self.stats["low"] += 1
        else:
            geo.confidence, geo.ttl_days = "medium", DEFAULT_CACHE_DAYS; self.stats["medium"] += 1
        return geo


# ========================================================================
# GEO OVERRIDES
# ========================================================================

def load_geo_overrides(path):
    """Load geo overrides from YAML. Returns dict {ip: override_entry}."""
    if not path or not os.path.exists(path): return {}
    if not HAS_YAML: return {}
    try:
        with open(path) as f:
            data = yaml.safe_load(f) or {}
        return {e["ip"]: e for e in data.get("overrides", []) if e.get("ip")}
    except Exception as e:
        logger.warning(f"⚠️  geo_overrides load: {e}"); return {}

def save_geo_overrides(path, overrides_dict):
    """Save geo overrides to YAML. Merges DZ auto + admin entries."""
    if not path or not HAS_YAML: return
    entries = sorted(overrides_dict.values(), key=lambda e: (e.get("source",""), e.get("ip","")))
    try:
        with open(path, 'w') as f:
            yaml.dump({"overrides": entries}, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
        logger.info(f"  💾 Saved {len(entries)} geo overrides to {path}")
    except Exception as e:
        logger.warning(f"⚠️  geo_overrides save: {e}")

def generate_dz_overrides(dz_device_records):
    """Generate geo overrides from DZ device locations (protocol-verified)."""
    overrides = {}
    for rec in dz_device_records:
        if rec.ip_address and rec.dz_location:
            overrides[rec.ip_address] = {
                "ip": rec.ip_address,
                "city": rec.dz_location,
                "source": "doublezero-verified",
                "dz_device_name": rec.dz_device_name,
            }
    return overrides


# ========================================================================
# ENDPOINT LOADER
# ========================================================================

def load_endpoints(config_path, cluster):
    if not os.path.exists(config_path):
        logger.warning(f"⚠️  Config not found: {config_path}"); return []
    if not HAS_YAML:
        logger.error("❌ PyYAML required"); return []
    with open(config_path) as f: config = yaml.safe_load(f)
    if not config: return []
    cluster_key = CLUSTER_YAML_MAP.get(cluster, cluster); eps = []
    for provider, cd in config.items():
        if not isinstance(cd, dict): continue
        cdata = cd.get(cluster_key, {})
        if not cdata: continue
        for service, locations in cdata.items():
            if not isinstance(locations, dict): continue
            for label, value in locations.items():
                # Support both simple "ip:port" and dict {ip: ..., bam_id: ...}
                bam_id = None
                if isinstance(value, dict):
                    bam_id = value.get("bam_id")
                    value = str(value.get("ip", ""))
                else:
                    value = str(value)
                if not value: continue
                if re.match(r'^\d+\.\d+\.\d+\.\d+', value):
                    ip, port = parse_ip_port(value)
                    eps.append({"provider": provider, "service": service, "label": label,
                                "ip": ip, "port": port, "source": value, "resolved_via": "direct",
                                "bam_id": bam_id})
                else:
                    for rip in resolve_dns(value):
                        eps.append({"provider": provider, "service": service, "label": label,
                                    "ip": rip, "port": None, "source": value, "resolved_via": "dns",
                                    "bam_id": bam_id})
    return eps


# ========================================================================
# ANALYZER
# ========================================================================

class SolanaNetworkAnalyzer:
    def __init__(self, dbip_key="", ipinfo_token="", cluster="mainnet-beta", endpoints_config="endpoints.yaml",
                 rpc_url=None, geo_overrides_path=None):
        self.cluster = cluster
        self.cluster_url = rpc_url or CLUSTER_URLS.get(cluster, CLUSTER_URLS["mainnet-beta"])
        self.geo_service = GeolocationService(dbip_key, ipinfo_token)
        self.api_cache = APICache()
        self.endpoints_config = endpoints_config
        self.geo_overrides_path = geo_overrides_path
        self.geo_overrides = {}
        self.records: List[NodeInfo] = []
        self.all_ips: Set[str] = set()
        self.ip_to_records: Dict[str, List[NodeInfo]] = defaultdict(list)
        self.geo_data: Dict[str, GeolocationData] = {}
        self.geo_discrepancies = []
        self.validator_data = {}; self.trillium_data = {}; self.cluster_health = {}
        self.bam_nodes_api = []; self.bam_validators_api = {}; self.bam_ibrl_api = {}
        self.bam_stake_api = {}; self.bam_api_available = False
        self.dz_multicast_groups = []
        self.dz_publishers_by_ip = {}     # client_ip -> [group_codes]
        self.dz_publishers_by_pubkey = {} # node_pubkey -> [group_codes]
        self.dz_network_health = {}       # device + link telemetry
        self.dz_contributors_data = []    # contributor list
        self.dz_malbec_available = False   # True if Malbec API responded
        self.dz_device_details = {}       # device_code -> {max_users, current_users, ...}
        self.rakurai_data = {}
        self.endpoint_records: List[NodeInfo] = []
        self.current_epoch = None; self.current_slot = None; self.epoch_completed_percent = None
        self.run_timestamp = datetime.now(timezone.utc).isoformat()
        self._check_config()

    def _check_config(self):
        try:
            r = subprocess.run(["solana","config","get"], capture_output=True, text=True, timeout=5)
            if r.returncode == 0:
                o = r.stdout
                cur = "testnet" if "testnet" in o else "devnet" if "devnet" in o else "mainnet-beta" if "mainnet" in o else "unknown"
                if cur != self.cluster: logger.warning(f"⚠️  CLI={cur}, script={self.cluster.upper()}")
        except: pass
        rpc_note = f" (custom RPC)" if self.cluster_url not in CLUSTER_URLS.values() else ""
        logger.info(f"💡 Cluster: {self.cluster} | RPC: {self.cluster_url}{rpc_note} | Run: {self.run_timestamp}")

    def fetch_all_data(self):
        logger.info("="*60); logger.info(f"🚀 Solana {self.cluster.upper()} Network Analysis"); logger.info("="*60)
        if not self._fetch_gossip(): return False
        if not self._fetch_validators(): return False
        if self.cluster == "mainnet-beta":
            self._fetch_trillium()
            if not self.trillium_data: self._fetch_validator_info()
        else: self._fetch_validator_info()
        if self.cluster == "mainnet-beta": self._fetch_doublezero()
        if self.cluster == "mainnet-beta": self._fetch_dz_multicast()
        if self.cluster == "mainnet-beta": self._fetch_bam()
        if self.cluster == "mainnet-beta": self._fetch_rakurai()
        if self.cluster == "mainnet-beta": self._fetch_dz_health()
        if self.cluster == "mainnet-beta": self._fetch_dz_contributors()
        self._fetch_endpoints_from_config()
        if self.cluster == "mainnet-beta": self._create_bam_endpoints_from_api()
        self._check_endpoints_reachability()
        self._fetch_epoch()
        try: self._process_geolocation()
        except PrimarySourceError as e:
            logger.error(f"❌ DB-IP failed: {e}"); return False
        self._apply_geo_overrides()
        self._detect_co_hosted(); self._determine_version_status(); self._calculate_superminority()
        return True

    def _fetch_gossip(self):
        logger.info("📡 Fetching gossip...")
        data = run_solana_cmd(["solana","--url",self.cluster_url,"gossip","--output","json"], desc="gossip", critical=True)
        if data is None: logger.error("❌ Gossip unavailable"); return False
        for e in data:
            ip = e.get("ipAddress")
            if ip:
                node = NodeInfo(identity_pubkey=e["identityPubkey"], ip_address=ip,
                    gossip_port=e.get("gossipPort"), tpu_port=e.get("tpuPort"),
                    tpu_quic_port=e.get("tpuQuicPort"), rpc_port=e.get("rpcPort"),
                    version=e.get("version"), feature_set=e.get("featureSet"),
                    is_rpc=e.get("rpcHost") is not None)
                if node.is_rpc: node.role = "rpc"
                self.records.append(node); self.all_ips.add(ip); self.ip_to_records[ip].append(node)
        logger.info(f"✅ {len(self.records)} gossip nodes")
        multi = 0
        for ip, nodes in self.ip_to_records.items():
            if len(nodes)>1:
                for n in nodes: n.is_multi_ip=True; n.multi_ip_count=len(nodes); multi+=1
        if multi: logger.info(f"⚠️  {multi} sharing IPs")
        return True

    def _fetch_validators(self):
        logger.info("🔍 Fetching validators...")
        data = run_solana_cmd(["solana","--url",self.cluster_url,"validators","--output","json"], desc="validators", critical=True)
        if data is None: logger.error("❌ Validators unavailable"); return False
        ts = data.get("totalActiveStake", 0)
        self.cluster_health = {
            "total_active_stake": ts, "total_current_stake": data.get("totalCurrentStake",0),
            "total_delinquent_stake": data.get("totalDelinquentStake",0),
            "delinquent_stake_percent": round(data.get("totalDelinquentStake",0)/ts*100, 2) if ts else 0,
            "average_skip_rate": data.get("averageSkipRate"),
            "average_stake_weighted_skip_rate": data.get("averageStakeWeightedSkipRate"),
            "stake_by_version": data.get("stakeByVersion", {}),
        }
        validators = data.get("validators", []); logger.info(f"✅ {len(validators)} validators")
        for val in validators:
            pk = val["identityPubkey"]; self.validator_data[pk] = val
            found = False
            for rec in self.records:
                if rec.identity_pubkey == pk:
                    found = True; rec.role = "validator"; rec.is_validator = True
                    rec.vote_account = val.get("voteAccountPubkey"); rec.commission = val.get("commission")
                    rec.epoch_credits = val.get("epochCredits")
                    rec.activated_stake_lamports = val.get("activatedStake",0)
                    rec.delinquent = val.get("delinquent", False); rec.skip_rate = val.get("skipRate",0)
                    rec.version = val.get("version", rec.version)
                    if ts>0: rec.stake_percentage = rec.activated_stake_lamports/ts*100
                    break
            if not found:
                dl = val.get("delinquent", False)
                off = NodeInfo(identity_pubkey=pk, role="validator-inactive" if dl else "validator-hidden",
                    is_validator=True, is_offline=True, delinquent=dl,
                    vote_account=val.get("voteAccountPubkey"), commission=val.get("commission"),
                    epoch_credits=val.get("epochCredits"), activated_stake_lamports=val.get("activatedStake",0),
                    skip_rate=val.get("skipRate",0), version=val.get("version"))
                if ts>0: off.stake_percentage = off.activated_stake_lamports/ts*100
                self.records.append(off)
        return True

    def _fetch_validator_info(self):
        logger.info("📝 Fetching validator-info...")
        data = self.api_cache.get("validator_info", self.cluster, API_TTL["validator_info"])
        if data is None:
            fm = {"devnet":"-ud","testnet":"-ut","mainnet-beta":"-um"}
            data = run_solana_cmd(["solana",fm.get(self.cluster,"-um"),"validator-info","get","--output","json"], timeout=20, desc="validator-info")
            if not data: logger.warning("⚠️  validator-info unavailable"); return
            self.api_cache.set("validator_info", self.cluster, data)
        else:
            logger.info("  📦 validator-info from cache")
        c = 0
        for entry in data:
            pk = entry.get("identityPubkey"); info = entry.get("info",{})
            if pk and info:
                for rec in self.records:
                    if rec.identity_pubkey == pk:
                        if not rec.name: rec.name = info.get("name")
                        if not rec.website: rec.website = info.get("website")
                        rec.details = info.get("details"); rec.icon_url = info.get("iconUrl")
                        c += 1; break
        logger.info(f"✅ {c} validator-info applied")

    def _fetch_trillium(self):
        logger.info("📈 Fetching Trillium...")
        try:
            raw = self.api_cache.get("trillium", "validators", API_TTL["trillium"])
            if raw is None:
                r = requests.get("https://api.trillium.so/validator_rewards", timeout=10)
                if r.status_code != 200: logger.warning(f"⚠️  Trillium HTTP {r.status_code}"); return
                raw = r.json()
                self.api_cache.set("trillium", "validators", raw)
            else:
                logger.info("  📦 Trillium from cache")
            for v in raw:
                pk = v.get("identity_pubkey")
                if pk: self.trillium_data[pk] = v
            upd = 0
            for rec in self.records:
                td = self.trillium_data.get(rec.identity_pubkey)
                if td:
                    if td.get("name"): rec.name = td["name"]
                    rec.client_type = td.get("client_type")
                    rec.mev_commission = td.get("mev_commission")
                    rec.is_sfdp = td.get("is_sfdp")
                    rec.sfdp_state = td.get("sfdp_state")
                    rec.icon_url = td.get("icon_url"); rec.website = td.get("website")
                    rec.testnet_pubkey = td.get("testnet_pubkey")
                    try:
                        if td.get("median_vote_latency") is not None: rec.median_vote_latency = float(td["median_vote_latency"])
                    except (ValueError,TypeError): pass
                    try:
                        if td.get("slot_duration_median") is not None: rec.slot_duration_median = float(td["slot_duration_median"])
                    except (ValueError,TypeError): pass
                    upd += 1
            logger.info(f"✅ Trillium: {upd} updated")
        except Exception as e: logger.warning(f"⚠️  Trillium: {e}")

    def _fetch_doublezero(self):
        logger.info("🔌 Fetching DoubleZero...")
        dz_env = {"mainnet-beta":"mainnet-beta","testnet":"testnet","devnet":"devnet"}.get(self.cluster, "mainnet-beta")

        # === METROS (map metro_code → metro_name, needed for devices API) ===
        metro_names = {}  # code -> name
        metros_resp = _fetch_malbec_single("metros?limit=100&offset=0")
        if metros_resp:
            items = metros_resp.get("items", metros_resp) if isinstance(metros_resp, dict) else metros_resp
            for m in (items if isinstance(items, list) else []):
                metro_names[m.get("code", "")] = m.get("name", "")
            if metro_names: self.dz_malbec_available = True

        # === DEVICES (Malbec API primary, CLI fallback) ===
        devices = self.api_cache.get("dz_devices", dz_env, API_TTL["dz_devices"])
        if devices is not None:
            logger.info("  📦 DZ devices from cache")
        else:
            devices = _fetch_malbec_paginated("devices")
            if devices is not None:
                self.dz_malbec_available = True
                self.api_cache.set("dz_devices", dz_env, devices)
            else:
                # CLI fallback
                try:
                    r = subprocess.run(["doublezero","--env",dz_env,"device","list","--json"],
                                       capture_output=True, text=True, timeout=30)
                    if r.returncode == 0:
                        try: devices = json.loads(r.stdout)
                        except: devices = []
                        self.api_cache.set("dz_devices", dz_env, devices)
                    else: devices = []
                except FileNotFoundError: logger.warning("  ⚠️  doublezero CLI not found"); devices = []
                except Exception as e: logger.warning(f"  ⚠️  DZ devices CLI: {e}"); devices = []

        if devices:
            logger.info(f"✅ {len(devices)} DZ devices")
            for d in devices:
                ip = d.get("public_ip")
                if not ip: continue
                self.all_ips.add(ip)
                mc = d.get("metro_code", "")
                loc = d.get("metro_name") or metro_names.get(mc, "") or d.get("location_name", "")
                code = d.get("code", "")
                self.records.append(NodeInfo(
                    identity_pubkey=d.get("pk", d.get("account", "")), ip_address=ip, role="dz-device",
                    is_dz_device=True, dz_connected=True,
                    dz_device_account=d.get("pk", d.get("account")),
                    dz_device_name=code,
                    dz_location=loc,
                    dz_exchange=d.get("exchange_name"),
                    dz_metro_code=mc,
                    dz_contributor=d.get("contributor_code"),
                ))
                # Store device capacity/traffic details for export
                if code:
                    self.dz_device_details[code] = {
                        "device_type": d.get("device_type"),
                        "max_users": d.get("max_users"),
                        "current_users": d.get("current_users"),
                        "in_bps": d.get("in_bps"),
                        "out_bps": d.get("out_bps"),
                        "validator_count": d.get("validator_count"),
                        "stake_sol": d.get("stake_sol"),
                    }

        # === USERS (Malbec API primary, CLI fallback) ===
        users = self.api_cache.get("dz_users", dz_env, API_TTL["dz_users"])
        if users is not None:
            logger.info("  📦 DZ users from cache")
        else:
            users = _fetch_malbec_paginated("users")
            if users is not None:
                self.dz_malbec_available = True
                self.api_cache.set("dz_users", dz_env, users)
            else:
                # CLI fallback
                try:
                    r = subprocess.run(["doublezero","--env",dz_env,"user","list","--json"],
                                       capture_output=True, text=True, timeout=30)
                    if r.returncode == 0:
                        try: users = json.loads(r.stdout)
                        except: users = []
                        self.api_cache.set("dz_users", dz_env, users)
                    else: users = []
                except Exception as e: logger.warning(f"  ⚠️  DZ users CLI: {e}"); users = []

        if users:
            logger.info(f"✅ {len(users)} DZ users")
            # Build IP → connection types map
            ip_kinds = defaultdict(set)   # client_ip -> {"ibrl", "multicast"}
            ip_device = {}                # client_ip -> device_code
            ip_metro = {}                 # client_ip -> (metro_code, metro_name)
            for u in users:
                cip = u.get("client_ip")
                if not cip: continue
                kind = u.get("kind", "")
                if kind: ip_kinds[cip].add(kind)
                if cip not in ip_device:
                    ip_device[cip] = u.get("device_code", u.get("device_name", ""))
                    mc = u.get("metro_code", "")
                    mn = u.get("metro_name") or metro_names.get(mc, "") or u.get("location_name", "")
                    ip_metro[cip] = (mc, mn)
                # Old-style CLI fallback: accesspass regex
                if not kind and u.get("accesspass"):
                    m = re.search(r'SolanaValidator: \(([^)]+)\)', u.get("accesspass", ""))
                    if m:
                        for rec in self.records:
                            if rec.identity_pubkey == m.group(1):
                                rec.dz_connected = True
                                rec.dz_device_name = rec.dz_device_name or u.get("device_name", "")
                                rec.dz_location = rec.dz_location or u.get("location_name", "")

            # Map users to validator records by client_ip
            for rec in self.records:
                if not rec.ip_address or rec.is_dz_device: continue
                if rec.ip_address in ip_kinds:
                    rec.dz_connected = True
                    kinds = ip_kinds[rec.ip_address]
                    if "ibrl" in kinds and "multicast" in kinds:
                        rec.dz_connection_type = "ibrl+multicast"
                    elif "multicast" in kinds:
                        rec.dz_connection_type = "multicast"
                    elif "ibrl" in kinds:
                        rec.dz_connection_type = "ibrl"
                    if not rec.dz_device_name:
                        rec.dz_device_name = ip_device.get(rec.ip_address, "")
                    metro = ip_metro.get(rec.ip_address, ("", ""))
                    if not rec.dz_location: rec.dz_location = metro[1]
                    if not rec.dz_metro_code: rec.dz_metro_code = metro[0]

        # === Auto-generate geo overrides from DZ devices ===
        if self.geo_overrides_path:
            dz_devices = [r for r in self.records if r.is_dz_device]
            dz_overrides = generate_dz_overrides(dz_devices)
            existing = load_geo_overrides(self.geo_overrides_path)
            admin_entries = {ip: e for ip, e in existing.items() if e.get("source") != "doublezero-verified"}
            merged = {**dz_overrides, **admin_entries}
            save_geo_overrides(self.geo_overrides_path, merged)
            self.geo_overrides = merged
            logger.info(f"  🗺️  Geo overrides: {len(dz_overrides)} DZ + {len(admin_entries)} admin")

    def _fetch_dz_multicast(self):
        """Fetch DZ multicast groups and publisher lists via Malbec API."""
        logger.info("📡 Fetching DZ multicast...")
        dz_env = {"mainnet-beta":"mainnet-beta","testnet":"testnet","devnet":"devnet"}.get(self.cluster, "mainnet-beta")
        try:
            # === GROUPS ===
            groups = self.api_cache.get("dz_multicast", dz_env, API_TTL["dz_multicast"])
            if groups is not None:
                logger.info("  📦 DZ multicast from cache")
            else:
                # Try Malbec API first
                api_groups = _fetch_malbec_single("multicast-groups", timeout=MALBEC_API_TIMEOUT)
                if api_groups and isinstance(api_groups, list):
                    groups = api_groups
                    self.dz_malbec_available = True
                else:
                    # CLI fallback
                    try:
                        r = subprocess.run(["doublezero","--env",dz_env,"multicast","group","list","--json"],
                                           capture_output=True, text=True, timeout=30)
                        if r.returncode == 0:
                            try: groups = json.loads(r.stdout)
                            except: groups = self._parse_dz_multicast_table(r.stdout)
                        else: groups = []
                    except FileNotFoundError: groups = []
                    except Exception as e: logger.warning(f"  ⚠️  DZ multicast CLI: {e}"); groups = []
                self.api_cache.set("dz_multicast", dz_env, groups)

            self.dz_multicast_groups = groups or []
            logger.info(f"✅ {len(self.dz_multicast_groups)} DZ multicast groups")

            # === PUBLISHERS per active group ===
            active_groups = [g for g in self.dz_multicast_groups
                            if (g.get("publisher_count") or g.get("publishers", 0)) > 0]
            if active_groups and self.dz_malbec_available:
                self._fetch_dz_multicast_publishers(active_groups)

        except Exception as e:
            logger.warning(f"⚠️  DZ multicast: {e}"); self.dz_multicast_groups = []

    def _fetch_dz_multicast_publishers(self, active_groups):
        """Fetch publisher lists for active multicast groups and map to validators."""
        all_publishers_cache_key = "dz_publishers"
        dz_env = self.cluster
        cached = self.api_cache.get(all_publishers_cache_key, dz_env, API_TTL["dz_publishers"])
        if cached is not None:
            logger.info("  📦 DZ publishers from cache")
            publishers_data = cached
        else:
            publishers_data = {}  # group_code -> list of publisher dicts
            def _fetch_group_publishers(g):
                code = g.get("code", "")
                pk = g.get("pk", g.get("account", ""))
                if not pk: return code, []
                members = _fetch_malbec_single(
                    f"multicast-groups/{pk}/members?tab=publishers&limit=1000")
                if members and "items" in members:
                    return code, members["items"]
                return code, None  # None = fetch failed, [] = empty but successful

            failed_groups = []
            with ThreadPoolExecutor(max_workers=min(len(active_groups), 5)) as ex:
                futures = {ex.submit(_fetch_group_publishers, g): g for g in active_groups}
                for f in as_completed(futures):
                    try:
                        code, items = f.result()
                        if items is None:
                            failed_groups.append(code)
                        elif items:
                            publishers_data[code] = items
                            logger.info(f"    📡 {code}: {len(items)} publishers")
                    except Exception as e:
                        logger.warning(f"    ⚠️  Publisher fetch error: {e}")

            # Partial merge: if some groups failed, try to recover from stale cache
            if failed_groups:
                # Ignore TTL: we'd rather use stale data than lose it entirely
                stale = self.api_cache.get(all_publishers_cache_key, dz_env, ttl_seconds=10**9)
                if stale:
                    recovered = 0
                    for code in failed_groups:
                        if code in stale and code not in publishers_data:
                            publishers_data[code] = stale[code]
                            recovered += 1
                    if recovered:
                        logger.info(f"    ♻️  Recovered {recovered}/{len(failed_groups)} groups from previous cache")
                logger.warning(f"    ⚠️  {len(failed_groups)} groups failed to fetch: {', '.join(failed_groups[:5])}")

            self.api_cache.set(all_publishers_cache_key, dz_env, publishers_data)

        # Build lookup maps: IP -> groups, pubkey -> groups
        for code, pubs in publishers_data.items():
            for p in pubs:
                cip = p.get("client_ip", "")
                npk = p.get("node_pubkey", "")
                if cip:
                    if cip not in self.dz_publishers_by_ip: self.dz_publishers_by_ip[cip] = []
                    if code not in self.dz_publishers_by_ip[cip]: self.dz_publishers_by_ip[cip].append(code)
                if npk:
                    if npk not in self.dz_publishers_by_pubkey: self.dz_publishers_by_pubkey[npk] = []
                    if code not in self.dz_publishers_by_pubkey[npk]: self.dz_publishers_by_pubkey[npk].append(code)

        # Map publishers to validator records
        pub_matched = 0
        for rec in self.records:
            if not rec.is_validator: continue
            groups = set()
            # Match by identity pubkey (node_pubkey in publisher data)
            if rec.identity_pubkey in self.dz_publishers_by_pubkey:
                groups.update(self.dz_publishers_by_pubkey[rec.identity_pubkey])
            # Match by IP as fallback
            if rec.ip_address and rec.ip_address in self.dz_publishers_by_ip:
                groups.update(self.dz_publishers_by_ip[rec.ip_address])
            if groups:
                rec.dz_multicast_publisher = True
                rec.dz_multicast_groups = sorted(groups)
                pub_matched += 1
                # Ensure connection type reflects multicast
                if rec.dz_connection_type == "ibrl":
                    rec.dz_connection_type = "ibrl+multicast"
                elif not rec.dz_connection_type:
                    rec.dz_connection_type = "multicast"

        total_pubs = sum(len(v) for v in publishers_data.values())
        logger.info(f"  ✅ DZ publishers: {total_pubs} total, {pub_matched} validators matched")

    @staticmethod
    def _parse_dz_multicast_table(text):
        """Parse table output from doublezero multicast group list."""
        groups = []; lines = text.strip().split('\n')
        for line in lines[1:]:  # Skip header
            parts = [p.strip() for p in line.split('|')]
            if len(parts) >= 7:
                try:
                    groups.append({
                        "account": parts[0], "code": parts[1], "multicast_ip": parts[2],
                        "max_bandwidth": parts[3], "publishers": int(parts[4]),
                        "subscribers": int(parts[5]), "status": parts[6],
                        "owner": parts[7] if len(parts) > 7 else "",
                    })
                except (ValueError, IndexError): continue
        return groups

    def _fetch_bam(self):
        """Fetch BAM data from explorer.bam.dev API (4 endpoints)."""
        logger.info("🎯 Fetching BAM...")

        def _bam_get(path, desc):
            for attempt in range(BAM_API_RETRIES):
                try:
                    r = requests.get(f"{BAM_API_BASE}{path}", timeout=BAM_API_TIMEOUT)
                    if r.status_code == 200:
                        return r.json()
                    logger.warning(f"  ⚠️  BAM {desc} HTTP {r.status_code}")
                except requests.exceptions.RequestException as e:
                    if attempt < BAM_API_RETRIES - 1:
                        logger.warning(f"  ⚠️  BAM {desc} retry ({attempt+1}): {e}")
                        time.sleep(1)
                    else:
                        logger.warning(f"  ⚠️  BAM {desc} failed: {e}")
                except Exception as e:
                    logger.warning(f"  ⚠️  BAM {desc} error: {e}")
            return None

        # 1. /nodes — list of BAM nodes with stats
        nodes = self.api_cache.get("bam_nodes", "default", API_TTL["bam_nodes"])
        if nodes is None:
            nodes = _bam_get("/nodes", "nodes")
            if nodes is not None: self.api_cache.set("bam_nodes", "default", nodes)
        else: logger.info("  📦 BAM nodes from cache")
        if nodes is not None:
            self.bam_nodes_api = nodes
            self.bam_api_available = True
            logger.info(f"  ✅ BAM nodes: {len(nodes)}")
        else:
            logger.warning("  ⚠️  BAM API unavailable, skipping BAM data")
            return

        # 2. /validators — validator→bam_node mapping
        validators = self.api_cache.get("bam_validators", "default", API_TTL["bam_validators"])
        if validators is None:
            validators = _bam_get("/validators", "validators")
            if validators: self.api_cache.set("bam_validators", "default", validators)
        else: logger.info("  📦 BAM validators from cache")
        if validators:
            for v in validators:
                pk = v.get("validator_pubkey")
                if pk:
                    self.bam_validators_api[pk] = v
            logger.info(f"  ✅ BAM validators: {len(self.bam_validators_api)}")

        # 3. /ibrl_validators — IBRL scores
        ibrl_data = self.api_cache.get("bam_ibrl", "default", API_TTL["bam_ibrl"])
        if ibrl_data is None:
            ibrl_data = _bam_get("/ibrl_validators", "IBRL")
            if ibrl_data: self.api_cache.set("bam_ibrl", "default", ibrl_data)
        else: logger.info("  📦 BAM IBRL from cache")
        if ibrl_data:
            # API returns {"data": [...]} or just [...]
            entries = ibrl_data.get("data", ibrl_data) if isinstance(ibrl_data, dict) else ibrl_data
            if isinstance(entries, list):
                for v in entries:
                    pk = v.get("identity")
                    if pk:
                        self.bam_ibrl_api[pk] = v
                logger.info(f"  ✅ IBRL scores: {len(self.bam_ibrl_api)}")

        # 4. /bam_stake — total BAM stake
        stake_data = self.api_cache.get("bam_stake", "default", API_TTL["bam_stake"])
        if stake_data is None:
            stake_data = _bam_get("/bam_stake", "stake")
            if stake_data: self.api_cache.set("bam_stake", "default", stake_data)
        else: logger.info("  📦 BAM stake from cache")
        if stake_data:
            self.bam_stake_api = stake_data
            pct = stake_data.get("bam_stake_percentage", 0)
            logger.info(f"  ✅ BAM stake: {pct:.1f}%")

        # Apply BAM data to validator records
        bam_applied = 0
        for rec in self.records:
            if not rec.is_validator:
                continue
            # BAM node connection
            bam_v = self.bam_validators_api.get(rec.identity_pubkey)
            if bam_v:
                rec.bam_node = bam_v.get("bam_node_connection")
                # Find region from nodes API
                for n in self.bam_nodes_api:
                    if n.get("bam_node") == rec.bam_node:
                        rec.bam_region = n.get("region")
                        break
                bam_applied += 1
            # IBRL scores (independent of BAM connection)
            ibrl_v = self.bam_ibrl_api.get(rec.identity_pubkey)
            if ibrl_v:
                rec.ibrl_score = ibrl_v.get("ibrl_score")
                rec.ibrl_build_time_score = ibrl_v.get("build_time_score")
                rec.ibrl_vote_packing_score = ibrl_v.get("vote_packing_score")
                rec.ibrl_non_vote_packing_score = ibrl_v.get("non_vote_packing_score")
                rec.ibrl_median_block_build_ms = ibrl_v.get("median_block_build_ms")

        logger.info(f"  ✅ BAM applied: {bam_applied} validators, IBRL: {len(self.bam_ibrl_api)}")

    def _fetch_rakurai(self):
        """Fetch Rakurai validator data from API."""
        logger.info("⚡ Fetching Rakurai...")
        try:
            data = self.api_cache.get("rakurai", "overview", API_TTL["rakurai"])
            if data is None:
                r = requests.get(f"{RAKURAI_API_BASE}/validators/overview", timeout=RAKURAI_API_TIMEOUT)
                if r.status_code != 200:
                    logger.warning(f"  ⚠️  Rakurai HTTP {r.status_code}"); return
                data = r.json()
                self.api_cache.set("rakurai", "overview", data)
            else:
                logger.info("  📦 Rakurai from cache")

            validators = data.get("validators", [])
            rak_applied = 0
            for v in validators:
                pk = v.get("identity")
                if pk:
                    self.rakurai_data[pk] = v
                    for rec in self.records:
                        if rec.identity_pubkey == pk:
                            rec.is_rakurai = True; rak_applied += 1; break
            logger.info(f"  ✅ Rakurai: {len(validators)} validators, {rak_applied} matched")
        except Exception as e:
            logger.warning(f"  ⚠️  Rakurai: {e}")

    def _fetch_dz_health(self):
        """Fetch DZ network health: device telemetry + link telemetry."""
        logger.info("🏥 Fetching DZ network health...")
        health = {}
        try:
            # Device telemetry
            dev_telem = self.api_cache.get("dz_health_devices", "mainnet", API_TTL["dz_health_devices"])
            if dev_telem is not None:
                logger.info("  📦 DZ device health from cache")
            else:
                dev_telem = _fetch_dz_health_api("current-device-telemetry", "mainnet")
                if dev_telem is not None:
                    self.api_cache.set("dz_health_devices", "mainnet", dev_telem)

            if dev_telem:
                reporting = [d for d in dev_telem if d.get("status") == "reporting"]
                no_telem = [d for d in dev_telem if d.get("status") != "reporting"]
                health["devices_total"] = len(dev_telem)
                health["devices_reporting"] = len(reporting)
                health["devices_no_telemetry"] = len(no_telem)
                if no_telem:
                    health["devices_issues"] = [
                        {"device_id": d.get("device_id"), "location": d.get("location"), "status": d.get("status")}
                        for d in no_telem
                    ]

            # Link telemetry
            link_telem = self.api_cache.get("dz_health_links", "mainnet", API_TTL["dz_health_links"])
            if link_telem is not None:
                logger.info("  📦 DZ link health from cache")
            else:
                link_telem = _fetch_dz_health_api("current-link-telemetry", "mainnet")
                if link_telem is not None:
                    self.api_cache.set("dz_health_links", "mainnet", link_telem)

            if link_telem:
                healthy = [l for l in link_telem if l.get("quality_status") == "healthy"]
                degraded = [l for l in link_telem if l.get("quality_status") not in ("healthy", "unavailable") and l.get("quality_status")]
                unavail = [l for l in link_telem if l.get("quality_status") == "unavailable"]
                health["links_total"] = len(link_telem)
                health["links_healthy"] = len(healthy)
                health["links_degraded"] = len(degraded)
                health["links_unavailable"] = len(unavail)
                issues = [l for l in link_telem if l.get("quality_status") in ("unavailable",) or (l.get("packet_loss") or 0) > 0]
                if issues:
                    health["link_issues"] = [
                        {"link": l.get("link_name"), "source_target": l.get("source_target"),
                         "contributor": l.get("contributor_name"), "quality": l.get("quality_status"),
                         "rtt_ms": round(l.get("rtt_ms", 0), 2), "packet_loss": l.get("packet_loss", 0)}
                        for l in issues[:20]  # cap at 20 issues
                    ]

            if health:
                self.dz_network_health = health
                dev_info = f"devices: {health.get('devices_reporting', '?')}/{health.get('devices_total', '?')}" if "devices_total" in health else ""
                link_info = f"links: {health.get('links_healthy', '?')}/{health.get('links_total', '?')} healthy" if "links_total" in health else ""
                parts = [p for p in [dev_info, link_info] if p]
                logger.info(f"  ✅ DZ health: {', '.join(parts)}")
            else:
                logger.info("  ℹ️  DZ health: no telemetry data available")

        except Exception as e:
            logger.warning(f"  ⚠️  DZ health: {e}")

    def _fetch_dz_contributors(self):
        """Fetch DZ infrastructure contributors with bandwidth details."""
        try:
            contribs = self.api_cache.get("dz_contributors", "mainnet", API_TTL["dz_contributors"])
            if contribs is not None:
                logger.info("  📦 DZ contributors from cache")
            else:
                contribs = _fetch_malbec_paginated("contributors")
                if contribs is not None:
                    # Enrich with detail data (bandwidth) — parallel fetch
                    def _fetch_detail(c):
                        pk = c.get("pk", "")
                        if not pk: return c
                        detail = _fetch_malbec_single(f"contributors/{pk}")
                        if detail:
                            c["in_bps"] = detail.get("in_bps")
                            c["out_bps"] = detail.get("out_bps")
                            c["user_count"] = detail.get("user_count")
                        return c
                    with ThreadPoolExecutor(max_workers=min(len(contribs), 7)) as ex:
                        list(ex.map(_fetch_detail, contribs))
                    self.api_cache.set("dz_contributors", "mainnet", contribs)
            if contribs:
                self.dz_contributors_data = contribs
                logger.info(f"  ✅ DZ contributors: {len(contribs)}")
        except Exception as e:
            logger.warning(f"  ⚠️  DZ contributors: {e}")

    def _fetch_endpoints_from_config(self):
        logger.info(f"🔗 Loading endpoints from {self.endpoints_config}...")
        eps = load_endpoints(self.endpoints_config, self.cluster)
        if not eps: logger.info("  No endpoints"); return
        c = 0
        for ep in eps:
            role = f"{ep['provider']}-{ep['service']}"
            name = f"{ep['provider']}-{ep['service']}-{ep['label']}"
            node = NodeInfo(identity_pubkey=ep['ip'], ip_address=ep['ip'], role=role, name=name,
                endpoint_provider=ep['provider'], endpoint_service=ep['service'],
                endpoint_label=ep['label'], endpoint_port=ep.get('port'),
                endpoint_bam_id=ep.get('bam_id'),
                is_jito=(ep['provider']=='jito'), _ep_source=ep.get('source',''))
            self.all_ips.add(ep['ip']); self.records.append(node); self.endpoint_records.append(node); c+=1
        logger.info(f"✅ {c} endpoints loaded")

    def _create_bam_endpoints_from_api(self):
        """Create BAM endpoint records dynamically from BAM API data.

        BAM nodes auto-rotate names frequently (e.g. '-1-tee' becomes '-2-tee',
        new regions appear, old ones disappear). Hardcoding them in
        endpoints.yaml led to constant false 'missing from API' and 'new BAM
        node' warnings.

        Instead we treat /api/v1/nodes as the source of truth: every entry
        becomes an endpoint record with role 'jito-bam' and reachability
        derived directly from connected_validators count. The bam_node field
        is the unique ID (region is unstable — Jito sometimes sets it to a
        short alias, sometimes to the bam_node name itself).

        Applies to mainnet only. Testnet BAM stays in endpoints.yaml as
        there is no public BAM API for testnet.
        """
        if not self.bam_api_available or not self.bam_nodes_api:
            logger.info("🎯 BAM endpoints: API unavailable, skipping")
            return

        logger.info(f"🎯 Creating BAM endpoints from API ({len(self.bam_nodes_api)} nodes)...")
        created = 0
        for n in self.bam_nodes_api:
            bam_id = n.get("bam_node", "")
            region = n.get("region", "")
            connected = n.get("connected_validators", 0)
            if not bam_id:
                continue

            # Derive region_short from bam_node name for DNS resolution.
            # Pattern: "{region_short}-mainnet-bam-{N}-tee" → region_short
            # Examples seen: amsterdam, dublin, dallas, frankfurt, london, lax,
            # ny, pittsburgh, slc, singapore, tokyo.
            # DNS endpoint: {region_short}.mainnet.bam.jito.wtf
            region_short = bam_id.split("-mainnet-")[0] if "-mainnet-" in bam_id else (region or bam_id)
            dns_host = f"{region_short}.mainnet.bam.jito.wtf"
            ips = resolve_dns(dns_host)
            ip = ips[0] if ips else dns_host

            # Reachability: connected_validators > 0 → True, 0 → None (probe
            # state, node might be warming up). Disappearance from API is
            # detected separately by timeseries.py (missing_cycles logic).
            if connected > 0:
                reachable = True
            else:
                reachable = None
                logger.info(f"  ⚠️  BAM {bam_id}: 0 connected validators (probe state)")

            node = NodeInfo(
                identity_pubkey=bam_id,  # use bam_node as unique identity
                ip_address=ip,
                role="jito-bam",
                name=f"jito-bam-{bam_id}",  # unique across tee rotations (bam_id includes -N-tee suffix)
                endpoint_provider="jito",
                endpoint_service="bam",
                endpoint_label=region_short,  # short label (e.g. "amsterdam") for UI
                endpoint_port=None,
                endpoint_bam_id=bam_id,
                endpoint_reachable=reachable,
                is_jito=True,
                _ep_source="bam-api",
            )
            if ips:
                self.all_ips.add(ip)
            self.records.append(node)
            self.endpoint_records.append(node)
            created += 1
        logger.info(f"  ✅ BAM endpoints: {created} created from API")

    def _check_endpoints_reachability(self):
        if not self.endpoint_records: return
        logger.info(f"🔍 Checking reachability for {len(self.endpoint_records)} endpoints...")

        # BAM endpoints from mainnet BAM API are created by
        # _create_bam_endpoints_from_api() with reachability already set.
        # We skip them here; everything else (including testnet BAM from
        # endpoints.yaml) goes through TCP/HTTPS check below.
        already_checked = [n for n in self.endpoint_records if n._ep_source == "bam-api"]
        other_eps = [n for n in self.endpoint_records if n._ep_source != "bam-api"]

        # Non-BAM endpoints: TCP/HTTP check
        with ThreadPoolExecutor(max_workers=min(CONCURRENT_WORKERS, 10)) as ex:
            futures = {}
            for node in other_eps:
                method = EP_CHECK_METHOD.get(node.endpoint_service, "https")
                f = ex.submit(check_endpoint, node.ip_address, node.endpoint_port, node._ep_source, method)
                futures[f] = node
                time.sleep(EP_CHECK_COOLDOWN)

            for f in as_completed(futures):
                node = futures[f]
                try: node.endpoint_reachable = f.result()
                except Exception as e:
                    logger.warning(f"  Check error {node.ip_address}: {e}"); node.endpoint_reachable = False

        reach = sum(1 for n in self.endpoint_records if n.endpoint_reachable is True)
        unreach = sum(1 for n in self.endpoint_records if n.endpoint_reachable is False)
        unverif = sum(1 for n in self.endpoint_records if n.endpoint_reachable is None)
        logger.info(f"✅ Endpoints: {reach} reachable, {unreach} unreachable, {unverif} unverifiable")

        for n in self.endpoint_records:
            if n.endpoint_reachable is False:
                logger.warning(f"  ❌ Unreachable: {n.name} ({n.ip_address}:{n.endpoint_port or 'https'})")

    def _fetch_epoch(self):
        logger.info("📅 Fetching epoch...")
        data = run_solana_cmd(["solana","--url",self.cluster_url,"epoch-info","--output","json"], desc="epoch", critical=True)
        if data:
            self.current_epoch = data.get("epoch"); self.current_slot = data.get("absoluteSlot")
            si, se = data.get("slotIndex",0), data.get("slotsInEpoch",432000)
            self.epoch_completed_percent = (si/se*100) if se else 0
            logger.info(f"✅ Epoch: {self.current_epoch}, Slot: {self.current_slot}, {self.epoch_completed_percent:.1f}%")

    def _process_geolocation(self):
        logger.info(f"🌍 Geolocation for {len(self.all_ips)} IPs...")
        self.geo_data = self.geo_service.process_ips(list(self.all_ips))
        self.geo_discrepancies = [g for g in self.geo_data.values() if g.discrepancy]
        if self.geo_discrepancies:
            logger.warning(f"⚠️  {len(self.geo_discrepancies)} discrepancies")
            for d in self.geo_discrepancies[:5]: logger.warning(f"  {d.ip}: {d.discrepancy_details}")

    def _apply_geo_overrides(self):
        """Apply geo overrides from DZ devices and admin entries.
        DZ auto: only applies when there's a discrepancy (city-level diff with same country = OK).
        Admin: always tries to apply (has explicit country_code).
        When switching primary: updates ALL geo fields from expanded alternatives.
        """
        if not self.geo_overrides: return
        applied = 0; skipped_no_disc = 0; unresolved = 0
        for ip, override in self.geo_overrides.items():
            geo = self.geo_data.get(ip)
            if not geo: continue
            override_city = override.get("city", "")
            override_cc = override.get("country_code")  # None/missing for DZ, set for admin
            is_dz_auto = override.get("source") == "doublezero-verified"

            # DZ auto without discrepancy → geo is probably fine, skip
            if is_dz_auto and not override_cc and not geo.discrepancy:
                skipped_no_disc += 1; continue

            # Check if current primary already matches
            if override_cc and geo.country_code == override_cc: continue
            if not override_cc and override_city and normalize_city(geo.city).lower() == override_city.lower(): continue

            # Find best matching source in alternatives
            best_source = None
            if geo.discrepancy_alternatives:
                # Pass 1: exact city match
                for src_name, alt in geo.discrepancy_alternatives.items():
                    alt_city = alt.get("city", "")
                    if override_cc and alt.get("country") == override_cc:
                        best_source = src_name; break
                    elif override_city and alt_city:
                        if override_city.lower() in alt_city.lower() or alt_city.lower() in override_city.lower():
                            best_source = src_name; break
                # Pass 2: if DZ auto, try any source with different country (carrier IP)
                if not best_source and is_dz_auto and not override_cc:
                    for src_name, alt in geo.discrepancy_alternatives.items():
                        if alt.get("country") and alt["country"] != geo.country_code:
                            best_source = src_name; break

            if best_source and geo.discrepancy_alternatives:
                alt = geo.discrepancy_alternatives[best_source]
                original_cc = geo.country_code; original_primary = geo.primary_source

                # Update ALL geo fields from expanded alternative
                geo.country_code = alt.get("country", geo.country_code)
                geo.city = alt.get("city") or override_city or geo.city
                geo.region = alt.get("region", "")
                if alt.get("latitude") is not None: geo.latitude = alt["latitude"]
                if alt.get("longitude") is not None: geo.longitude = alt["longitude"]
                # Country name: derive from raw source or clear
                raw_src = geo.sources.get(best_source)
                if raw_src:
                    if best_source == "ipinfo":
                        geo.country = raw_src.get("country", geo.country_code)  # ipinfo only has CC
                    elif best_source == "ipapi":
                        geo.country = raw_src.get("country", geo.country_code)
                    elif best_source == "geojs":
                        geo.country = raw_src.get("name", geo.country_code)
                else:
                    geo.country = geo.country_code  # Fallback: use CC as name
                geo.primary_source = best_source
                geo.confidence = "high"

                # Parse ASN from alternative
                alt_org = alt.get("org", "")
                alt_asn = alt.get("asn", "")
                if alt_asn and isinstance(alt_asn, str) and alt_asn.startswith("AS"):
                    geo.asn = alt_asn.split()[0]
                    try: geo.asn_number = int(geo.asn.replace("AS",""))
                    except: pass
                if alt_org:
                    geo.asn_name = alt_org.split(None,1)[-1] if alt_org.startswith("AS") else alt_org

                geo.sources["_geo_override"] = {
                    "source": override.get("source", "unknown"),
                    "original_primary": original_primary,
                    "original_country": original_cc,
                    "matched_via": best_source,
                }
                applied += 1
            else:
                # No matching source — annotate only if admin override
                if not is_dz_auto:
                    geo.sources["_geo_override"] = {
                        "source": override.get("source", "unknown"),
                        "original_primary": geo.primary_source,
                        "original_country": geo.country_code,
                        "matched_via": None,
                        "suggested_city": override_city,
                    }
                    unresolved += 1
                # DZ auto with discrepancy but no matching source — skip silently

        parts = [f"{applied} applied"]
        if skipped_no_disc: parts.append(f"{skipped_no_disc} skipped (no discrepancy)")
        if unresolved: parts.append(f"{unresolved} unresolved")
        if applied or unresolved:
            logger.info(f"🗺️  Geo overrides: {', '.join(parts)}")

    def _detect_co_hosted(self):
        c = 0
        for ip, nodes in self.ip_to_records.items():
            if len(nodes)<=1: continue
            vals = [n for n in nodes if n.is_validator]; others = [n for n in nodes if n.role=="unknown-node"]
            if vals and others:
                vpks = [v.identity_pubkey for v in vals]
                for o in others: o.role="co-hosted"; o.is_co_hosted=True; o.co_hosted_with=vpks; c+=1
        if c: logger.info(f"🔗 {c} co-hosted nodes")

    def _determine_version_status(self):
        fc = Counter(); fmax = {}
        for r in self.records:
            if r.is_validator and r.version:
                fam = version_family(r.version)
                if fam:
                    fc[fam] += 1; vt = version_tuple(r.version)
                    if fam not in fmax or vt > fmax[fam]: fmax[fam] = vt
        if not fc: return
        cons_fams = set(f for f,_ in fc.most_common(2))
        def branch(fam): return "fd" if fam.startswith("0.") else "ag"
        bmax = {}
        for fam in cons_fams:
            b = branch(fam)
            if b not in bmax or fmax[fam] > bmax[b]: bmax[b] = fmax[fam]
        for r in self.records:
            fam = version_family(r.version)
            if fam is None: r.version_status = None; continue
            b = branch(fam); vt = version_tuple(r.version)
            if fam in cons_fams: r.version_status = "current"
            elif b in bmax and vt >= bmax[b]: r.version_status = "current"
            else:
                if b in bmax:
                    mx = bmax[b]
                    diff = (mx[1] if b=="fd" and len(mx)>1 else mx[0]) - (vt[1] if b=="fd" and len(vt)>1 else vt[0])
                    r.version_status = "outdated" if diff <= 1 else "ancient"
                else: r.version_status = "ancient"

    def _calculate_superminority(self):
        vals = sorted([r for r in self.records if r.is_validator and r.stake_percentage is not None],
                      key=lambda r: r.stake_percentage, reverse=True)
        cum = 0
        for r in vals:
            prev = cum; cum += r.stake_percentage
            r.cumulative_stake_percent = round(cum, 6)
            r.is_superminority = prev < 33.33

    # ----------------------------------------------------------------
    # METRICS
    # ----------------------------------------------------------------

    def calculate_metrics(self):
        online = [r for r in self.records if r.ip_address]
        all_val = [r for r in self.records if r.is_validator]
        online_val = [r for r in all_val if r.ip_address]
        rpc = [r for r in online if r.is_rpc]

        # Endpoint metrics with reachability included
        ep_metrics = {}
        for prov in set(r.endpoint_provider for r in self.endpoint_records if r.endpoint_provider):
            peps = [r for r in self.endpoint_records if r.endpoint_provider == prov]
            m = self._cat_metrics(peps)
            m["reachable"] = sum(1 for r in peps if r.endpoint_reachable is True)
            m["unreachable"] = sum(1 for r in peps if r.endpoint_reachable is False)
            m["unverifiable"] = sum(1 for r in peps if r.endpoint_reachable is None)
            ep_metrics[prov] = m

        # DZ
        dz_all = [r for r in self.records if r.dz_connected or r.is_dz_device]
        dz_v = [r for r in dz_all if r.is_validator]
        tv = len(all_val)
        dvs = sum(r.stake_percentage or 0 for r in dz_v)

        # Connection type breakdown
        dz_ibrl = len([r for r in dz_v if r.dz_connection_type == "ibrl"])
        dz_mc = len([r for r in dz_v if r.dz_connection_type == "multicast"])
        dz_both = len([r for r in dz_v if r.dz_connection_type == "ibrl+multicast"])
        dz_unknown = len([r for r in dz_v if r.dz_connected and not r.dz_connection_type])

        # Multicast publisher stats
        mc_pubs = [r for r in all_val if r.dz_multicast_publisher]
        mc_pub_stake = sum(r.stake_percentage or 0 for r in mc_pubs)

        # Multicast groups — normalize field names from API vs CLI
        mc_groups = []
        for g in self.dz_multicast_groups:
            pubs = g.get("publisher_count", g.get("publishers", 0)) or 0
            subs = g.get("subscriber_count", g.get("subscribers", 0)) or 0
            mc_groups.append({
                "code": g.get("code"), "multicast_ip": g.get("multicast_ip"),
                "max_bandwidth": _format_bandwidth(g.get("max_bandwidth")),
                "publishers": pubs,
                "subscribers": subs, "status": g.get("status"),
            })

        # Contributors summary
        dz_contribs = None
        if self.dz_contributors_data:
            dz_contribs = []
            for c in sorted(self.dz_contributors_data, key=lambda x: x.get("device_count", 0), reverse=True):
                entry = {"code": c.get("code"), "name": c.get("name"), "devices": c.get("device_count", 0),
                         "links": c.get("link_count", 0)}
                in_bps = c.get("in_bps"); out_bps = c.get("out_bps")
                if in_bps is not None: entry["in_bandwidth"] = _format_bandwidth(in_bps)
                if out_bps is not None: entry["out_bandwidth"] = _format_bandwidth(out_bps)
                dz_contribs.append(entry)

        dz_metrics = {
            "total": len(dz_all),
            "validators": len(dz_v), "validators_percent": round(len(dz_v)/tv*100,1) if tv else 0,
            "validators_stake_percent": round(dvs, 2),
            "connection_types": {
                "ibrl_only": dz_ibrl, "multicast_only": dz_mc,
                "ibrl_and_multicast": dz_both, "unknown": dz_unknown,
            },
            "multicast_publishers": {
                "total": len(mc_pubs), "stake_percent": round(mc_pub_stake, 2),
            },
            "rpc": len([r for r in dz_all if r.is_rpc]),
            "devices": len([r for r in dz_all if r.is_dz_device]),
            "other": dict(Counter(r.role for r in dz_all
                if not r.is_validator and not r.is_rpc and not r.is_dz_device).most_common()),
            "multicast_groups": mc_groups if mc_groups else [],
        }
        if dz_contribs: dz_metrics["contributors"] = dz_contribs
        if self.dz_network_health: dz_metrics["network_health"] = self.dz_network_health

        # DZ geo distributions
        dz_devs = [r for r in dz_all if r.is_dz_device and r.ip_address]
        if dz_devs:
            dz_metrics["device_geo"] = self._geo_distributions(dz_devs)
        if dz_v:
            dz_metrics["validator_geo"] = self._geo_distributions(dz_v, include_stake=True)

        return {
            "overall": self._cat_metrics(online),
            "validators": self._val_metrics(online_val, all_val),
            "rpc": self._cat_metrics(rpc),
            "endpoints": ep_metrics,
            "doublezero": dz_metrics,
            "bam": self._bam_metrics(all_val),
            "rakurai": self._rakurai_metrics(all_val),
            "cluster_health": self.cluster_health,
        }

    def _bam_metrics(self, all_val):
        """BAM metrics: nodes, stake, IBRL aggregates by geography."""
        if not self.bam_api_available:
            return None

        # Node list with stake in lamports
        nodes = []
        for n in self.bam_nodes_api:
            stake_sol = n.get("node_stake", 0)
            nodes.append({
                "node": n.get("bam_node"),
                "region": n.get("region"),
                "connected_validators": n.get("connected_validators", 0),
                "node_stake_lamports": int(stake_sol * LAMPORTS_PER_SOL),
            })

        # Total BAM stake in lamports
        total_stake_sol = self.bam_stake_api.get("bam_stake", 0)
        bam_connected = [r for r in all_val if r.bam_node]

        m = {
            "total_stake_lamports": int(total_stake_sol * LAMPORTS_PER_SOL),
            "stake_percentage": self.bam_stake_api.get("bam_stake_percentage", 0),
            "node_count": len(self.bam_nodes_api),
            "total_connected_validators": len(bam_connected),
            "nodes": nodes,
        }

        # IBRL aggregates by country/asn/city
        ibrl_vals = [r for r in all_val if r.ibrl_score is not None and r.ip_address]
        if ibrl_vals:
            m["ibrl_aggregates"] = self._ibrl_aggregates(ibrl_vals)
            m["ibrl_total_validators"] = len(ibrl_vals)

        # BAM geo distributions
        if bam_connected:
            m["validator_geo"] = self._geo_distributions(bam_connected, include_stake=True)
        # BAM nodes geo (from endpoints with role jito-bam)
        bam_node_records = [r for r in self.records if r.endpoint_service == "bam" and r.ip_address]
        if bam_node_records:
            m["node_geo"] = self._geo_distributions(bam_node_records)

        return m

    def _rakurai_metrics(self, all_val):
        """Rakurai metrics: validator list, stake analysis, geo distribution."""
        if not self.rakurai_data:
            return None

        rak_vals = [r for r in all_val if r.is_rakurai]
        tv = len(all_val) or 1
        total_stake = sum(r.activated_stake_lamports or 0 for r in rak_vals)
        network_stake = sum(r.activated_stake_lamports or 0 for r in all_val)

        m = {
            "total_validators": len(self.rakurai_data),
            "matched_validators": len(rak_vals),
            "validators_percent": round(len(rak_vals) / tv * 100, 2),
            "total_stake_lamports": total_stake,
            "stake_percent": round(total_stake / network_stake * 100, 2) if network_stake else 0,
        }

        # Geo distribution of Rakurai validators
        if rak_vals:
            m.update(self._geo_distributions(rak_vals, include_stake=True))

        return m

    def _geo_distributions(self, records, include_stake=False):
        """Compute country/asn/city distributions for a set of records."""
        cc, ac, cic = Counter(), Counter(), Counter()
        for r in records:
            g = self.geo_data.get(r.ip_address)
            if g:
                cc[g.country_code] += 1; ac[g.asn_name] += 1
                cic[f"{normalize_city(g.city)}, {g.country_code}"] += 1
        result = {
            "country_distribution": dict(cc.most_common()),
            "asn_distribution": dict(ac.most_common()),
            "city_distribution": dict(cic.most_common()),
        }
        if include_stake:
            scc = Counter()
            for r in records:
                g = self.geo_data.get(r.ip_address)
                if g: scc[g.country_code] += r.stake_percentage or 0
            result["stake_by_country"] = {k: round(v, 4) for k, v in scc.most_common()}
        return result

    def _ibrl_aggregates(self, ibrl_vals):
        """Calculate average IBRL metrics by country, ASN, city."""
        fields = ["ibrl_score", "ibrl_build_time_score", "ibrl_vote_packing_score",
                   "ibrl_non_vote_packing_score", "ibrl_median_block_build_ms"]

        def _avg_group(group_fn):
            groups = defaultdict(list)
            for r in ibrl_vals:
                key = group_fn(r)
                if key:
                    groups[key].append(r)
            result = {}
            for key, recs in sorted(groups.items(), key=lambda x: len(x[1]), reverse=True):
                agg = {"count": len(recs)}
                for f in fields:
                    vals = [getattr(r, f) for r in recs if getattr(r, f) is not None]
                    if vals:
                        agg[f"avg_{f.replace('ibrl_', '')}"] = round(sum(vals) / len(vals), 2)
                result[key] = agg
            return result

        return {
            "by_country": _avg_group(lambda r: self.geo_data.get(r.ip_address, GeolocationData(ip="")).country_code),
            "by_asn": _avg_group(lambda r: self.geo_data.get(r.ip_address, GeolocationData(ip="")).asn_name),
            "by_city": _avg_group(lambda r: f"{normalize_city(self.geo_data.get(r.ip_address, GeolocationData(ip='')).city)}, {self.geo_data.get(r.ip_address, GeolocationData(ip='')).country_code}"),
        }

    def _cat_metrics(self, records):
        m = {"total": len(records), "unique_ips": len(set(r.ip_address for r in records if r.ip_address))}
        if not records: return m
        cc, ac, cic = Counter(), Counter(), Counter()
        for r in records:
            if r.ip_address:
                g = self.geo_data.get(r.ip_address)
                if g:
                    cc[g.country_code] += 1; ac[g.asn_name] += 1
                    cic[f"{normalize_city(g.city)}, {g.country_code}"] += 1
        m["country_distribution"] = dict(cc.most_common())
        m["asn_distribution"] = dict(ac.most_common())
        m["city_distribution"] = dict(cic.most_common())
        return m

    def _val_metrics(self, online, all_val):
        m = self._cat_metrics(online)
        m["total_all"] = len(all_val); m["offline"] = len(all_val)-len(online)
        m["hidden"] = len([r for r in all_val if r.role=="validator-hidden"])
        m["inactive"] = len([r for r in all_val if r.role=="validator-inactive"])

        sc, sa, sci = defaultdict(float), defaultdict(float), defaultdict(float)
        vs = []
        for r in all_val:
            sp = r.stake_percentage or 0; vs.append(sp)
            if r.ip_address:
                g = self.geo_data.get(r.ip_address)
                if g: sc[g.country_code]+=sp; sa[g.asn_name]+=sp; sci[f"{normalize_city(g.city)}, {g.country_code}"]+=sp
            else: sc["OFFLINE"]+=sp; sa["OFFLINE"]+=sp; sci["OFFLINE"]+=sp

        m["stake_by_country"] = dict(sorted(sc.items(), key=lambda x:x[1], reverse=True))
        m["stake_by_asn"] = dict(sorted(sa.items(), key=lambda x:x[1], reverse=True))
        m["stake_by_city"] = dict(sorted(sci.items(), key=lambda x:x[1], reverse=True))

        cs, as_, cis, vss = list(sc.values()), list(sa.values()), list(sci.values()), [s for s in vs if s>0]
        m["decentralization"] = {"methodology": METRIC_METHODOLOGY, "current": {}}
        cur = m["decentralization"]["current"]

        for l, sh in [("country",cs),("asn",as_),("city",cis),("validator",vss)]:
            n = calc_nakamoto(sh); cur[f"nakamoto_{l}"] = {"value":n, "rating":rate_nakamoto(n)}
        for thr in [33.33,50.0,66.67]:
            ti = int(thr)
            for l, sh in [("country",cs),("asn",as_),("validator",vss)]:
                cur[f"superminority_{l}_{ti}"] = {"value": calc_nakamoto(sh,thr)}
        for l, sh in [("country",cs),("asn",as_),("validator",vss)]:
            h = calc_hhi(sh); cur[f"hhi_{l}"] = {"value":round(h,1), "rating":rate_hhi(h)}
        g = calc_gini(vs); cur["gini_validators"] = {"value":round(g,4), "rating":rate_gini(g)}
        for l, sh in [("country",cs),("asn",as_),("validator",vss)]:
            h, hn = calc_shannon(sh); cur[f"shannon_{l}"] = {"entropy":round(h,4), "normalized":round(hn,4), "rating":rate_shannon(hn)}
        return m

    # ----------------------------------------------------------------
    # SORT KEY
    # ----------------------------------------------------------------

    def _sort_key(self, r):
        ro = {"validator":0,"validator-hidden":1,"validator-inactive":2,"rpc":3,"co-hosted":4,"unknown-node":5,"dz-device":6}
        if r.endpoint_provider:
            po = {"jito":100,"harmonic":200,"solana":300}
            so = {"block-engine":0,"shred-receiver":1,"ntp":2,"bam":3,"auction":0,"tpu-relayer":1,"bundles":3,"rpc-official":0,"entrypoint":1}
            order = po.get(r.endpoint_provider,400) + so.get(r.endpoint_service,10)
        else:
            order = ro.get(r.role, 7)
        # Validators sorted by stake desc
        stake = -(r.stake_percentage or 0) if r.is_validator else 0
        sd = r.slot_duration_median if r.slot_duration_median is not None else 999999
        geo = self.geo_data.get(r.ip_address, GeolocationData(ip="")) if r.ip_address else None
        cc = geo.country_code if geo else "ZZ"
        # DZ devices sorted by location
        dz_loc = r.dz_location or "zzz" if r.is_dz_device else ""
        return (order, stake, r.endpoint_label or "", sd, dz_loc, cc, r.ip_address or "zzz")

    # ----------------------------------------------------------------
    # REPORT
    # ----------------------------------------------------------------

    def print_full_report(self):
        W = 160
        print("\n" + "="*W)
        print(f"{'SOLANA '+self.cluster.upper()+' NETWORK DECENTRALIZATION REPORT':^{W}}")
        print("="*W)
        if self.current_epoch:
            print(f"\n📅 Epoch: {self.current_epoch} | Slot: {self.current_slot} | {self.epoch_completed_percent:.1f}% | {self.run_timestamp}")
        print("\n"+"-"*W)
        print(f"{'#':>5} | {'Identity':13} | {'Name':20} | {'IP Address':15} | {'CC':2} | {'Cf':2} | "
              f"{'City':15} | {'ASN':9} | {'Provider':20} | {'Role':20} | {'DZ':3} | {'BT':>10} | {'Ver':7} | {'VS':3}")
        print("-"*W)

        for i, rec in enumerate(sorted(self.records, key=self._sort_key), 1):
            if rec.ip_address:
                geo = self.geo_data.get(rec.ip_address, GeolocationData(ip=rec.ip_address))
                ip_d, city, cc = rec.ip_address, (geo.city or "?")[:15], geo.country_code
                conf, asn, prov = geo.confidence[0].upper(), (geo.asn or "N/A")[:9], (geo.asn_name or "?")[:20]
            else:
                ip_d, city, cc, conf, asn, prov = "offline", "-", "-", "-", "-", "-"
            ident = truncate_id(rec.identity_pubkey); nm = (rec.name or "-")[:20]; role = rec.role[:20]
            dz = "Yes" if rec.dz_connected else "-"
            vs = (rec.version_status or "-")[:3].upper() if rec.version_status else "-"
            bt = f"{float(rec.slot_duration_median):.3f}" if rec.slot_duration_median is not None else "N/A"
            ver = (rec.version or "-")[:7]
            if rec.endpoint_reachable is True: role = f"{role[:18]} ✓"
            elif rec.endpoint_reachable is False: role = f"{role[:18]} ✗"
            elif rec.endpoint_reachable is None and rec.endpoint_provider: role = f"{role[:18]} ?"
            print(f"{i:5} | {ident:13} | {nm:20} | {ip_d:15} | {cc:2} | {conf:2} | {city:15} | {asn:9} | {prov:20} | {role:20} | {dz:3} | {bt:>10} | {ver:7} | {vs:3}")
        self._print_summary()

    def _print_summary(self):
        metrics = self.calculate_metrics(); vm = metrics["validators"]; W = 160
        print("\n"+"="*W); print(f"{'SUMMARY':^{W}}"); print("="*W)
        print(f"\n📊 Overview:")
        print(f"  Records: {len(self.records)} | Online: {metrics['overall']['total']} | IPs: {metrics['overall']['unique_ips']}")
        print(f"  Validators: {vm.get('total_all')} (online={vm['total']}, hidden={vm.get('hidden',0)}, inactive={vm.get('inactive',0)})")
        print(f"  RPC: {metrics['rpc']['total']}")

        ep = metrics["endpoints"]
        if ep:
            print(f"\n🔗 Endpoints:")
            for prov, m in ep.items():
                print(f"  {prov.title()}: {m['total']} (✓{m.get('reachable',0)} ✗{m.get('unreachable',0)} ?{m.get('unverifiable',0)})")

        dz = metrics["doublezero"]
        print(f"\n🔌 DoubleZero: {dz['total']} total")
        print(f"  Validators: {dz['validators']} ({dz['validators_percent']}% of all, {dz['validators_stake_percent']}% stake)")
        dz_other = dz['other']
        other_str = ", ".join(f"{r}={c}" for r, c in dz_other.items()) if dz_other else "0"
        print(f"  RPC: {dz['rpc']} | Devices: {dz['devices']} | Other: {other_str}")

        print(f"\n🌍 Geo (Top 10):")
        for cc, cnt in list(metrics['overall']['country_distribution'].items())[:10]:
            print(f"  {cc}: {cnt} ({cnt/metrics['overall']['total']*100:.1f}%)")

        if 'stake_by_country' in vm:
            for title, key in [("💰 Stake/Country","stake_by_country"),("🏢 Stake/Provider","stake_by_asn"),("🏙️  Stake/City","stake_by_city")]:
                print(f"\n{title} (Top 5):")
                for k, p in list(vm[key].items())[:5]: print(f"  {k[:40]}: {p:.2f}%")

        if "decentralization" in vm:
            cur = vm["decentralization"]["current"]
            print(f"\n🔐 Nakamoto:")
            for d in ["country","asn","city","validator"]:
                k = f"nakamoto_{d}"
                if k in cur: print(f"  {d.title():12}: {cur[k]['value']:>4} — {cur[k]['rating']}")
            print(f"\n🛡️  Superminority:")
            for t in [33,50,66]:
                parts = [f"{d}={cur[f'superminority_{d}_{t}']['value']}" for d in ["validator","country","asn"] if f"superminority_{d}_{t}" in cur]
                print(f"  {t}%: {', '.join(parts)}")
            print(f"\n📏 HHI:")
            for d in ["country","asn","validator"]:
                k = f"hhi_{d}"
                if k in cur: print(f"  {d.title():12}: {cur[k]['value']:>8.0f} — {cur[k]['rating']}")
            if "gini_validators" in cur: print(f"\n⚖️  Gini: {cur['gini_validators']['value']:.4f} — {cur['gini_validators']['rating']}")
            print(f"\n🔀 Shannon:")
            for d in ["country","asn","validator"]:
                k = f"shannon_{d}"
                if k in cur: print(f"  {d.title():12}: {cur[k]['normalized']:.4f} — {cur[k]['rating']}")

        ch = self.cluster_health
        if ch:
            print(f"\n🏥 Health: delinquent={ch.get('delinquent_stake_percent',0):.2f}%", end="")
            if ch.get('average_skip_rate') is not None:
                print(f" | skip={ch['average_skip_rate']:.2f}% (weighted={ch.get('average_stake_weighted_skip_rate',0):.2f}%)")
            else: print()

        gs = self.geo_service.stats; cs = self.geo_service.cache.stats()
        print(f"\n📡 Geo: cache={gs['cache_hits']}, DB-IP={gs['dbip']}, IPInfo={gs['ipinfo']}, GeoJS={gs['geojs']}, ip-api={gs['ipapi']}, disc={gs['discrepancies']}")
        print(f"  Conf: H={gs['high']} M={gs['medium']} L={gs['low']} | Cache: {cs['valid']}v {cs['expired']}e")
        print("\n"+"="*W+"\n✅ Complete!")

    # ----------------------------------------------------------------
    # EXPORT
    # ----------------------------------------------------------------

    def _build_record(self, rec):
        """Build JSON record with fixed field sets per role."""
        geo = self.geo_data.get(rec.ip_address) if rec.ip_address else None

        # Base fields (ALL records)
        d = {
            "identity_pubkey": rec.identity_pubkey,
            "role": rec.role,
            "ip_address": rec.ip_address,
            "name": rec.name,
            "gossip_port": rec.gossip_port,
            "tpu_port": rec.tpu_port,
            "tpu_quic_port": rec.tpu_quic_port,
            "rpc_port": rec.rpc_port,
            "version": rec.version,
            "version_status": rec.version_status,
            "feature_set": rec.feature_set,
        }

        # Geolocation (all records with IP)
        if geo:
            d["geolocation"] = {
                "country": geo.country, "country_code": geo.country_code,
                "city": geo.city, "region": geo.region,
                "latitude": geo.latitude, "longitude": geo.longitude,
                "asn": geo.asn, "asn_name": geo.asn_name, "asn_number": geo.asn_number,
                "isp": geo.isp, "continent_code": geo.continent_code,
                "confidence": geo.confidence, "discrepancy": geo.discrepancy,
                "discrepancy_details": geo.discrepancy_details,
                "primary_source": geo.primary_source,
            }
            if geo.discrepancy_alternatives:
                d["geolocation"]["discrepancy_alternatives"] = geo.discrepancy_alternatives
            if geo.sources.get("_geo_override"):
                d["geolocation"]["geo_override"] = geo.sources["_geo_override"]
        else:
            d["geolocation"] = None

        # Validator fields (ALWAYS present for validators, even if null)
        if rec.is_validator:
            d.update({
                "vote_account": rec.vote_account,
                "commission": rec.commission,
                "epoch_credits": rec.epoch_credits,
                "activated_stake_lamports": rec.activated_stake_lamports,
                "stake_percentage": rec.stake_percentage,
                "cumulative_stake_percent": rec.cumulative_stake_percent,
                "is_superminority": rec.is_superminority,
                "delinquent": rec.delinquent,
                "skip_rate": rec.skip_rate,
                "client_type": rec.client_type,
                "is_rakurai": rec.is_rakurai or None,  # Only present when True
                "slot_duration_median": rec.slot_duration_median,
                "median_vote_latency": rec.median_vote_latency,
                "mev_commission": rec.mev_commission,
                "is_sfdp": rec.is_sfdp,
                "sfdp_state": rec.sfdp_state,
                "testnet_pubkey": rec.testnet_pubkey,
                "website": rec.website,
                "icon_url": rec.icon_url,
                "bam_node": rec.bam_node,
                "bam_region": rec.bam_region,
                "ibrl": {
                    "ibrl_score": rec.ibrl_score,
                    "build_time_score": rec.ibrl_build_time_score,
                    "vote_packing_score": rec.ibrl_vote_packing_score,
                    "non_vote_packing_score": rec.ibrl_non_vote_packing_score,
                    "median_block_build_ms": rec.ibrl_median_block_build_ms,
                } if any(v is not None for v in [rec.ibrl_score, rec.ibrl_build_time_score]) else None,
            })

        # DZ fields (ALWAYS present for DZ nodes)
        if rec.dz_connected or rec.is_dz_device:
            dz_block = {
                "dz_connected": rec.dz_connected,
                "dz_device_name": rec.dz_device_name,
                "dz_location": rec.dz_location,
                "dz_exchange": rec.dz_exchange,
                "dz_metro_code": rec.dz_metro_code,
            }
            if rec.dz_connection_type: dz_block["dz_connection_type"] = rec.dz_connection_type
            if rec.dz_multicast_publisher: dz_block["dz_multicast_publisher"] = True
            if rec.dz_multicast_groups: dz_block["dz_multicast_groups"] = rec.dz_multicast_groups
            if rec.is_dz_device and rec.dz_contributor:
                dz_block["dz_contributor"] = rec.dz_contributor
            # Device capacity/traffic (only for dz-device role)
            if rec.is_dz_device and rec.dz_device_name and rec.dz_device_name in self.dz_device_details:
                dd = self.dz_device_details[rec.dz_device_name]
                dz_block["dz_device_type"] = dd.get("device_type")
                if dd.get("max_users") is not None:
                    dz_block["dz_capacity"] = f"{dd.get('current_users', 0)}/{dd['max_users']}"
                if dd.get("in_bps") is not None:
                    dz_block["dz_in_bandwidth"] = _format_bandwidth(dd["in_bps"])
                    dz_block["dz_out_bandwidth"] = _format_bandwidth(dd.get("out_bps"))
            d.update(dz_block)

        # Endpoint fields (ALWAYS present for endpoints)
        if rec.endpoint_provider:
            d["endpoint"] = {
                "provider": rec.endpoint_provider,
                "service": rec.endpoint_service,
                "label": rec.endpoint_label,
                "port": rec.endpoint_port,
                "reachable": rec.endpoint_reachable,
            }

        # Co-hosted
        if rec.is_co_hosted:
            d["co_hosted_with"] = rec.co_hosted_with

        # Multi-IP
        if rec.is_multi_ip:
            d["multi_ip_count"] = rec.multi_ip_count

        return d

    def export_data(self, output_file=None):
        # All known roles — always present in record_counts (0 if absent)
        ALL_ROLES = [
            "validator", "validator-hidden", "validator-inactive", "backup-node",
            "rpc", "co-hosted", "unknown-node", "dz-device",
            "jito-block-engine", "jito-shred-receiver", "jito-ntp", "jito-bam",
            "harmonic-auction", "harmonic-tpu-relayer", "harmonic-shred-receiver", "harmonic-bundles",
            "solana-rpc-official", "solana-entrypoint",
        ]
        role_counts = Counter(r.role for r in self.records)
        # Merge: all known roles (0 default) + any unexpected roles from data
        full_counts = {role: role_counts.get(role, 0) for role in ALL_ROLES}
        for role, count in role_counts.items():
            if role not in full_counts:
                full_counts[role] = count
        sorted_recs = sorted(self.records, key=self._sort_key)
        records_data = [self._build_record(r) for r in sorted_recs]

        full = {
            "timestamp": self.run_timestamp, "cluster": self.cluster,
            "epoch": self.current_epoch, "slot": self.current_slot,
            "epoch_completed_percent": self.epoch_completed_percent,
            "record_counts": full_counts,
            "records": records_data,
            "metrics": self.calculate_metrics(),
        }
        out = json.dumps(full, indent=2, ensure_ascii=False)
        if output_file:
            with open(output_file, "w") as f: f.write(out)
            logger.info(f"💾 Exported to {output_file} ({len(out)//1024}KB)")
        else: print(out)
        return full


def main():
    parser = argparse.ArgumentParser(description="Solana Network Decentralization Analyzer v3.7")
    parser.add_argument("--dbip-key", default=os.getenv("DBIP_KEY",""))
    parser.add_argument("--ipinfo-token", default=os.getenv("IPINFO_TOKEN",""))
    parser.add_argument("--token", default=None, help="(deprecated)")
    parser.add_argument("--cluster", choices=["mainnet-beta","testnet","devnet"], default="mainnet-beta")
    parser.add_argument("--rpc-url", default=None, help="Custom RPC URL (overrides cluster default)")
    parser.add_argument("--endpoints", default="endpoints.yaml")
    parser.add_argument("--geo-overrides", default="geo_overrides.yaml", help="Geo overrides YAML (auto-generated from DZ + admin)")
    parser.add_argument("--export", action="store_true")
    parser.add_argument("--output", help="Output file")
    args = parser.parse_args()
    ipinfo = args.ipinfo_token or args.token or ""
    if not args.dbip_key: logger.warning("⚠️  No DB-IP key")
    a = SolanaNetworkAnalyzer(dbip_key=args.dbip_key, ipinfo_token=ipinfo, cluster=args.cluster,
                              endpoints_config=args.endpoints, rpc_url=args.rpc_url,
                              geo_overrides_path=args.geo_overrides)
    if not a.fetch_all_data(): sys.exit(EXIT_CRITICAL)
    if args.export or args.output: a.export_data(output_file=args.output)
    else: a.print_full_report()
    sys.exit(EXIT_OK)

if __name__ == "__main__":
    main()