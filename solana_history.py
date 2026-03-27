#!/usr/bin/env python3
"""
SONDA Historical Data Collector v1.5.3
====================================
Collects historical validator geo/datacenter data from Jito kobe + SFDP APIs.

Sources:
  - Jito kobe: IP per epoch (epoch ~500+, mainnet/testnet)
  - SFDP: ASN + location per epoch (epoch ~196+, SFDP validators only)
  - StakeWiz: epoch -> date mapping
  - RIPE Stat + DB-IP: ASN name enrichment (for SFDP-only ASNs without IP)

Output: validator_history_{cluster}.json
  Per-validator: location_changes[] with full geo data. No raw history dump.

Usage:
  # Initial collection:
  python solana_history.py --dbip-key KEY --ipinfo-token TOKEN

  # Resume interrupted SFDP phase (Jito always re-fetched, fast):
  python solana_history.py --resume

  # Incremental update (new Jito epochs only, SFDP untouched):
  python solana_history.py --update --dbip-key KEY

  # Other clusters:
  python solana_history.py --cluster testnet --dbip-key KEY
  python solana_history.py --cluster devnet   # empty output

  # Slower SFDP if timeouts:
  python solana_history.py --resume --sfdp-delay 0.5

Design decisions:
  - No "history" field: only location_changes with full enriched geo data.
    Frontend groups by country/ASN/city as needed.
  - Jito always re-fetched (~40s, parallel). SFDP cached in SQLite forever
    (historical data never changes). Resume only re-runs missing SFDP.
  - SFDP "no_data" (404 + message) IS cached -- stakebot gap is permanent.
    Network errors are NOT cached -- retry next run.
  - ASN names: from geo_lookup dict first (free), then RIPE Stat (free, no key),
    then DB-IP fallback. Cached in SQLite.
  - Progressive retry for both SFDP membership (7 attempts) and epoch (6 attempts).
  - location_changes key: IP for Jito records, ASN+location for SFDP records.

Changes v1.5.3:
  - Fix: fetch_sfdp_membership no longer hangs for validators where mnStats/tnStats
    is null in the API response (Retired/Rejected validators).
    data.get("mnStats", {}) returns None when key exists with null value,
    causing AttributeError on .get("epochs") which was caught+retried 7 times
    (~41 seconds wasted per validator). Fixed with (data.get("mnStats") or {}).

Changes v1.5.2:
  - Fix: --resume no longer skips timed-out validators. The recovery block that added
    validators to sfdp_done based on location_changes presence was incorrectly marking
    guarantee-pass validators (Jito-only fallbacks) as "SFDP done". Removed — sfdp_done
    now relies solely on progress["sfdp_done"] which is only written when SFDP is
    actually processed.
  - Fix: epoch_dates for testnet/devnet now empty ({}). StakeWiz provides mainnet epoch
    dates only. Testnet/devnet have different epoch numbering -- mainnet dates do not apply.
  - Added emoji to log messages for easier reading of long runs.
  - Added percentage to SFDP progress: "SFDP 50/788 (6.3%)".
  - Jito fetch progress now shows percentage too.

Changes v1.5.1:
  - Fix: membership retry now catches ALL exceptions (not just Timeout/ConnectionError).
    Previously non-network exceptions caused immediate exit after 1 attempt, so
    validators like 5HCTsoKM/LitxAVo3/RBFiUqjY were skipped every --resume run.
  - Fix: timed-out validators now saved with Jito-only data instead of lost entirely.
  - Fix: "not listed" case (in SFDP, no epochs for cluster) now writes to state + saves.
  - New: guarantee pass at end of Phase 6 -- every validator in the list is guaranteed
    to appear in output. 798 validators in -> 798 validators out, always.
"""

import json
import os
import sys
import time
import sqlite3
import argparse
import logging
import requests
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from solana_analyzer import GeolocationService, normalize_city, run_solana_cmd
except ImportError:
    print("ERROR: solana_analyzer.py must be in the same directory")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

JITO_KOBE_URLS = {
    "mainnet-beta": "https://kobe.mainnet.jito.network/api/v1",
    "testnet": "https://kobe.testnet.jito.network/api/v1",
}
SFDP_API_BASE = "https://api.solana.org/api/validators"
STAKEWIZ_API = "https://api.stakewiz.com/all_epochs_history"
CLUSTER_URLS = {
    "mainnet-beta": "https://api.mainnet-beta.solana.com",
    "testnet": "https://api.testnet.solana.com",
    "devnet": "https://api.devnet.solana.com",
}

JITO_WORKERS = 10
JITO_TIMEOUT = 30
INVALID_IP = "255.255.255.255"
CACHE_DB = Path.home() / ".solana_network_cache" / "api_cache.db"

# Progressive retry delays (seconds) between attempts.
# First attempt has no pre-delay; subsequent attempts wait progressively longer.
MEMBERSHIP_RETRY_DELAYS = [0, 1, 3, 5, 10, 10, 10]  # 7 attempts
EPOCH_RETRY_DELAYS = [0, 1, 3, 5, 10, 10]            # 6 attempts
ASN_RETRY_DELAYS = [0, 2, 5]                          # 3 attempts


# ---------------------------------------------------------------------------
# SQLite caches
# ---------------------------------------------------------------------------

class SFDPCache:
    """Cache for SFDP epoch API responses.

    Stores full stats JSON per (identity, epoch).
    Both 'ok' (200 with data) and 'no_data' (legitimate stakebot gap) are cached.
    Network errors / 5xx are NOT cached (retry next run).
    """

    def __init__(self, db_path=CACHE_DB):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db_path = str(db_path)
        self._init()

    def _init(self):
        with sqlite3.connect(self.db_path) as c:
            c.execute("""
                CREATE TABLE IF NOT EXISTS sfdp_epoch_cache (
                    identity TEXT NOT NULL,
                    epoch    INTEGER NOT NULL,
                    status   TEXT NOT NULL,   -- 'ok' | 'no_data'
                    data     TEXT,            -- full stats JSON if ok, NULL if no_data
                    fetched_at TEXT NOT NULL,
                    PRIMARY KEY (identity, epoch)
                )
            """)

    def get(self, identity, epoch):
        """Returns ('ok', stats_dict) | ('no_data', None) | None (not cached)."""
        with sqlite3.connect(self.db_path) as c:
            row = c.execute(
                "SELECT status, data FROM sfdp_epoch_cache WHERE identity=? AND epoch=?",
                (identity, epoch)
            ).fetchone()
        if row is None:
            return None
        status, data_str = row
        if status == "ok" and data_str:
            return ("ok", json.loads(data_str))
        return ("no_data", None)

    def set_ok(self, identity, epoch, stats_dict):
        self._write(identity, epoch, "ok", json.dumps(stats_dict, separators=(",", ":")))

    def set_no_data(self, identity, epoch):
        self._write(identity, epoch, "no_data", None)

    def _write(self, identity, epoch, status, data_str):
        with sqlite3.connect(self.db_path) as c:
            c.execute(
                """INSERT OR REPLACE INTO sfdp_epoch_cache
                   (identity, epoch, status, data, fetched_at) VALUES (?,?,?,?,?)""",
                (identity, epoch, status, data_str,
                 datetime.now(timezone.utc).isoformat())
            )


class ASNCache:
    """Cache for ASN name lookups (RIPE Stat / DB-IP).

    Keyed by ASN number (integer). Permanent — ASN registrations don't change.
    """

    def __init__(self, db_path=CACHE_DB):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db_path = str(db_path)
        self._init()

    def _init(self):
        with sqlite3.connect(self.db_path) as c:
            c.execute("""
                CREATE TABLE IF NOT EXISTS asn_info_cache (
                    asn_number   INTEGER PRIMARY KEY,
                    name         TEXT,
                    organization TEXT,
                    registry     TEXT,
                    announced    INTEGER,
                    fetched_at   TEXT NOT NULL
                )
            """)

    def get(self, asn_number):
        """Returns dict with name/organization/registry/announced or None."""
        with sqlite3.connect(self.db_path) as c:
            row = c.execute(
                "SELECT name, organization, registry, announced FROM asn_info_cache WHERE asn_number=?",
                (asn_number,)
            ).fetchone()
        if row is None:
            return None
        return {"name": row[0], "organization": row[1],
                "registry": row[2], "announced": bool(row[3])}

    def set(self, asn_number, name, organization, registry, announced):
        with sqlite3.connect(self.db_path) as c:
            c.execute(
                """INSERT OR REPLACE INTO asn_info_cache
                   (asn_number, name, organization, registry, announced, fetched_at)
                   VALUES (?,?,?,?,?,?)""",
                (asn_number, name, organization, registry, int(announced),
                 datetime.now(timezone.utc).isoformat())
            )


# ---------------------------------------------------------------------------
# Jito fetcher
# ---------------------------------------------------------------------------

def fetch_jito_all(validators, cluster):
    """Fetch Jito history for all validators in parallel.

    Returns dict: vote_account -> {"records": [...], "invalid_epochs": [...]}
    - records: epochs with valid IPs
    - invalid_epochs: epoch numbers where Jito had 255.255.255.255
    """
    if cluster not in JITO_KOBE_URLS:
        logger.info(f"  Jito not available for {cluster}")
        return {}

    base_url = JITO_KOBE_URLS[cluster]
    result = {}

    def _fetch_one(v):
        va = v["vote_account"]
        try:
            r = requests.get(f"{base_url}/validator_history/{va}", timeout=JITO_TIMEOUT)
            if r.status_code != 200:
                return va, [], []
            records = []
            invalid = []
            for h in r.json().get("history", []):
                ip = h.get("ip", "")
                epoch = h.get("epoch")
                if not epoch:
                    continue
                if ip == INVALID_IP:
                    invalid.append(epoch)
                    continue
                if h.get("activated_stake_lamports") == 1.8446744073709552e+19:
                    continue
                records.append({
                    "epoch": epoch,
                    "ip": ip,
                    "version": h.get("version", ""),
                    "client_type": h.get("client_type", ""),
                })
            return va, records, invalid
        except Exception:
            return va, [], []

    logger.info(f"🔮 Jito: fetching {len(validators)} validators...")
    for i in range(0, len(validators), 50):
        batch = validators[i:i + 50]
        with ThreadPoolExecutor(max_workers=JITO_WORKERS) as ex:
            futures = {ex.submit(_fetch_one, v): v for v in batch}
            for f in as_completed(futures):
                va, records, invalid = f.result()
                result[va] = {"records": records, "invalid_epochs": invalid}
        done = min(i + 50, len(validators))
        pct = done / len(validators) * 100
        logger.info(f"  ⚡ Jito: {done}/{len(validators)} ({pct:.0f}%)")

    return result


# ---------------------------------------------------------------------------
# SFDP fetchers
# ---------------------------------------------------------------------------

def fetch_sfdp_membership(identity):
    """Check SFDP membership and get epoch lists.

    Returns:
      dict with mn_epochs/tn_epochs/mainnet_identity/testnet_identity
      {"not_found": True}  -- validator not in SFDP
      None                 -- timeout/error (will retry on next resume)
    """
    for attempt, delay in enumerate(MEMBERSHIP_RETRY_DELAYS):
        if delay:
            time.sleep(delay)
        try:
            r = requests.get(f"{SFDP_API_BASE}/{identity}", timeout=(10, 30))
            if r.status_code == 429:
                time.sleep(10)
                continue
            if r.status_code >= 500:
                continue  # retry
            if r.status_code != 200:
                return {"not_found": True}
            data = r.json()
            if "message" in data:
                return {"not_found": True}
            result = {
                "mainnet_identity": data.get("mainnetBetaPubkey"),
                "testnet_identity": data.get("testnetPubkey"),
                "mn_epochs": [],
                "tn_epochs": [],
            }
            mn = (data.get("mnStats") or {}).get("epochs") or {}
            tn = (data.get("tnStats") or {}).get("epochs") or {}
            if mn:
                result["mn_epochs"] = sorted(int(e) for e in mn)
            if tn:
                result["tn_epochs"] = sorted(int(e) for e in tn)
            return result
        except Exception:
            pass  # retry all exceptions (ConnectionError, Timeout, etc.)
    return None  # all attempts exhausted


def _parse_sfdp_stats(epoch, stats):
    """Extract the fields we need from a full SFDP stats dict."""
    edc = stats.get("epoch_data_center", {})
    asn = edc.get("asn")
    if asn is None:
        return None
    return {
        "epoch": epoch,
        "asn": asn,                            # integer
        "location": edc.get("location", ""),   # "GB-London" or ""
        "aso": edc.get("aso", ""),             # "StackPath, LLC." or ""
        "dc_stake_percent": stats.get("data_center_stake_percent"),
        "data_center_stake": stats.get("data_center_stake"),  # lamports
        "source": "sfdp",
    }


def _fetch_sfdp_epoch_api(sfdp_id, epoch):
    """Fetch one SFDP epoch from API with progressive retry.

    Returns: ('ok', stats_dict) | ('no_data', None) | ('error', None)
    Caller decides what to cache.
    """
    for attempt, delay in enumerate(EPOCH_RETRY_DELAYS):
        if delay:
            time.sleep(delay)
        try:
            r = requests.get(
                f"{SFDP_API_BASE}/details",
                params={"pk": sfdp_id, "epoch": epoch},
                timeout=(10, 30),
            )
            if r.status_code == 200:
                data = r.json()
                if "message" in data:
                    return ("no_data", None)
                stats = data.get("stats", {})
                edc = stats.get("epoch_data_center")
                if not edc or edc.get("asn") is None:
                    return ("no_data", None)
                return ("ok", stats)
            if r.status_code == 404:
                # Legitimate stakebot gap: {"message": "No data for validator..."}
                return ("no_data", None)
            if r.status_code == 429:
                time.sleep(10)
                continue
            if r.status_code >= 500:
                continue  # retry
            return ("no_data", None)
        except (requests.Timeout, requests.ConnectionError):
            pass  # retry
        except Exception:
            return ("error", None)
    return ("error", None)


def fetch_sfdp_epoch_cached(sfdp_id, epoch, cache):
    """Fetch SFDP epoch with SQLite caching.

    Returns: parsed dict | 'no_data' | 'error'
    """
    cached = cache.get(sfdp_id, epoch)
    if cached is not None:
        status, stats = cached
        if status == "no_data":
            return "no_data"
        return _parse_sfdp_stats(epoch, stats) or "no_data"

    status, stats = _fetch_sfdp_epoch_api(sfdp_id, epoch)
    if status == "ok":
        cache.set_ok(sfdp_id, epoch, stats)
        return _parse_sfdp_stats(epoch, stats) or "no_data"
    elif status == "no_data":
        cache.set_no_data(sfdp_id, epoch)
        return "no_data"
    else:
        return "error"  # not cached


# ---------------------------------------------------------------------------
# ASN name lookup
# ---------------------------------------------------------------------------

def build_asn_dict_from_geo(geo_lookup):
    """Build {asn_number: asn_name} from geo_lookup (Jito IP geolocation results)."""
    asn_dict = {}
    for geo in geo_lookup.values():
        asn_str = geo.get("asn", "")   # "AS20473"
        asn_name = geo.get("asn_name", "")
        if asn_str and asn_name and asn_str.startswith("AS"):
            try:
                asn_dict[int(asn_str[2:])] = asn_name
            except ValueError:
                pass
    return asn_dict


def enrich_asn_names(sfdp_records_by_va, asn_dict, asn_cache, dbip_key):
    """Fetch ASN names for SFDP-only ASNs not in asn_dict (from Jito geo).

    Uses RIPE Stat (primary, free, no key) with DB-IP as fallback.
    Results cached in SQLite.
    """
    needed = set()
    for records in sfdp_records_by_va.values():
        for rec in records:
            asn_num = rec.get("asn")
            if asn_num and asn_num not in asn_dict:
                needed.add(asn_num)

    if not needed:
        return

    logger.info(f"  ASN enrichment: {len(needed)} ASNs not in geo_lookup")
    for asn_num in sorted(needed):
        # Check SQLite cache
        cached = asn_cache.get(asn_num)
        if cached:
            name = cached.get("organization") or cached.get("name") or ""
            asn_dict[asn_num] = name
            continue

        # Try RIPE Stat
        info = _fetch_asn_ripe(asn_num)
        if info:
            asn_cache.set(asn_num, info["name"], info["organization"],
                          info["registry"], info["announced"])
            asn_dict[asn_num] = info["organization"] or info["name"]
            time.sleep(0.2)
            continue

        # Fallback: DB-IP
        if dbip_key:
            info = _fetch_asn_dbip(asn_num, dbip_key)
            if info:
                asn_cache.set(asn_num, info["name"], info["organization"],
                              info["registry"], True)
                asn_dict[asn_num] = info["organization"] or info["name"]
                time.sleep(0.2)
                continue

        # Not found — store empty to avoid re-querying
        asn_cache.set(asn_num, "", "", "unknown", True)
        asn_dict[asn_num] = ""
        time.sleep(0.2)


def _fetch_asn_ripe(asn_number):
    """Fetch ASN info from RIPE Stat API (free, no key required)."""
    for attempt, delay in enumerate(ASN_RETRY_DELAYS):
        if delay:
            time.sleep(delay)
        try:
            r = requests.get(
                f"https://stat.ripe.net/data/as-overview/data.json?resource=AS{asn_number}",
                timeout=10
            )
            if r.status_code == 200:
                data = r.json().get("data", {})
                holder = data.get("holder", "")
                # holder format: "AS-VULTR - The Constant Company"
                parts = holder.split(" - ", 1)
                name = parts[0].strip() if parts else ""
                org = parts[1].strip() if len(parts) > 1 else name
                announced = data.get("announced", True)
                # Detect registry from block description
                block = data.get("block", {}).get("name", "").lower()
                registry = next(
                    (r for r in ["ripe", "arin", "apnic", "lacnic", "afrinic"]
                     if r in block), "unknown"
                )
                return {"name": name, "organization": org,
                        "registry": registry, "announced": announced}
        except Exception:
            pass
    return None


def _fetch_asn_dbip(asn_number, dbip_key):
    """Fetch ASN info from DB-IP /as/{number} endpoint."""
    for attempt, delay in enumerate(ASN_RETRY_DELAYS):
        if delay:
            time.sleep(delay)
        try:
            r = requests.get(
                f"http://api.db-ip.com/v2/{dbip_key}/as/{asn_number}",
                timeout=10
            )
            if r.status_code == 200:
                data = r.json()
                if "errorCode" in data:
                    return None
                return {
                    "name": data.get("name", ""),
                    "organization": data.get("organization", ""),
                    "registry": data.get("registry", ""),
                }
        except Exception:
            pass
    return None


# ---------------------------------------------------------------------------
# Location changes computation
# ---------------------------------------------------------------------------

def parse_sfdp_location(loc_str):
    """Parse 'CC-City' -> (country_code, city)."""
    if not loc_str or "-" not in loc_str:
        return ("", "")
    parts = loc_str.split("-", 1)
    return (parts[0].strip(), parts[1].strip())


def compute_location_changes(jito_data, sfdp_records, geo_lookup, asn_dict):
    """Build location_changes list with full geo data.

    Each entry = one continuous period at same location.
    Change detection:
      - Jito records: keyed by IP (each unique IP = new entry)
      - SFDP records: keyed by ASN+location (no IP available)

    Jito takes precedence for epochs where both sources have data.

    Returns sorted list of period dicts.
    """
    jito_records = jito_data.get("records", [])
    invalid_epochs = set(jito_data.get("invalid_epochs", []))

    # Build epoch -> record map, Jito wins
    epoch_map = {}
    for rec in sfdp_records:
        epoch_map[rec["epoch"]] = ("sfdp", rec)
    for rec in jito_records:
        epoch_map[rec["epoch"]] = ("jito", rec)

    # Also add invalid-IP epochs as SFDP-only if SFDP has data for them
    # (they are already in epoch_map if sfdp_records covers them)

    if not epoch_map:
        return []

    changes = []
    current_key = None

    for epoch in sorted(epoch_map):
        src, rec = epoch_map[epoch]

        if src == "jito":
            ip = rec.get("ip", "")
            geo = geo_lookup.get(ip, {})
            asn_str = geo.get("asn", "")
            asn_num = _asn_str_to_int(asn_str)
            asn_name = geo.get("asn_name", "") or asn_dict.get(asn_num, "")
            loc_key = f"jito|{ip}"

            if loc_key != current_key:
                _close_period(changes, epoch)
                changes.append({
                    "from_epoch": epoch,
                    "to_epoch": epoch,
                    "ip": ip,
                    "country_code": geo.get("country_code", ""),
                    "country": geo.get("country", ""),
                    "city": normalize_city(geo.get("city", "")),
                    "region": geo.get("region", ""),
                    "latitude": geo.get("latitude"),
                    "longitude": geo.get("longitude"),
                    "asn": asn_str,
                    "asn_name": asn_name,
                    "isp": geo.get("isp", ""),
                    "source": "jito",
                })
                current_key = loc_key
            else:
                changes[-1]["to_epoch"] = epoch

        else:  # sfdp
            asn_num = rec.get("asn")       # integer from SFDP
            location = rec.get("location", "")
            aso = rec.get("aso", "")
            cc, city = parse_sfdp_location(location)
            asn_str = f"AS{asn_num}" if asn_num else ""
            asn_name = asn_dict.get(asn_num, aso)
            loc_key = f"sfdp|{asn_num}|{location or aso}"

            if loc_key != current_key:
                _close_period(changes, epoch)
                entry = {
                    "from_epoch": epoch,
                    "to_epoch": epoch,
                    "ip": None,
                    "country_code": cc,
                    "city": city,
                    "asn": asn_str,
                    "asn_name": asn_name,
                    "source": "sfdp",
                }
                if location:
                    entry["sfdp_location"] = location
                if rec.get("dc_stake_percent") is not None:
                    entry["dc_stake_percent"] = rec["dc_stake_percent"]
                if rec.get("data_center_stake") is not None:
                    entry["data_center_stake"] = rec["data_center_stake"]
                changes.append(entry)
                current_key = loc_key
            else:
                changes[-1]["to_epoch"] = epoch
                # Keep latest dc_stake_percent
                if rec.get("dc_stake_percent") is not None:
                    changes[-1]["dc_stake_percent"] = rec["dc_stake_percent"]

    return changes


def _close_period(changes, next_epoch):
    """Finalize to_epoch of last period before starting a new one."""
    if changes:
        changes[-1]["to_epoch"] = next_epoch - 1


def _asn_str_to_int(asn_str):
    """Convert 'AS20473' -> 20473, or '' -> 0."""
    if asn_str and asn_str.startswith("AS"):
        try:
            return int(asn_str[2:])
        except ValueError:
            pass
    return 0


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def fetch_epoch_dates():
    """Fetch epoch -> date mapping from StakeWiz."""
    logger.info("📅 Fetching epoch dates from StakeWiz...")
    try:
        r = requests.get(STAKEWIZ_API, timeout=15)
        if r.status_code != 200:
            logger.warning(f"  StakeWiz HTTP {r.status_code}")
            return {}
        dates = {}
        for e in r.json():
            epoch = e.get("epoch")
            start = e.get("start", "")
            if epoch is not None and start:
                dates[str(epoch)] = start[:10]
        if dates:
            logger.info(f"  {len(dates)} epoch dates "
                        f"({min(int(k) for k in dates)} -> {max(int(k) for k in dates)})")
        return dates
    except Exception as ex:
        logger.warning(f"  StakeWiz error: {ex}")
        return {}


def fetch_current_validators(cluster):
    """Fetch current validator set from Solana CLI."""
    logger.info(f"🔍 Fetching validators ({cluster})...")
    rpc_url = CLUSTER_URLS.get(cluster, CLUSTER_URLS["mainnet-beta"])
    data = run_solana_cmd(
        ["solana", "--url", rpc_url, "validators", "--output", "json"],
        desc="validators", timeout=30
    )
    if not data:
        logger.warning(f"  No data from CLI for {cluster}")
        return []
    validators = []
    for v in list(data.get("validators", [])) + list(data.get("delinquent", [])):
        validators.append({
            "identity": v.get("identityPubkey", ""),
            "vote_account": v.get("voteAccountPubkey", ""),
        })
    logger.info(f"  {len(validators)} validators")
    return validators


def _build_geo_entry(gd):
    """Build geo dict from GeolocationData object."""
    return {
        "country_code": getattr(gd, "country_code", ""),
        "country": getattr(gd, "country", ""),
        "city": getattr(gd, "city", ""),
        "region": getattr(gd, "region", ""),
        "latitude": getattr(gd, "latitude", None),
        "longitude": getattr(gd, "longitude", None),
        "asn": getattr(gd, "asn", ""),
        "asn_name": getattr(gd, "asn_name", ""),
        "isp": getattr(gd, "isp", ""),
        "confidence": getattr(gd, "confidence", ""),
    }


def _save(state, path):
    """Atomic save (write to .tmp then rename)."""
    tmp = str(path) + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, separators=(",", ":"), ensure_ascii=False)
    os.replace(tmp, path)


# ---------------------------------------------------------------------------
# run_collection (initial + resume)
# ---------------------------------------------------------------------------

def run_collection(args):
    """Main collection pipeline.

    Phase 1: Epoch dates (StakeWiz)
    Phase 2: Validator list (Solana CLI)
    Phase 3: Jito history for all validators (fast, parallel, always)
    Phase 4: Geolocate all unique IPs
    Phase 5: Build ASN dict from geo_lookup
    Phase 6: SFDP history (slow, resumable, SQLite-cached)
              For each validator: compute and save location_changes immediately
    Phase 7: Final meta + save
    """
    output_path = Path(args.output)
    cluster = args.cluster

    state = {"meta": {}, "epoch_dates": {}, "validators": {}, "_progress": {}}
    if args.resume and output_path.exists():
        logger.info(f"📂 Resuming from {output_path}...")
        with open(output_path) as f:
            state = json.load(f)
        state.setdefault("_progress", {})
        state.setdefault("validators", {})

    progress = state["_progress"]

    # Phase 1: Epoch dates (mainnet only — StakeWiz provides mainnet epoch dates only)
    # testnet/devnet have different epoch numbering, mainnet dates do not apply
    if cluster == "mainnet-beta":
        if not state.get("epoch_dates"):
            state["epoch_dates"] = fetch_epoch_dates()
            _save(state, output_path)
        else:
            fresh = fetch_epoch_dates()
            if fresh:
                state["epoch_dates"].update(fresh)
    else:
        state["epoch_dates"] = {}  # testnet/devnet: no date mapping available

    # Phase 2: Validator list
    if "validators_list" not in progress:
        validators = fetch_current_validators(cluster)
        if not validators:
            logger.warning(f"No validators found for {cluster} -- saving empty output")
            state["meta"] = {
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "cluster": cluster, "sources": [], "total_validators": 0,
                "validators_with_history": 0,
                "note": "No validators found for this cluster",
            }
            _save(state, output_path)
            return
        progress["validators_list"] = validators
        _save(state, output_path)
    else:
        validators = progress["validators_list"]
        logger.info(f"  Using {len(validators)} validators from progress")

    # Phase 3: Jito (always re-fetch — fast ~40s)
    has_jito = cluster in JITO_KOBE_URLS
    jito_all = {}
    if has_jito:
        jito_all = fetch_jito_all(validators, cluster)
        logger.info(f"  ✅ Jito: {len(jito_all)} validators fetched")
    else:
        logger.info(f"  ℹ️  Jito not available for {cluster}")

    # Phase 4: Geolocate all unique IPs from Jito
    all_ips = set()
    for va_data in jito_all.values():
        for rec in va_data.get("records", []):
            ip = rec.get("ip", "")
            if ip and ip != INVALID_IP:
                all_ips.add(ip)

    geo_lookup = progress.get("geo_lookup", {})
    new_ips = all_ips - set(geo_lookup.keys())

    if new_ips and args.dbip_key:
        logger.info(f"🌍 Geolocating {len(new_ips)} new IPs ({len(all_ips)-len(new_ips)} cached)...")
        geo = GeolocationService(args.dbip_key, args.ipinfo_token or "")
        for ip, gd in geo.process_ips(list(new_ips)).items():
            geo_lookup[ip] = _build_geo_entry(gd)
        progress["geo_lookup"] = geo_lookup
        logger.info(f"  Geolocated {len(new_ips)} IPs")
        _save(state, output_path)
    elif new_ips:
        logger.warning(f"  {len(new_ips)} IPs without geo (no --dbip-key)")

    # Phase 5: ASN dict from geo
    asn_dict = build_asn_dict_from_geo(geo_lookup)
    logger.info(f"  ASN dict: {len(asn_dict)} ASNs from geo_lookup")

    # Phase 6: SFDP
    sfdp_cache = SFDPCache()
    asn_cache = ASNCache()

    sfdp_done = set(progress.get("sfdp_done", []))
    # Note: we rely solely on progress["sfdp_done"] — do NOT add based on
    # location_changes presence, which would incorrectly mark guarantee-pass
    # validators (Jito-only fallbacks) as SFDP-done.

    sfdp_remaining = [v for v in validators if v["identity"] not in sfdp_done]

    if sfdp_remaining and not args.skip_sfdp:
        epoch_key = "mn_epochs" if cluster == "mainnet-beta" else "tn_epochs"
        jito_min_epoch = _find_jito_min_epoch(jito_all, state)

        logger.info(f"🏛️  SFDP: {len(sfdp_remaining)} remaining, "
                    f"{len(sfdp_done)} done, Jito min epoch={jito_min_epoch}")

        sfdp_count = sfdp_skipped = 0
        sfdp_records_by_va = {}  # for ASN enrichment batch

        for idx, v in enumerate(sfdp_remaining):
            identity = v["identity"]
            va = v["vote_account"]

            info = fetch_sfdp_membership(identity)
            time.sleep(args.sfdp_delay)

            if info is None:
                logger.warning(f"  ⚠️  {identity[:8]}... timeout/error (retry with --resume)")
                # Still persist Jito-only data so validator is not lost from output
                if va not in state["validators"] or not state["validators"][va].get("location_changes"):
                    jito_data = jito_all.get(va, {"records": [], "invalid_epochs": []})
                    lc = compute_location_changes(jito_data, [], geo_lookup, asn_dict)
                    state["validators"].setdefault(va, {}).update({
                        "identity": identity, "vote_account": va,
                        "location_changes": lc,
                    })
                    _save(state, output_path)
                continue

            if info.get("not_found"):
                # Not in SFDP — still compute location_changes from Jito only
                jito_data = jito_all.get(va, {"records": [], "invalid_epochs": []})
                lc = compute_location_changes(jito_data, [], geo_lookup, asn_dict)
                state["validators"].setdefault(va, {}).update({
                    "identity": identity, "vote_account": va,
                    "location_changes": lc,
                })
                sfdp_done.add(identity)
                sfdp_skipped += 1
                progress["sfdp_done"] = list(sfdp_done)
                _save(state, output_path)
                if (idx + 1) % 50 == 0:
                    logger.info(f"  SFDP {idx+1}/{len(sfdp_remaining)}: "
                                f"found={sfdp_count}, not_sfdp={sfdp_skipped}")
                continue

            # Build scan epoch set: pre-Jito + invalid-IP epochs
            listed = info.get(epoch_key, [])
            if not listed:
                # In SFDP but no epochs for this cluster — Jito-only
                jito_data = jito_all.get(va, {"records": [], "invalid_epochs": []})
                lc = compute_location_changes(jito_data, [], geo_lookup, asn_dict)
                state["validators"].setdefault(va, {}).update({
                    "identity": identity, "vote_account": va,
                    "location_changes": lc,
                })
                sfdp_done.add(identity)
                sfdp_skipped += 1
                progress["sfdp_done"] = list(sfdp_done)
                _save(state, output_path)
                continue

            jito_data = jito_all.get(va, {"records": [], "invalid_epochs": []})
            valid_jito_epochs = {r["epoch"] for r in jito_data.get("records", [])}
            invalid_ip_epochs = set(jito_data.get("invalid_epochs", []))
            pre_jito = set(range(min(listed), jito_min_epoch))
            scan_epochs = sorted((pre_jito | invalid_ip_epochs) - valid_jito_epochs)

            if not scan_epochs:
                # No SFDP epochs to fetch — compute from Jito only
                lc = compute_location_changes(jito_data, [], geo_lookup, asn_dict)
                state["validators"].setdefault(va, {}).update({
                    "identity": identity, "vote_account": va,
                    "location_changes": lc,
                })
                sfdp_done.add(identity)
                sfdp_skipped += 1
                progress["sfdp_done"] = list(sfdp_done)
                _save(state, output_path)
                continue

            sfdp_id = (info.get("mainnet_identity", identity)
                       if cluster == "mainnet-beta"
                       else info.get("testnet_identity", identity)) or identity

            # Fetch epochs (all cached after first run)
            records = []
            errors = no_data_count = 0
            for epoch in scan_epochs:
                result = fetch_sfdp_epoch_cached(sfdp_id, epoch, sfdp_cache)
                if isinstance(result, dict):
                    records.append(result)
                elif result == "error":
                    errors += 1
                else:
                    no_data_count += 1
                time.sleep(args.sfdp_delay)

            sfdp_records_by_va[va] = records

            if records:
                sfdp_count += 1
                extras = "".join([
                    f", {no_data_count} gaps" if no_data_count else "",
                    f", {errors} errors" if errors else "",
                    f", {len(invalid_ip_epochs & set(scan_epochs))} 255-IP" if invalid_ip_epochs else "",
                ])
                logger.info(f"    ✅ {identity[:8]}...: {len(records)}/{len(scan_epochs)} epochs"
                            f" (pre-jito={len(pre_jito & set(scan_epochs))}{extras})")

            # Enrich ASN names for this validator's SFDP records
            enrich_asn_names({va: records}, asn_dict, asn_cache, args.dbip_key)

            # Compute and save location_changes immediately
            lc = compute_location_changes(jito_data, records, geo_lookup, asn_dict)
            state["validators"].setdefault(va, {}).update({
                "identity": identity, "vote_account": va,
                "location_changes": lc,
            })
            sfdp_done.add(identity)
            progress["sfdp_done"] = list(sfdp_done)
            _save(state, output_path)

            if (idx + 1) % 50 == 0:
                pct = (idx + 1) / len(sfdp_remaining) * 100
                logger.info(f"  📦 SFDP {idx+1}/{len(sfdp_remaining)} ({pct:.0f}%): "
                            f"found={sfdp_count}, not_sfdp={sfdp_skipped}")

        progress["sfdp_done"] = list(sfdp_done)
        logger.info(f"✅ SFDP done: {sfdp_count} with data, {sfdp_skipped} not in SFDP")

    else:
        # No SFDP (skip_sfdp or all done): compute location_changes for new Jito-only validators
        for v in validators:
            va = v["vote_account"]
            if va not in state["validators"]:
                jito_data = jito_all.get(va, {"records": [], "invalid_epochs": []})
                lc = compute_location_changes(jito_data, [], geo_lookup, asn_dict)
                state["validators"][va] = {
                    "identity": v["identity"], "vote_account": va,
                    "location_changes": lc,
                }

    # Guarantee: every validator in the list must be in state (catches timeouts, edge cases)
    added_missing = 0
    for v in validators:
        va = v["vote_account"]
        if va not in state["validators"]:
            jito_data = jito_all.get(va, {"records": [], "invalid_epochs": []})
            lc = compute_location_changes(jito_data, [], geo_lookup, asn_dict)
            state["validators"][va] = {
                "identity": v["identity"], "vote_account": va,
                "location_changes": lc,
            }
            added_missing += 1
    if added_missing:
        logger.info(f"  Guarantee pass: added {added_missing} missing validators (Jito-only)")

    # Phase 7: Finalize
    progress["geo_lookup"] = geo_lookup
    progress["validators_list"] = validators
    state["meta"] = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "cluster": cluster,
        "sources": (["jito_kobe", "sfdp", "stakewiz"] if not args.skip_sfdp and has_jito
                    else ["sfdp", "stakewiz"] if not has_jito
                    else ["jito_kobe", "stakewiz"]),
        "total_validators": len(state["validators"]),
        "validators_with_history": sum(
            1 for v in state["validators"].values()
            if v.get("location_changes")
        ),
    }
    _save(state, output_path)
    size_kb = output_path.stat().st_size // 1024
    logger.info(f"Saved {output_path} ({size_kb}KB) — "
                f"{state['meta']['validators_with_history']} validators with location data")


def _find_jito_min_epoch(jito_all, state):
    """Find earliest Jito epoch across all validators (for SFDP range boundary)."""
    min_epoch = 999999
    for va_data in jito_all.values():
        for rec in va_data.get("records", []):
            e = rec.get("epoch", 999999)
            if e < min_epoch:
                min_epoch = e
    # Also check already-merged state (for resume)
    for vd in state.get("validators", {}).values():
        for lc in vd.get("location_changes", []):
            if lc.get("source") == "jito":
                e = lc.get("from_epoch", 999999)
                if e < min_epoch:
                    min_epoch = e
    return min_epoch if min_epoch < 999999 else 500


# ---------------------------------------------------------------------------
# run_update (incremental: new Jito epochs only)
# ---------------------------------------------------------------------------

def run_update(args):
    """Add new Jito epochs to existing file. SFDP data is never touched.

    1. Load existing file
    2. Re-fetch Jito for all current validators (~40s)
    3. Find new epochs: Jito epoch > max(to_epoch) in validator's location_changes
    4. Geolocate new IPs
    5. Extend location_changes with new Jito entries (preserving SFDP periods)
    6. Save
    """
    output_path = Path(args.output)
    if not output_path.exists():
        logger.error(f"File not found: {output_path}. Run initial collection first.")
        sys.exit(1)

    logger.info(f"Update mode: loading {output_path}...")
    with open(output_path) as f:
        state = json.load(f)
    state.setdefault("_progress", {})
    state.setdefault("validators", {})
    progress = state["_progress"]

    logger.info(f"  {len(state['validators'])} validators, "
                f"{sum(1 for v in state['validators'].values() if v.get('location_changes'))} with data")

    # Refresh epoch dates
    fresh = fetch_epoch_dates()
    if fresh:
        state["epoch_dates"].update(fresh)

    # Current validators
    current_validators = fetch_current_validators(args.cluster)
    if not current_validators:
        logger.warning("No validators returned -- aborting")
        return

    # Re-fetch all Jito
    if args.cluster not in JITO_KOBE_URLS:
        logger.info(f"Jito not available for {args.cluster} -- nothing to update")
        return

    jito_all = fetch_jito_all(current_validators, args.cluster)

    # Find new epochs and new IPs
    new_by_va = {}  # va -> list of new jito records
    new_ips = set()
    geo_lookup = progress.get("geo_lookup", {})

    for v in current_validators:
        va = v["vote_account"]
        fresh_records = jito_all.get(va, {}).get("records", [])
        if not fresh_records:
            continue

        # Max epoch already in location_changes for this validator
        existing_lc = state["validators"].get(va, {}).get("location_changes", [])
        max_existing = max((e["to_epoch"] for e in existing_lc), default=0)

        new_recs = [r for r in fresh_records if r["epoch"] > max_existing]
        if new_recs:
            new_by_va[va] = {"identity": v["identity"], "records": new_recs}
            for rec in new_recs:
                ip = rec.get("ip", "")
                if ip and ip != INVALID_IP:
                    new_ips.add(ip)

    if not new_by_va:
        logger.info("No new epochs -- history is up to date")
        state["meta"]["updated_at"] = datetime.now(timezone.utc).isoformat()
        progress["validators_list"] = current_validators
        _save(state, output_path)
        return

    total_new = sum(len(v["records"]) for v in new_by_va.values())
    logger.info(f"Found: {len(new_by_va)} validators, {total_new} new epochs, {len(new_ips)} new IPs")

    # Geolocate new IPs
    truly_new = new_ips - set(geo_lookup.keys())
    if truly_new and args.dbip_key:
        cached_count = len(new_ips) - len(truly_new)
        logger.info(f"Geolocating {len(truly_new)} new IPs ({cached_count} cached)...")
        geo = GeolocationService(args.dbip_key, args.ipinfo_token or "")
        for ip, gd in geo.process_ips(list(truly_new)).items():
            geo_lookup[ip] = _build_geo_entry(gd)
        logger.info(f"  Done")
    elif truly_new:
        logger.warning(f"  {len(truly_new)} new IPs without geo (no --dbip-key)")

    asn_dict = build_asn_dict_from_geo(geo_lookup)

    # Extend location_changes for each updated validator
    updated = new_validators = 0
    for va, data in new_by_va.items():
        identity = data["identity"]
        if va not in state["validators"]:
            state["validators"][va] = {
                "identity": identity, "vote_account": va, "location_changes": []
            }
            new_validators += 1

        vd = state["validators"][va]
        lc = vd.get("location_changes", [])

        for rec in sorted(data["records"], key=lambda r: r["epoch"]):
            ip = rec.get("ip", "")
            epoch = rec["epoch"]
            geo = geo_lookup.get(ip, {})
            asn_str = geo.get("asn", "")
            asn_num = _asn_str_to_int(asn_str)
            asn_name = geo.get("asn_name", "") or asn_dict.get(asn_num, "")

            last = lc[-1] if lc else None
            if last and last.get("ip") == ip and last.get("source") == "jito":
                # Same IP as last Jito entry — extend period
                last["to_epoch"] = epoch
            else:
                # New IP (or first entry) — new period
                lc.append({
                    "from_epoch": epoch,
                    "to_epoch": epoch,
                    "ip": ip,
                    "country_code": geo.get("country_code", ""),
                    "country": geo.get("country", ""),
                    "city": normalize_city(geo.get("city", "")),
                    "region": geo.get("region", ""),
                    "latitude": geo.get("latitude"),
                    "longitude": geo.get("longitude"),
                    "asn": asn_str,
                    "asn_name": asn_name,
                    "isp": geo.get("isp", ""),
                    "source": "jito",
                })

        vd["location_changes"] = lc
        updated += 1

    # Sync _progress
    progress["geo_lookup"] = geo_lookup
    progress["validators_list"] = current_validators
    all_done = set(progress.get("sfdp_done", []))
    for va, vd in state["validators"].items():
        if vd.get("location_changes") is not None:
            identity = vd.get("identity", "")
            if identity:
                all_done.add(identity)
    progress["sfdp_done"] = list(all_done)

    state["meta"]["updated_at"] = datetime.now(timezone.utc).isoformat()
    state["meta"]["total_validators"] = len(state["validators"])
    state["meta"]["validators_with_history"] = sum(
        1 for v in state["validators"].values() if v.get("location_changes")
    )

    _save(state, output_path)
    size_kb = output_path.stat().st_size // 1024
    logger.info(f"Saved {output_path} ({size_kb}KB)")
    logger.info(f"Done: {updated} updated, {new_validators} new, {total_new} new epochs")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="SONDA Historical Data Collector v1.5")
    parser.add_argument("--cluster", default="mainnet-beta",
                        choices=["mainnet-beta", "testnet", "devnet"])
    parser.add_argument("--dbip-key", default="", help="DB-IP API key")
    parser.add_argument("--ipinfo-token", default="", help="IPInfo token")
    parser.add_argument("--output", default="",
                        help="Output file (default: validator_history_{cluster}.json)")
    parser.add_argument("--sfdp-delay", type=float, default=0.3,
                        help="Base delay between SFDP API requests (default: 0.3s)")

    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--resume", action="store_true",
                      help="Resume: re-fetch Jito, continue SFDP from where it stopped")
    mode.add_argument("--update", action="store_true",
                      help="Update: add new Jito epochs only, SFDP data unchanged")

    parser.add_argument("--skip-sfdp", action="store_true",
                        help="Skip SFDP phase (initial collection only)")
    args = parser.parse_args()

    if args.update and args.skip_sfdp:
        parser.error("--skip-sfdp has no effect with --update")

    if not args.output:
        suffix = args.cluster.replace("-", "_")
        args.output = f"validator_history_{suffix}.json"
        logger.info(f"Output: {args.output}")

    if args.update:
        run_update(args)
    else:
        run_collection(args)


if __name__ == "__main__":
    main()