#!/usr/bin/env python3
"""
pools_fetch.py - performance stake pool ranks and pool stake for SONDA (v1.1, 2026-10-05)

Writes <paths.data_dir>/pools_cache.json, which the analyzer (v3.13 step 3)
glues onto validator records as `pool_ranks` and into `metrics.pools`.
Run by run_sonda.py every 30 minutes (and by hand).

Sources (mainnet only):
  jito          Jito kobe steward_events ScoreComponents. The steward scores
                validators only at the start of its cycle (every few epochs),
                so the latest scored epoch is found first (query without
                `epoch`), then all events of that epoch are read with one
                large `limit` (the API's `page` parameter returns nothing).
                Rank = position by (score desc, raw_score desc); eligible =
                score > 0.
  shinobi       xSHIN pool files https://xshin.fi/data/pool/<ts>/pool.bin,
                non_pool_voters.bin, overview.bin (bincode, versions 0..2;
                layout ported from 1000xsh/xshin-data xshin.js). Rank = position
                by total_score desc over pool and non-pool voters. Downloaded
                only when `newest` reports a new timestamp.
  solstrategies Stakewiz https://api.stakewiz.com/validators (rank, wiz_score,
                four concentration components, first_epoch_with_stake).
  jpool_perf    svt.one jpool-scores/<epoch> (perfRank, 0 = not ranked; tries
                the current epoch, then the previous one).
  stake         on-chain SPL stake pool ValidatorList accounts of the four
                pools (owner SPoo1Ku8WFXoNDMHPsrGSTSG1Y47rzgn41SLUNakuHy,
                73-byte entries): active and transient lamports per vote
                account. Layout checked against solana-program/stake-pool
                program/src/state.rs.

Every source is independent: on an error the previous cache entry of that
source is kept (with an `error` field) and the rest is refreshed.

v1.1: "places" per pool (how many validators the pool funds right now):
  jito          num_delegation_validators from the on-chain steward Config
                (jitoVjT9jRUyeXHzvCwzPgHj7yWNRhLcUoXtes4wtjv; layout from
                stakenet programs/steward/src/state/parameters.rs), plus
                num_epochs_between_scoring and minimum_voting_epochs
  shinobi       validators in the pool and the lowest total_score among them
  solstrategies validators with stake in the on-chain list
  jpool_perf    validators with a performance stake pot; per validator
                perf_stake_sol and has_perf_stake

Usage:
  pools_fetch.py --config /home/solya/sonda/automation/config.yaml [--out PATH]
                 [--only jito,shinobi,solstrategies,jpool_perf,stake] [--force]
                 [--dry-run] [--rpc URL]
"""
import argparse
import base64
import json
import logging
import os
import struct
import sys
import tempfile
import time
from datetime import datetime, timezone

import requests
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("pools_fetch")

CACHE_VERSION = 1
CACHE_FILE = "pools_cache.json"
UA = {"User-Agent": "SONDA pools_fetch/1.0 (sonda.network)"}
HTTP_TIMEOUT = 60

KOBE_EVENTS = "https://kobe.mainnet.jito.network/api/v1/steward_events"
KOBE_LIMIT = 5000
XSHIN_BASE = "https://xshin.fi/data/pool"
STAKEWIZ_URL = "https://api.stakewiz.com/validators"
SVT_JPOOL = "https://api.validators.svt.one/jpool-scores/{epoch}?select=actualStakeDetails,perfRank,voteId"

SPL_STAKE_POOL_PROGRAM = "SPoo1Ku8WFXoNDMHPsrGSTSG1Y47rzgn41SLUNakuHy"
STEWARD_CONFIG = "jitoVjT9jRUyeXHzvCwzPgHj7yWNRhLcUoXtes4wtjv"   # JitoSOL steward Config (stakenet README)
STEWARD_PARAMS_OFF = 8 + 5 * 32 + 313 * 8     # discriminator, five pubkeys, LargeBitMask(20000 bits)
STEWARD_LAYOUT = {   # offset inside Parameters (repr(C)), struct format
    "epoch_credits_range": (2, "<H"), "num_delegation_validators": (32, "<I"),
    "num_epochs_between_scoring": (72, "<Q"), "minimum_stake_lamports": (80, "<Q"), "minimum_voting_epochs": (88, "<Q"),
}
VL_HEADER = 5          # account_type u8 + max_validators u32
VL_ENTRY = 73
POOLS = {
    # key: (stake pool account, validator list account) - solanacompass 2026-09-22
    "jito": ("Jito4APyf642JPZPx3hGc6WWJ8zPKtRbRs4P815Awbb", "3R3nGZpQs2aZo5FDQvd2MUQ6R7KhAPainds6uT6uE2mn"),
    "shinobi": ("spp1mo6shdcrRyqDK2zdurJ8H5uttZE6H6oVjHxN1QN", "3At6ZhUsTshb7Nf9Vebf5LTJG49K9X4aqGQ7PEw1NGSq"),
    "solstrategies": ("StKeDUdSu7jMSnPJ1MPqDnk3RdEwD2QbJaisHMebGhw", "StkemQE9eEzPZViA52YwYQqZU5cnYLvVfvch4p4Lsez"),
    "jpool": ("CtMyWsrUtAwXWiGr9WjHT5fC3p3fgV8cyGpLTo2LJzG1", "Ei2LhH2tDKPERnoNjQV5darTToZmbg45vDvftFFLNNWd"),
}
RANK_SOURCES = ("jito", "shinobi", "solstrategies", "jpool_perf")

STAKEWIZ_COMPONENTS = ("asn_concentration_score", "city_concentration_score",
                       "asncity_concentration_score", "tpu_ip_concentration_score")

_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def b58encode(b):
    n = int.from_bytes(b, "big")
    out = ""
    while n:
        n, r = divmod(n, 58)
        out = _B58[r] + out
    return "1" * (len(b) - len(b.lstrip(b"\x00"))) + out


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def lamports_to_sol(x):
    return round(int(x) / 1e9, 3)


# ---------------------------------------------------------------------------
# bincode (varint integer encoding) reader for the xSHIN files
# ---------------------------------------------------------------------------
class Bincode:
    def __init__(self, buf):
        self.buf = buf
        self.off = 0

    def u8(self):
        v = self.buf[self.off]; self.off += 1
        return v

    def uint(self):
        """Varint: <251 = the byte; 251 = u16; 252 = u32; 253 = u64; 254 = u128."""
        b = self.buf[self.off]
        if b < 251:
            self.off += 1; return b
        if b == 251:
            v = struct.unpack_from("<H", self.buf, self.off + 1)[0]; self.off += 3; return v
        if b == 252:
            v = struct.unpack_from("<I", self.buf, self.off + 1)[0]; self.off += 5; return v
        if b == 253:
            v = struct.unpack_from("<Q", self.buf, self.off + 1)[0]; self.off += 9; return v
        v = int.from_bytes(self.buf[self.off + 1:self.off + 17], "little"); self.off += 17
        return v

    def boolean(self):
        v = self.u8()
        if v not in (0, 1):
            raise ValueError(f"bad bool {v} at {self.off - 1}")
        return v == 1

    def f32(self):
        v = struct.unpack_from("<f", self.buf, self.off)[0]; self.off += 4
        return v

    def f64(self):
        v = struct.unpack_from("<d", self.buf, self.off)[0]; self.off += 8
        return v

    def string(self):
        n = self.uint()
        s = self.buf[self.off:self.off + n].decode("utf-8", "replace"); self.off += n
        return s

    def opt_string(self):
        return self.string() if self.boolean() else None

    def pubkey(self):
        s = b58encode(bytes(self.buf[self.off:self.off + 32])); self.off += 32
        return s

    def uint_list(self):
        return [self.uint() for _ in range(self.uint())]


SCORE_KEYS = ("skip_rate", "prior_skip_rate", "subsequent_skip_rate", "cu", "latency", "llv", "cv",
              "vote_inclusion", "apy", "pool_extra_lamports", "city_concentration", "country_concentration")


def x_score(r, version):
    s = {}
    s["skip_rate"] = r.f64(); s["prior_skip_rate"] = r.f64(); s["subsequent_skip_rate"] = r.f64()
    s["cu"] = r.f64(); s["latency"] = r.f64(); s["llv"] = r.f64(); s["cv"] = r.f64()
    s["vote_inclusion"] = r.f64() if version >= 1 else 0.0
    s["apy"] = r.f32()
    s["pool_extra_lamports"] = r.f64(); s["city_concentration"] = r.f64(); s["country_concentration"] = r.f64()
    return s


def x_stake(r):
    return {"active": r.uint(), "activating": r.uint(), "deactivating": r.uint()}


def x_best(r, f32=False):
    out = []
    for _ in range(r.uint()):
        pk = r.pubkey(); metric = r.f32() if f32 else r.f64(); ranking = r.uint()
        out.append((pk, metric, ranking))
    return out


def x_reason(r):
    idx = r.uint()
    if idx == 0:
        return f"Blacklisted ({r.string()})"
    if idx == 1:
        return "In superminority"
    if idx == 2:
        return "Not leader in recent epochs (" + ", ".join(str(e) for e in r.uint_list()) + ")"
    if idx == 3:
        return "Low credits in recent epochs (" + ", ".join(str(e) for e in r.uint_list()) + ")"
    if idx == 4:
        return "Excessive delinquency in recent epochs (" + ", ".join(str(e) for e in r.uint_list()) + ")"
    if idx == 5:
        return "Shared vote accounts"
    if idx == 6:
        return f"Commission too high: {r.u8()}"
    if idx == 7:
        return "APY too low in recent epochs (" + ", ".join(str(e) for e in r.uint_list()) + ")"
    if idx == 8:
        return "Insufficient branding"
    if idx == 9:
        return "Insufficient non-pool stake"
    raise ValueError(f"unknown noneligibility reason {idx}")


def x_voter_details(r, version):
    d = {}
    d["name"] = r.opt_string(); d["icon_url"] = r.opt_string(); d["details"] = r.opt_string()
    d["website_url"] = r.opt_string(); d["city"] = r.opt_string(); d["country"] = r.opt_string()
    d["stake"] = x_stake(r)
    d["target_pool_stake"] = r.uint()
    d["raw_score"] = x_score(r, version)
    d["normalized_score"] = x_score(r, version) if version >= 2 else {k: 0.0 for k in SCORE_KEYS}
    d["total_score"] = r.f64()
    return d


def x_weights(r):
    keys = ("skip_rate", "prior_skip_rate", "subsequent_skip_rate", "cu", "latency", "llv", "cv",
            "vote_inclusion", "apy", "city_concentration", "country_concentration")
    return {k: r.f64() for k in keys}


def parse_xshin_pool(buf):
    """pool.bin -> dict(version, pool_validator_count, best_overall, pool_voters{pubkey: {...}}, weights)."""
    r = Bincode(buf)
    version = r.u8()
    if version > 2:
        raise ValueError(f"pool.bin version {version} not supported")
    out = {"version": version, "pool_validator_count": r.uint()}
    for _ in range(5):            # best_skip_rate, best_cu, best_latency, best_llv, best_cv
        x_best(r)
    if version >= 1:
        x_best(r)                 # best_vote_inclusion
    x_best(r, f32=True)           # best_apy
    for _ in range(3):            # best_pool_extra_lamports, best_city_concentration, best_country_concentration
        x_best(r)
    out["best_overall"] = x_best(r)
    for _ in range(2):            # compare_by_current, compare_by_target
        x_score(r, version); x_score(r, version)
    voters = {}
    for _ in range(r.uint()):
        pk = r.pubkey()
        d = x_voter_details(r, version)
        d["pool_stake"] = x_stake(r)
        d["noneligibility_reasons"] = [x_reason(r) for _ in range(r.uint())]
        voters[pk] = d
    out["pool_voters"] = voters
    if version >= 1:
        out["inclusion_weights"] = x_weights(r); out["ranking_weights"] = x_weights(r)
    out["bytes_left"] = len(buf) - r.off
    return out


def parse_xshin_non_pool(buf):
    r = Bincode(buf)
    version = r.u8()
    if version > 2:
        raise ValueError(f"non_pool_voters.bin version {version} not supported")
    voters = {}
    for _ in range(r.uint()):
        pk = r.pubkey()
        d = x_voter_details(r, version)
        d["noneligibility_reasons"] = [x_reason(r) for _ in range(r.uint())]
        voters[pk] = d
    return {"version": version, "voters": voters, "bytes_left": len(buf) - r.off}


def parse_xshin_overview(buf):
    r = Bincode(buf)
    version = r.u8()
    if version != 0:
        raise ValueError(f"overview.bin version {version} not supported")
    return {"price": r.f32(), "epoch": r.uint(), "epoch_start": r.uint(), "epoch_duration": r.uint(),
            "pool_stake": x_stake(r), "reserve": r.uint(), "apy": r.f32()}


# ---------------------------------------------------------------------------
# SPL stake pool ValidatorList
# ---------------------------------------------------------------------------
def parse_validator_list(raw):
    """{vote: {"active": lamports, "transient": lamports, "status": int, "last_update_epoch": int}}.
    Raises on a wrong account type."""
    if len(raw) < VL_HEADER + 4:
        raise ValueError("account too short")
    account_type = raw[0]
    if account_type != 2:
        raise ValueError(f"account_type {account_type} is not ValidatorList (2)")
    max_validators = struct.unpack_from("<I", raw, 1)[0]
    n = struct.unpack_from("<I", raw, VL_HEADER)[0]
    out = {}
    off = VL_HEADER + 4
    for _ in range(n):
        if off + VL_ENTRY > len(raw):
            break
        active, transient, last_epoch, _seed, _unused, _vseed, status = struct.unpack_from("<QQQQIIB", raw, off)
        vote = b58encode(raw[off + 41:off + 73])
        out[vote] = {"active": active, "transient": transient, "status": status, "last_update_epoch": last_epoch}
        off += VL_ENTRY
    return {"max_validators": max_validators, "entries": out}


# ---------------------------------------------------------------------------
# RPC
# ---------------------------------------------------------------------------
class Rpc:
    def __init__(self, urls):
        self.urls = [u for u in urls if u]

    def call(self, method, params, timeout=HTTP_TIMEOUT):
        last = None
        for url in self.urls:
            try:
                r = requests.post(url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                                  timeout=timeout, headers=UA)
                r.raise_for_status()
                j = r.json()
                if "error" in j:
                    raise RuntimeError(j["error"])
                return j.get("result")
            except Exception as e:
                last = e
                logger.warning(f"rpc {method} on {url.split('?')[0]}: {e}")
        raise RuntimeError(f"{method}: all RPC urls failed: {last}")

    def epoch(self):
        return int(self.call("getEpochInfo", [])["epoch"])

    def account_raw(self, pubkey):
        res = self.call("getAccountInfo", [pubkey, {"encoding": "base64"}], timeout=120)
        v = (res or {}).get("value")
        if not v:
            return None, None
        data = v.get("data")
        b64 = data[0] if isinstance(data, list) else data
        return base64.b64decode(b64 or ""), v.get("owner")


# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------
def fetch_jito(prev, force):
    """Latest ScoreComponents epoch, all its events, ranked."""
    r = requests.get(KOBE_EVENTS, params={"event_type": "ScoreComponents", "limit": 1}, timeout=HTTP_TIMEOUT, headers=UA)
    r.raise_for_status()
    ev = (r.json().get("events") or [])
    if not ev:
        raise RuntimeError("no ScoreComponents events at all")
    latest_epoch = int(ev[0]["epoch"])
    if prev and prev.get("source_epoch") == latest_epoch and not force and prev.get("ranked"):
        logger.info(f"jito: epoch {latest_epoch} already cached ({len(prev['ranked'])} ranked)")
        return prev
    r = requests.get(KOBE_EVENTS, params={"event_type": "ScoreComponents", "epoch": latest_epoch, "limit": KOBE_LIMIT},
                     timeout=120, headers=UA)
    r.raise_for_status()
    events = r.json().get("events") or []
    if len(events) >= KOBE_LIMIT:
        logger.warning(f"jito: {len(events)} events = limit, the list may be truncated")
    best = {}
    for e in events:
        vote = e.get("vote_account"); data = e.get("data") or {}
        if not vote or "score" not in data:
            continue
        ts = e.get("timestamp") or ""
        if vote in best and best[vote][0] > ts:
            continue
        best[vote] = (ts, data, e.get("event_type"))
    rows = []
    for vote, (ts, data, etype) in best.items():
        rows.append((int(data.get("score") or 0), int(data.get("raw_score") or 0), vote, data, etype))
    rows.sort(key=lambda x: (-x[0], -x[1], x[2]))
    ranked = {}
    comp_keys = ("mev_commission_score", "blacklisted_score", "superminority_score", "delinquency_score",
                 "running_bam_score", "running_jito_score", "commission_score", "historical_commission_score",
                 "merkle_root_upload_authority_score", "priority_fee_commission_score",
                 "priority_fee_merkle_root_upload_authority_score")
    for i, (score, raw, vote, data, etype) in enumerate(rows, 1):
        ranked[vote] = {
            "rank": i, "score": score, "raw_score": raw, "eligible": score > 0,
            "validator_age": data.get("validator_age"), "vote_credits_avg": data.get("vote_credits_avg"),
            "commission_max": data.get("commission_max"), "mev_commission_avg": data.get("mev_commission_avg"),
            "components": {k: data[k] for k in comp_keys if k in data},
            "event_type": etype,
        }
    logger.info(f"jito: epoch {latest_epoch}, {len(ranked)} ranked, {sum(1 for v in ranked.values() if v['eligible'])} eligible")
    return {"source": "kobe steward_events ScoreComponents", "source_epoch": latest_epoch, "updated": now_iso(),
            "total_ranked": len(ranked), "eligible": sum(1 for v in ranked.values() if v["eligible"]), "ranked": ranked}


def fetch_shinobi(prev, force):
    r = requests.get(f"{XSHIN_BASE}/newest", timeout=HTTP_TIMEOUT, headers=UA)
    r.raise_for_status()
    ts = int(r.text.strip())
    if prev and prev.get("xshin_ts") == ts and not force and prev.get("ranked"):
        logger.info(f"shinobi: timestamp {ts} already cached ({len(prev['ranked'])} ranked)")
        return prev

    def get(name):
        rr = requests.get(f"{XSHIN_BASE}/{ts}/{name}", timeout=120, headers=UA)
        rr.raise_for_status()
        return rr.content

    overview = None
    try:
        overview = parse_xshin_overview(get("overview.bin"))
    except Exception as e:
        logger.warning(f"shinobi: overview.bin: {e}")
    pool = parse_xshin_pool(get("pool.bin"))
    non_pool = parse_xshin_non_pool(get("non_pool_voters.bin"))
    if pool["bytes_left"] or non_pool["bytes_left"]:
        raise RuntimeError(f"xSHIN parse did not consume the files (left {pool['bytes_left']}/{non_pool['bytes_left']} bytes)")
    in_pool_rank = {pk: rk for pk, _m, rk in pool["best_overall"]}
    rows = []
    for pk, d in pool["pool_voters"].items():
        rows.append((d["total_score"], pk, True, d))
    for pk, d in non_pool["voters"].items():
        if pk in pool["pool_voters"]:
            continue
        rows.append((d["total_score"], pk, False, d))
    rows.sort(key=lambda x: (-x[0], x[1]))
    ranked = {}
    for i, (score, pk, in_pool, d) in enumerate(rows, 1):
        ps = d.get("pool_stake") or {}
        ranked[pk] = {
            "rank": i, "score": round(score, 6), "in_pool": in_pool,
            "pool_rank": in_pool_rank.get(pk),
            "pool_stake_sol": lamports_to_sol(ps.get("active", 0)) if in_pool else 0,
            "target_pool_stake_sol": lamports_to_sol(d.get("target_pool_stake") or 0),
            "noneligibility": d.get("noneligibility_reasons") or [],
            "normalized": {k: round(v, 4) for k, v in (d.get("normalized_score") or {}).items()},
            "city": d.get("city"), "country": d.get("country"),
        }
    in_pool_scores = [d["total_score"] for d in pool["pool_voters"].values()]
    cutoff = round(min(in_pool_scores), 6) if in_pool_scores else None
    logger.info(f"shinobi: ts {ts}, format v{pool['version']}, {len(pool['pool_voters'])} in pool (cutoff {cutoff}), "
                f"{len(ranked)} ranked, epoch {(overview or {}).get('epoch')}")
    return {"source": "xshin.fi pool.bin + non_pool_voters.bin", "xshin_ts": ts, "format_version": pool["version"],
            "source_epoch": (overview or {}).get("epoch"), "updated": now_iso(), "total_ranked": len(ranked),
            "pool_size": len(pool["pool_voters"]), "places": len(pool["pool_voters"]), "cutoff_score": cutoff,
            "ranked": ranked}


def fetch_solstrategies(prev, force):
    r = requests.get(STAKEWIZ_URL, timeout=120, headers=UA)
    r.raise_for_status()
    rows = r.json()
    if not isinstance(rows, list) or len(rows) < 100:
        raise RuntimeError(f"unexpected Stakewiz payload ({type(rows).__name__}, {len(rows) if hasattr(rows, '__len__') else '?'})")
    ranked = {}
    epochs = set()
    for v in rows:
        vote = v.get("vote_identity")
        if not vote or v.get("rank") is None:
            continue
        comps = {k: v.get(k) for k in STAKEWIZ_COMPONENTS}
        penalty = sum(float(x) for x in comps.values() if isinstance(x, (int, float)))
        ranked[vote] = {
            "rank": v.get("rank"), "score": v.get("wiz_score"),
            "location_penalty": round(penalty, 4), "components": comps,
            "first_epoch_with_stake": v.get("first_epoch_with_stake"),
            "is_jito": v.get("is_jito"), "score_version": v.get("score_version"),
        }
        if v.get("epoch") is not None:
            epochs.add(int(v["epoch"]))
    logger.info(f"solstrategies: {len(ranked)} ranked (Stakewiz epoch {max(epochs) if epochs else None})")
    return {"source": "api.stakewiz.com/validators", "source_epoch": max(epochs) if epochs else None,
            "updated": now_iso(), "total_ranked": len(ranked), "ranked": ranked}


def fetch_jpool(prev, force, epoch):
    for ep in (epoch, epoch - 1):
        if prev and prev.get("source_epoch") == ep and not force and prev.get("ranked"):
            logger.info(f"jpool_perf: epoch {ep} already cached ({len(prev['ranked'])} ranked)")
            return prev
        r = requests.get(SVT_JPOOL.format(epoch=ep), timeout=HTTP_TIMEOUT, headers=UA)
        if r.status_code != 200:
            logger.info(f"jpool_perf: epoch {ep} -> http {r.status_code}")
            continue
        rows = (r.json() or {}).get("data") or []
        if not rows:
            logger.info(f"jpool_perf: epoch {ep} empty")
            continue
        ranked = {}
        for v in rows:
            vote = v.get("voteId")
            pr = v.get("perfRank")
            if not vote or not pr:
                continue
            details = v.get("actualStakeDetails") or {}
            perf = details.get("performance")
            ranked[vote] = {"rank": int(pr), "stake_details": details,
                            "has_perf_stake": perf is not None,
                            "perf_stake_sol": lamports_to_sol(perf) if perf is not None else 0}
        places = sum(1 for v in ranked.values() if v["has_perf_stake"])
        logger.info(f"jpool_perf: epoch {ep}, {len(ranked)} ranked of {len(rows)} rows, {places} with performance stake")
        return {"source": "svt.one jpool-scores", "source_epoch": ep, "updated": now_iso(),
                "total_ranked": len(ranked), "places": places, "ranked": ranked}
    raise RuntimeError(f"no jpool-scores for epochs {epoch} and {epoch - 1}")


def fetch_steward(rpc):
    """Jito steward parameters from the on-chain Config account (places = num_delegation_validators)."""
    raw, _owner = rpc.account_raw(STEWARD_CONFIG)
    if not raw or len(raw) < STEWARD_PARAMS_OFF + 96:
        raise RuntimeError("steward config account missing or too short")
    pool = b58encode(raw[8:40]); vlist = b58encode(raw[40:72])
    if pool != POOLS["jito"][0] or vlist != POOLS["jito"][1]:
        raise RuntimeError(f"steward config layout check failed: pool {pool}, list {vlist}")
    out = {}
    for name, (off, fmt) in STEWARD_LAYOUT.items():
        out[name] = struct.unpack_from(fmt, raw, STEWARD_PARAMS_OFF + off)[0]
    out["minimum_stake_sol"] = lamports_to_sol(out.pop("minimum_stake_lamports"))
    out["updated"] = now_iso()
    logger.info(f"jito steward: places {out['num_delegation_validators']}, scoring every "
                f"{out['num_epochs_between_scoring']} epochs, min voting epochs {out['minimum_voting_epochs']}")
    return out


def fetch_stake(rpc):
    """On-chain ValidatorList of every pool: {pool: {"meta": {...}, "entries": {vote: {...}}}}."""
    out = {}
    for pool, (pool_acc, list_acc) in POOLS.items():
        try:
            raw, owner = rpc.account_raw(list_acc)
            if raw is None:
                raise RuntimeError("account missing")
            if owner != SPL_STAKE_POOL_PROGRAM:
                raise RuntimeError(f"owner {owner} is not the SPL stake pool program")
            parsed = parse_validator_list(raw)
            entries = {}
            for vote, e in parsed["entries"].items():
                entries[vote] = {"active_sol": lamports_to_sol(e["active"]), "transient_sol": lamports_to_sol(e["transient"]),
                                 "status": e["status"]}
            with_stake = sum(1 for e in parsed["entries"].values() if e["active"] + e["transient"] > 0)
            out[pool] = {"meta": {"pool": pool_acc, "validator_list": list_acc, "max_validators": parsed["max_validators"],
                                  "entries": len(entries), "with_stake": with_stake,
                                  "total_active_sol": round(sum(e["active"] for e in parsed["entries"].values()) / 1e9, 1),
                                  "updated": now_iso()},
                         "entries": entries}
            logger.info(f"stake {pool}: {len(entries)} entries, {with_stake} with stake, "
                        f"{out[pool]['meta']['total_active_sol']} SOL active")
        except Exception as e:
            logger.warning(f"stake {pool}: {e}")
            out[pool] = {"meta": {"pool": pool_acc, "validator_list": list_acc, "error": str(e), "updated": now_iso()},
                         "entries": {}}
    return out


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def load_cache(path):
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception as e:
        logger.warning(f"cache unreadable, starting fresh: {e}")
        return {}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", required=True)
    ap.add_argument("--out", default=None, help="default <paths.data_dir>/pools_cache.json")
    ap.add_argument("--only", default=None, help="comma list of jito,shinobi,solstrategies,jpool_perf,stake")
    ap.add_argument("--force", action="store_true", help="ignore 'already cached' checks")
    ap.add_argument("--dry-run", action="store_true", help="fetch and report, do not write")
    ap.add_argument("--rpc", default=None, help="override RPC url (default mainnet-beta rpc_urls from config)")
    args = ap.parse_args()

    cfg = yaml.safe_load(open(args.config))
    data_dir = (cfg.get("paths") or {}).get("data_dir") or "."
    out_path = args.out or os.path.join(data_dir, CACHE_FILE)
    mc = (cfg.get("clusters") or {}).get("mainnet-beta") or {}
    urls = [args.rpc] if args.rpc else (mc.get("rpc_urls") or [mc.get("rpc_url")])
    rpc = Rpc(urls)
    only = set(x.strip() for x in args.only.split(",")) if args.only else set(RANK_SOURCES) | {"stake"}

    cache = load_cache(out_path)
    pools = dict(cache.get("pools") or {})
    stake = dict(cache.get("stake") or {})
    t0 = time.time()
    try:
        epoch = rpc.epoch()
    except Exception as e:
        epoch = cache.get("epoch")
        logger.warning(f"getEpochInfo failed ({e}); using cached epoch {epoch}")

    def run(name, fn, *a):
        if name not in only:
            return
        prev = pools.get(name)
        try:
            res = fn(prev, args.force, *a)
            res.pop("error", None)
            pools[name] = res
        except Exception as e:
            logger.warning(f"{name}: {e}")
            if prev:
                prev = dict(prev); prev["error"] = f"{now_iso()}: {e}"; pools[name] = prev
            else:
                pools[name] = {"source": name, "error": f"{now_iso()}: {e}", "updated": None, "total_ranked": 0, "ranked": {}}

    run("jito", fetch_jito)
    run("shinobi", fetch_shinobi)
    run("solstrategies", fetch_solstrategies)
    if epoch is not None:
        run("jpool_perf", fetch_jpool, epoch)
    if "stake" in only:
        stake = fetch_stake(rpc)
    if "jito" in only and pools.get("jito"):
        try:
            steward = fetch_steward(rpc)
            pools["jito"]["steward"] = steward
            pools["jito"]["places"] = steward["num_delegation_validators"]
        except Exception as e:
            logger.warning(f"jito steward config: {e}")
            if "places" not in pools["jito"]:
                pools["jito"]["places"] = None
    # solstrategies funds whoever is in its on-chain list; that count is its live number of places
    if pools.get("solstrategies") and (stake.get("solstrategies") or {}).get("meta", {}).get("with_stake") is not None:
        pools["solstrategies"]["places"] = stake["solstrategies"]["meta"]["with_stake"]

    doc = {"version": CACHE_VERSION, "generated": now_iso(), "epoch": epoch, "pools": pools, "stake": stake}
    summary = {k: (v.get("total_ranked"), v.get("places"), v.get("source_epoch"), "ERR" if v.get("error") else "ok") for k, v in pools.items()}
    summary["stake"] = {k: v["meta"].get("with_stake", "ERR") for k, v in stake.items()}
    logger.info(f"done in {time.time() - t0:.1f}s: {summary}")
    if args.dry_run:
        print(json.dumps({"epoch": epoch, "summary": summary}, default=str))
        return
    tmp = tempfile.NamedTemporaryFile("w", dir=os.path.dirname(out_path) or ".", delete=False, suffix=".tmp")
    with tmp:
        json.dump(doc, tmp, separators=(",", ":"), default=str)
    os.replace(tmp.name, out_path)
    logger.info(f"written {out_path} ({os.path.getsize(out_path) // 1024} KB)")


if __name__ == "__main__":
    main()
