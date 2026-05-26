#!/usr/bin/env python3
"""
timeseries.py
=============
SONDA time series database: tracks state changes over time in SQLite.

Philosophy: store CHANGES, not states. Most checks reveal nothing new —
we only write rows when something actually changes. Cluster-level metrics
are written every tick (always changing). This keeps the DB compact:
~a few MB per year even with 60s mainnet cycles.

Tables:
  node_changes        any field changed on any node (validator, rpc, dz-device)
  ip_changes          IP changed (for backup-node detection + timeline viz)
  endpoint_status     endpoint reachability every tick (for uptime charts)
  cluster_metrics     aggregate metrics every tick (for trend graphs)
  new_entities        new nodes appeared
  asn_metrics         top-N ASNs hourly snapshot
  current_state       latest known state per entity (source of truth for diffs)

Usage as library:
  from timeseries import TimeSeries
  ts = TimeSeries(config)
  ts.record_snapshot(cluster, snapshot_dict)  # diffs against current_state, writes changes
  events = ts.get_recent_endpoint_changes(cluster, minutes=5)
  ts.build_aggregates(cluster)  # writes JSON files for R2

Usage CLI:
  python timeseries.py --config config.yaml --record /path/to/snapshot.json --cluster mainnet-beta
  python timeseries.py --config config.yaml --aggregate --cluster mainnet-beta
  python timeseries.py --config config.yaml --init  # create empty DB
  python timeseries.py --config config.yaml --backup  # gzip DB for R2 upload
"""

import argparse
import gzip
import json
import logging
import shutil
import sqlite3
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS node_changes (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT NOT NULL,
    cluster     TEXT NOT NULL,
    identity    TEXT NOT NULL,
    role        TEXT,
    field       TEXT NOT NULL,
    old_value   TEXT,
    new_value   TEXT
);
CREATE INDEX IF NOT EXISTS idx_node_changes_ts ON node_changes(cluster, ts);
CREATE INDEX IF NOT EXISTS idx_node_changes_identity ON node_changes(identity, ts);

CREATE TABLE IF NOT EXISTS ip_changes (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT NOT NULL,
    cluster     TEXT NOT NULL,
    identity    TEXT NOT NULL,
    role        TEXT,
    old_ip      TEXT,
    new_ip      TEXT,
    old_country TEXT,
    new_country TEXT,
    old_asn     TEXT,
    new_asn     TEXT
);
CREATE INDEX IF NOT EXISTS idx_ip_changes_ts ON ip_changes(cluster, ts);
CREATE INDEX IF NOT EXISTS idx_ip_changes_identity ON ip_changes(identity, ts);

CREATE TABLE IF NOT EXISTS endpoint_status (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT NOT NULL,
    cluster     TEXT NOT NULL,
    name        TEXT,
    provider    TEXT NOT NULL,
    service     TEXT,
    label       TEXT,
    ip          TEXT,
    reachable   INTEGER
);
CREATE INDEX IF NOT EXISTS idx_endpoint_ts ON endpoint_status(cluster, ts);
CREATE INDEX IF NOT EXISTS idx_endpoint_name ON endpoint_status(cluster, name, ts);

CREATE TABLE IF NOT EXISTS cluster_metrics (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    ts                     TEXT NOT NULL,
    cluster                TEXT NOT NULL,
    epoch                  INTEGER,
    validator_count        INTEGER,
    delinquent_count       INTEGER,
    delinquent_stake_pct   REAL,
    dz_validator_count     INTEGER,
    dz_stake_pct           REAL,
    bam_validator_count    INTEGER,
    bam_stake_pct          REAL,
    skip_rate              REAL,
    nakamoto_country       INTEGER,
    nakamoto_asn           INTEGER,
    nakamoto_city          INTEGER,
    nakamoto_validator     INTEGER
);
CREATE INDEX IF NOT EXISTS idx_cluster_metrics_ts ON cluster_metrics(cluster, ts);

CREATE TABLE IF NOT EXISTS new_entities (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT NOT NULL,
    cluster     TEXT NOT NULL,
    role        TEXT,
    identity    TEXT NOT NULL,
    ip          TEXT,
    country_code TEXT,
    asn         TEXT
);
CREATE INDEX IF NOT EXISTS idx_new_entities_ts ON new_entities(cluster, ts);

CREATE TABLE IF NOT EXISTS asn_metrics (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT NOT NULL,
    cluster     TEXT NOT NULL,
    top_asns_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_asn_metrics_ts ON asn_metrics(cluster, ts);

-- current_state is the "last known value" per entity. Used to diff against
-- incoming snapshots. Rewritten on every successful record_snapshot().
CREATE TABLE IF NOT EXISTS current_state (
    cluster     TEXT NOT NULL,
    identity    TEXT NOT NULL,
    state_json  TEXT NOT NULL,  -- dict of {field: value}
    updated_at  TEXT NOT NULL,
    PRIMARY KEY (cluster, identity)
);

-- endpoint current state — primary key is 'name' because it uniquely identifies
-- the endpoint across providers and services (e.g. "jito-ntp-amsterdam" vs
-- "jito-bam-amsterdam-mainnet-bam-2-tee"). 'label' is just the short display
-- name (e.g. "amsterdam") and can collide between services.
-- missing_cycles tracks how many consecutive ticks the endpoint has been
-- absent from the snapshot. Used to distinguish transient API glitches
-- (1 cycle) from real disappearance (2+ cycles).
-- zero_connected_cycles tracks how many consecutive ticks a BAM endpoint
-- has had 0 connected validators (probe state). Escalates to alert after
-- a configurable threshold.
CREATE TABLE IF NOT EXISTS current_endpoints (
    cluster                TEXT NOT NULL,
    name                   TEXT NOT NULL,
    label                  TEXT,
    reachable              INTEGER,
    ip                     TEXT,
    provider               TEXT,
    service                TEXT,
    missing_cycles         INTEGER DEFAULT 0,
    zero_connected_cycles  INTEGER DEFAULT 0,
    first_seen             TEXT,
    updated_at             TEXT NOT NULL,
    PRIMARY KEY (cluster, name)
);

-- epoch_snapshots: full picture at the end of each epoch.
-- snapshot_json contains a rich dict (see record_epoch_snapshot docstring).
-- Used for epoch summaries, delta calculation, and historical metrics.
CREATE TABLE IF NOT EXISTS epoch_snapshots (
    cluster       TEXT NOT NULL,
    epoch         INTEGER NOT NULL,
    ts            TEXT NOT NULL,
    snapshot_json TEXT NOT NULL,
    PRIMARY KEY (cluster, epoch)
);
CREATE INDEX IF NOT EXISTS idx_epoch_snapshots_ts ON epoch_snapshots(cluster, ts);

-- cluster_state: latest known genesis_hash and slot per cluster (v6.1).
-- Used for rollback detection: comparing current snapshot's genesis_hash
-- and slot against last known values to detect regenesis / epoch rollback /
-- slot rollback. See section 8.5 of system prompt for the 3 detection types.
CREATE TABLE IF NOT EXISTS cluster_state (
    cluster        TEXT PRIMARY KEY,
    genesis_hash   TEXT,
    last_epoch     INTEGER,
    last_slot      INTEGER,
    last_seen_ts   TEXT NOT NULL
);

-- bam_regions_seen: tracks unique BAM regions (region_short derived from
-- bam_id). First time we see a new region_short, it's a real "new region"
-- event worth a public alert. Subsequent bam_id rotations within the same
-- region_short (e.g. '-1-tee' -> '-2-tee') are silent.
CREATE TABLE IF NOT EXISTS bam_regions_seen (
    region_short   TEXT PRIMARY KEY,
    first_seen     TEXT NOT NULL,
    first_bam_id   TEXT,
    last_seen      TEXT,
    last_bam_id    TEXT
);

-- milestones_reached: remembers which milestones have been announced so we
-- don't re-alert when metrics oscillate around a threshold. Key is
-- (cluster, metric, threshold, direction). When a new crossing happens in
-- the opposite direction, we log it as a new milestone.
CREATE TABLE IF NOT EXISTS milestones_reached (
    cluster       TEXT NOT NULL,
    metric        TEXT NOT NULL,
    threshold     REAL NOT NULL,
    direction     TEXT NOT NULL,  -- 'up' or 'down'
    value         REAL,
    ts            TEXT NOT NULL,
    PRIMARY KEY (cluster, metric, threshold, direction)
);

-- endpoint_health_state holds the rolled-up health state for each endpoint.
-- This is computed from endpoint_status uptime over a sliding window
-- (configurable, default 30 min). It absorbs short flaps:
-- a 1-min dropout in a 30-min window only shifts uptime by 3.3%, so a
-- normally-95%+ endpoint stays "healthy" through brief blips.
-- States: healthy | degraded | down | unknown
-- 'unknown' = not enough data points yet (less than min_data_points samples)
CREATE TABLE IF NOT EXISTS endpoint_health_state (
    cluster           TEXT NOT NULL,
    name              TEXT NOT NULL,
    state             TEXT NOT NULL,
    uptime_pct        REAL,
    samples_in_window INTEGER,
    last_changed_ts   TEXT NOT NULL,
    last_evaluated_ts TEXT NOT NULL,
    PRIMARY KEY (cluster, name)
);
"""

# Fields we track for changes per node. Keep this list small and meaningful —
# every field here is a potential telegram alert.
TRACKED_FIELDS = [
    "role",           # validator-hidden -> validator, etc.
    "delinquent",
    "version",
    "is_sfdp",
    "sfdp_state",
    "dz_connected",
    "dz_connection_type",
    "is_rakurai",
    "bam_node",
]

# Node roles that count as "validators" for metrics
VALIDATOR_ROLES = {"validator", "validator-hidden", "validator-inactive", "co-hosted"}

# Roles we track in current_state for change detection. Validators get full
# TRACKED_FIELDS tracking; RPC nodes get just version + IP. We don't track
# anything else (gossip-only "unknown" nodes are too noisy, endpoints are
# handled by current_endpoints with a different key structure, Harmonic nodes
# have non-unique identity_pubkey=IP with multiple services per IP).
TRACKED_ROLES = VALIDATOR_ROLES | {"rpc"}

# Milestone thresholds. We alert when metric crosses any of these in either
# direction. Once a threshold is crossed upward, we won't re-announce until
# it's crossed downward first (to avoid oscillation spam).
MILESTONE_THRESHOLDS = {
    "dz_stake_percent": [30, 40, 50, 60, 70],
    "bam_stake_percent": [20, 25, 30, 35, 40, 50],
    "rakurai_stake_percent": [5, 10, 15],
    "firedancer_stake_percent": [5, 10, 20, 30, 50],
    "delinquent_stake_percent": [5, 10, 15, 20],  # health alerts (raised from [1,3,5,10] on 2026-04-30: smaller clusters were spamming on single-validator delinquency)
    "superminority_validators": [],  # change of any size is interesting
}

# Client name fragments that indicate Firedancer family (for version milestones)
FIREDANCER_FAMILY = {"firedancer", "frankendancer", "fd_harmonic"}

# Datacenter incident detection thresholds
DC_INCIDENT_MIN_VALIDATORS = 5      # minimum cluster size to consider
DC_INCIDENT_AFFECTED_PCT = 70       # >= this % affected -> alert

# Epoch snapshot retention
EPOCH_SNAPSHOTS_KEEP = 365          # keep last ~2 years at 2d per epoch

# Endpoint health state defaults. All overridable via config.endpoint_alerts.*
# State machine: each endpoint carries one of healthy/degraded/down/unknown.
# 'unknown' applies until we have at least min_data_points cycles of history,
# so freshly-discovered endpoints don't immediately fire a "down" alert.
ENDPOINT_HEALTH_DEFAULTS = {
    "mode": "state_based",          # "state_based" | "realtime"
    "health_window_minutes": 30,    # rolling window for uptime calculation
    "threshold_healthy": 95,        # uptime% >= this -> healthy
    "threshold_down": 50,           # uptime% < this -> down (between -> degraded)
    "min_data_points": 5,           # need this many samples in window to evaluate
}


def get_endpoint_alert_settings(config: dict) -> dict:
    """Merge config.endpoint_alerts over defaults so missing keys are filled in."""
    settings = dict(ENDPOINT_HEALTH_DEFAULTS)
    settings.update(config.get("endpoint_alerts") or {})
    return settings


def load_config(config_path: str) -> dict:
    import yaml
    with open(config_path) as f:
        return yaml.safe_load(f)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# TimeSeries main class
# ---------------------------------------------------------------------------

class TimeSeries:
    def __init__(self, config: dict):
        self.config = config
        self.data_dir = Path(config["paths"]["data_dir"])
        self.db_path = self.data_dir / "timeseries.db"
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _conn(self):
        conn = sqlite3.connect(str(self.db_path))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _init_schema(self):
        with self._conn() as c:
            for stmt in SCHEMA.split(";"):
                s = stmt.strip()
                if s:
                    c.execute(s)

    # -----------------------------------------------------------------------
    # Record changes from a snapshot
    # -----------------------------------------------------------------------

    def record_snapshot(self, cluster: str, snapshot: dict) -> dict:
        """
        Compare snapshot against current_state; write only changes.
        Returns stats dict with counts.
        """
        ts = now_iso()
        records = snapshot.get("records", [])
        meta = {
            "ts": ts,
            "cluster": cluster,
            "epoch": snapshot.get("epoch"),
        }

        stats = {
            "node_changes": 0, "ip_changes": 0, "new_entities": 0,
            "endpoint_changes": 0, "records_processed": len(records),
        }

        with self._conn() as c:
            prev_state = self._load_current_state(c, cluster)

            new_state = {}
            seen_identities = set()

            for rec in records:
                identity = rec.get("identity_pubkey")
                if not identity:
                    continue

                role = rec.get("role", "")

                # current_state tracks per-node fields (validators: delinquent,
                # version, dz_connected, bam_node etc.; RPC nodes: version only).
                # Non-tracked records (Harmonic/Jito endpoints, gossip-only nodes)
                # are either handled separately (endpoint_status with unique
                # 'name' key) or too noisy to persist. Harmonic specifically has
                # identity_pubkey=IP with multiple services per IP, so tracking
                # them here would create spurious role-flip changes.
                if role not in TRACKED_ROLES:
                    continue

                seen_identities.add(identity)

                ip = rec.get("ip_address")
                geo = rec.get("geolocation") or {}
                country = geo.get("country_code") or ""
                asn = geo.get("asn") or ""

                # Build the "tracked state" for this node
                tracked = {f: rec.get(f) for f in TRACKED_FIELDS}
                tracked["ip_address"] = ip
                tracked["country_code"] = country
                tracked["asn"] = asn

                new_state[identity] = tracked

                prev = prev_state.get(identity)

                if prev is None:
                    # New entity
                    c.execute(
                        "INSERT INTO new_entities (ts, cluster, role, identity, ip, country_code, asn) "
                        "VALUES (?,?,?,?,?,?,?)",
                        (ts, cluster, role, identity, ip, country, asn)
                    )
                    stats["new_entities"] += 1
                    continue

                # Check IP change separately — goes to ip_changes table
                old_ip = prev.get("ip_address")
                if ip and old_ip and ip != old_ip:
                    c.execute(
                        "INSERT INTO ip_changes (ts, cluster, identity, role, "
                        "old_ip, new_ip, old_country, new_country, old_asn, new_asn) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?)",
                        (ts, cluster, identity, role,
                         old_ip, ip,
                         prev.get("country_code", ""), country,
                         prev.get("asn", ""), asn)
                    )
                    stats["ip_changes"] += 1

                # Check other tracked field changes
                for field in TRACKED_FIELDS:
                    old_val = prev.get(field)
                    new_val = tracked.get(field)
                    if old_val != new_val:
                        c.execute(
                            "INSERT INTO node_changes (ts, cluster, identity, role, field, old_value, new_value) "
                            "VALUES (?,?,?,?,?,?,?)",
                            (ts, cluster, identity, role, field,
                             json.dumps(old_val) if old_val is not None else None,
                             json.dumps(new_val) if new_val is not None else None)
                        )
                        stats["node_changes"] += 1

            # Write current state (replace all for this cluster)
            c.execute("DELETE FROM current_state WHERE cluster=?", (cluster,))
            c.executemany(
                "INSERT INTO current_state (cluster, identity, state_json, updated_at) "
                "VALUES (?,?,?,?)",
                [(cluster, ident, json.dumps(s), ts) for ident, s in new_state.items()]
            )

            # Endpoint status tracking
            stats["endpoint_changes"] = self._record_endpoints(c, cluster, ts, records)

            # Cluster metrics (always written, every tick)
            self._record_cluster_metrics(c, cluster, ts, snapshot)

            c.commit()

        # v6.1: persist cluster state (genesis_hash, epoch, slot) for next-cycle
        # rollback detection. Called every cycle, not just on epoch change.
        self.update_cluster_state(
            cluster,
            snapshot.get("genesis_hash"),
            snapshot.get("epoch"),
            snapshot.get("slot"),
        )

        return stats

    def _load_current_state(self, c, cluster: str) -> dict:
        """Returns {identity: {field: value}} for current state."""
        rows = c.execute(
            "SELECT identity, state_json FROM current_state WHERE cluster=?",
            (cluster,)
        ).fetchall()
        return {r[0]: json.loads(r[1]) for r in rows}

    def _record_endpoints(self, c, cluster: str, ts: str, records: list) -> int:
        """Record endpoint reachability and detect state changes.

        Three kinds of events tracked here:

        1. Reachability transition (reachable <-> unreachable) — written to
           endpoint_status for every tick, state change counter returned.

        2. Endpoint disappeared from API (missing_cycles logic) — if an endpoint
           was in the previous snapshot but absent now, missing_cycles
           increments. On threshold (2 cycles) we mark it unreachable and
           alert. On DISAPPEARED_TTL cycles (e.g. 60 on mainnet = 1h), we
           remove it from current_endpoints entirely.

        3. BAM probe state (0 connected validators) — for endpoints where
           reachable is None (meaning API shows 0 validators), we track
           zero_connected_cycles. If stuck at 0 for a long time, it's worth
           an alert; but brief warm-up periods should be silent.

        Returns count of reachability transitions (for stats).
        """
        endpoint_roles = {
            "jito-block-engine", "jito-shred-receiver", "jito-ntp", "jito-bam",
            "harmonic-auction", "harmonic-tpu-relayer", "harmonic-shred-receiver", "harmonic-bundles",
            "solana-rpc-official", "solana-entrypoint",
        }

        # Thresholds (cycles, not seconds — cycle length depends on cluster)
        # MISSING_ALERT_AFTER = 3 means we wait 3 consecutive missing cycles
        # before declaring an endpoint unreachable. On mainnet (60s cycle)
        # that's ~3 minutes; short API flaps (1-2 cycles) are filtered out
        # at this level so they don't even reach telegram. telegram.py
        # additionally applies flap suppression on the recovery side.
        MISSING_ALERT_AFTER = 3     # flag unreachable after N consecutive missing cycles
        DISAPPEARED_TTL = 60        # fully drop from current_endpoints after N cycles missing
        ZERO_CONN_ALERT_AFTER = 60  # alert on BAM stuck at 0 validators after N cycles

        endpoints = [r for r in records if r.get("role") in endpoint_roles]

        # Previous endpoint state from DB (keyed by unique 'name')
        prev_rows = c.execute(
            "SELECT name, label, reachable, ip, provider, service, missing_cycles, "
            "zero_connected_cycles, first_seen "
            "FROM current_endpoints WHERE cluster=?",
            (cluster,)
        ).fetchall()
        prev_endpoints = {
            r[0]: {
                "label": r[1], "reachable": r[2], "ip": r[3],
                "provider": r[4], "service": r[5],
                "missing_cycles": r[6] or 0,
                "zero_connected_cycles": r[7] or 0,
                "first_seen": r[8],
            } for r in prev_rows
        }

        changes = 0
        new_state = {}
        seen_names = set()

        # Process endpoints present in current snapshot
        for ep in endpoints:
            # Endpoint fields live in a nested "endpoint" object in the snapshot.
            # Example: {"role": "jito-bam", "name": "jito-bam-amsterdam-...-bam-2-tee",
            #           "endpoint": {"provider": "jito", "service": "bam",
            #                        "label": "amsterdam", "port": null,
            #                        "reachable": true}}
            # 'name' is the unique identifier (may include bam_id for BAM).
            # 'label' is the short display name (can collide across services).
            ep_info = ep.get("endpoint") or {}
            name = ep.get("name") or ""
            if not name:
                continue
            seen_names.add(name)

            label = ep_info.get("label") or ""
            provider = ep_info.get("provider", "")
            service = ep_info.get("service", "")
            ip = ep.get("ip_address", "")
            reachable = ep_info.get("reachable")
            reachable_int = None if reachable is None else (1 if reachable else 0)

            prev = prev_endpoints.get(name)

            # Register BAM regions — label is region_short for BAM endpoints.
            # register_bam_region returns True if region_short was never seen
            # before, which is a signal caller can use to emit public alerts.
            # We just mark it in the DB here; the alert decision is in telegram.py.
            if service == "bam" and label:
                bam_id = (ep.get("endpoint") or {}).get("bam_id") or name
                self.register_bam_region(label, bam_id, ts, conn=c)

            # Track zero_connected cycles for BAM probe state.
            # reachable=None for BAM means 0 connected validators from API.
            zero_cycles = 0
            if service == "bam" and reachable is None:
                zero_cycles = (prev["zero_connected_cycles"] + 1) if prev else 1
                if zero_cycles == ZERO_CONN_ALERT_AFTER:
                    logger.info(f"  ⚠️  BAM {name} stuck at 0 validators for {zero_cycles} cycles")

            # Write tick to endpoint_status (always — this feeds uptime charts)
            c.execute(
                "INSERT INTO endpoint_status (ts, cluster, name, provider, service, label, ip, reachable) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (ts, cluster, name, provider, service, label, ip, reachable_int)
            )

            # Count reachability transitions for stats
            if prev is not None and prev["reachable"] != reachable_int:
                changes += 1

            new_state[name] = {
                "label": label,
                "reachable": reachable_int, "ip": ip,
                "provider": provider, "service": service,
                "missing_cycles": 0,  # present now -> reset missing
                "zero_connected_cycles": zero_cycles,
                "first_seen": prev["first_seen"] if prev else ts,
            }

        # Process endpoints MISSING from current snapshot (disappeared from API)
        for name, prev in prev_endpoints.items():
            if name in seen_names:
                continue
            missing = prev["missing_cycles"] + 1

            if missing == MISSING_ALERT_AFTER:
                c.execute(
                    "INSERT INTO endpoint_status (ts, cluster, name, provider, service, label, ip, reachable) "
                    "VALUES (?,?,?,?,?,?,?,?)",
                    (ts, cluster, name, prev["provider"], prev["service"], prev["label"], prev["ip"], 0)
                )
                changes += 1
                logger.info(f"  🔴 Endpoint disappeared: {name} ({missing} cycles missing)")

            if missing >= DISAPPEARED_TTL:
                logger.info(f"  🗑️  Dropping {name} from current_endpoints "
                            f"(missing {missing} cycles, TTL reached)")
                continue

            new_state[name] = {
                "label": prev["label"],
                "reachable": 0 if missing >= MISSING_ALERT_AFTER else prev["reachable"],
                "ip": prev["ip"],
                "provider": prev["provider"],
                "service": prev["service"],
                "missing_cycles": missing,
                "zero_connected_cycles": prev["zero_connected_cycles"],
                "first_seen": prev["first_seen"],
            }

        # Replace current_endpoints for this cluster
        c.execute("DELETE FROM current_endpoints WHERE cluster=?", (cluster,))
        c.executemany(
            "INSERT INTO current_endpoints (cluster, name, label, reachable, ip, provider, service, "
            "missing_cycles, zero_connected_cycles, first_seen, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            [(cluster, nm, s["label"], s["reachable"], s["ip"], s["provider"], s["service"],
              s["missing_cycles"], s["zero_connected_cycles"],
              s["first_seen"] or ts, ts)
             for nm, s in new_state.items()]
        )

        return changes

    def _record_cluster_metrics(self, c, cluster: str, ts: str, snapshot: dict):
        """Write cluster-level metrics (always, every tick).

        All `.get(...)` calls use `or {}` fallback because snapshot JSON can
        contain explicit null values (especially for devnet where DZ/BAM/
        Rakurai don't exist — those sections are null, not missing).
        `{}.get(key, {})` returns None for null values, hence the `or {}` idiom.
        """
        metrics = snapshot.get("metrics") or {}
        v = metrics.get("validators") or {}
        dz = metrics.get("doublezero") or {}
        bam = metrics.get("bam") or {}
        ch = metrics.get("cluster_health") or {}
        decen = (v.get("decentralization") or {}).get("current") or {}

        def _val(obj, *keys, default=None):
            cur = obj
            for k in keys:
                if not isinstance(cur, dict):
                    return default
                cur = cur.get(k)
                if cur is None:
                    return default
            return cur

        c.execute(
            "INSERT INTO cluster_metrics (ts, cluster, epoch, validator_count, delinquent_count, "
            "delinquent_stake_pct, dz_validator_count, dz_stake_pct, bam_validator_count, "
            "bam_stake_pct, skip_rate, nakamoto_country, nakamoto_asn, nakamoto_city, nakamoto_validator) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                ts, cluster, snapshot.get("epoch"),
                v.get("total"),
                v.get("offline"),  # includes hidden+inactive
                ch.get("delinquent_stake_percent"),
                dz.get("validators"),
                dz.get("validators_stake_percent"),
                bam.get("total_connected_validators"),
                bam.get("stake_percentage"),
                ch.get("average_stake_weighted_skip_rate"),
                _val(decen, "nakamoto_country", "value"),
                _val(decen, "nakamoto_asn", "value"),
                _val(decen, "nakamoto_city", "value"),
                _val(decen, "nakamoto_validator", "value"),
            )
        )

    # -----------------------------------------------------------------------
    # Epoch snapshots — full cluster picture per epoch, for summaries + deltas
    # -----------------------------------------------------------------------

    def detect_epoch_change(self, cluster: str, current_epoch: int) -> int:
        """
        Return the previous epoch if current_epoch is newer than what we have
        in epoch_snapshots, otherwise None. Caller uses this to decide whether
        to record a new epoch snapshot and emit summary events.
        """
        if current_epoch is None:
            return None
        with self._conn() as c:
            row = c.execute(
                "SELECT MAX(epoch) FROM epoch_snapshots WHERE cluster=?", (cluster,)
            ).fetchone()
        last = row[0] if row else None
        if last is None:
            return None  # fresh DB, first snapshot — no "epoch change" event
        if current_epoch > last:
            return last
        return None

    # -----------------------------------------------------------------------
    # Cluster rollback detection (v6.1, for Alpenglow + safety on other clusters)
    # -----------------------------------------------------------------------

    def detect_rollback(self, cluster: str, current_genesis: str, current_epoch: int,
                        current_slot: int) -> dict:
        """Detect cluster rollback (3 types).

        Returns dict:
          {"type": "regenesis"|"epoch_rollback"|"slot_rollback"|None,
           "previous_genesis": ..., "previous_epoch": ..., "previous_slot": ...}

        See section 8.5 of system prompt for details. Key cases:

        - regenesis: genesis_hash changed → new chain entirely (caller should
          skip all comparisons; old data still kept for historical reference)
        - epoch_rollback: same genesis but current_epoch < last_epoch
          (cluster restart on earlier epoch; slot may be increasing again!)
        - slot_rollback: same genesis, same epoch, but current_slot < last_slot
          (cluster restart from snapshot mid-epoch; info-level only)
        """
        if not current_genesis or current_epoch is None or current_slot is None:
            return {"type": None}
        with self._conn() as c:
            row = c.execute(
                "SELECT genesis_hash, last_epoch, last_slot FROM cluster_state WHERE cluster=?",
                (cluster,)
            ).fetchone()
        if not row:
            return {"type": None}  # fresh cluster, no previous state
        prev_genesis, prev_epoch, prev_slot = row[0], row[1], row[2]
        if prev_genesis and current_genesis != prev_genesis:
            return {
                "type": "regenesis",
                "previous_genesis": prev_genesis,
                "previous_epoch": prev_epoch,
                "previous_slot": prev_slot,
            }
        if prev_epoch is not None and current_epoch < prev_epoch:
            return {
                "type": "epoch_rollback",
                "previous_genesis": prev_genesis,
                "previous_epoch": prev_epoch,
                "previous_slot": prev_slot,
            }
        # Same epoch — check slot rollback (within-epoch restart from snapshot).
        if prev_epoch is not None and current_epoch == prev_epoch \
           and prev_slot is not None and current_slot < prev_slot:
            return {
                "type": "slot_rollback",
                "previous_genesis": prev_genesis,
                "previous_epoch": prev_epoch,
                "previous_slot": prev_slot,
            }
        return {"type": None}

    def update_cluster_state(self, cluster: str, genesis_hash: str,
                             epoch: int, slot: int) -> None:
        """Persist current cluster state (genesis_hash, epoch, slot) for next
        rollback detection. Called after each successful snapshot record.
        """
        if not genesis_hash or epoch is None or slot is None:
            return  # Don't overwrite valid state with incomplete data
        ts = now_iso()
        with self._conn() as c:
            c.execute(
                "INSERT OR REPLACE INTO cluster_state "
                "(cluster, genesis_hash, last_epoch, last_slot, last_seen_ts) "
                "VALUES (?, ?, ?, ?, ?)",
                (cluster, genesis_hash, epoch, slot, ts)
            )

    def get_cluster_state(self, cluster: str) -> dict:
        """Return last known cluster state or empty dict."""
        with self._conn() as c:
            row = c.execute(
                "SELECT genesis_hash, last_epoch, last_slot, last_seen_ts "
                "FROM cluster_state WHERE cluster=?", (cluster,)
            ).fetchone()
        if not row:
            return {}
        return {
            "genesis_hash": row[0],
            "last_epoch": row[1],
            "last_slot": row[2],
            "last_seen_ts": row[3],
        }

    def record_epoch_snapshot(self, cluster: str, snapshot: dict) -> dict:
        """
        Build and store a rich epoch snapshot for historical tracking and
        delta calculation in epoch summaries.

        Returns the snapshot dict that was stored (for logging / inspection).

        Structure is documented inline below — it's designed to contain
        everything needed for:
          - validator set change detection (new/left/moved between epochs)
          - decentralization trend graphs (all Nakamoto/HHI/Gini/Shannon)
          - client/version shift tracking
          - ASN+city baseline for DC incident detection
          - BAM/DZ/Rakurai adoption trend
          - Infrastructure health score computation (future)
        """
        from collections import defaultdict, Counter

        ts = snapshot.get("timestamp") or now_iso()
        epoch = snapshot.get("epoch")
        if epoch is None:
            logger.warning("  ⚠️  No epoch in snapshot, skipping epoch_snapshot")
            return {}

        # v6.1: Cluster rollback detection (3 types).
        # If detected, skip comparison against previous epoch snapshots (since they
        # may be from a different chain or restart state). We still record the
        # current state for forward tracking.
        current_genesis = snapshot.get("genesis_hash")
        current_slot = snapshot.get("slot")
        rollback = self.detect_rollback(cluster, current_genesis, epoch, current_slot)
        if rollback["type"] == "regenesis":
            logger.warning(
                f"  ⚠️  {cluster}: REGENESIS detected "
                f"(genesis {rollback['previous_genesis'][:8] if rollback['previous_genesis'] else '?'}... "
                f"→ {current_genesis[:8] if current_genesis else '?'}...). "
                f"Skipping epoch comparison."
            )
        elif rollback["type"] == "epoch_rollback":
            logger.warning(
                f"  ⚠️  {cluster}: EPOCH ROLLBACK detected "
                f"(epoch {rollback['previous_epoch']} → {epoch}). "
                f"Cluster restart on earlier epoch. Skipping epoch comparison."
            )
        elif rollback["type"] == "slot_rollback":
            logger.info(
                f"  ℹ️  {cluster}: SLOT ROLLBACK within epoch {epoch} "
                f"(slot {rollback['previous_slot']} → {current_slot}). "
                f"Cluster restart from snapshot mid-epoch."
            )

        records = snapshot.get("records", [])
        metrics = snapshot.get("metrics") or {}
        v_metrics = metrics.get("validators") or {}
        decen = (v_metrics.get("decentralization") or {}).get("current") or {}
        ch = metrics.get("cluster_health") or {}
        dz_m = metrics.get("doublezero") or {}
        bam_m = metrics.get("bam") or {}
        rak_m = metrics.get("rakurai") or {}

        # ---- Filter validators (active ones that count for metrics) ----
        all_vals = [r for r in records if r.get("role") in VALIDATOR_ROLES]
        active_vals = [r for r in all_vals if r.get("role") == "validator"]

        # ---- Validator locations (for move detection between epochs) ----
        validator_locations = []
        for r in all_vals:
            geo = r.get("geolocation") or {}
            validator_locations.append({
                "id": r.get("identity_pubkey", ""),
                "vote": r.get("vote_account", ""),
                "role": r.get("role", ""),
                "asn": geo.get("asn_name", ""),
                "country": geo.get("country_code", ""),
                "city": geo.get("city", ""),
                "stake_pct": r.get("stake_percentage") or 0,
                "client": r.get("client_type", ""),
                "version": r.get("version", ""),
                "delinquent": r.get("delinquent", False),
            })

        # ---- ASN+City clustering (baseline for DC incident detection) ----
        asn_city = defaultdict(int)
        for v in validator_locations:
            if v["asn"] and v["city"]:
                asn_city[(v["asn"], v["city"])] += 1
        asn_city_clusters = [
            {"asn": a, "city": c, "count": n}
            for (a, c), n in sorted(asn_city.items(), key=lambda x: -x[1])
            if n >= 2  # keep anything with 2+ validators
        ]

        # ---- Client distribution with stake percentages ----
        client_counts = defaultdict(lambda: {"count": 0, "stake_pct": 0.0})
        for v in validator_locations:
            c = v["client"] or "Unknown"
            client_counts[c]["count"] += 1
            client_counts[c]["stake_pct"] += v["stake_pct"]

        # ---- DZ metros (unique list for "new metro" detection) ----
        dz_metros = sorted({
            r.get("dz_metro_code", "")
            for r in records if r.get("dz_metro_code")
        })

        # ---- BAM regions (unique region_short values) ----
        # Jito API sometimes returns the full bam_id as region (e.g.
        # "london-mainnet-bam-2-tee") instead of just "london". When Jito
        # rotates tee-suffix nodes within an existing region, the raw region
        # field changes string-wise even though the locality didn't change,
        # which produced false-positive "🆕 New BAM region" alerts on epoch
        # wrap. Normalize to region_short matching analyzer's BAM endpoint
        # logic (split on "-mainnet-" or "-testnet-").
        def _bam_region_short(region: str) -> str:
            if not region:
                return ""
            for sep in ("-mainnet-", "-testnet-", "-devnet-"):
                if sep in region:
                    return region.split(sep)[0]
            return region

        bam_regions = sorted({
            _bam_region_short(r.get("bam_region", ""))
            for r in records if r.get("bam_region")
        })

        # ---- Geography ----
        countries = {v["country"] for v in validator_locations if v["country"]}
        cities = {(v["country"], v["city"]) for v in validator_locations if v["city"]}
        city_counts = Counter((v["country"], v["city"]) for v in validator_locations if v["city"])
        single_validator_cities = sum(1 for n in city_counts.values() if n == 1)
        asns = {v["asn"] for v in validator_locations if v["asn"]}

        # ---- Top ASNs by stake ----
        asn_stake = defaultdict(float)
        asn_val_count = defaultdict(int)
        for v in validator_locations:
            if v["asn"]:
                asn_stake[v["asn"]] += v["stake_pct"]
                asn_val_count[v["asn"]] += 1
        top_asns = [
            {"asn": a, "stake_pct": round(s, 2), "validators": asn_val_count[a]}
            for a, s in sorted(asn_stake.items(), key=lambda x: -x[1])[:10]
        ]

        # ---- IBRL aggregates ----
        ibrl_scores = [r.get("ibrl", {}).get("ibrl_score") for r in records
                       if r.get("ibrl") and r.get("ibrl", {}).get("ibrl_score") is not None]
        ibrl_bbt = [r.get("ibrl", {}).get("median_block_build_ms") for r in records
                    if r.get("ibrl") and r.get("ibrl", {}).get("median_block_build_ms") is not None]

        # ---- Version distribution (from cluster_health.stake_by_version) ----
        version_dist = []
        total_stake = ch.get("total_active_stake") or 0
        for ver, stats in (ch.get("stake_by_version") or {}).items():
            cur_stake = stats.get("currentActiveStake", 0)
            del_stake = stats.get("delinquentActiveStake", 0)
            version_dist.append({
                "version": ver,
                "current": stats.get("currentValidators", 0),
                "delinquent": stats.get("delinquentValidators", 0),
                "current_stake_pct": round(cur_stake / total_stake * 100, 2) if total_stake else 0,
                "delinquent_stake_pct": round(del_stake / total_stake * 100, 2) if total_stake else 0,
            })
        version_dist.sort(key=lambda x: -(x["current"] + x["delinquent"]))
        version_dist = version_dist[:15]

        # ---- Decentralization metrics (all 20+) ----
        # Most metrics have 'value' field, but shannon_* uses 'entropy'
        def _d(key, field="value"):
            return ((decen.get(key) or {}).get(field))

        decen_block = {
            "nakamoto_country": _d("nakamoto_country"),
            "nakamoto_asn": _d("nakamoto_asn"),
            "nakamoto_city": _d("nakamoto_city"),
            "nakamoto_validator": _d("nakamoto_validator"),
            "superminority_country_33": _d("superminority_country_33"),
            "superminority_country_50": _d("superminority_country_50"),
            "superminority_country_66": _d("superminority_country_66"),
            "superminority_asn_33": _d("superminority_asn_33"),
            "superminority_asn_50": _d("superminority_asn_50"),
            "superminority_asn_66": _d("superminority_asn_66"),
            "superminority_validator_33": _d("superminority_validator_33"),
            "superminority_validator_50": _d("superminority_validator_50"),
            "superminority_validator_66": _d("superminority_validator_66"),
            "hhi_country": _d("hhi_country"),
            "hhi_asn": _d("hhi_asn"),
            "hhi_validator": _d("hhi_validator"),
            "gini_validators": _d("gini_validators"),
            # Shannon uses 'entropy' field, not 'value'
            "shannon_country": _d("shannon_country", "entropy"),
            "shannon_asn": _d("shannon_asn", "entropy"),
            "shannon_validator": _d("shannon_validator", "entropy"),
            # Shannon normalized 0-1 is also useful for scoring
            "shannon_country_normalized": _d("shannon_country", "normalized"),
            "shannon_asn_normalized": _d("shannon_asn", "normalized"),
            "shannon_validator_normalized": _d("shannon_validator", "normalized"),
        }

        # ---- Final snapshot ----
        epoch_snap = {
            "cluster": cluster,
            "epoch": epoch,
            "timestamp": ts,

            "validators": {
                "total": len(all_vals),
                "active": len(active_vals),
                "delinquent": sum(1 for r in all_vals if r.get("delinquent")),
                "hidden": v_metrics.get("hidden", 0),
                "inactive": v_metrics.get("inactive", 0),
                "superminority_count": sum(1 for r in all_vals if r.get("is_superminority")),
            },

            "stake": {
                "total_active_lamports": ch.get("total_active_stake", 0),
                "total_delinquent_lamports": ch.get("total_delinquent_stake", 0),
                "delinquent_stake_percent": ch.get("delinquent_stake_percent", 0),
            },

            "decentralization": decen_block,

            "validator_locations": validator_locations,

            "doublezero": {
                "validators": dz_m.get("validators", 0),
                "validators_percent": dz_m.get("validators_percent", 0),
                "stake_percent": dz_m.get("validators_stake_percent", 0),
                "multicast_publishers_count": (dz_m.get("multicast_publishers") or {}).get("total", 0),
                "connection_types": dict(dz_m.get("connection_types") or {}),
                "devices_total": dz_m.get("devices", 0),
                "metros": dz_metros,
                "network_health_device_pct": (dz_m.get("network_health") or {}).get("devices_healthy_percent"),
                "network_health_link_pct": (dz_m.get("network_health") or {}).get("links_healthy_percent"),
            },

            "bam": {
                "validators": bam_m.get("total_connected_validators", 0),
                "stake_percent": bam_m.get("stake_percentage", 0),
                "node_count": bam_m.get("node_count", 0),
                "regions": bam_regions,
            },

            "rakurai": {
                "validators": rak_m.get("matched_validators", 0),
                "stake_percent": rak_m.get("stake_percent", 0),
            },

            "top_asns": top_asns,
            "asn_city_clusters": asn_city_clusters,

            "client_distribution": {
                name: {"count": d["count"], "stake_pct": round(d["stake_pct"], 2)}
                for name, d in sorted(client_counts.items(),
                                      key=lambda x: -x[1]["count"])
            },

            "version_distribution": version_dist,

            "geography": {
                "countries": len(countries),
                "cities": len(cities),
                "single_validator_cities": single_validator_cities,
                "asns": len(asns),
            },

            "ibrl": {
                "total_scored": len(ibrl_scores),
                "avg_score": round(sum(ibrl_scores) / len(ibrl_scores), 2) if ibrl_scores else None,
                "avg_median_block_build_ms": round(sum(ibrl_bbt) / len(ibrl_bbt), 1) if ibrl_bbt else None,
            },

            "health": {
                "skip_rate_avg": ch.get("average_skip_rate"),
                "stake_weighted_skip_rate": ch.get("average_stake_weighted_skip_rate"),
            },
        }

        with self._conn() as c:
            c.execute(
                "INSERT OR REPLACE INTO epoch_snapshots (cluster, epoch, ts, snapshot_json) "
                "VALUES (?,?,?,?)",
                (cluster, epoch, ts, json.dumps(epoch_snap, separators=(",", ":"), ensure_ascii=False))
            )

        logger.info(f"💾 Epoch snapshot saved: {cluster}/{epoch} "
                    f"({len(validator_locations)} validators)")
        return epoch_snap

    def get_epoch_snapshot(self, cluster: str, epoch: int) -> dict:
        """Load a stored epoch snapshot. Returns None if not found."""
        with self._conn() as c:
            row = c.execute(
                "SELECT snapshot_json FROM epoch_snapshots WHERE cluster=? AND epoch=?",
                (cluster, epoch)
            ).fetchone()
        return json.loads(row[0]) if row else None

    def get_previous_epoch_snapshot(self, cluster: str, before_epoch: int) -> dict:
        """Get the most recent epoch snapshot before the given epoch."""
        with self._conn() as c:
            row = c.execute(
                "SELECT snapshot_json FROM epoch_snapshots "
                "WHERE cluster=? AND epoch<? ORDER BY epoch DESC LIMIT 1",
                (cluster, before_epoch)
            ).fetchone()
        return json.loads(row[0]) if row else None

    # -----------------------------------------------------------------------
    # BAM regions (distinct from bam_nodes — region_short is the "locality")
    # -----------------------------------------------------------------------

    def register_bam_region(self, region_short: str, bam_id: str, ts: str = None, conn=None) -> bool:
        """
        Register a BAM region sighting. Returns True if this region_short is
        being seen for the first time (caller may emit a 'new region' alert).

        If `conn` is provided, uses that sqlite3 Connection (for calls from
        within an existing transaction). Otherwise opens its own connection.
        """
        if not region_short:
            return False
        ts = ts or now_iso()

        def _do(c):
            row = c.execute(
                "SELECT region_short FROM bam_regions_seen WHERE region_short=?",
                (region_short,)
            ).fetchone()
            if row is None:
                c.execute(
                    "INSERT INTO bam_regions_seen (region_short, first_seen, "
                    "first_bam_id, last_seen, last_bam_id) VALUES (?,?,?,?,?)",
                    (region_short, ts, bam_id, ts, bam_id)
                )
                return True
            c.execute(
                "UPDATE bam_regions_seen SET last_seen=?, last_bam_id=? "
                "WHERE region_short=?",
                (ts, bam_id, region_short)
            )
            return False

        if conn is not None:
            return _do(conn)
        with self._conn() as c:
            return _do(c)

    # -----------------------------------------------------------------------
    # Milestone tracking (thresholds crossed for public bot alerts)
    # -----------------------------------------------------------------------

    def check_milestones(self, cluster: str, snapshot: dict) -> list:
        """
        Check all configured thresholds against current snapshot; return
        list of newly-crossed milestones. A milestone is only reported once
        per direction — if DZ reaches 50% upward, we won't alert again until
        it has dropped below 50% (downward cross) and then climbs back.

        Return format: list of dicts with {metric, threshold, direction,
        value, old_value}.
        """
        events = []
        metrics = snapshot.get("metrics") or {}
        dz_m = metrics.get("doublezero") or {}
        bam_m = metrics.get("bam") or {}
        rak_m = metrics.get("rakurai") or {}
        ch = metrics.get("cluster_health") or {}

        # Current values
        current = {
            "dz_stake_percent": dz_m.get("validators_stake_percent") or 0,
            "bam_stake_percent": bam_m.get("stake_percentage") or 0,
            "rakurai_stake_percent": rak_m.get("stake_percent") or 0,
            "delinquent_stake_percent": ch.get("delinquent_stake_percent") or 0,
        }

        # Firedancer family stake % — derived from client_distribution
        fd_stake = 0.0
        for r in snapshot.get("records", []):
            if r.get("role") not in VALIDATOR_ROLES:
                continue
            ct = (r.get("client_type") or "").lower()
            if any(fam in ct for fam in FIREDANCER_FAMILY):
                fd_stake += r.get("stake_percentage") or 0
        current["firedancer_stake_percent"] = round(fd_stake, 2)

        with self._conn() as c:
            for metric, thresholds in MILESTONE_THRESHOLDS.items():
                if not thresholds:
                    continue
                val = current.get(metric)
                if val is None:
                    continue

                # Load previous state for this metric
                prev_rows = c.execute(
                    "SELECT threshold, direction, value FROM milestones_reached "
                    "WHERE cluster=? AND metric=?", (cluster, metric)
                ).fetchall()
                prev = {(r[0], r[1]): r[2] for r in prev_rows}

                for t in thresholds:
                    # Upward cross: current > t AND we don't have 'up' record,
                    # OR last was 'down' (oscillation -> re-alert fresh)
                    up_seen = (t, "up") in prev
                    down_seen = (t, "down") in prev
                    # Up crossing
                    if val >= t and not up_seen:
                        events.append({
                            "metric": metric, "threshold": t, "direction": "up",
                            "value": val,
                        })
                        c.execute(
                            "INSERT OR REPLACE INTO milestones_reached "
                            "(cluster, metric, threshold, direction, value, ts) "
                            "VALUES (?,?,?,?,?,?)",
                            (cluster, metric, t, "up", val, now_iso())
                        )
                        # Clear the opposite direction so future down-cross re-alerts
                        c.execute(
                            "DELETE FROM milestones_reached WHERE cluster=? "
                            "AND metric=? AND threshold=? AND direction=?",
                            (cluster, metric, t, "down")
                        )
                    # Down crossing
                    elif val < t and up_seen and not down_seen:
                        events.append({
                            "metric": metric, "threshold": t, "direction": "down",
                            "value": val,
                        })
                        c.execute(
                            "INSERT OR REPLACE INTO milestones_reached "
                            "(cluster, metric, threshold, direction, value, ts) "
                            "VALUES (?,?,?,?,?,?)",
                            (cluster, metric, t, "down", val, now_iso())
                        )
                        c.execute(
                            "DELETE FROM milestones_reached WHERE cluster=? "
                            "AND metric=? AND threshold=? AND direction=?",
                            (cluster, metric, t, "up")
                        )
            c.commit()

        return events

    # -----------------------------------------------------------------------
    # Datacenter incident detection
    # -----------------------------------------------------------------------

    def detect_dc_incidents(self, cluster: str, snapshot: dict) -> list:
        """
        Find (ASN, city) clusters where a large fraction of validators are
        affected (delinquent or offline). Returns list of dicts:
          {asn, city, total, affected, pct}

        Minimum cluster size: DC_INCIDENT_MIN_VALIDATORS.
        Threshold: DC_INCIDENT_AFFECTED_PCT.
        """
        from collections import defaultdict

        clusters = defaultdict(lambda: {"total": 0, "affected": 0, "ids": []})
        for r in snapshot.get("records", []):
            if r.get("role") not in VALIDATOR_ROLES:
                continue
            geo = r.get("geolocation") or {}
            asn = geo.get("asn_name") or ""
            city = geo.get("city") or ""
            if not asn or not city:
                continue
            key = (asn, city)
            clusters[key]["total"] += 1
            clusters[key]["ids"].append(r.get("identity_pubkey", ""))
            # "Affected" means delinquent or role is validator-inactive
            if r.get("delinquent") or r.get("role") == "validator-inactive":
                clusters[key]["affected"] += 1

        incidents = []
        for (asn, city), data in clusters.items():
            if data["total"] < DC_INCIDENT_MIN_VALIDATORS:
                continue
            pct = data["affected"] / data["total"] * 100
            if pct >= DC_INCIDENT_AFFECTED_PCT:
                incidents.append({
                    "asn": asn,
                    "city": city,
                    "total": data["total"],
                    "affected": data["affected"],
                    "pct": round(pct, 1),
                })
        incidents.sort(key=lambda x: -x["affected"])
        return incidents

    # -----------------------------------------------------------------------
    # Endpoint incident tracking (for "recovered after Xh Ym" messages)
    # -----------------------------------------------------------------------

    # -- Endpoint health state (state_based alert mode) --------------------
    # Three states absorb short flaps without spam:
    #   healthy  - uptime >= threshold_healthy% over last N min
    #   degraded - threshold_down% <= uptime < threshold_healthy%
    #   down     - uptime < threshold_down%
    #   unknown  - not enough samples yet (< min_data_points)
    # Alerts fire only on state TRANSITIONS, not on every cycle. A 1-min
    # blip in a 30-min window only shifts uptime by ~3.3%, so a normally
    # 95%+ endpoint stays "healthy" through brief blips.

    def _compute_endpoint_health(self, c, cluster: str, name: str,
                                  settings: dict) -> tuple:
        """Compute health state for one endpoint over the configured window.
        Returns (state, uptime_pct_or_None, samples_in_window)."""
        window_min = int(settings["health_window_minutes"])
        healthy_t = float(settings["threshold_healthy"])
        down_t = float(settings["threshold_down"])
        min_points = int(settings["min_data_points"])

        since = (datetime.now(timezone.utc) - timedelta(minutes=window_min)).isoformat()
        rows = c.execute(
            "SELECT reachable FROM endpoint_status "
            "WHERE cluster=? AND name=? AND ts >= ?",
            (cluster, name, since),
        ).fetchall()

        samples = len(rows)
        if samples < min_points:
            return ("unknown", None, samples)

        # Treat NULL reachable as not-reachable for health math (BAM endpoints
        # with reachable=NULL are "0 connected validators", which we count as
        # not-fully-up for health purposes).
        up_count = sum(1 for r in rows if r[0] == 1)
        uptime_pct = (up_count / samples) * 100

        if uptime_pct >= healthy_t:
            state = "healthy"
        elif uptime_pct < down_t:
            state = "down"
        else:
            state = "degraded"
        return (state, round(uptime_pct, 1), samples)

    def detect_endpoint_health_changes(self, cluster: str,
                                        config: dict) -> list:
        """For every endpoint currently tracked in this cluster, compute its
        new health state, compare with the previously stored state, and
        return the list of state TRANSITIONS as dicts:
          {name, label, provider, service, ip,
           old_state, new_state, uptime_pct, samples,
           prev_changed_ts, downtime_seconds (only when new=healthy)}

        Side effect: persists the new state to endpoint_health_state.

        Skips alerting transitions where old=='unknown' (we don't know if
        the endpoint was ever healthy) — they update state silently.
        """
        settings = get_endpoint_alert_settings(config)
        ts = now_iso()
        transitions = []

        with self._conn() as c:
            # All endpoints we currently know about
            current = c.execute(
                "SELECT name, label, provider, service, ip "
                "FROM current_endpoints WHERE cluster=?",
                (cluster,),
            ).fetchall()

            # Previous health states (may be empty on first run)
            prev_rows = c.execute(
                "SELECT name, state, last_changed_ts "
                "FROM endpoint_health_state WHERE cluster=?",
                (cluster,),
            ).fetchall()
            prev_states = {r[0]: {"state": r[1], "last_changed_ts": r[2]}
                           for r in prev_rows}

            for name, label, provider, service, ip in current:
                new_state, uptime_pct, samples = self._compute_endpoint_health(
                    c, cluster, name, settings,
                )
                prev = prev_states.get(name)
                old_state = prev["state"] if prev else "unknown"

                # State unchanged: just refresh last_evaluated_ts
                if new_state == old_state:
                    if prev:
                        c.execute(
                            "UPDATE endpoint_health_state "
                            "SET uptime_pct=?, samples_in_window=?, last_evaluated_ts=? "
                            "WHERE cluster=? AND name=?",
                            (uptime_pct, samples, ts, cluster, name),
                        )
                    else:
                        # First time we see this endpoint at all
                        c.execute(
                            "INSERT INTO endpoint_health_state "
                            "(cluster, name, state, uptime_pct, samples_in_window, "
                            "last_changed_ts, last_evaluated_ts) "
                            "VALUES (?,?,?,?,?,?,?)",
                            (cluster, name, new_state, uptime_pct, samples, ts, ts),
                        )
                    continue

                # new_state == "unknown" with a known old_state means the
                # rolling window doesn't yet have min_data_points samples.
                # This is NOT a real transition — it happens whenever the
                # service restarts (samples were in process memory, not in
                # the SQLite endpoint_status — wait, they ARE in SQLite, so
                # actually this happens when the window slides and old
                # samples age out faster than new ones come in, which after
                # a restart is normal for ~min_data_points cycles).
                #
                # We must NOT:
                #   - alert on this transition (telegram noise)
                #   - overwrite the prior state in the DB (loss of history)
                #   - reset last_changed_ts (would falsify "down for X min"
                #     duration calculations on the next real recovery)
                #
                # We just bump last_evaluated_ts so we know we saw this
                # endpoint this cycle. Once samples accumulate past the
                # threshold, _compute_endpoint_health will return a real
                # state and the proper transition (or no change) is emitted.
                if new_state == "unknown":
                    if prev:
                        c.execute(
                            "UPDATE endpoint_health_state "
                            "SET last_evaluated_ts=?, samples_in_window=? "
                            "WHERE cluster=? AND name=?",
                            (ts, samples, cluster, name),
                        )
                        logger.info(
                            f"  [{cluster}] endpoint {name}: holding {old_state} "
                            f"(window has only {samples} samples, need "
                            f"{settings['min_data_points']}) — silent"
                        )
                    else:
                        # Truly new endpoint, not enough data yet — seed.
                        c.execute(
                            "INSERT INTO endpoint_health_state "
                            "(cluster, name, state, uptime_pct, samples_in_window, "
                            "last_changed_ts, last_evaluated_ts) "
                            "VALUES (?,?,?,?,?,?,?)",
                            (cluster, name, "unknown", None, samples, ts, ts),
                        )
                    continue

                # State changed: persist and (maybe) emit transition
                c.execute(
                    "INSERT OR REPLACE INTO endpoint_health_state "
                    "(cluster, name, state, uptime_pct, samples_in_window, "
                    "last_changed_ts, last_evaluated_ts) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (cluster, name, new_state, uptime_pct, samples, ts, ts),
                )

                # Alerting rule: skip if we're transitioning from 'unknown'.
                # We don't know if the endpoint was ever healthy, so an
                # initial 'down' could just be "we joined while it was down".
                if old_state == "unknown":
                    logger.info(
                        f"  [{cluster}] endpoint {name}: unknown -> {new_state} "
                        f"(uptime {uptime_pct}%, {samples} samples) — silent"
                    )
                    continue

                # Compute downtime when fully recovering. We measure as the
                # time since the LAST state change away from 'healthy'.
                # That's not exact "outage duration" — it's "time spent in
                # non-healthy states" — but it's the right metric here because
                # state-based alerting batches flaps together.
                downtime_seconds = None
                if new_state == "healthy" and prev and prev.get("last_changed_ts"):
                    try:
                        last_changed = datetime.fromisoformat(
                            prev["last_changed_ts"].replace("Z", "+00:00")
                        )
                        now = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                        downtime_seconds = int((now - last_changed).total_seconds())
                    except Exception:
                        pass

                transitions.append({
                    "name": name,
                    "label": label,
                    "provider": provider,
                    "service": service,
                    "ip": ip,
                    "old_state": old_state,
                    "new_state": new_state,
                    "uptime_pct": uptime_pct,
                    "samples": samples,
                    "downtime_seconds": downtime_seconds,
                    "ts": ts,
                })
                logger.info(
                    f"  [{cluster}] endpoint {name}: {old_state} -> {new_state} "
                    f"(uptime {uptime_pct}%, {samples} samples)"
                )

            # Cleanup: remove health rows for endpoints no longer in
            # current_endpoints (they exceeded DISAPPEARED_TTL upstream).
            current_names = {r[0] for r in current}
            stale_rows = c.execute(
                "SELECT name FROM endpoint_health_state WHERE cluster=?",
                (cluster,),
            ).fetchall()
            for (n,) in stale_rows:
                if n not in current_names:
                    c.execute(
                        "DELETE FROM endpoint_health_state WHERE cluster=? AND name=?",
                        (cluster, n),
                    )

        return transitions

    def get_endpoint_downtime(self, cluster: str, name: str) -> dict:
        """
        For an endpoint that is currently reachable, find out how long it
        was unreachable before recovery. Looks at the most recent
        'unreachable' streak in endpoint_status.

        Returns {since: ts, until: ts, duration_seconds: N} or None if no
        recent outage found.
        """
        with self._conn() as c:
            # Query endpoint_status by unique name (not label, which can collide).
            rows = c.execute("""
                SELECT ts, reachable FROM endpoint_status
                WHERE cluster=? AND name=?
                ORDER BY ts DESC LIMIT 2000
            """, (cluster, name)).fetchall()

        if not rows:
            return None

        # rows are newest first. Find where the stream is reachable=1 now,
        # then find the 0-run immediately before the current 1-run.
        first_reachable_ts = None
        outage_end_ts = None
        outage_start_ts = None
        state = "looking_for_recovery"
        for ts_v, reach in rows:
            if state == "looking_for_recovery":
                if reach == 1:
                    first_reachable_ts = ts_v
                    state = "looking_for_outage_end"
                else:
                    # Already unreachable at top — no recent recovery
                    return None
            elif state == "looking_for_outage_end":
                if reach == 0:
                    outage_end_ts = first_reachable_ts
                    outage_start_ts = ts_v
                    state = "looking_for_outage_start"
                else:
                    first_reachable_ts = ts_v
            elif state == "looking_for_outage_start":
                if reach == 0:
                    outage_start_ts = ts_v
                else:
                    break
        if outage_start_ts and outage_end_ts:
            try:
                start = datetime.fromisoformat(outage_start_ts.replace("Z", "+00:00"))
                end = datetime.fromisoformat(outage_end_ts.replace("Z", "+00:00"))
                return {
                    "since": outage_start_ts,
                    "until": outage_end_ts,
                    "duration_seconds": int((end - start).total_seconds()),
                }
            except Exception:
                return None
        return None

    # -----------------------------------------------------------------------
    # Epoch summary builders (for telegram messages and tweet drafts)
    # -----------------------------------------------------------------------

    def build_epoch_summary(self, cluster: str, epoch: int) -> dict:
        """
        Build a rich dict for the epoch summary message. Compares the
        just-recorded epoch_snapshot[epoch] against the previous one.

        Returns a dict with:
          - basic epoch info
          - validator delta (new, left, country moves, ASN moves)
          - DZ/BAM adoption deltas
          - concentration numbers
          - notable facts
          - any open incidents this epoch
        """
        cur = self.get_epoch_snapshot(cluster, epoch)
        if not cur:
            return {"error": f"no snapshot for {cluster} epoch {epoch}"}
        prev = self.get_previous_epoch_snapshot(cluster, epoch)

        # Validator set delta
        cur_ids = {v["id"]: v for v in cur["validator_locations"]}
        prev_ids = {v["id"]: v for v in prev["validator_locations"]} if prev else {}

        new_validators = [cur_ids[i] for i in cur_ids if i not in prev_ids]
        left_validators = [prev_ids[i] for i in prev_ids if i not in cur_ids]

        country_moves = []
        asn_moves = []
        for vid, cur_v in cur_ids.items():
            pv = prev_ids.get(vid)
            if not pv:
                continue
            if pv.get("country") and cur_v.get("country") and pv["country"] != cur_v["country"]:
                country_moves.append({
                    "id": vid, "from": pv["country"], "to": cur_v["country"]
                })
            if pv.get("asn") and cur_v.get("asn") and pv["asn"] != cur_v["asn"]:
                asn_moves.append({
                    "id": vid, "from": pv["asn"], "to": cur_v["asn"]
                })

        # Infrastructure deltas
        def _delta(field_path):
            cur_val = cur
            prev_val = prev or {}
            for p in field_path:
                cur_val = (cur_val or {}).get(p) if isinstance(cur_val, dict) else None
                prev_val = (prev_val or {}).get(p) if isinstance(prev_val, dict) else None
            if cur_val is None:
                return None
            if prev_val is None:
                return {"now": cur_val, "delta": None}
            try:
                return {"now": cur_val, "delta": round(cur_val - prev_val, 2)}
            except Exception:
                return {"now": cur_val, "delta": None}

        # Top ASN concentration
        top3_asn_stake = sum(a["stake_pct"] for a in cur["top_asns"][:3])

        # New BAM regions / DZ metros
        new_bam_regions = []
        new_dz_metros = []
        if prev:
            new_bam_regions = sorted(set(cur["bam"]["regions"]) - set(prev["bam"]["regions"]))
            new_dz_metros = sorted(set(cur["doublezero"]["metros"]) - set(prev["doublezero"]["metros"]))

        # Superminority change (delta only, not absolute)
        superminority_delta = None
        if prev:
            superminority_delta = cur["validators"]["superminority_count"] - prev["validators"]["superminority_count"]

        return {
            "cluster": cluster,
            "epoch": epoch,
            "previous_epoch": prev["epoch"] if prev else None,
            "timestamp": cur["timestamp"],

            "validator_set": {
                "total": cur["validators"]["total"],
                "active": cur["validators"]["active"],
                "new_count": len(new_validators),
                "left_count": len(left_validators),
                "country_moves": len(country_moves),
                "asn_moves": len(asn_moves),
                "superminority": cur["validators"]["superminority_count"],
                "superminority_delta": superminority_delta,
            },

            "infrastructure": {
                "dz_stake_pct": _delta(["doublezero", "stake_percent"]),
                "dz_validators": _delta(["doublezero", "validators"]),
                "dz_multicast_combo": (cur["doublezero"].get("connection_types") or {}).get("ibrl_and_multicast", 0),
                "bam_stake_pct": _delta(["bam", "stake_percent"]),
                "bam_validators": _delta(["bam", "validators"]),
                "rakurai_stake_pct": _delta(["rakurai", "stake_percent"]),
                "new_bam_regions": new_bam_regions,
                "new_dz_metros": new_dz_metros,
            },

            "concentration": {
                "top_asn": cur["top_asns"][0] if cur["top_asns"] else None,
                "top3_asn_stake_pct": round(top3_asn_stake, 1),
                "countries": cur["geography"]["countries"],
                "cities": cur["geography"]["cities"],
                "single_validator_cities": cur["geography"]["single_validator_cities"],
            },

            "client_distribution": cur["client_distribution"],

            "decentralization": cur["decentralization"],
            "prev_decentralization": prev["decentralization"] if prev else None,

            "health": cur["health"],
        }

    def build_tweet_draft(self, cluster: str, epoch: int) -> str:
        """
        Build an ASCII-only tweet draft (only standard keyboard chars, a few
        emoji allowed). No arrow symbols (->), no bullets (*), no dashes as
        separators. Include link to t.me for live events.

        Target length: ~500 characters (X/Twitter premium long-form).
        """
        s = self.build_epoch_summary(cluster, epoch)
        if "error" in s:
            return ""

        vs = s["validator_set"]
        infra = s["infrastructure"]
        conc = s["concentration"]
        decen = s.get("decentralization") or {}
        prev_decen = s.get("prev_decentralization") or {}
        health = s.get("health") or {}

        cluster_display = "mainnet" if cluster == "mainnet-beta" else cluster

        lines = [f"🏁 Solana {cluster_display} entered epoch {epoch}", ""]

        # Validator set — only show deltas if we have previous epoch
        if s.get("previous_epoch") is not None:
            delta_parts = []
            if vs["new_count"]:
                delta_parts.append(f"+{vs['new_count']} new")
            if vs["left_count"]:
                delta_parts.append(f"-{vs['left_count']} left")
            delta_str = f" ({', '.join(delta_parts)})" if delta_parts else ""
            lines.append(f"{vs['active']} active validators{delta_str}")
            if vs["country_moves"]:
                lines.append(f"{vs['country_moves']} validators changed country")
            if vs["asn_moves"]:
                lines.append(f"{vs['asn_moves']} migrated between datacenters")
        else:
            lines.append(f"{vs['active']} active validators")

        # Superminority change
        if vs.get("superminority_delta"):
            sign = "+" if vs["superminority_delta"] > 0 else ""
            direction = "grew" if vs["superminority_delta"] > 0 else "shrank"
            lines.append(f"Superminority {direction} to {vs['superminority']} validators ({sign}{vs['superminority_delta']})")

        lines.append("")

        # Infrastructure adoption — only show subsystems that are actually
        # present in this cluster. Devnet has no DZ/BAM/Rakurai (their counts
        # are 0), so suppress the section entirely rather than printing
        # "BAM: 0.0% of stake".
        dz_pct = infra.get("dz_stake_pct") or {}
        bam_pct = infra.get("bam_stake_pct") or {}
        rak_pct = infra.get("rakurai_stake_pct") or {}
        dz_val = infra.get("dz_validators") or {}
        bam_val = infra.get("bam_validators") or {}
        rak_val = infra.get("rakurai_validators") or {}

        if (dz_val.get("now") or 0) > 0:
            dz_line = f"🔌 DoubleZero: {dz_pct['now']:.1f}% of stake"
            if dz_pct.get("delta") is not None and abs(dz_pct["delta"]) >= 0.1:
                sign = "+" if dz_pct["delta"] > 0 else ""
                dz_line += f" ({sign}{dz_pct['delta']:.1f})"
            lines.append(dz_line)
        if infra.get("dz_multicast_combo"):
            lines.append(f"{infra['dz_multicast_combo']} validators run DZ + multicast combo")
        if (bam_val.get("now") or 0) > 0:
            bam_line = f"🎯 BAM: {bam_pct['now']:.1f}% of stake"
            if bam_pct.get("delta") is not None and abs(bam_pct["delta"]) >= 0.1:
                sign = "+" if bam_pct["delta"] > 0 else ""
                bam_line += f" ({sign}{bam_pct['delta']:.1f})"
            lines.append(bam_line)
        if (rak_val.get("now") or 0) > 0:
            rak_line = f"⚡ Rakurai: {rak_pct['now']:.1f}% of stake"
            if rak_pct.get("delta") is not None and abs(rak_pct["delta"]) >= 0.1:
                sign = "+" if rak_pct["delta"] > 0 else ""
                rak_line += f" ({sign}{rak_pct['delta']:.1f})"
            lines.append(rak_line)

        # New infrastructure
        if infra.get("new_bam_regions"):
            lines.append(f"🆕 New BAM region: {', '.join(infra['new_bam_regions'])}")
        if infra.get("new_dz_metros"):
            lines.append(f"🆕 New DZ metro: {', '.join(infra['new_dz_metros'])}")

        lines.append("")

        # Concentration
        if conc.get("top_asn"):
            lines.append(f"🏢 Infrastructure check:")
            lines.append(f"{conc['top_asn']['asn']} hosts {conc['top_asn']['stake_pct']:.0f}% of stake ({conc['top_asn']['validators']} validators)")
            lines.append(f"Top 3 ASNs: {conc['top3_asn_stake_pct']:.0f}% combined")
        if conc.get("single_validator_cities"):
            lines.append(f"{conc['single_validator_cities']} cities host just one validator")

        # Decentralization changes (only if changed between epochs)
        if prev_decen:
            decen_changes = []
            for metric in ["nakamoto_country", "nakamoto_asn", "nakamoto_city"]:
                cur_v = decen.get(metric)
                prev_v = prev_decen.get(metric)
                if cur_v is not None and prev_v is not None and cur_v != prev_v:
                    label = metric.replace("nakamoto_", "Nakamoto-").title()
                    decen_changes.append(f"{label}: {prev_v} -> {cur_v}")
            if decen_changes:
                lines.append("")
                lines.append("📊 Decentralization shifts:")
                for c in decen_changes:
                    lines.append(c)

        # Network health
        swsr = health.get("stake_weighted_skip_rate")
        if swsr is not None and swsr > 0.5:  # noteworthy only when elevated
            lines.append("")
            lines.append(f"⚠️ Stake-weighted skip rate: {swsr:.2f}%")

        lines.append("")
        lines.append("Full data: sonda.network")
        lines.append("Live events: t.me/sonda_network_events")

        return "\n".join(lines)

    # -----------------------------------------------------------------------
    # Query methods (for telegram.py and run_sonda.py)
    # -----------------------------------------------------------------------

    def get_recent_endpoint_changes(self, cluster: str, minutes: int = 5) -> list:
        """Return list of transitions for endpoints. Groups by unique 'name'
        because 'label' can collide across services (e.g. 'dublin' exists as
        both jito-ntp-dublin AND jito-bam-dublin-mainnet-bam-2-tee).

        Returns list of {name, label, provider, service, from, to, ts}.
        """
        since = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()
        with self._conn() as c:
            rows = c.execute("""
                SELECT name, label, provider, service, reachable, ts
                FROM endpoint_status
                WHERE cluster = ? AND ts >= ? AND name IS NOT NULL
                ORDER BY name, ts
            """, (cluster, since)).fetchall()

        # Detect transitions keyed by unique name
        transitions = []
        seen = {}
        for name, label, provider, service, reachable, ts in rows:
            last = seen.get(name)
            if last is not None and last != reachable:
                transitions.append({
                    "name": name,
                    "label": label,
                    "provider": provider,
                    "service": service,
                    "from": last,
                    "to": reachable,
                    "ts": ts,
                })
            seen[name] = reachable
        return transitions

    def get_recent_node_changes(self, cluster: str, minutes: int = 5, field: str = None) -> list:
        """Return recent node changes, optionally filtered by field."""
        since = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()
        query = """
            SELECT ts, identity, role, field, old_value, new_value
            FROM node_changes
            WHERE cluster = ? AND ts >= ?
        """
        params = [cluster, since]
        if field:
            query += " AND field = ?"
            params.append(field)
        query += " ORDER BY ts DESC LIMIT 500"

        with self._conn() as c:
            rows = c.execute(query, params).fetchall()

        return [
            {"ts": r[0], "identity": r[1], "role": r[2], "field": r[3],
             "old_value": json.loads(r[4]) if r[4] else None,
             "new_value": json.loads(r[5]) if r[5] else None}
            for r in rows
        ]

    def get_recent_ip_changes(self, cluster: str, minutes: int = 5) -> list:
        since = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()
        with self._conn() as c:
            rows = c.execute("""
                SELECT ts, identity, role, old_ip, new_ip, old_country, new_country, old_asn, new_asn
                FROM ip_changes
                WHERE cluster = ? AND ts >= ?
                ORDER BY ts DESC LIMIT 500
            """, (cluster, since)).fetchall()
        return [
            {"ts": r[0], "identity": r[1], "role": r[2],
             "old_ip": r[3], "new_ip": r[4],
             "old_country": r[5], "new_country": r[6],
             "old_asn": r[7], "new_asn": r[8]}
            for r in rows
        ]

    def get_new_entities(self, cluster: str, minutes: int = 5) -> list:
        since = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()
        with self._conn() as c:
            rows = c.execute("""
                SELECT ts, role, identity, ip, country_code, asn
                FROM new_entities
                WHERE cluster = ? AND ts >= ?
                ORDER BY ts DESC
            """, (cluster, since)).fetchall()
        return [
            {"ts": r[0], "role": r[1], "identity": r[2],
             "ip": r[3], "country_code": r[4], "asn": r[5]}
            for r in rows
        ]

    # -----------------------------------------------------------------------
    # Aggregate builders (R2 JSON files for frontend)
    # -----------------------------------------------------------------------

    def build_aggregates(self, cluster: str) -> dict:
        """Build JSON files for R2. Returns dict of {filename: path}."""
        out_dir = self.data_dir / "timeseries" / cluster
        out_dir.mkdir(parents=True, exist_ok=True)

        results = {}

        # 1. Endpoints recent (last 24h) — for uptime charts
        results["endpoints_recent.json"] = self._build_endpoints_recent(cluster, out_dir)

        # 2. Cluster metrics — 7 days (~10k points) and 30 days (~43k points raw, we'll thin)
        results["cluster_metrics_7d.json"] = self._build_cluster_metrics(cluster, out_dir, days=7, suffix="7d")
        results["cluster_metrics_30d.json"] = self._build_cluster_metrics(cluster, out_dir, days=30, suffix="30d")

        # 3. Validator events — last 24h
        results["validator_events_24h.json"] = self._build_validator_events(cluster, out_dir)

        return results

    def _build_endpoints_recent(self, cluster: str, out_dir: Path) -> str:
        since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        with self._conn() as c:
            rows = c.execute("""
                SELECT name, label, provider, service, ts, reachable
                FROM endpoint_status
                WHERE cluster = ? AND ts >= ? AND name IS NOT NULL
                ORDER BY name, ts
            """, (cluster, since)).fetchall()

        by_endpoint = {}
        for name, label, provider, service, ts, reachable in rows:
            if name not in by_endpoint:
                by_endpoint[name] = {
                    "name": name, "label": label,
                    "provider": provider, "service": service,
                    "ticks": [],
                }
            by_endpoint[name]["ticks"].append([ts, reachable])

        data = {
            "cluster": cluster,
            "window_hours": 24,
            "generated_at": now_iso(),
            "endpoints": list(by_endpoint.values()),
        }
        path = out_dir / "endpoints_recent.json"
        with open(path, "w") as f:
            json.dump(data, f, separators=(",", ":"))
        return str(path)

    def _build_cluster_metrics(self, cluster: str, out_dir: Path, days: int, suffix: str) -> str:
        since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        with self._conn() as c:
            rows = c.execute("""
                SELECT ts, epoch, validator_count, delinquent_count, delinquent_stake_pct,
                       dz_validator_count, dz_stake_pct, bam_validator_count, bam_stake_pct,
                       skip_rate, nakamoto_country, nakamoto_asn, nakamoto_city, nakamoto_validator
                FROM cluster_metrics
                WHERE cluster = ? AND ts >= ?
                ORDER BY ts
            """, (cluster, since)).fetchall()

        # Thin out: for 30d keep every 10th point, for 7d keep all
        thin = 10 if days == 30 else 1
        rows = rows[::thin]

        series = []
        for r in rows:
            series.append({
                "ts": r[0], "epoch": r[1],
                "validators": r[2], "delinquent": r[3],
                "delinquent_stake_pct": r[4],
                "dz_validators": r[5], "dz_stake_pct": r[6],
                "bam_validators": r[7], "bam_stake_pct": r[8],
                "skip_rate": r[9],
                "nakamoto_country": r[10], "nakamoto_asn": r[11],
                "nakamoto_city": r[12], "nakamoto_validator": r[13],
            })

        data = {
            "cluster": cluster,
            "window_days": days,
            "point_count": len(series),
            "thinned": thin,
            "generated_at": now_iso(),
            "series": series,
        }
        path = out_dir / f"cluster_metrics_{suffix}.json"
        with open(path, "w") as f:
            json.dump(data, f, separators=(",", ":"))
        return str(path)

    def _build_validator_events(self, cluster: str, out_dir: Path) -> str:
        since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        with self._conn() as c:
            changes = c.execute("""
                SELECT ts, identity, role, field, old_value, new_value
                FROM node_changes
                WHERE cluster = ? AND ts >= ?
                ORDER BY ts DESC LIMIT 1000
            """, (cluster, since)).fetchall()

            ip_chgs = c.execute("""
                SELECT ts, identity, role, old_ip, new_ip, old_country, new_country
                FROM ip_changes
                WHERE cluster = ? AND ts >= ?
                ORDER BY ts DESC LIMIT 500
            """, (cluster, since)).fetchall()

            new_ents = c.execute("""
                SELECT ts, role, identity, country_code, asn
                FROM new_entities
                WHERE cluster = ? AND ts >= ?
                ORDER BY ts DESC LIMIT 500
            """, (cluster, since)).fetchall()

        data = {
            "cluster": cluster,
            "window_hours": 24,
            "generated_at": now_iso(),
            "field_changes": [
                {"ts": r[0], "identity": r[1], "role": r[2], "field": r[3],
                 "old": json.loads(r[4]) if r[4] else None,
                 "new": json.loads(r[5]) if r[5] else None}
                for r in changes
            ],
            "ip_changes": [
                {"ts": r[0], "identity": r[1], "role": r[2],
                 "old_ip": r[3], "new_ip": r[4],
                 "old_country": r[5], "new_country": r[6]}
                for r in ip_chgs
            ],
            "new_entities": [
                {"ts": r[0], "role": r[1], "identity": r[2],
                 "country": r[3], "asn": r[4]}
                for r in new_ents
            ],
        }
        path = out_dir / "validator_events_24h.json"
        with open(path, "w") as f:
            json.dump(data, f, separators=(",", ":"))
        return str(path)

    # -----------------------------------------------------------------------
    # Backup and retention
    # -----------------------------------------------------------------------

    def backup_db(self) -> str:
        """Create a gzipped backup of the DB. Returns path."""
        backup_dir = self.data_dir / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        backup_path = backup_dir / f"timeseries-{today}.db.gz"

        with self._conn() as c:
            # Ensure consistent backup via SQLite backup API
            tmp_db = backup_dir / f"timeseries-{today}.db.tmp"
            bck = sqlite3.connect(str(tmp_db))
            c.backup(bck)
            bck.close()

        with open(tmp_db, "rb") as src, gzip.open(backup_path, "wb") as dst:
            shutil.copyfileobj(src, dst)
        tmp_db.unlink()

        size_mb = backup_path.stat().st_size / 1024 / 1024
        logger.info(f"💾 Backup: {backup_path.name} ({size_mb:.1f}MB)")
        return str(backup_path)

    def prune_old_data(self, days_keep: int = 90) -> dict:
        """Delete rows older than N days. Returns counts of deleted rows."""
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days_keep)).isoformat()
        stats = {}
        with self._conn() as c:
            for table in ["node_changes", "ip_changes", "endpoint_status",
                          "cluster_metrics", "new_entities", "asn_metrics"]:
                cur = c.execute(f"DELETE FROM {table} WHERE ts < ?", (cutoff,))
                stats[table] = cur.rowcount
            c.commit()
            c.execute("VACUUM")
        return stats

    def db_stats(self) -> dict:
        """Return table sizes and row counts."""
        stats = {"db_size_mb": round(self.db_path.stat().st_size / 1024 / 1024, 2)}
        with self._conn() as c:
            for table in ["node_changes", "ip_changes", "endpoint_status",
                          "cluster_metrics", "new_entities", "asn_metrics",
                          "current_state", "current_endpoints",
                          "epoch_snapshots", "bam_regions_seen", "milestones_reached"]:
                stats[table] = c.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="SONDA time series DB")
    parser.add_argument("--config", required=True, help="Path to config.yaml")
    parser.add_argument("--cluster", default="mainnet-beta")
    parser.add_argument("--init", action="store_true", help="Initialize empty DB")
    parser.add_argument("--record", help="Path to snapshot JSON to record")
    parser.add_argument("--record-epoch", help="Path to snapshot JSON — record as epoch snapshot")
    parser.add_argument("--aggregate", action="store_true", help="Build R2 aggregate files")
    parser.add_argument("--backup", action="store_true", help="Gzipped DB backup")
    parser.add_argument("--prune", type=int, help="Prune data older than N days")
    parser.add_argument("--stats", action="store_true", help="Show DB statistics")
    parser.add_argument("--check-milestones", help="Snapshot JSON path to check milestones against")
    parser.add_argument("--detect-dc-incidents", help="Snapshot JSON path to scan for DC incidents")
    parser.add_argument("--epoch-summary", type=int, help="Build epoch summary for given epoch")
    parser.add_argument("--tweet-draft", type=int, help="Build tweet draft for given epoch")
    args = parser.parse_args()

    cfg = load_config(args.config)
    ts = TimeSeries(cfg)

    if args.init:
        logger.info(f"✅ DB initialized: {ts.db_path}")

    if args.record:
        with open(args.record) as f:
            snap = json.load(f)
        stats = ts.record_snapshot(args.cluster, snap)
        logger.info(f"📊 Recorded: {stats}")

    if args.record_epoch:
        with open(args.record_epoch) as f:
            snap = json.load(f)
        result = ts.record_epoch_snapshot(args.cluster, snap)
        logger.info(f"📸 Epoch snapshot: epoch={result.get('epoch')}, "
                    f"validators={result.get('validators', {}).get('total')}")

    if args.check_milestones:
        with open(args.check_milestones) as f:
            snap = json.load(f)
        events = ts.check_milestones(args.cluster, snap)
        print(json.dumps(events, indent=2))

    if args.detect_dc_incidents:
        with open(args.detect_dc_incidents) as f:
            snap = json.load(f)
        incidents = ts.detect_dc_incidents(args.cluster, snap)
        print(json.dumps(incidents, indent=2))

    if args.epoch_summary is not None:
        summary = ts.build_epoch_summary(args.cluster, args.epoch_summary)
        print(json.dumps(summary, indent=2, default=str))

    if args.tweet_draft is not None:
        draft = ts.build_tweet_draft(args.cluster, args.tweet_draft)
        print(draft)

    if args.aggregate:
        results = ts.build_aggregates(args.cluster)
        for name, path in results.items():
            size_kb = Path(path).stat().st_size // 1024
            logger.info(f"  ✅ {name}: {size_kb}KB")

    if args.backup:
        ts.backup_db()

    if args.prune is not None:
        stats = ts.prune_old_data(args.prune)
        logger.info(f"🗑️  Pruned: {stats}")

    if args.stats:
        stats = ts.db_stats()
        print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()