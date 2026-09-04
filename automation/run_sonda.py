#!/usr/bin/env python3
"""
run_sonda.py
============
Main SONDA orchestrator. Runs three cluster workers (mainnet-beta, testnet,
devnet) in parallel threads, each executing its own analyze → split → upload →
record → telegram pipeline on its own interval.

The main thread owns periodic maintenance:
  - Daily summary via debug bot at 09:00 UTC
  - SQLite backup daily at 03:00 UTC (local + R2 upload)
  - Database pruning weekly (Sunday 04:00 UTC)
  - SQLite WAL checkpoint every hour

Design principles:
  - Single pidfile enforcement prevents double-start.
  - Workers are independent — one cluster failing doesn't stop others.
  - Each pipeline stage has its own try/except and distinct error key so
    different failure modes alert separately (e.g. analyze vs upload).
  - Analyzer runs as subprocess with 900s timeout (covers the periodic
    full-IP-rescan cycle which is ~15-minute-long on mainnet).
  - SIGTERM/SIGINT gracefully shuts down: workers finish current stage,
    debug bot gets a shutdown message, everything exits cleanly.
  - Worker crashes (unhandled exceptions) restart with exponential backoff,
    max 5 minutes between attempts. Never give up.
  - Disk space is checked before each analyzer run; if < 500MB free, we
    skip the cycle and alert instead of trying to write a snapshot.

Usage:
  python run_sonda.py --config /path/to/config.yaml

Typical config.yaml clusters section:
  clusters:
    mainnet-beta:
      interval: 60
      enabled: true
    testnet:
      interval: 300
      enabled: true
    devnet:
      interval: 60
      enabled: true
"""

import argparse
import gzip
import json
import logging
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
import traceback
from datetime import datetime, timezone, timedelta
from pathlib import Path

import yaml

# Module logger — formatters are set in main() based on config
logger = logging.getLogger("run_sonda")

# ---------------------------------------------------------------------------
# Timeouts and limits (seconds unless noted)
# ---------------------------------------------------------------------------
ANALYZER_TIMEOUT = 900          # 15 min. Covers periodic full-IP-rescan cycles
                                # where analyzer revalidates all cached geodata.
MIN_DISK_FREE_MB = 500          # Skip cycle if less than this on sonda_data dir
SNAPSHOTS_KEEP_PER_CLUSTER = 5  # Keep last N snapshots locally (rest is in R2)
WORKER_RESTART_BACKOFF_MAX = 300  # Exponential backoff cap on worker restart
TELEGRAM_ERROR_SOFT = True      # Telegram failures never block the pipeline

# Maintenance schedules (UTC hours)
DAILY_SUMMARY_HOUR = 9          # 09:00 UTC
BACKUP_HOUR = 3                 # 03:00 UTC
PRUNE_HOUR = 4                  # 04:00 UTC Sunday
WAL_CHECKPOINT_INTERVAL = 3600  # Every 1 hour

BACKUP_RETENTION_DAYS = 7       # How long to keep local backups
PRUNE_DATA_KEEP_DAYS = 0        # 0 = never prune. SONDA is an accumulating archive (CHARTER).


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _disk_free_mb(path: Path) -> int:
    """Return free space in MB for the filesystem containing path."""
    try:
        usage = shutil.disk_usage(path)
        return usage.free // (1024 * 1024)
    except Exception as e:
        logger.warning(f"Disk usage check failed for {path}: {e}")
        return -1


def _format_elapsed(seconds: float) -> str:
    """Short human-readable elapsed time."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    m = int(seconds // 60)
    s = int(seconds % 60)
    return f"{m}m {s}s"


def _cleanup_old_snapshots(snapshots_dir: Path, cluster: str, keep: int):
    """Keep the N newest snapshot files for this cluster. Remove the rest.
    File naming matches run_once.py (sonda-{cluster_short}-*.json where
    cluster_short = 'mainnet' for 'mainnet-beta', etc.)"""
    cluster_short = cluster.replace("-beta", "")
    pattern = f"sonda-{cluster_short}-*.json"
    files = sorted(snapshots_dir.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    for old in files[keep:]:
        try:
            old.unlink()
        except Exception as e:
            logger.warning(f"[{cluster}] cleanup: couldn't remove {old}: {e}")


# ---------------------------------------------------------------------------
# PID file locking — prevent double-start
# ---------------------------------------------------------------------------

class PidFile:
    """Best-effort pidfile. Detects orphaned files (stale pids) and removes
    them. On exit, removes our own pidfile."""

    def __init__(self, path: Path):
        self.path = path
        self.acquired = False

    def acquire(self):
        if self.path.exists():
            try:
                old_pid = int(self.path.read_text().strip())
                # Check if process is still alive (signal 0 = check only)
                os.kill(old_pid, 0)
                logger.error(f"Another run_sonda.py is running (pid={old_pid}). Exiting.")
                sys.exit(2)
            except ProcessLookupError:
                logger.warning(f"Stale pidfile for pid={old_pid}, removing")
                self.path.unlink()
            except (ValueError, PermissionError) as e:
                logger.warning(f"Couldn't verify pid in {self.path}: {e}. Removing.")
                try:
                    self.path.unlink()
                except Exception:
                    pass
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(str(os.getpid()))
        self.acquired = True

    def release(self):
        if self.acquired and self.path.exists():
            try:
                # Only remove if it still has our pid
                if self.path.read_text().strip() == str(os.getpid()):
                    self.path.unlink()
            except Exception as e:
                logger.warning(f"Couldn't remove pidfile: {e}")


# ---------------------------------------------------------------------------
# Worker — runs the per-cluster pipeline
# ---------------------------------------------------------------------------

class ClusterWorker:
    """One worker per cluster. Runs the analyze → split → upload → record →
    telegram pipeline on a timer loop.

    Each pipeline stage is independently tracked for errors so we can alert
    on specific failure modes. A stage failing does not always abort the
    cycle — e.g. upload failure lets record/telegram still run on the local
    snapshot, and the next cycle will re-upload.
    """

    def __init__(self, cluster: str, cluster_cfg: dict, global_cfg: dict,
                 timeseries, notifier, shutdown_event: threading.Event):
        self.cluster = cluster
        self.cfg = cluster_cfg
        self.global_cfg = global_cfg
        self.ts = timeseries
        self.notifier = notifier
        self.shutdown = shutdown_event
        self.interval = int(cluster_cfg.get("interval", 60))

        # Paths from global config
        paths = global_cfg.get("paths") or {}
        self.sonda_scripts = Path(paths["sonda_scripts"])
        self.automation_dir = Path(paths["automation_dir"])
        self.snapshots_dir = Path(paths["snapshots_dir"])
        self.snapshots_dir.mkdir(parents=True, exist_ok=True)

        # Python executable (use the same interpreter running us)
        self.python_exe = sys.executable
        self.config_path = global_cfg["_config_path"]  # stashed for subprocess

        # Cycle metrics for daily summary
        self.metrics = {
            "cycles_ok": 0,
            "cycles_failed": 0,
            "last_cycle_duration": 0,
            "last_successful_cycle_ts": None,
        }

    # -- subprocess helpers --------------------------------------------------

    def _write_subprocess_log(self, stage: str, rc, stdout: str, stderr: str,
                              duration: float, timed_out: bool = False) -> None:
        """Write full stdout+stderr from a subprocess run to a per-cluster file.

        Only invoked for stages where deep visibility matters (currently
        analyze). The file is rotated when it exceeds 10 MB.
        """
        logs_dir = self.automation_dir / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        log_path = logs_dir / f"{stage}-{self.cluster}.log"

        # Rotate if file exceeds 10 MB (keep one .1 backup)
        try:
            if log_path.exists() and log_path.stat().st_size > 10 * 1024 * 1024:
                backup = log_path.with_suffix(".log.1")
                if backup.exists():
                    backup.unlink()
                log_path.rename(backup)
        except OSError:
            pass  # Rotation is best-effort; don't fail the cycle on it

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        rc_label = f"rc={rc}" if not timed_out else f"TIMEOUT after {duration:.0f}s"
        header = f"\n{'=' * 78}\n=== {ts} UTC | {stage} | {self.cluster} | {rc_label} | {duration:.1f}s\n{'=' * 78}\n"

        try:
            with open(log_path, "a") as f:
                f.write(header)
                if stdout:
                    f.write("--- STDOUT ---\n")
                    f.write(stdout)
                    if not stdout.endswith("\n"):
                        f.write("\n")
                if stderr:
                    f.write("--- STDERR ---\n")
                    f.write(stderr)
                    if not stderr.endswith("\n"):
                        f.write("\n")
                if not stdout and not stderr:
                    f.write("(no output)\n")
        except OSError as e:
            logger.warning(f"[{self.cluster}] failed to write {stage} log: {e}")

    def _run_subprocess(self, argv: list, timeout: int, stage: str) -> int:
        """Run a subprocess and return the return code. Raises on timeout.
        Stdout/stderr are captured so they don't pollute our logs; if the
        process failed, we log the last few lines of stderr at WARNING.

        For 'analyze' stage, full stdout+stderr is also written to a per-cluster
        file (logs/analyze-{cluster}.log) for diagnostics — including on
        timeout, where we'd otherwise lose all context.
        """
        logger.info(f"[{self.cluster}] {stage}: running {argv[1:]}")
        deep_log = (stage == "analyze")
        t0 = time.monotonic()
        try:
            result = subprocess.run(
                argv, capture_output=True, text=True, timeout=timeout,
            )
            duration = time.monotonic() - t0
            if deep_log:
                self._write_subprocess_log(
                    stage, result.returncode, result.stdout or "",
                    result.stderr or "", duration,
                )
            if result.returncode != 0:
                err_tail = (result.stderr or "").strip().split("\n")[-5:]
                logger.warning(f"[{self.cluster}] {stage} exited rc={result.returncode}: "
                               f"{' | '.join(err_tail)[:400]}")
                return result.returncode
            return 0
        except subprocess.TimeoutExpired as e:
            duration = time.monotonic() - t0
            # On timeout, e.stdout / e.stderr may contain partial output (bytes
            # if capture_output=True without text=True; with text=True they're str).
            partial_stdout = e.stdout or ""
            partial_stderr = e.stderr or ""
            if isinstance(partial_stdout, bytes):
                partial_stdout = partial_stdout.decode("utf-8", errors="replace")
            if isinstance(partial_stderr, bytes):
                partial_stderr = partial_stderr.decode("utf-8", errors="replace")
            if deep_log:
                self._write_subprocess_log(
                    stage, None, partial_stdout, partial_stderr,
                    duration, timed_out=True,
                )
            # Surface the last bit of stdout in the warning too — gives a hint
            # at where the analyzer was when it got killed.
            tail = (partial_stdout or partial_stderr or "").strip().split("\n")[-3:]
            if tail and tail != [""]:
                logger.warning(f"[{self.cluster}] {stage} timeout, last output: "
                               f"{' | '.join(tail)[:300]}")
            raise RuntimeError(f"{stage} timed out after {timeout}s")

    def _stage_analyze(self) -> Path:
        """Run analyzer, return path to produced snapshot. Raises on failure.

        Analyzer is invoked with absolute paths for --endpoints and
        --geo-overrides so cwd doesn't matter. API keys come in as CLI args
        (that's the interface analyzer expects), not env vars. The snapshot
        path is built and passed explicitly via --output so we know exactly
        which file was produced (no directory scanning afterward).
        """
        # Disk space guardrail before we start writing
        free_mb = _disk_free_mb(self.snapshots_dir)
        if 0 <= free_mb < MIN_DISK_FREE_MB:
            raise RuntimeError(f"Disk space too low: {free_mb}MB free "
                               f"(need ≥ {MIN_DISK_FREE_MB}MB)")

        # Build explicit output path matching run_once.py's convention
        # (run_once uses cluster.replace("-beta", ""), giving "mainnet" for
        # "mainnet-beta"). We keep the same format so split_snapshot.py etc.
        # see consistent naming.
        cluster_short = self.cluster.replace("-beta", "")
        ts_stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
        output_path = self.snapshots_dir / f"sonda-{cluster_short}-{ts_stamp}.json"

        analyzer = self.sonda_scripts / "solana_analyzer.py"
        endpoints = self.sonda_scripts / "endpoints.yaml"
        geo_overrides = self.sonda_scripts / "geo_overrides.yaml"

        api_keys = self.global_cfg.get("api_keys") or {}
        # v6.9: rpc_urls is an ordered list (primary first). The old single
        # rpc_url key still works so older configs need no change.
        rpc_urls = self.cfg.get("rpc_urls") or ([self.cfg["rpc_url"]] if self.cfg.get("rpc_url") else [])
        if isinstance(rpc_urls, str):
            rpc_urls = [rpc_urls]

        argv = [
            self.python_exe, str(analyzer),
            "--dbip-key", api_keys.get("dbip_key", ""),
            "--ipinfo-token", api_keys.get("ipinfo_token", ""),
            "--cluster", self.cluster,
            "--export",
            "--output", str(output_path),
        ]
        for url in rpc_urls:
            argv += ["--rpc-url", url]
        if endpoints.exists():
            argv += ["--endpoints", str(endpoints)]
        if geo_overrides.exists():
            argv += ["--geo-overrides", str(geo_overrides)]

        rc = self._run_subprocess(argv, timeout=ANALYZER_TIMEOUT, stage="analyze")
        if rc != 0:
            raise RuntimeError(f"analyzer exited with rc={rc}")

        if not output_path.exists():
            raise RuntimeError(f"analyzer produced no snapshot at {output_path}")
        # Sanity: file must be fresh (protects against races)
        age = time.time() - output_path.stat().st_mtime
        if age > ANALYZER_TIMEOUT:
            raise RuntimeError(f"newest snapshot is {int(age)}s old (stale)")
        return output_path

    def _stage_publish(self, snapshot_path: Path):
        """Split snapshot and upload to R2. Best-effort — failures are logged
        as errors but don't abort the cycle. The next cycle will re-upload
        since the R2 files are overwritten each time."""
        split_script = self.automation_dir / "split_snapshot.py"
        upload_script = self.automation_dir / "r2_upload.py"

        rc = self._run_subprocess(
            [self.python_exe, str(split_script),
             "--config", self.config_path,
             "--snapshot", str(snapshot_path)],
            timeout=60, stage="split",
        )
        if rc != 0:
            raise RuntimeError(f"split exited rc={rc}")

        rc = self._run_subprocess(
            [self.python_exe, str(upload_script),
             "--config", self.config_path,
             "--cluster", self.cluster],
            timeout=120, stage="upload",
        )
        if rc != 0:
            raise RuntimeError(f"upload exited rc={rc}")

    def _stage_record(self, snapshot_path: Path) -> dict:
        """Record snapshot to SQLite. Returns loaded snapshot dict (for
        the telegram stage, so we don't re-read the 6MB file)."""
        with open(snapshot_path) as f:
            snapshot = json.load(f)
        stats = self.ts.record_snapshot(self.cluster, snapshot)
        logger.info(f"[{self.cluster}] record: {stats}")
        return snapshot

    def _stage_telegram(self, snapshot: dict):
        """Dispatch telegram events. Failures never block the pipeline."""
        try:
            result = self.notifier.process_snapshot(self.cluster, snapshot)
            # Log only non-zero counts for brevity
            nz = {k: v for k, v in result.items() if v}
            if nz:
                logger.info(f"[{self.cluster}] telegram: {nz}")
        except Exception as e:
            # Telegram failures are soft — log and continue.
            if TELEGRAM_ERROR_SOFT:
                logger.warning(f"[{self.cluster}] telegram error: {e}")
            else:
                raise

    # -- main loop -----------------------------------------------------------

    def run(self):
        """Main worker loop. Returns only on shutdown_event."""
        logger.info(f"[{self.cluster}] worker starting (interval={self.interval}s)")

        while not self.shutdown.is_set():
            cycle_start = time.monotonic()
            snapshot_path = None
            snapshot = None

            # --- Stage 1: analyze ---
            try:
                snapshot_path = self._stage_analyze()
                self.notifier.clear_error(self.cluster, "analyze")
            except Exception as e:
                self.notifier.send_error(self.cluster, "analyze", str(e))
                self.metrics["cycles_failed"] += 1
                # Analyzer failure: wait the full interval before retry.
                # Analyzer is the critical stage; without it nothing works.
                self._sleep_remaining(cycle_start)
                continue

            # --- Stage 2: publish (split + R2) ---
            try:
                self._stage_publish(snapshot_path)
                self.notifier.clear_error(self.cluster, "publish")
            except Exception as e:
                # Soft failure: we keep going, local data is fine and R2
                # will catch up next cycle.
                self.notifier.send_error(self.cluster, "publish", str(e))
                logger.warning(f"[{self.cluster}] publish failed, continuing: {e}")

            # --- Stage 3: record to SQLite ---
            try:
                snapshot = self._stage_record(snapshot_path)
                self.notifier.clear_error(self.cluster, "record")
            except Exception as e:
                self.notifier.send_error(self.cluster, "record", str(e))
                self.metrics["cycles_failed"] += 1
                self._sleep_remaining(cycle_start)
                continue

            # --- Stage 4: telegram events ---
            self._stage_telegram(snapshot)

            # --- Stage 5: cleanup ---
            try:
                _cleanup_old_snapshots(self.snapshots_dir, self.cluster,
                                        SNAPSHOTS_KEEP_PER_CLUSTER)
            except Exception as e:
                logger.warning(f"[{self.cluster}] cleanup error: {e}")

            # --- Record metrics + sleep ---
            duration = time.monotonic() - cycle_start
            self.metrics["cycles_ok"] += 1
            self.metrics["last_cycle_duration"] = duration
            self.metrics["last_successful_cycle_ts"] = datetime.now(timezone.utc).isoformat()
            logger.info(f"[{self.cluster}] ✅ cycle complete in {_format_elapsed(duration)}")

            self._sleep_remaining(cycle_start)

        logger.info(f"[{self.cluster}] worker stopped")

    def _sleep_remaining(self, cycle_start: float):
        """Sleep until `interval` seconds since cycle_start. Wakes immediately
        on shutdown. If the cycle took longer than interval, starts next one
        right away (interval is a minimum, not an absolute rate)."""
        elapsed = time.monotonic() - cycle_start
        remaining = self.interval - elapsed
        if remaining <= 0:
            return
        # Use Event.wait so shutdown signals wake us early
        self.shutdown.wait(timeout=remaining)


# ---------------------------------------------------------------------------
# Worker supervisor — restarts on crash
# ---------------------------------------------------------------------------

def _worker_supervisor(cluster: str, build_worker, shutdown: threading.Event):
    """Run a worker inside a loop that catches uncaught exceptions and
    restarts with exponential backoff. Only exits when shutdown is set."""
    backoff = 5
    while not shutdown.is_set():
        try:
            worker = build_worker()
            worker.run()  # returns only on shutdown
            break
        except Exception:
            tb = traceback.format_exc()
            logger.error(f"[{cluster}] worker crashed:\n{tb}")
            # Sleep with exponential backoff, max 5 min
            logger.info(f"[{cluster}] restarting in {backoff}s")
            shutdown.wait(timeout=backoff)
            backoff = min(backoff * 2, WORKER_RESTART_BACKOFF_MAX)


# ---------------------------------------------------------------------------
# Maintenance tasks (run on main thread)
# ---------------------------------------------------------------------------

class Maintenance:
    """Periodic housekeeping run from the main thread. These tasks aren't
    critical — if they fail, the workers keep going.

    State (last_daily_date, last_backup_date, last_prune_date) is persisted
    to logs/maintenance_state.json so restarts don't re-trigger tasks that
    already ran today.
    """

    STATE_FILENAME = "maintenance_state.json"

    def __init__(self, config: dict, timeseries, notifier,
                 shutdown_event: threading.Event, workers: dict):
        self.config = config
        self.ts = timeseries
        self.notifier = notifier
        self.shutdown = shutdown_event
        self.workers = workers

        paths = config.get("paths") or {}
        logs_dir = Path(paths.get("logs_dir", "/tmp"))
        logs_dir.mkdir(parents=True, exist_ok=True)
        self.state_path = logs_dir / self.STATE_FILENAME
        self.state = self._load_state()

        self.last_wal_checkpoint = 0

        self.backups_dir = Path(paths.get("backups_dir",
                                          Path(paths["data_dir"]) / "backups"))
        self.backups_dir.mkdir(parents=True, exist_ok=True)

    def _load_state(self) -> dict:
        if self.state_path.exists():
            try:
                return json.loads(self.state_path.read_text())
            except Exception:
                pass
        # Fresh install / no state file: seed task dates to today for any
        # task whose trigger time has already passed today. This prevents
        # immediate self-trigger on first start (e.g. starting at 07:13 UTC
        # would otherwise immediately fire the 03:00 UTC backup, and starting
        # mid-morning would fire a bogus daily summary). The next real run
        # of each task will happen tomorrow.
        now = datetime.now(timezone.utc)
        today = now.strftime("%Y-%m-%d")
        is_sunday = now.weekday() == 6
        return {
            "last_daily_date": today if now.hour >= DAILY_SUMMARY_HOUR else None,
            "last_backup_date": today if now.hour >= BACKUP_HOUR else None,
            "last_prune_date": today if (is_sunday and now.hour >= PRUNE_HOUR) else None,
        }

    def _save_state(self):
        try:
            self.state_path.write_text(json.dumps(self.state, indent=2))
        except Exception as e:
            logger.warning(f"Failed to save maintenance state: {e}")

    def tick(self):
        """Called once per main-loop iteration. Decides what (if anything) to do."""
        now = datetime.now(timezone.utc)
        today = now.strftime("%Y-%m-%d")

        # Daily summary at DAILY_SUMMARY_HOUR:00 UTC
        if (now.hour >= DAILY_SUMMARY_HOUR
                and self.state.get("last_daily_date") != today):
            try:
                self.notifier.send_daily_summary()
                self.state["last_daily_date"] = today
                self._save_state()
                logger.info("📊 daily summary sent")
            except Exception as e:
                logger.warning(f"daily summary failed: {e}")

        # SQLite backup at BACKUP_HOUR:00 UTC
        if (now.hour >= BACKUP_HOUR
                and self.state.get("last_backup_date") != today):
            try:
                self._do_backup()
                self.state["last_backup_date"] = today
                self._save_state()
            except Exception as e:
                logger.warning(f"backup failed: {e}")
                try:
                    self.notifier.send_data_quality_warning(
                        "maintenance", f"SQLite backup failed: {str(e)[:200]}"
                    )
                except Exception:
                    pass

        # Weekly prune on Sunday PRUNE_HOUR:00 UTC. Disabled when
        # PRUNE_DATA_KEEP_DAYS is 0 (v6.9): the 90-day prune deleted a month
        # of node_changes/ip_changes/new_entities before it was noticed.
        if (PRUNE_DATA_KEEP_DAYS > 0 and now.weekday() == 6 and now.hour >= PRUNE_HOUR
                and self.state.get("last_prune_date") != today):
            try:
                stats = self.ts.prune_old_data(PRUNE_DATA_KEEP_DAYS)
                logger.info(f"🗑  pruned old data: {stats}")
                self.state["last_prune_date"] = today
                self._save_state()
            except Exception as e:
                logger.warning(f"prune failed: {e}")

        # Hourly WAL checkpoint (prevent WAL file from growing indefinitely)
        if time.monotonic() - self.last_wal_checkpoint > WAL_CHECKPOINT_INTERVAL:
            try:
                with self.ts._conn() as c:
                    c.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                self.last_wal_checkpoint = time.monotonic()
            except Exception as e:
                logger.debug(f"WAL checkpoint: {e}")

    def _do_backup(self):
        """Create a gzipped SQLite backup and try to upload to R2."""
        # Use timeseries' own backup_db to get a consistent copy
        try:
            # Prefer the method built into timeseries if available
            backup_path = self.ts.backup_db()
        except Exception:
            # Fallback: gzip-copy the DB file directly
            db_src = self.ts.db_path
            ts_stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            backup_path = self.backups_dir / f"timeseries-{ts_stamp}.db.gz"
            with open(db_src, "rb") as src, gzip.open(backup_path, "wb") as dst:
                shutil.copyfileobj(src, dst)

        logger.info(f"💾 backup saved: {backup_path}")

        # Retention: remove backups older than BACKUP_RETENTION_DAYS
        cutoff = datetime.now(timezone.utc) - timedelta(days=BACKUP_RETENTION_DAYS)
        for old in self.backups_dir.glob("timeseries-*.db.gz"):
            try:
                mtime = datetime.fromtimestamp(old.stat().st_mtime, tz=timezone.utc)
                if mtime < cutoff:
                    old.unlink()
            except Exception:
                pass

        # Best-effort R2 upload of the backup (non-critical). The upload uses
        # the r2_upload.py helper if present; otherwise we skip silently and
        # rely on local copy alone.
        try:
            paths = self.config.get("paths") or {}
            upload_script = Path(paths["automation_dir"]) / "r2_upload.py"
            if upload_script.exists():
                key = f"backups/{Path(backup_path).name}"
                subprocess.run(
                    [sys.executable, str(upload_script),
                     "--config", self.config["_config_path"],
                     "--file", str(backup_path),
                     "--key", key],
                    capture_output=True, timeout=120, text=True,
                )
                logger.info(f"☁️  backup uploaded to R2: {key}")
        except Exception as e:
            logger.debug(f"backup R2 upload skipped: {e}")


# ---------------------------------------------------------------------------
# Main entrypoint
# ---------------------------------------------------------------------------

def setup_logging(config: dict):
    """Log to stdout (for systemd journalctl) + optional file."""
    fmt = "%(asctime)s %(levelname)s %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()

    # stdout handler (for journald)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(logging.Formatter(fmt, datefmt))
    root.addHandler(sh)

    # Optional file handler
    paths = config.get("paths") or {}
    logs_dir = paths.get("logs_dir")
    if logs_dir:
        log_path = Path(logs_dir) / "run_sonda.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_path)
        fh.setFormatter(logging.Formatter(fmt, datefmt))
        root.addHandler(fh)


def main():
    parser = argparse.ArgumentParser(description="SONDA main orchestrator")
    parser.add_argument("--config", required=True, help="Path to config.yaml")
    args = parser.parse_args()

    config_path = Path(args.config).resolve()
    if not config_path.exists():
        print(f"Config not found: {config_path}", file=sys.stderr)
        sys.exit(2)

    with open(config_path) as f:
        config = yaml.safe_load(f)
    # Stash path for subprocess calls
    config["_config_path"] = str(config_path)

    setup_logging(config)

    # --- Pidfile locking ---
    paths = config.get("paths") or {}
    logs_dir = Path(paths.get("logs_dir",
                              Path(paths.get("automation_dir", "/tmp")) / "logs"))
    pidfile = PidFile(logs_dir / "run_sonda.pid")
    pidfile.acquire()

    # --- Load timeseries and notifier ---
    sys.path.insert(0, str(Path(paths["automation_dir"])))
    from timeseries import TimeSeries
    from telegram import Notifier

    ts = TimeSeries(config)
    notifier = Notifier(config, ts)

    # --- Shutdown event shared across all threads ---
    shutdown = threading.Event()

    def handle_signal(signum, frame):
        logger.info(f"received signal {signum}, shutting down")
        shutdown.set()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    # --- Startup announcement ---
    try:
        notifier.send_startup_message()
    except Exception as e:
        logger.warning(f"startup message failed: {e}")

    # --- Build workers ---
    clusters_cfg = config.get("clusters") or {}
    threads = []
    workers = {}

    for cluster, ccfg in clusters_cfg.items():
        if not ccfg.get("enabled", True):
            logger.info(f"[{cluster}] skipped (disabled in config)")
            continue

        # Build factory so supervisor can re-instantiate worker on crash
        def make_worker(c=cluster, cc=ccfg):
            return ClusterWorker(c, cc, config, ts, notifier, shutdown)

        t = threading.Thread(
            target=_worker_supervisor,
            args=(cluster, make_worker, shutdown),
            name=f"worker-{cluster}",
            daemon=True,
        )
        threads.append(t)
        workers[cluster] = make_worker
        t.start()

    if not threads:
        logger.error("No clusters enabled, exiting")
        pidfile.release()
        sys.exit(1)

    # --- Main loop: maintenance + supervision ---
    maintenance = Maintenance(config, ts, notifier, shutdown, workers)
    logger.info(f"🚀 SONDA running with {len(threads)} workers")

    try:
        while not shutdown.is_set():
            maintenance.tick()
            # Wake every 30s to check schedules + shutdown
            shutdown.wait(timeout=30)
    except Exception:
        logger.error(f"main loop crashed:\n{traceback.format_exc()}")
        shutdown.set()

    # --- Graceful shutdown ---
    logger.info("waiting for workers to finish current cycle...")
    # Give workers up to 180s to wrap up (analyzer may still be running)
    join_deadline = time.monotonic() + 180
    for t in threads:
        remaining = max(0, join_deadline - time.monotonic())
        t.join(timeout=remaining)
        if t.is_alive():
            logger.warning(f"worker {t.name} didn't stop in time")

    try:
        notifier.send_shutdown_message("graceful stop (SIGTERM/SIGINT)")
    except Exception:
        pass

    pidfile.release()
    logger.info("👋 SONDA stopped")


if __name__ == "__main__":
    main()