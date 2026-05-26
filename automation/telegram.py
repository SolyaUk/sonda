#!/usr/bin/env python3
"""
telegram.py
===========
SONDA Telegram bots — debug (private) and public events channel.

Design philosophy:
- Event detection lives in timeseries.py (diffs against current_state).
  telegram.py consumes already-detected events and decides what gets sent
  to which bot.
- First cycle after startup emits NO alerts. The DB is populating and
  everything would look "new". We write a flag to mark startup done after
  the first record, and from the second cycle onward alerts flow normally.
- Startup message is pinned in debug bot (user wants to keep all pinned
  messages for navigation history — we do NOT unpin old ones).
- Tweet drafts use ASCII + a few emoji, no arrow symbols, no bullets.
- Public bot filter: telegram.public_clusters in config decides which
  clusters reach the public channel. Debug bot sees all clusters.

Bot structure:
  Bot 1 (debug, private chat):
    - Startup (pinned): version, host uptime, clusters, intervals
    - Errors: after 3+ consecutive failures per cluster
    - Data quality warnings: when we know we lost/degraded data
    - Daily summary: 9:00 UTC, aggregate health per cluster
    - Tweet draft: after each epoch change, ready to copy-paste

  Bot 2 (public channel "SONDA Network Events"):
    - Category A (incidents, immediate):
        endpoint down (>=2 missing_cycles) / recovered (with duration)
        DC mass outage (ASN+city >=70% affected, min 5 validators)
        BAM region disappeared / new region
        cluster-level spike (skip rate, delinquent stake)
        Solana public RPC unreachable (3+ cycles)
    - Category B (milestones, on threshold crossing):
        DZ/BAM/Rakurai/Firedancer stake % thresholds (up or down)
        superminority size changes (any delta)
    - Category C (epoch summaries):
        rich message with validator deltas, infra shifts, concentration

Usage as library:
  from telegram import Notifier
  n = Notifier(config, ts)
  n.process_snapshot(cluster, snapshot)  # sends all triggered events
  n.send_startup_message()
  n.send_error(cluster, stage, consecutive_failures, error_msg)

Usage CLI (for testing):
  python telegram.py --config config.yaml --test         # sends startup + test msg
  python telegram.py --config config.yaml --startup      # sends startup message only
  python telegram.py --config config.yaml --daily-summary
  python telegram.py --config config.yaml --snapshot /path/to.json --cluster mainnet-beta
"""

import argparse
import json
import logging
import os
import subprocess
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

TELEGRAM_API = "https://api.telegram.org"
SEND_TIMEOUT = 15
MAX_MESSAGE_LEN = 4096  # Telegram hard limit per message


# ---------------------------------------------------------------------------
# Low-level bot wrapper
# ---------------------------------------------------------------------------

class TelegramBot:
    """Thin wrapper over Telegram Bot API with markdown and pin support."""

    def __init__(self, token: str, chat_id, label: str = ""):
        self.token = token
        self.chat_id = chat_id
        self.label = label  # "debug" or "public" for logs

    def send(self, text: str, parse_mode: str = "Markdown",
             disable_preview: bool = True, silent: bool = False) -> dict:
        """Send a message. Returns response dict (includes message_id on success)."""
        if not self.token or not self.chat_id:
            logger.warning(f"[{self.label}] No token/chat_id configured, skipping send")
            return {}

        # Chunk long messages (rare but possible for summaries)
        if len(text) > MAX_MESSAGE_LEN:
            logger.warning(f"[{self.label}] Message too long ({len(text)} chars), truncating")
            text = text[:MAX_MESSAGE_LEN - 20] + "\n...(truncated)"

        try:
            r = requests.post(
                f"{TELEGRAM_API}/bot{self.token}/sendMessage",
                json={
                    "chat_id": self.chat_id,
                    "text": text,
                    "parse_mode": parse_mode,
                    "disable_web_page_preview": disable_preview,
                    "disable_notification": silent,
                },
                timeout=SEND_TIMEOUT,
            )
            data = r.json() if r.status_code == 200 else {}
            if not data.get("ok"):
                logger.warning(f"[{self.label}] Telegram send failed: "
                               f"{r.status_code} {r.text[:200]}")
            return data.get("result", {})
        except Exception as e:
            logger.warning(f"[{self.label}] Telegram send error: {e}")
            return {}

    def pin(self, message_id: int, disable_notification: bool = True) -> bool:
        """Pin a message. Does NOT unpin previous pins (user preference)."""
        if not message_id:
            return False
        try:
            r = requests.post(
                f"{TELEGRAM_API}/bot{self.token}/pinChatMessage",
                json={
                    "chat_id": self.chat_id,
                    "message_id": message_id,
                    "disable_notification": disable_notification,
                },
                timeout=SEND_TIMEOUT,
            )
            return r.status_code == 200 and r.json().get("ok", False)
        except Exception as e:
            logger.warning(f"[{self.label}] Telegram pin error: {e}")
            return False


# ---------------------------------------------------------------------------
# Formatting helpers — keep messages consistent and readable
# ---------------------------------------------------------------------------

def _esc(text: str) -> str:
    """Escape markdown special chars that break Telegram rendering."""
    if text is None:
        return ""
    text = str(text)
    # Markdown v1 only cares about _ * ` [
    for ch in ("_", "*", "`", "["):
        text = text.replace(ch, f"\\{ch}")
    return text


def _cluster_tag(cluster: str) -> str:
    """Short cluster label for message prefixes."""
    if cluster == "mainnet-beta":
        return "mainnet"
    return cluster


def _fmt_duration(seconds: int) -> str:
    """Human-readable duration: '2h 15m' or '47m' or '4d 3h'."""
    if seconds is None:
        return "?"
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    m = s // 60
    if m < 60:
        return f"{m}m"
    h = m // 60
    m = m % 60
    if h < 24:
        return f"{h}h {m}m" if m else f"{h}h"
    d = h // 24
    h = h % 24
    return f"{d}d {h}h" if h else f"{d}d"


def _short_id(pubkey: str, keep: int = 8) -> str:
    """Truncated pubkey for display."""
    if not pubkey:
        return "?"
    return pubkey[:keep]


# ---------------------------------------------------------------------------
# Notifier — event logic
# ---------------------------------------------------------------------------

class Notifier:
    """
    Central dispatch. Owns two TelegramBot instances and knows which events
    go where. Uses a simple state file on disk to remember what was already
    sent to avoid duplicate alerts across service restarts.
    """

    # Config-file-relative state path for things we don't want to put in SQLite
    STATE_FILENAME = "telegram_state.json"

    def __init__(self, config: dict, timeseries):
        self.config = config
        self.ts = timeseries

        tg = config.get("telegram", {}) or {}
        self.debug_bot = TelegramBot(
            tg.get("debug_bot_token", ""),
            tg.get("debug_chat_id", ""),
            label="debug",
        )
        self.public_bot = TelegramBot(
            tg.get("public_bot_token", ""),
            tg.get("public_chat_id", ""),
            label="public",
        )

        # Which clusters reach the public channel
        self.public_clusters = set(tg.get("public_clusters", ["mainnet-beta"]))
        # Toggle for individual validator delinquency alerts (spammy)
        self.alert_validator_delinquent = tg.get("alert_validator_delinquent", False)

        # Persistent state (ephemeral but must survive restarts)
        logs_dir = Path(config.get("paths", {}).get("logs_dir", "/tmp"))
        logs_dir.mkdir(parents=True, exist_ok=True)
        self.state_path = logs_dir / self.STATE_FILENAME
        self.state = self._load_state()

    # -- state persistence ---------------------------------------------------

    def _load_state(self) -> dict:
        if self.state_path.exists():
            try:
                return json.loads(self.state_path.read_text())
            except Exception:
                pass
        return {
            "startup_cycles_done": {},  # per-cluster bool: "first cycle processed"
            "consecutive_failures": {},
            "last_daily_summary": None,
            # Per-endpoint last alert: "{cluster}:{name}:{kind}" -> event_ts (ISO).
            # Prevents re-sending the same transition when it falls inside the
            # 3-minute sliding window on subsequent cycles.
            "endpoint_alerts": {},
        }

    def _save_state(self):
        try:
            self.state_path.write_text(json.dumps(self.state, indent=2))
        except Exception as e:
            logger.warning(f"Failed to save telegram state: {e}")

    def _first_cycle_done(self, cluster: str) -> bool:
        return self.state.get("startup_cycles_done", {}).get(cluster, False)

    def _mark_first_cycle_done(self, cluster: str):
        self.state.setdefault("startup_cycles_done", {})[cluster] = True
        self._save_state()

    # -- endpoint alert dedup & flap suppression ----------------------------

    def _endpoint_alert_key(self, cluster: str, name: str, kind: str) -> str:
        """State key for the last time we alerted about a specific event kind
        on a specific endpoint. kind = 'down' | 'recovered'."""
        return f"{cluster}:{name}:{kind}"

    def _should_send_endpoint_event(self, cluster: str, event: dict,
                                     flap_threshold_seconds: int) -> bool:
        """Decide whether to actually emit an alert for this endpoint event.

        Returns False if:
          - We already alerted for this (cluster, name, kind, ts) combo.
            get_recent_endpoint_changes uses a 3-minute sliding window, so
            without dedup the same transition gets re-emitted on each cycle
            until it rolls out of the window.
          - The event is a recovery whose preceding downtime was shorter than
            flap_threshold_seconds (i.e. a brief API flap, not a real outage).
        """
        name = event.get("name") or ""
        went_to = event.get("to")
        ev_ts = event.get("ts")
        if not name or went_to is None:
            return False

        kind = "down" if went_to == 0 else "recovered"
        key = self._endpoint_alert_key(cluster, name, kind)
        last_ts = self.state.get("endpoint_alerts", {}).get(key)

        # Already emitted this exact transition? Skip.
        if last_ts and last_ts == ev_ts:
            return False

        # Flap suppression for recoveries: if the preceding outage was short,
        # the "recovery" is noise. Don't send it.
        if kind == "recovered":
            try:
                outage = self.ts.get_endpoint_downtime(cluster, name)
                if outage and outage.get("duration_seconds") is not None:
                    if outage["duration_seconds"] < flap_threshold_seconds:
                        logger.info(
                            f"[{cluster}] suppressing flap recovery for {name} "
                            f"(down {outage['duration_seconds']}s < "
                            f"{flap_threshold_seconds}s threshold)"
                        )
                        return False
            except Exception as e:
                logger.debug(f"flap check failed for {name}: {e}")

        return True

    def _mark_endpoint_event_sent(self, cluster: str, event: dict):
        """Record that we sent this endpoint event so we don't re-send."""
        name = event.get("name") or ""
        went_to = event.get("to")
        ev_ts = event.get("ts")
        if not name or went_to is None or not ev_ts:
            return
        kind = "down" if went_to == 0 else "recovered"
        key = self._endpoint_alert_key(cluster, name, kind)
        alerts = self.state.setdefault("endpoint_alerts", {})
        alerts[key] = ev_ts
        # Bound state size: prune entries older than 24h
        try:
            cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
            to_drop = [k for k, v in alerts.items() if v and v < cutoff]
            for k in to_drop:
                del alerts[k]
        except Exception:
            pass
        self._save_state()

    # -- startup / shutdown / errors ----------------------------------------

    def send_startup_message(self):
        """Send a pinned startup message to debug bot."""
        import platform
        # Host uptime (Linux)
        host_uptime = ""
        try:
            result = subprocess.run(
                ["uptime", "-p"], capture_output=True, text=True, timeout=3
            )
            if result.returncode == 0:
                host_uptime = result.stdout.strip().replace("up ", "")
        except Exception:
            pass

        # Analyzer version from the script's docstring
        analyzer_version = ""
        try:
            sonda_dir = Path(self.config.get("paths", {}).get("sonda_scripts", ""))
            analyzer_path = sonda_dir / "solana_analyzer.py"
            if analyzer_path.exists():
                with open(analyzer_path) as f:
                    for i, line in enumerate(f):
                        if "Solana Network Decentralization Analyzer" in line:
                            analyzer_version = line.strip().replace(
                                "Solana Network Decentralization Analyzer ", "")
                            break
                        if i > 10:
                            break
        except Exception:
            pass

        clusters_cfg = self.config.get("clusters", {}) or {}
        cluster_lines = []
        for name, c in clusters_cfg.items():
            interval = c.get("interval", "?")
            cluster_lines.append(f"  {_esc(name)}: {interval}s")

        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

        text_lines = [
            "🚀 *SONDA monitoring started*",
            "",
            f"Analyzer: `{_esc(analyzer_version)}`" if analyzer_version else "Analyzer: (version unknown)",
            f"Host: `{_esc(platform.node())}`",
        ]
        if host_uptime:
            text_lines.append(f"Server uptime: {_esc(host_uptime)}")
        text_lines.extend([
            "",
            "*Clusters:*",
            *cluster_lines,
            "",
            f"Started at {now_utc}",
        ])

        result = self.debug_bot.send("\n".join(text_lines))
        mid = result.get("message_id")
        if mid:
            self.debug_bot.pin(mid)

    def send_shutdown_message(self, reason: str = "graceful stop"):
        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        self.debug_bot.send(
            f"⏸ *SONDA monitoring stopping*\n\n"
            f"Reason: {_esc(reason)}\n"
            f"At {now_utc}"
        )

    def send_error(self, cluster: str, stage: str, error_msg: str):
        """Called by run_sonda.py on a cycle failure. Tracks consecutive failures
        per cluster; alerts only on 3+ consecutive."""
        key = f"{cluster}:{stage}"
        cf = self.state.setdefault("consecutive_failures", {})
        cf[key] = cf.get(key, 0) + 1
        self._save_state()

        count = cf[key]
        if count < 3:
            return

        # Send on 3rd failure, and every 10th after that
        if count == 3 or count % 10 == 0:
            lines = [
                f"🔴 *SONDA error: {_esc(_cluster_tag(cluster))}*",
                "",
                f"Stage: `{_esc(stage)}`",
                f"Consecutive failures: *{count}*",
                "",
                f"Last error:",
                f"`{_esc(error_msg[:300])}`",
            ]
            self.debug_bot.send("\n".join(lines))

    def clear_error(self, cluster: str, stage: str):
        """Called when a stage succeeds. If we were in alerted state (>=3),
        send a recovery message."""
        key = f"{cluster}:{stage}"
        cf = self.state.setdefault("consecutive_failures", {})
        prev = cf.get(key, 0)
        if prev >= 3:
            self.debug_bot.send(
                f"🟢 *Recovery: {_esc(_cluster_tag(cluster))}*\n\n"
                f"Stage `{_esc(stage)}` recovered after {prev} consecutive failures."
            )
        if key in cf:
            del cf[key]
            self._save_state()

    def send_data_quality_warning(self, cluster: str, message: str):
        """For silent-data-loss situations (e.g. Malbec degraded mid-cycle)."""
        self.debug_bot.send(
            f"⚠️ *Data quality: {_esc(_cluster_tag(cluster))}*\n\n"
            f"{_esc(message)}"
        )

    # -- main dispatch per snapshot -----------------------------------------

    def process_snapshot(self, cluster: str, snapshot: dict) -> dict:
        """
        Called by run_sonda.py after each successful record_snapshot.
        Runs all event detectors and dispatches messages.

        Returns a summary dict of what was sent (for logging).
        """
        sent = {
            "endpoint_events": 0,
            "incidents": 0,
            "milestones": 0,
            "epoch_summary": False,
            "tweet_draft": False,
        }

        # First cycle after startup — populate DB silently, no alerts
        if not self._first_cycle_done(cluster):
            logger.info(f"[{cluster}] First cycle after startup — events suppressed")
            self._mark_first_cycle_done(cluster)
            return sent

        is_public = cluster in self.public_clusters

        # Clear error counter — we got a successful snapshot
        # (Success of record_snapshot is implicit here; stage errors are
        # separately tracked via send_error/clear_error)

        # --- Category A: incidents ---
        # Endpoint alerting has two modes selected via config.endpoint_alerts.mode:
        #
        #   mode "state_based" (default): rolling-window health states
        #     (healthy/degraded/down). Alerts only fire on STATE TRANSITIONS,
        #     so flappy endpoints don't spam — a 1-min blip in a 30-min window
        #     barely budges uptime%.
        #
        #   mode "realtime": every reachable→unreachable transition alerts
        #     immediately (with dedup + short-flap recovery suppression).
        #     Use this when DC outages or per-cycle visibility matter more
        #     than noise reduction.
        #
        # DC incidents always run real-time — they're not endpoint-flap noise.
        ep_settings = self.ts_settings_endpoint_alerts()
        ep_mode = ep_settings.get("mode", "state_based")

        if ep_mode == "realtime":
            # Legacy behavior: alert on every transition with dedup + flap suppression
            FLAP_THRESHOLD_SECONDS = 30
            ep_events = self.ts.get_recent_endpoint_changes(cluster, minutes=3)
            for ev in ep_events:
                if not self._should_send_endpoint_event(cluster, ev, FLAP_THRESHOLD_SECONDS):
                    continue
                sent["endpoint_events"] += 1
                if is_public:
                    self._send_endpoint_event(cluster, ev, snapshot)
                self._mark_endpoint_event_sent(cluster, ev)
        else:
            # state_based: emit only when health state actually transitions.
            health_changes = self.ts.detect_endpoint_health_changes(cluster, self.config)
            for ch in health_changes:
                sent["endpoint_events"] += 1
                if is_public:
                    self._send_health_state_change(cluster, ch)

        # DC incidents (ASN+city mass outages) — always immediate
        incidents = self.ts.detect_dc_incidents(cluster, snapshot)
        for inc in incidents:
            sent["incidents"] += 1
            if is_public:
                self._send_dc_incident(cluster, inc)

        # --- Category B: milestones ---
        milestones = self.ts.check_milestones(cluster, snapshot)
        for m in milestones:
            sent["milestones"] += 1
            if is_public:
                self._send_milestone(cluster, m, snapshot)

        # Superminority change (separate from milestones — any change alerts)
        self._maybe_send_superminority_change(cluster, snapshot, is_public)

        # --- Category C: epoch summary ---
        current_epoch = snapshot.get("epoch")
        prev_epoch = self.ts.detect_epoch_change(cluster, current_epoch)
        if prev_epoch is not None:
            # New epoch — record snapshot, build summary, emit
            self.ts.record_epoch_snapshot(cluster, snapshot)
            if is_public:
                self._send_epoch_summary(cluster, current_epoch)
                sent["epoch_summary"] = True
            # Tweet draft always goes to debug bot (user posts manually)
            self._send_tweet_draft(cluster, current_epoch)
            sent["tweet_draft"] = True
        elif current_epoch is not None:
            # No epoch change — check if we've ever stored a snapshot at all
            stored = self.ts.get_epoch_snapshot(cluster, current_epoch)
            if stored is None:
                # First epoch we see for this cluster — record it silently
                self.ts.record_epoch_snapshot(cluster, snapshot)

        return sent

    # -- individual event senders -------------------------------------------

    def ts_settings_endpoint_alerts(self) -> dict:
        """Return effective endpoint_alerts settings, defaults filled in.
        Cached on the Notifier so we don't re-merge per-cycle."""
        cached = getattr(self, "_ep_settings_cache", None)
        if cached is not None:
            return cached
        try:
            from timeseries import get_endpoint_alert_settings
            settings = get_endpoint_alert_settings(self.config)
        except Exception:
            settings = {"mode": "state_based"}
        self._ep_settings_cache = settings
        return settings

    def _send_health_state_change(self, cluster: str, change: dict):
        """state_based alert: health-state transition for one endpoint.

        We compose a short label-style message similar to the realtime path,
        but driven by the new_state (healthy/degraded/down) rather than a raw
        reachable transition.
        """
        name = change.get("name") or "?"
        service = change.get("service") or ""
        old_state = change.get("old_state")
        new_state = change.get("new_state")
        uptime_pct = change.get("uptime_pct")
        downtime_seconds = change.get("downtime_seconds")

        # Same friendly name map as realtime path so wording is consistent
        service_names = {
            "block-engine": "Jito Block Engine",
            "shred-receiver": "Shred receiver",
            "ntp": "Jito NTP server",
            "bam": "Jito BAM node",
            "auction": "Harmonic auction",
            "tpu-relayer": "Harmonic TPU relayer",
            "bundles": "Harmonic bundles",
            "rpc-official": "Solana public RPC",
            "entrypoint": "Solana entrypoint",
        }
        service_display = service_names.get(service, service or "Endpoint")

        # Window for context in the message
        ep_settings = self.ts_settings_endpoint_alerts()
        window_min = int(ep_settings.get("health_window_minutes", 30))

        # Pick emoji + headline from new_state
        # Edge transitions (degraded -> down, down -> degraded) get a
        # combined header so users see what changed without reading
        # both messages.
        if new_state == "down":
            header_emoji = "🔴"
            verb = "down"
        elif new_state == "degraded":
            header_emoji = "🟡"
            verb = "degraded"
        elif new_state == "healthy":
            # Different wording depending on where we came from
            header_emoji = "🟢"
            if old_state == "down":
                verb = "fully recovered"
            elif old_state == "degraded":
                verb = "recovered"
            else:
                verb = "healthy"
        else:
            return  # 'unknown' shouldn't reach here, but be defensive

        # Body: include uptime% + downtime when recovering
        body_lines = []
        if uptime_pct is not None:
            body_lines.append(
                f"Uptime over last {window_min} min: *{uptime_pct}%*"
            )
        if new_state == "healthy" and downtime_seconds:
            body_lines.append(f"Was {old_state} for *{_fmt_duration(downtime_seconds)}*.")

        body = "\n".join(body_lines) if body_lines else ""

        text = (
            f"{header_emoji} *{service_display} {verb}: {_esc(_cluster_tag(cluster))}*\n\n"
            f"`{_esc(name)}`"
        )
        if body:
            text += f"\n{body}"

        self.public_bot.send(text)

    def _send_endpoint_event(self, cluster: str, event: dict, snapshot: dict):
        """Endpoint reachability transition. Uses the unique 'name' from the
        transition event (not 'label' which can collide — e.g. 'dublin'
        exists as both jito-ntp-dublin AND jito-bam-dublin-mainnet-bam-2-tee).

        Includes service-specific friendly name and BAM validator count
        context where applicable.
        """
        endpoint_name = event.get("name") or event.get("label") or "?"
        label = event.get("label") or ""
        service = event.get("service") or ""
        went_to = event.get("to")  # 0 = unreachable, 1 = reachable

        # Nicer service label for humans
        service_names = {
            "block-engine": "Jito Block Engine",
            "shred-receiver": "Shred receiver",
            "ntp": "Jito NTP server",
            "bam": "Jito BAM node",
            "auction": "Harmonic auction",
            "tpu-relayer": "Harmonic TPU relayer",
            "bundles": "Harmonic bundles",
            "rpc-official": "Solana public RPC",
            "entrypoint": "Solana entrypoint",
        }
        service_display = service_names.get(service, service or "Endpoint")

        # For BAM endpoints, extract how many validators were connected
        bam_context = ""
        if service == "bam" and label:
            for r in snapshot.get("records", []):
                ep_info = r.get("endpoint") or {}
                if ep_info.get("label") == label and ep_info.get("service") == "bam":
                    bam_id = ep_info.get("bam_id") or r.get("identity_pubkey")
                    bam_nodes = (snapshot.get("metrics") or {}).get("bam", {}).get("nodes", [])
                    for n in bam_nodes:
                        if n.get("bam_node") == bam_id:
                            connected = n.get("connected_validators", 0)
                            if connected:
                                bam_context = f"\nServing *{connected}* validators."
                            break
                    break

        if went_to == 0:
            text = (
                f"🔴 *{service_display} down: {_esc(_cluster_tag(cluster))}*\n\n"
                f"`{_esc(endpoint_name)}` is unreachable.{bam_context}"
            )
            self.public_bot.send(text)
        elif went_to == 1:
            # Recovered — include downtime duration
            duration_text = ""
            try:
                outage = self.ts.get_endpoint_downtime(cluster, endpoint_name)
                if outage and outage.get("duration_seconds"):
                    duration_text = f"\nWas down for *{_fmt_duration(outage['duration_seconds'])}*."
            except Exception:
                pass
            text = (
                f"🟢 *{service_display} recovered: {_esc(_cluster_tag(cluster))}*\n\n"
                f"`{_esc(endpoint_name)}` is back online.{duration_text}"
            )
            self.public_bot.send(text)

    def _send_dc_incident(self, cluster: str, incident: dict):
        """DC (ASN+city) mass outage alert. Label:value layout for scanning."""
        lines = [
            f"🔴 🏢 *Datacenter incident: {_esc(_cluster_tag(cluster))}*",
            "",
            f"Provider: *{_esc(incident['asn'])}*",
            f"City: *{_esc(incident['city'])}*",
            f"Affected: *{incident['affected']}* of *{incident['total']}* validators "
            f"(*{incident['pct']}%*)",
            "",
            f"Possible datacenter outage or network partition.",
        ]
        self.public_bot.send("\n".join(lines))

    def _send_milestone(self, cluster: str, m: dict, snapshot: dict):
        """Threshold crossed (DZ/BAM/Rakurai/Firedancer/delinquent stake).

        Format is label:value style for visual scanning — service-specific
        emoji in the header gives quick visual recognition, numbers are bold.

        Example:
          📈 🔌 *DoubleZero milestone: mainnet*

          Stake: *51.0%* (crossed *30%* up)
          Validators: *462*
        """
        metric = m["metric"]
        threshold = m["threshold"]
        direction = m["direction"]
        value = m["value"]

        # Service-specific emoji matches the same emoji we use elsewhere for
        # these services — consistent visual identity across all bot messages.
        metric_config = {
            "dz_stake_percent": {
                "emoji": "🔌",
                "name": "DoubleZero",
                "context_label": "Validators on DZ",
            },
            "bam_stake_percent": {
                "emoji": "🎯",
                "name": "BAM",
                "context_label": "Validators on BAM",
            },
            "rakurai_stake_percent": {
                "emoji": "⚡",
                "name": "Rakurai",
                "context_label": "Validators running Rakurai",
            },
            "firedancer_stake_percent": {
                "emoji": "🔥",
                "name": "Firedancer family",
                "context_label": "Validators in Firedancer family",
            },
            "delinquent_stake_percent": {
                "emoji": "⚠️",
                "name": "Delinquent stake",
                "context_label": "Delinquent validators",
            },
        }
        cfg = metric_config.get(metric, {"emoji": "📊", "name": metric,
                                        "context_label": ""})

        # Direction indicator in the header
        if direction == "up":
            trend_emoji = "📈"
            action = "crossed"
            direction_word = "up"
        else:
            trend_emoji = "📉"
            action = "dropped below"
            direction_word = "down"

        # Compute context count from snapshot
        metrics = snapshot.get("metrics") or {}
        dz_m = metrics.get("doublezero") or {}
        bam_m = metrics.get("bam") or {}
        rak_m = metrics.get("rakurai") or {}
        ch = metrics.get("cluster_health") or {}
        context_count = None
        if metric == "dz_stake_percent":
            context_count = dz_m.get("validators", 0)
        elif metric == "bam_stake_percent":
            context_count = bam_m.get("total_connected_validators", 0)
        elif metric == "rakurai_stake_percent":
            context_count = rak_m.get("matched_validators", 0)
        elif metric == "firedancer_stake_percent":
            VALIDATOR_ROLES = {"validator", "validator-hidden", "validator-inactive", "co-hosted"}
            FIREDANCER_FAMILY = {"firedancer", "frankendancer", "fd_harmonic"}
            context_count = sum(
                1 for r in snapshot.get("records", [])
                if r.get("role") in VALIDATOR_ROLES
                and any(fam in (r.get("client_type") or "").lower()
                        for fam in FIREDANCER_FAMILY)
            )
        elif metric == "delinquent_stake_percent":
            context_count = ch.get("delinquent_validators", 0)

        lines = [
            f"{trend_emoji} {cfg['emoji']} *{cfg['name']} milestone: {_esc(_cluster_tag(cluster))}*",
            "",
            f"Stake: *{value:.1f}%* ({action} *{threshold}%* {direction_word})",
        ]
        if context_count is not None:
            lines.append(f"{cfg['context_label']}: *{context_count}*")

        self.public_bot.send("\n".join(lines))

    def _maybe_send_superminority_change(self, cluster: str, snapshot: dict,
                                          is_public: bool):
        """Alert on any change in superminority validator count.

        This is separate from check_milestones because we want to catch any
        change, not just threshold crossings. We store last known count in
        telegram state per cluster.
        """
        records = snapshot.get("records", [])
        VALIDATOR_ROLES = {"validator", "validator-hidden",
                           "validator-inactive", "co-hosted"}
        sm_validators = [
            r for r in records
            if r.get("role") in VALIDATOR_ROLES and r.get("is_superminority")
        ]
        current = len(sm_validators)
        key = f"superminority:{cluster}"
        prev = self.state.get(key)
        self.state[key] = current
        self._save_state()

        if prev is None or prev == current:
            return

        delta = current - prev
        if not is_public:
            return

        if delta > 0:
            trend_emoji = "📈"
            direction = "grew"
            comment = "Stake concentrating — less decentralized."
        else:
            trend_emoji = "📉"
            direction = "shrank"
            comment = "Stake spreading — more decentralized."
        sign = "+" if delta > 0 else ""

        # Calculate total stake held by superminority
        sm_stake = sum(r.get("stake_percentage") or 0 for r in sm_validators)

        lines = [
            f"{trend_emoji} 🏛 *Superminority {direction}: {_esc(_cluster_tag(cluster))}*",
            "",
            f"Validators: *{prev}* -> *{current}* ({sign}{delta})",
            f"Stake held: *{sm_stake:.1f}%*",
            "",
            comment,
        ]
        self.public_bot.send("\n".join(lines))

    # -- epoch summary and tweet --------------------------------------------

    def _send_epoch_summary(self, cluster: str, epoch: int):
        """Rich epoch summary to public channel."""
        s = self.ts.build_epoch_summary(cluster, epoch)
        if "error" in s:
            return

        vs = s["validator_set"]
        infra = s["infrastructure"]
        conc = s["concentration"]
        decen = s.get("decentralization") or {}
        prev_decen = s.get("prev_decentralization") or {}

        lines = [
            f"🏁 *{_esc(_cluster_tag(cluster))} entered epoch {epoch}*",
            "",
            f"📊 *Validator activity*",
            f"{vs['active']} active | +{vs['new_count']} new | -{vs['left_count']} left",
        ]
        if vs["country_moves"]:
            lines.append(f"{vs['country_moves']} moved to different country")
        if vs["asn_moves"]:
            lines.append(f"{vs['asn_moves']} migrated between datacenters")
        if vs.get("superminority_delta"):
            sign = "+" if vs["superminority_delta"] > 0 else ""
            lines.append(
                f"Superminority: {vs['superminority']} "
                f"({sign}{vs['superminority_delta']})"
            )

        # Infrastructure — show only subsystems that are actually present
        # in this cluster. Devnet has no DZ/BAM, so suppress entire sections
        # (heading + body) rather than printing zeros.
        dz_pct = infra.get("dz_stake_pct") or {}
        dz_val = infra.get("dz_validators") or {}
        bam_pct = infra.get("bam_stake_pct") or {}
        bam_val = infra.get("bam_validators") or {}

        if (dz_val.get("now") or 0) > 0:
            lines.append("")
            lines.append("🔌 *DoubleZero*")
            delta_str = ""
            if dz_pct.get("delta") is not None and abs(dz_pct["delta"]) >= 0.1:
                sign = "+" if dz_pct["delta"] > 0 else ""
                delta_str = f" ({sign}{dz_pct['delta']:.1f})"
            lines.append(
                f"{dz_val['now']} validators / "
                f"{dz_pct.get('now', 0):.1f}% of stake{delta_str}"
            )
            if infra.get("dz_multicast_combo"):
                lines.append(
                    f"{infra['dz_multicast_combo']} also running DZ + multicast combo"
                )

        if (bam_val.get("now") or 0) > 0:
            lines.append("")
            lines.append("🎯 *BAM*")
            delta_str = ""
            if bam_pct.get("delta") is not None and abs(bam_pct["delta"]) >= 0.1:
                sign = "+" if bam_pct["delta"] > 0 else ""
                delta_str = f" ({sign}{bam_pct['delta']:.1f})"
            lines.append(
                f"{bam_val['now']} validators / "
                f"{bam_pct.get('now', 0):.1f}% of stake{delta_str}"
            )

        if infra.get("new_bam_regions"):
            lines.append(
                f"🆕 New BAM region: {', '.join(_esc(r) for r in infra['new_bam_regions'])}"
            )
        if infra.get("new_dz_metros"):
            lines.append(
                f"🆕 New DZ metro: {', '.join(_esc(m) for m in infra['new_dz_metros'])}"
            )

        # Concentration
        lines.append("")
        lines.append("🏢 *Infrastructure concentration*")
        if conc.get("top_asn"):
            lines.append(
                f"{_esc(conc['top_asn']['asn'])}: {conc['top_asn']['stake_pct']:.1f}% "
                f"of stake ({conc['top_asn']['validators']} validators)"
            )
        lines.append(f"Top 3 ASNs: {conc['top3_asn_stake_pct']:.1f}% combined")
        if conc.get("single_validator_cities"):
            lines.append(
                f"{conc['single_validator_cities']} cities host just 1 validator"
            )

        # Decentralization shifts (only if changed)
        if prev_decen:
            decen_changes = []
            for key in ("nakamoto_country", "nakamoto_asn", "nakamoto_city"):
                cur_v = decen.get(key)
                prev_v = prev_decen.get(key)
                if cur_v is not None and prev_v is not None and cur_v != prev_v:
                    pretty = key.replace("nakamoto_", "Nakamoto-").title().replace("_", "-")
                    decen_changes.append(f"{pretty}: {prev_v} -> {cur_v}")
            if decen_changes:
                lines.append("")
                lines.append("📊 *Decentralization shifts*")
                lines.extend(decen_changes)

        # Full URL
        lines.append("")
        lines.append("Full data: sonda.network")

        self.public_bot.send("\n".join(lines))

    def _send_tweet_draft(self, cluster: str, epoch: int):
        """Send tweet draft to debug bot for manual copy-paste."""
        draft = self.ts.build_tweet_draft(cluster, epoch)
        if not draft:
            return
        # Wrap in code block so user can copy entire thing cleanly
        text = (
            f"📝 *Tweet draft — {_esc(_cluster_tag(cluster))} epoch {epoch}*\n\n"
            f"```\n{draft}\n```"
        )
        self.debug_bot.send(text)

    # -- daily summary ------------------------------------------------------

    def send_daily_summary(self):
        """Summary sent to debug bot at 9:00 UTC. Covers last 24h per cluster."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if self.state.get("last_daily_summary") == today:
            return  # already sent today

        clusters = list((self.config.get("clusters") or {}).keys())
        lines = [
            f"📊 *SONDA daily summary — {today}*",
            "",
        ]

        for cluster in clusters:
            # Aggregate last 24h from cluster_metrics and endpoint_status
            try:
                since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
                with self.ts._conn() as c:
                    cycles = c.execute(
                        "SELECT COUNT(*) FROM cluster_metrics WHERE cluster=? AND ts>=?",
                        (cluster, since),
                    ).fetchone()[0]
                    endpoint_transitions = c.execute(
                        "SELECT COUNT(*) FROM endpoint_status WHERE cluster=? AND ts>=? "
                        "AND reachable=0",
                        (cluster, since),
                    ).fetchone()[0]
                    ip_changes = c.execute(
                        "SELECT COUNT(*) FROM ip_changes WHERE cluster=? AND ts>=?",
                        (cluster, since),
                    ).fetchone()[0]
                    new_ents = c.execute(
                        "SELECT COUNT(*) FROM new_entities WHERE cluster=? AND ts>=?",
                        (cluster, since),
                    ).fetchone()[0]
            except Exception as e:
                logger.warning(f"daily summary error for {cluster}: {e}")
                continue

            lines.append(f"*{_esc(_cluster_tag(cluster))}*")
            lines.append(f"  Cycles: {cycles}")
            lines.append(f"  Endpoint unreachable events: {endpoint_transitions}")
            lines.append(f"  IP changes: {ip_changes}")
            lines.append(f"  New entities seen: {new_ents}")
            lines.append("")

        self.debug_bot.send("\n".join(lines))
        self.state["last_daily_summary"] = today
        self._save_state()


# ---------------------------------------------------------------------------
# CLI — for testing and manual invocations
# ---------------------------------------------------------------------------

def _load_config_and_ts(config_path: str):
    """Helper: load config and TimeSeries instance."""
    import yaml
    with open(config_path) as f:
        cfg = yaml.safe_load(f)
    # Lazy import to avoid circular deps
    import sys
    sys.path.insert(0, str(Path(__file__).parent))
    from timeseries import TimeSeries
    ts = TimeSeries(cfg)
    return cfg, ts


def main():
    parser = argparse.ArgumentParser(description="SONDA Telegram bot orchestrator")
    parser.add_argument("--config", required=True, help="Path to config.yaml")
    parser.add_argument("--test", action="store_true",
                        help="Send a test message to both bots")
    parser.add_argument("--startup", action="store_true",
                        help="Send and pin startup message to debug bot")
    parser.add_argument("--shutdown", action="store_true",
                        help="Send shutdown message to debug bot")
    parser.add_argument("--daily-summary", action="store_true",
                        help="Send daily summary to debug bot")
    parser.add_argument("--snapshot", help="Process a snapshot JSON file "
                                           "(detects and sends all events)")
    parser.add_argument("--cluster", default="mainnet-beta",
                        help="Cluster name for --snapshot")
    parser.add_argument("--tweet-draft", type=int,
                        help="Send tweet draft for given epoch to debug bot")
    parser.add_argument("--epoch-summary", type=int,
                        help="Send epoch summary for given epoch to public bot")
    args = parser.parse_args()

    cfg, ts = _load_config_and_ts(args.config)
    n = Notifier(cfg, ts)

    if args.test:
        n.debug_bot.send("✅ SONDA debug bot — test OK")
        n.public_bot.send("✅ SONDA public bot — test OK")
        print("Sent test messages")

    if args.startup:
        n.send_startup_message()
        print("Sent startup")

    if args.shutdown:
        n.send_shutdown_message("manual CLI shutdown")
        print("Sent shutdown")

    if args.daily_summary:
        n.send_daily_summary()
        print("Sent daily summary")

    if args.snapshot:
        with open(args.snapshot) as f:
            snap = json.load(f)
        result = n.process_snapshot(args.cluster, snap)
        print(f"Processed: {result}")

    if args.tweet_draft is not None:
        n._send_tweet_draft(args.cluster, args.tweet_draft)
        print(f"Sent tweet draft for epoch {args.tweet_draft}")

    if args.epoch_summary is not None:
        n._send_epoch_summary(args.cluster, args.epoch_summary)
        print(f"Sent epoch summary for epoch {args.epoch_summary}")


if __name__ == "__main__":
    main()