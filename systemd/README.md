# systemd/

Service unit and logrotate config for running SONDA as a systemd service.

## Files

- **`sonda.service`** — main systemd unit. Started/stopped/managed via
  `systemctl`. Reads config from `/home/solya/sonda/automation/config.yaml`.
- **`sonda-logrotate`** — logrotate config for `run_sonda.log`. Rotates
  daily, keeps 14 days compressed.

## Installation (one-time setup)

These files are version-controlled here, but actually used by the system
from `/etc/systemd/system/` and `/etc/logrotate.d/`. Two strategies:

### Strategy A: symlinks (recommended)

```bash
# Service unit
sudo ln -s /home/solya/sonda/systemd/sonda.service \
           /etc/systemd/system/sonda.service

# Logrotate
sudo ln -s /home/solya/sonda/systemd/sonda-logrotate \
           /etc/logrotate.d/sonda

# Reload systemd
sudo systemctl daemon-reload
sudo systemctl enable sonda.service
```

**Benefit:** `git pull` automatically updates the service unit. After pull
that touches systemd files, run `sudo systemctl daemon-reload`.

### Strategy B: copies

```bash
sudo cp /home/solya/sonda/systemd/sonda.service /etc/systemd/system/
sudo cp /home/solya/sonda/systemd/sonda-logrotate /etc/logrotate.d/sonda
sudo systemctl daemon-reload
sudo systemctl enable sonda.service
```

**Tradeoff:** safer (changes in git don't automatically affect production),
but requires manual sync after every change.

## Usage

```bash
# Start
sudo systemctl start sonda.service

# Stop
sudo systemctl stop sonda.service

# Restart (after code/config change)
sudo systemctl restart sonda.service

# Status
sudo systemctl status sonda.service

# Logs (live)
journalctl -u sonda.service -f

# Logs (recent)
journalctl -u sonda.service --since "1 hour ago"
```

## Critical lesson: ReadWritePaths and ProtectHome

The unit uses `ProtectHome=read-only`, which overlays `/home/` as read-only
for the service process. Only paths listed in `ReadWritePaths=` can be
written. Currently whitelisted:

- `/home/solya/sonda_data` — snapshots, SQLite, backups
- `/home/solya/.solana_network_cache` — geolocation cache (DB-IP/IPInfo)
- `/home/solya/sonda/analyzer` — for geo_overrides.yaml writes
- `/home/solya/sonda/automation/logs` — run_sonda.log

**If you add new write-needing paths**, update the unit and run
`sudo systemctl daemon-reload && sudo systemctl restart sonda.service`.

History note (2026-04-26): cache DB path was NOT in ReadWritePaths,
SQLite writes failed silently (the code had `except Exception: logger.debug(...)`),
DB-IP quota was exhausted in 2 hours. See system prompt section 8.5 for full
post-mortem. The `silent except → warning/error` audit (deployed 2026-04-30)
ensures any future read-only mount issues will surface immediately.
