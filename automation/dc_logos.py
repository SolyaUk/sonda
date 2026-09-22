#!/usr/bin/env python3
"""
dc_logos.py
===========
Datacenter/provider logos for SONDA (backend item #39).

For every "AS<n>" entry in analyzer/dc_overrides.yaml:
  0. a hand-made file analyzer/dc_logos_manual/AS<n>.png wins over everything
     and never expires (for providers without a usable favicon);
  1. icon_url in the entry wins (downloaded as is);
  2. otherwise Google's favicon service for the website host (sz=64);
  3. otherwise DuckDuckGo's icon service (ICO);
  4. Google's generic "no favicon" globe is detected and rejected (its hash is
     learned at start by asking for a domain that cannot have a favicon).
The image is squared (transparent padding) and resized to 64x64 PNG, saved
under <data_dir>/dc_logos/AS<n>.png, uploaded to R2 as assets/dc/AS<n>.png,
and assets/dc/index.json lists every ASN with source and sha256. Providers
with several ASNs get the same image under each key.

Idempotent: an existing local PNG younger than --max-age-days is reused and
only re-uploaded when its sha256 differs from the index. Nothing is ever
deleted on R2.

Usage (on syn-468):
  python3 dc_logos.py --config /home/solya/sonda/automation/config.yaml --dry-run
  python3 dc_logos.py --config /home/solya/sonda/automation/config.yaml
  python3 dc_logos.py --config ... --only AS20326 --force
Weekly run: called by run_sonda.py maintenance (Sunday, after the backup).
"""

import argparse
import hashlib
import io
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests
import yaml

try:
    from PIL import Image
except ImportError:
    sys.exit("Pillow is required: pip3 install --user pillow")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("dc_logos")

SIZE = 64
UA = {"User-Agent": "SONDA dc_logos/1.0 (+https://sonda.network)"}
GOOGLE = "https://www.google.com/s2/favicons?domain={host}&sz=64"
DDG = "https://icons.duckduckgo.com/ip3/{host}.ico"
GENERIC_HOST = "no-favicon-here-sonda-probe.invalid"


def host_of(url):
    h = urlparse(url if "://" in url else "https://" + url).hostname or ""
    return h[4:] if h.startswith("www.") else h


def fetch(url, timeout=15):
    r = requests.get(url, headers=UA, timeout=timeout, allow_redirects=True)
    if r.status_code != 200 or not r.content:
        raise RuntimeError(f"HTTP {r.status_code}")
    return r.content


def to_png64(data):
    """Any image bytes -> 64x64 RGBA PNG bytes, aspect kept, transparent padding."""
    im = Image.open(io.BytesIO(data))
    try:
        im.seek(0)
    except Exception:
        pass
    im = im.convert("RGBA")
    w, h = im.size
    if w == 0 or h == 0:
        raise RuntimeError("empty image")
    side = max(w, h)
    canvas = Image.new("RGBA", (side, side), (0, 0, 0, 0))
    canvas.paste(im, ((side - w) // 2, (side - h) // 2))
    canvas = canvas.resize((SIZE, SIZE), Image.LANCZOS)
    out = io.BytesIO()
    canvas.save(out, format="PNG", optimize=True)
    return out.getvalue()


def looks_empty(png_bytes):
    """Reject images that are (almost) fully transparent or a single colour."""
    im = Image.open(io.BytesIO(png_bytes)).convert("RGBA")
    px = list(im.getdata())
    opaque = [p for p in px if p[3] > 16]
    ratio = len(opaque) / len(px)
    if ratio < 0.05:
        return True  # (almost) fully transparent
    colours = {(p[0] // 16, p[1] // 16, p[2] // 16) for p in opaque}
    # a flat single-colour tile (e.g. a plain white square) carries no logo;
    # a one-colour glyph on transparent background is a real logo and stays
    return len(colours) <= 1 and ratio > 0.95


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--dc-overrides", default=None, help="default: <repo>/analyzer/dc_overrides.yaml from config paths")
    ap.add_argument("--manual-dir", default=None, help="hand-made logos AS<n>.png; default: <repo>/analyzer/dc_logos_manual")
    ap.add_argument("--only", default=None, help="single ASN, e.g. AS20326")
    ap.add_argument("--force", action="store_true", help="refetch and re-upload everything")
    ap.add_argument("--max-age-days", type=int, default=30)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    cfg = yaml.safe_load(open(args.config))
    paths = cfg.get("paths") or {}
    data_dir = Path(paths.get("data_dir", "/home/solya/sonda_data"))
    automation_dir = Path(paths.get("automation_dir", Path(args.config).resolve().parent))
    dc_path = Path(args.dc_overrides) if args.dc_overrides else automation_dir.parent / "analyzer" / "dc_overrides.yaml"
    manual_dir = Path(args.manual_dir) if args.manual_dir else automation_dir.parent / "analyzer" / "dc_logos_manual"
    out_dir = data_dir / "dc_logos"
    out_dir.mkdir(parents=True, exist_ok=True)
    index_path = out_dir / "index.json"
    index = json.load(open(index_path)) if index_path.exists() else {"generated": None, "logos": {}}

    dc = {k: (v or {}) for k, v in (yaml.safe_load(open(dc_path)) or {}).items()
          if isinstance(k, str) and k.startswith("AS") and "_" not in k}
    if args.only:
        dc = {k: v for k, v in dc.items() if k == args.only}
    log.info(f"{len(dc)} ASN entries in {dc_path}")

    # Learn Google's generic globe so we can reject it
    generic_hashes = set()
    try:
        generic = to_png64(fetch(GOOGLE.format(host=GENERIC_HOST)))
        generic_hashes.add(hashlib.sha256(generic).hexdigest())
        log.info("learned Google generic favicon hash")
    except Exception as e:
        log.warning(f"could not learn Google generic favicon: {e}")

    # R2 client via the project's own helper (same credentials as everything else)
    s3 = bucket = None
    if not args.dry_run:
        sys.path.insert(0, str(automation_dir))
        import r2_upload  # noqa: E402
        s3 = r2_upload.make_s3_client(cfg)
        bucket = cfg["r2"]["bucket"]

    # Providers with several ASNs share one image: fetch once per website host
    by_host_png = {}
    stats = {"fetched": 0, "cached": 0, "uploaded": 0, "no_logo": 0, "generic": 0, "errors": 0}
    now = time.time()
    for asn, entry in sorted(dc.items()):
        local = out_dir / f"{asn}.png"
        manual = manual_dir / f"{asn}.png"
        if manual.exists():
            # Hand-made logo wins over every automatic source and never expires
            try:
                png = to_png64(manual.read_bytes()); origin = "manual"
                local.write_bytes(png); stats["fetched"] += 1
            except Exception as e:
                log.warning(f"  {asn:10} manual logo unreadable: {e}"); stats["errors"] += 1; continue
            source = None
        else:
            source = entry.get("icon_url") or entry.get("website")
            if not source:
                stats["no_logo"] += 1
                index["logos"].pop(asn, None)
                continue
        fresh = local.exists() and (now - local.stat().st_mtime) < args.max_age_days * 86400
        if source is None:
            pass  # manual logo already in `png`
        elif fresh and not args.force:
            png = local.read_bytes(); origin = index["logos"].get(asn, {}).get("source", "cache")
            stats["cached"] += 1
        else:
            host = host_of(source)
            png = origin = None
            if entry.get("icon_url"):
                candidates = [("icon_url", entry["icon_url"])]
            else:
                if host in by_host_png:
                    png, origin = by_host_png[host]
                candidates = [("google", GOOGLE.format(host=host)), ("duckduckgo", DDG.format(host=host))]
            if png is None:
                for name, url in candidates:
                    try:
                        data = to_png64(fetch(url))
                        if hashlib.sha256(data).hexdigest() in generic_hashes or looks_empty(data):
                            stats["generic"] += 1
                            continue
                        png, origin = data, name
                        break
                    except Exception as e:
                        log.debug(f"{asn} {name}: {e}")
                if png is None:
                    stats["errors"] += 1
                    log.info(f"  {asn:10} {entry.get('display_name', ''):24} no usable favicon for {host}")
                    index["logos"].pop(asn, None)
                    if local.exists():
                        local.unlink()
                    continue
                by_host_png[host] = (png, origin)
            local.write_bytes(png)
            stats["fetched"] += 1
        sha = hashlib.sha256(png).hexdigest()
        key = f"assets/dc/{asn}.png"
        prev = index["logos"].get(asn, {})
        if prev.get("sha256") != sha or args.force:
            if args.dry_run:
                log.info(f"  [DRY RUN] would upload {key} ({len(png)} bytes, {origin})")
            else:
                s3.put_object(Bucket=bucket, Key=key, Body=png, ContentType="image/png",
                              CacheControl="public, max-age=86400")
                stats["uploaded"] += 1
        # In dry-run keep the previously uploaded sha (None if never uploaded),
        # so the next real run still uploads what was only simulated here.
        index["logos"][asn] = {"logo": key, "source": origin,
                               "sha256": prev.get("sha256") if args.dry_run else sha,
                               "provider": entry.get("display_name"), "updated": datetime.now(timezone.utc).isoformat()}

    index["generated"] = datetime.now(timezone.utc).isoformat()
    index_path.write_text(json.dumps(index, indent=1, ensure_ascii=False))
    if not args.dry_run:
        s3.put_object(Bucket=bucket, Key="assets/dc/index.json", Body=index_path.read_bytes(),
                      ContentType="application/json", CacheControl="public, max-age=300")
    log.info(f"done: {stats}; index {len(index['logos'])} logos -> {index_path}")


if __name__ == "__main__":
    main()
