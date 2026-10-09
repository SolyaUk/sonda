#!/usr/bin/env python3
"""
validator_age.py - validator age for SONDA (v1.2, 2026-10-09)

Age = number of epochs in which a vote account earned non-zero vote credits.
This is the Jito JIP-25 definition, and on mainnet the number itself is read
from the Jito ValidatorHistory program account of the vote account:

  ValidatorHistory (65856 bytes, anchor discriminator first)
    72  u32  validator_age                  total epochs with non-zero credits
    76  u16  validator_age_last_updated_epoch last completed epoch counted
   304  u64  history.idx
   312  u8   history.is_empty
   320  [512 x 128 bytes] history.arr      ring buffer of ValidatorHistoryEntry
         +8   u16 epoch          (65535 = empty slot)
         +12  u32 epoch_credits  (4294967295 = not set)

  Layout verified against stakenet programs/validator-history/src/state.rs
  (const_assert size_of::<ValidatorHistory>() == 65848, entry == 128) and
  against Solya's account on 2026-09-23 (age 649 = 138 oracle epochs before
  the ring + 511 ring epochs, same as the on-chain field).

Sources, in the order the analyzer uses them:
  mainnet   on-chain header (method "jip25"); ring buffer + Jito oracle CSV
            when the header is not initialised; oracle CSV / ring for the
            first epoch with credits
  testnet   svt.one validators-history (method "history"), then our own
            per-epoch increments from getVoteAccounts. svt.one keeps timely
            vote credits only from epoch 716, so older rows count as active
            when they show rewards or leader slots. Its data starts at epoch
            342 for everyone; a validator whose first row sits at that edge
            gets method "lower_bound" (v1.1)
  devnet, alpenglow  vote state epochCredits (up to 64 epochs) and our own
            increments: a floor, method "lower_bound"
  A vote account that exists but has never earned credits (empty
  epochCredits) is an age of 0 with method "lower_bound", not "no data";
  only a missing account is "no data" (v1.2).

No external dependencies except `requests` for the fetch helpers and the CLI.
Base58, sha256 PDA derivation and the ed25519 on-curve test are implemented
here so the analyzer needs no solders / anchorpy.

CLI:
  validator_age.py --build-oracle-index --csv validator_age_oracle.csv --out validator_age_oracle_index.json
  validator_age.py --vote <VOTE_PUBKEY> [--rpc URL] [--oracle-index PATH]
  validator_age.py --pda <VOTE_PUBKEY>
"""
import argparse
import base64
import csv
import hashlib
import json
import sys
import time

VALIDATOR_HISTORY_PROGRAM = "HistoryJTGbKQD2mRgLZ3XhqHnN811Qpez8X9kCcGHoa"
VH_SEED = b"validator-history"
VH_ACCOUNT_SIZE = 65856
VH_OFF_AGE = 72
VH_OFF_AGE_EPOCH = 76
VH_OFF_RING = 320
VH_ENTRY_SIZE = 128
VH_MAX_ITEMS = 512
VH_ENTRY_OFF_EPOCH = 8
VH_ENTRY_OFF_CREDITS = 12
U16_MAX = 65535
U32_MAX = 4294967295

# getVoteAccounts returns at most this many epochCredits entries per account
# (agave MAX_RPC_EPOCH_CREDITS_HISTORY). Fewer entries = the account's whole
# history; exactly this many = possibly truncated.
RPC_EPOCH_CREDITS_CAP = 5
# The vote state itself keeps at most this many epochs (jsonParsed account).
VOTE_STATE_EPOCH_CREDITS_CAP = 64
# svt.one testnet history begins at epoch 342 for every validator (checked
# 2026-10-04); a first row at or below this edge is the data floor, not the
# validator's first epoch.
SVT_TESTNET_FLOOR_EPOCH = 345

SVT_HISTORY_URL = ("https://api.validators.svt.one/validators-history/history"
                   "?network={network}&identity={identity}&epoch_count=2000")

# ---------------------------------------------------------------------------
# base58
# ---------------------------------------------------------------------------
_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_B58_INDEX = {c: i for i, c in enumerate(_B58)}


def b58decode(s):
    n = 0
    for ch in s:
        n = n * 58 + _B58_INDEX[ch]
    body = n.to_bytes((n.bit_length() + 7) // 8, "big") if n else b""
    pad = len(s) - len(s.lstrip("1"))
    return b"\x00" * pad + body


def b58encode(b):
    n = int.from_bytes(b, "big")
    out = ""
    while n:
        n, r = divmod(n, 58)
        out = _B58[r] + out
    pad = len(b) - len(b.lstrip(b"\x00"))
    return "1" * pad + out


# ---------------------------------------------------------------------------
# ed25519 on-curve test and program derived addresses
# ---------------------------------------------------------------------------
_P = 2 ** 255 - 19
_D = (-121665 * pow(121666, _P - 2, _P)) % _P
_SQRT_M1 = pow(2, (_P - 1) // 4, _P)


def is_on_curve(pubkey32):
    """True when the 32 bytes decompress to a point on ed25519 (RFC 8032)."""
    y = int.from_bytes(pubkey32, "little")
    sign = y >> 255
    y &= (1 << 255) - 1
    if y >= _P:
        return False
    y2 = y * y % _P
    u = (y2 - 1) % _P
    v = (_D * y2 + 1) % _P
    x2 = u * pow(v, _P - 2, _P) % _P
    x = pow(x2, (_P + 3) // 8, _P)
    if x * x % _P != x2:
        x = x * _SQRT_M1 % _P
        if x * x % _P != x2:
            return False
    if x == 0 and sign == 1:
        return False
    return True


def find_program_address(seeds, program_id):
    """Solana Pubkey::find_program_address. seeds: list of bytes; program_id: 32 bytes."""
    for bump in range(255, -1, -1):
        h = hashlib.sha256()
        for s in seeds:
            h.update(s)
        h.update(bytes([bump]))
        h.update(program_id)
        h.update(b"ProgramDerivedAddress")
        cand = h.digest()
        if not is_on_curve(cand):
            return cand, bump
    raise ValueError("no viable bump seed")


def validator_history_pda(vote_pubkey):
    """PDA (base58) of the ValidatorHistory account of a vote account."""
    pda, _bump = find_program_address([VH_SEED, b58decode(vote_pubkey)],
                                      b58decode(VALIDATOR_HISTORY_PROGRAM))
    return b58encode(pda)


# ---------------------------------------------------------------------------
# ValidatorHistory account parsing
# ---------------------------------------------------------------------------
def parse_vh_header(raw):
    """(validator_age, last_updated_epoch) from a full account or from the
    6-byte dataSlice at offset 72. Returns None when the bytes are too short."""
    if raw is None:
        return None
    if len(raw) == 6:
        return int.from_bytes(raw[0:4], "little"), int.from_bytes(raw[4:6], "little")
    if len(raw) < VH_OFF_AGE_EPOCH + 2:
        return None
    return (int.from_bytes(raw[VH_OFF_AGE:VH_OFF_AGE + 4], "little"),
            int.from_bytes(raw[VH_OFF_AGE_EPOCH:VH_OFF_AGE_EPOCH + 2], "little"))


def parse_vh_ring(raw):
    """Sorted [(epoch, credits or None)] from a full account; empty slots
    (epoch 65535) dropped, unset credits (u32::MAX) become None."""
    rows = []
    if raw is None or len(raw) < VH_OFF_RING + VH_ENTRY_SIZE:
        return rows
    n = min(VH_MAX_ITEMS, (len(raw) - VH_OFF_RING) // VH_ENTRY_SIZE)
    for i in range(n):
        o = VH_OFF_RING + i * VH_ENTRY_SIZE
        ep = int.from_bytes(raw[o + VH_ENTRY_OFF_EPOCH:o + VH_ENTRY_OFF_EPOCH + 2], "little")
        if ep == U16_MAX:
            continue
        cr = int.from_bytes(raw[o + VH_ENTRY_OFF_CREDITS:o + VH_ENTRY_OFF_CREDITS + 4], "little")
        rows.append((ep, None if cr == U32_MAX else cr))
    rows.sort()
    return rows


def ring_nonzero_epochs(rows, upto_epoch=None):
    """Epochs with credits > 0 in a parsed ring, optionally only <= upto_epoch."""
    return sorted(ep for ep, cr in rows if cr and (upto_epoch is None or ep <= upto_epoch))


# ---------------------------------------------------------------------------
# Jito oracle CSV (vote_account, epoch, credits) -> compact index
# ---------------------------------------------------------------------------
def _ranges(epochs):
    """Sorted epochs -> [[start, end], ...] inclusive ranges."""
    out = []
    for e in sorted(set(epochs)):
        if out and out[-1][1] == e - 1:
            out[-1][1] = e
        else:
            out.append([e, e])
    return out


def build_oracle_index(csv_path):
    """{vote: {"first": e, "last": e, "count": n, "ranges": [[a, b], ...]}} for
    every vote account with credits > 0 in the CSV. Header must contain
    vote_account, epoch, credits (Jito oracle file)."""
    per = {}
    with open(csv_path, newline="") as f:
        rd = csv.DictReader(f)
        cols = {c.lower().strip(): c for c in rd.fieldnames or []}
        vc = cols.get("vote_account"); ec = cols.get("epoch"); cc = cols.get("credits")
        if not (vc and ec and cc):
            raise ValueError(f"CSV must have vote_account, epoch, credits; got {rd.fieldnames}")
        for row in rd:
            try:
                credits = int(float(row[cc] or 0)); epoch = int(row[ec])
            except (ValueError, TypeError):
                continue
            if credits > 0:
                per.setdefault(row[vc].strip(), set()).add(epoch)
    index = {}
    for vote, eps in per.items():
        index[vote] = {"first": min(eps), "last": max(eps), "count": len(eps), "ranges": _ranges(eps)}
    return index


def save_oracle_index(index, out_path, csv_path=None):
    doc = {"built": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "source_csv": csv_path,
           "vote_accounts": len(index), "index": index}
    with open(out_path, "w") as f:
        json.dump(doc, f, separators=(",", ":"))
    return doc


def load_oracle_index(path):
    with open(path) as f:
        doc = json.load(f)
    return doc.get("index") or {}


def oracle_count_before(entry, epoch):
    """Number of oracle epochs with credits strictly before `epoch`."""
    if not entry:
        return 0
    n = 0
    for a, b in entry.get("ranges") or []:
        if a >= epoch:
            break
        n += min(b, epoch - 1) - a + 1
    return n


# ---------------------------------------------------------------------------
# Age computation
# ---------------------------------------------------------------------------
def jip25_age(header, ring_rows, oracle_entry, current_epoch):
    """Age of a mainnet vote account.

    header       (validator_age, last_updated_epoch) or None when the account
                 does not exist
    ring_rows    parsed ring (may be [] when only the header slice was read)
    oracle_entry the vote account's oracle index entry or None
    Returns dict(count, as_of, first, method, sources) or None when nothing
    is known (no account, no oracle data).
    """
    done = current_epoch - 1
    sources = []
    first = None
    if oracle_entry:
        first = oracle_entry["first"]
        sources.append("oracle_csv")
    ring_nz = ring_nonzero_epochs(ring_rows)
    if ring_nz:
        # a ring that starts at its own boundary may hide older epochs; then
        # only the oracle (or a cached value) knows the first epoch
        boundary = ring_rows[0][0] <= current_epoch - (VH_MAX_ITEMS - 2)
        if first is None and not boundary:
            first = ring_nz[0]
        elif first is not None:
            first = min(first, ring_nz[0])
    if header and (header[0] > 0 or header[1] > 0):
        age, upto = header
        return {"count": age, "as_of": upto, "first": first, "method": "jip25",
                "sources": ["validator_history"] + sources}
    if ring_rows:
        # header not initialised: Jito's own recipe, oracle epochs before the
        # first on-chain entry plus non-zero ring entries up to the last
        # completed epoch
        first_ring = ring_rows[0][0]
        pre = oracle_count_before(oracle_entry, first_ring)
        count = pre + len(ring_nonzero_epochs(ring_rows, done))
        return {"count": count, "as_of": done, "first": first, "method": "jip25",
                "sources": ["validator_history_ring"] + sources}
    if oracle_entry and header is None:
        # no history account at all; the oracle alone is a floor
        return {"count": oracle_entry["count"], "as_of": oracle_entry["last"], "first": first,
                "method": "lower_bound", "sources": sources}
    return None


def epochs_from_epoch_credits(entries):
    """[(epoch, earned)] from getVoteAccounts [[epoch, credits, prev], ...] or
    from jsonParsed [{"epoch", "credits", "previousCredits"}, ...]."""
    best = {}
    for e in entries or []:
        try:
            if isinstance(e, dict):
                ep = int(e["epoch"]); cr = int(e["credits"]); pr = int(e.get("previousCredits") or 0)
            else:
                ep = int(e[0]); cr = int(e[1]); pr = int(e[2])
        except (KeyError, IndexError, ValueError, TypeError):
            continue
        earned = max(0, cr - pr)
        # v1.1: devnet vote states were seen with the same epoch twice; keep one
        if ep not in best or earned > best[ep]:
            best[ep] = earned
    return sorted(best.items())


def lower_bound_age(entries, current_epoch, cap=VOTE_STATE_EPOCH_CREDITS_CAP):
    """Floor from a vote state's epochCredits: dict(count, as_of, first,
    method, sources). An empty list (the account exists, never earned
    credits) is a floor of 0; None (no account) returns None (v1.2)."""
    if entries is None:
        return None
    done = current_epoch - 1
    nz = sorted({ep for ep, earned in entries if earned > 0 and ep <= done})
    return {"count": len(nz), "as_of": done, "first": nz[0] if nz else None,
            "method": "lower_bound", "sources": ["vote_state"]}


def history_age(rows, current_epoch, floor_epoch=SVT_TESTNET_FLOOR_EPOCH):
    """From svt.one per-epoch rows as returned by fetch_svt_history:
    [(epoch, active, present)]. active = the validator earned something that
    epoch (credits, rewards or leader slots); present = it was in the set with
    stake. count = distinct active epochs up to the last completed one;
    first = first present epoch. When the first row sits at svt.one's data
    edge (floor_epoch) the history is cut, so the method is "lower_bound"."""
    done = current_epoch - 1
    if not rows:
        return None
    active = sorted({ep for ep, act, _pres in rows if act and ep <= done})
    present = sorted({ep for ep, act, pres in rows if (act or pres) and ep <= done})
    first_row = min(ep for ep, _a, _p in rows)
    cut = floor_epoch is not None and first_row <= floor_epoch
    return {"count": len(active), "as_of": done, "first": present[0] if present else None,
            "method": "lower_bound" if cut else "history", "sources": ["svt_one"],
            "svt_first_row": first_row}


def advance_ledger(entry, vote_entries, done, cap=RPC_EPOCH_CREDITS_CAP):
    """Add the epochs (entry.as_of, done] to a cached entry using this cycle's
    getVoteAccounts epochCredits [(epoch, earned)]. Returns the updated copy,
    the same entry when nothing is missing, or None when the list does not
    cover the gap (older than its earliest listed epoch)."""
    as_of = entry.get("as_of")
    if entry.get("count") is None:
        return None  # v1.1: a "no data" marker must be re-bootstrapped, not advanced
    if as_of is None or as_of >= done:
        return entry
    if not vote_entries:
        return None
    earned = {ep: e for ep, e in vote_entries}
    complete = len(vote_entries) < cap
    covered_from = 0 if complete else min(earned)
    if as_of + 1 < covered_from:
        return None
    added = [ep for ep in range(as_of + 1, done + 1) if earned.get(ep, 0) > 0]
    e2 = dict(entry)
    e2["count"] = int(entry.get("count") or 0) + len(added)
    e2["as_of"] = done
    if e2.get("first") is None and added:
        e2["first"] = added[0]
    srcs = list(entry.get("sources") or [])
    if added and "vote_state" not in srcs:
        srcs.append("vote_state")
    e2["sources"] = srcs
    return e2


# ---------------------------------------------------------------------------
# Fetch helpers (requests)
# ---------------------------------------------------------------------------
def rpc_call(url, method, params, timeout=60):
    import requests
    r = requests.post(url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                      timeout=timeout)
    r.raise_for_status()
    j = r.json()
    if "error" in j:
        raise RuntimeError(f"{method}: {j['error']}")
    return j.get("result")


def fetch_vh_headers(url, votes, timeout=60):
    """{vote: (age, last_updated) | None} for up to 100 vote accounts via one
    getMultipleAccounts with a 6-byte dataSlice. None = no account."""
    pdas = [validator_history_pda(v) for v in votes]
    res = rpc_call(url, "getMultipleAccounts",
                   [pdas, {"encoding": "base64", "dataSlice": {"offset": VH_OFF_AGE, "length": 6}}],
                   timeout=timeout) or {}
    out = {}
    for vote, acc in zip(votes, res.get("value") or []):
        if not acc:
            out[vote] = None
            continue
        data = acc.get("data")
        b64 = data[0] if isinstance(data, list) else data
        out[vote] = parse_vh_header(base64.b64decode(b64 or ""))
    return out


def fetch_vh_accounts(url, votes, timeout=90):
    """{vote: raw bytes | None} full ValidatorHistory accounts (65 KB each)."""
    pdas = [validator_history_pda(v) for v in votes]
    res = rpc_call(url, "getMultipleAccounts", [pdas, {"encoding": "base64"}], timeout=timeout) or {}
    out = {}
    for vote, acc in zip(votes, res.get("value") or []):
        if not acc:
            out[vote] = None
            continue
        data = acc.get("data")
        b64 = data[0] if isinstance(data, list) else data
        out[vote] = base64.b64decode(b64 or "")
    return out


def fetch_vote_states(url, votes, timeout=60):
    """{vote: [(epoch, earned)] | None} from jsonParsed vote accounts (up to 64 epochs)."""
    res = rpc_call(url, "getMultipleAccounts", [votes, {"encoding": "jsonParsed"}], timeout=timeout) or {}
    out = {}
    for vote, acc in zip(votes, res.get("value") or []):
        try:
            info = acc["data"]["parsed"]["info"]
            out[vote] = epochs_from_epoch_credits(info.get("epochCredits") or [])
        except (KeyError, TypeError, AttributeError):
            out[vote] = None
    return out


def _num(v):
    try:
        return float(v or 0)
    except (ValueError, TypeError):
        return 0.0


def svt_rows_to_history(rows):
    """svt.one rows (dicts) -> sorted [(epoch, active, present)], one per epoch.
    active: tvCredits, votingReward, commissionReward or leaderSlotsDone > 0;
    present: active or totalStake > 0. Numbers arrive as strings."""
    by_epoch = {}
    for row in rows or []:
        try:
            ep = int(row["epoch"])
        except (KeyError, ValueError, TypeError):
            continue
        active = (_num(row.get("tvCredits")) > 0 or _num(row.get("votingReward")) > 0
                  or _num(row.get("commissionReward")) > 0 or _num(row.get("leaderSlotsDone")) > 0)
        present = active or _num(row.get("totalStake")) > 0
        a, p = by_epoch.get(ep, (False, False))
        by_epoch[ep] = (a or active, p or present)
    return sorted((ep, a, p) for ep, (a, p) in by_epoch.items())


def fetch_svt_history(identity, network="testnet", timeout=15):
    """Sorted [(epoch, active, present)] from svt.one, None on any error."""
    import requests
    try:
        r = requests.get(SVT_HISTORY_URL.format(network=network, identity=identity), timeout=timeout)
        if r.status_code != 200:
            return None
        d = r.json()
        rows = d.get("data") if isinstance(d, dict) else d
        return svt_rows_to_history(rows)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--build-oracle-index", action="store_true")
    ap.add_argument("--csv", help="Jito oracle CSV (vote_account, epoch, credits)")
    ap.add_argument("--out", help="index JSON to write")
    ap.add_argument("--pda", help="print the ValidatorHistory PDA of a vote account")
    ap.add_argument("--vote", help="spot check: on-chain age of a vote account (mainnet)")
    ap.add_argument("--rpc", default="https://api.mainnet-beta.solana.com")
    ap.add_argument("--oracle-index", default=None)
    args = ap.parse_args()

    if args.build_oracle_index:
        if not (args.csv and args.out):
            ap.error("--build-oracle-index needs --csv and --out")
        t0 = time.time()
        idx = build_oracle_index(args.csv)
        doc = save_oracle_index(idx, args.out, args.csv)
        print(f"oracle index: {doc['vote_accounts']} vote accounts -> {args.out} ({time.time() - t0:.1f}s)")
        return
    if args.pda:
        print(validator_history_pda(args.pda))
        return
    if args.vote:
        pda = validator_history_pda(args.vote)
        print(f"vote {args.vote}\nPDA  {pda}")
        epoch_info = rpc_call(args.rpc, "getEpochInfo", []) or {}
        cur = int(epoch_info.get("epoch"))
        raw = fetch_vh_accounts(args.rpc, [args.vote]).get(args.vote)
        header = parse_vh_header(raw) if raw else None
        rows = parse_vh_ring(raw) if raw else []
        oracle = None
        if args.oracle_index:
            oracle = load_oracle_index(args.oracle_index).get(args.vote)
        print(f"epoch {cur}; account {'missing' if raw is None else str(len(raw)) + ' bytes'}")
        if header:
            print(f"on-chain validator_age {header[0]} as of epoch {header[1]}")
        if rows:
            nz = ring_nonzero_epochs(rows, cur - 1)
            print(f"ring {rows[0][0]}..{rows[-1][0]} ({len(rows)} entries), {len(nz)} completed epochs with credits")
        if oracle:
            print(f"oracle first {oracle['first']} last {oracle['last']} count {oracle['count']}; "
                  f"before ring start: {oracle_count_before(oracle, rows[0][0]) if rows else 'n/a'}")
        res = jip25_age(header, rows, oracle, cur)
        print("result:", json.dumps(res))
        if rows and header:
            recipe = oracle_count_before(oracle, rows[0][0]) + len(ring_nonzero_epochs(rows, header[1]))
            print(f"recipe check (oracle before ring + ring up to {header[1]}): {recipe} "
                  f"{'== on-chain' if recipe == header[0] else '!= on-chain ' + str(header[0])}")
        return
    ap.print_help()


if __name__ == "__main__":
    main()
