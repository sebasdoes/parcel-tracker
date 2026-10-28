#!/usr/bin/env python3
# Parcel tracker — small poller + web UI, stdlib only
# =========================================================
# Runs on a home-network box (docker1.lan). Every configured ACCOUNT of every
# configured SOURCE is polled in turn, everything lands in a local SQLite db,
# a dashboard shows what's waiting/where/until, and an optional generic
# webhook fires on change.
#
# Configuration lives in DATA_DIR/config.json (hot-reloaded; see
# config.example.json). Sources are lists of accounts — multiple accounts are
# the norm:
#   vinted  [{name, user_agent, domain, cookies_file}] — browser-cookie session
#           (session token + datadome clearance cookie), file mtime-watched.
#   postnl  [{name, email, password}] — headless PKCE login (tokens in
#           postnl_token_<name>.json, auto-refreshed), incoming + outgoing.
# Legacy env names (USER_AGENT, POSTNL_EMAIL, POSTNL_PASSWORD, VINTED_DOMAIN,
# POLL_SECONDS, WEB_PORT, WEBHOOK_URL) are honoured only while config.json is
# absent — a transition aid for existing installs.
#
# Run modes:
#   python3 tracker.py            # normal operation (config.json)
#   python3 tracker.py --sample   # serve built-in sample data, no network

import json, os, re, sqlite3, sys, threading, time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError, URLError

import postnl as postnl_mod

SAMPLE_MODE = "--sample" in sys.argv or os.environ.get("SAMPLE") == "1"

# In sample mode (host runs, CI) /data usually doesn't exist — fall back to
# a local dir unless DATA_DIR is set explicitly. Order matters: SAMPLE first.
DATA_DIR  = os.environ.get("DATA_DIR", (".data" if SAMPLE_MODE else "/data"))
CONFIG_PATH = os.path.join(DATA_DIR, "config.json")
DB_PATH   = os.path.join(DATA_DIR, "tracker.db")
STATEFILE = os.path.join(DATA_DIR, "snapshot.json")
LOGPATH   = os.path.join(DATA_DIR, "poll.log")

# Everything else (accounts, poll cadence, webhook, web port) lives in
# DATA_DIR/config.json — see config.example.json. Legacy env names
# (USER_AGENT, POSTNL_EMAIL, ...) are only honoured as a fallback while the
# file is absent, with a nudge in poll.log.

# ------------------------- config -------------------------

DEFAULT_POLL_S = int(os.environ.get("POLL_SECONDS", "900"))   # legacy fallback
DEFAULT_WEB_PORT = int(os.environ.get("WEB_PORT", "8080"))    # legacy fallback
DEFAULT_POLL_S = max(30, DEFAULT_POLL_S)

_cfg_cache = {"mtime": None, "cfg": None}
CFG_LOCK = threading.Lock()

def load_config() -> dict:
    """DATA_DIR/config.json, hot-reloaded by mtime. Missing file → {}."""
    with CFG_LOCK:
        try:
            mtime = os.stat(CONFIG_PATH).st_mtime
        except OSError:
            return _cfg_cache["cfg"] or {}
        if _cfg_cache["mtime"] == mtime and _cfg_cache["cfg"] is not None:
            return _cfg_cache["cfg"]
        try:
            cfg = json.loads(open(CONFIG_PATH).read())
            if not isinstance(cfg, dict):
                raise ValueError("config.json must be a JSON object")
        except Exception as e:
            log(f"config.json unreadable ({e}) — keeping previous config")
            return _cfg_cache["cfg"] or {}
        if _cfg_cache["cfg"] is not None:
            log("config reloaded")
        _cfg_cache.update(mtime=mtime, cfg=cfg)
        return cfg

def poll_seconds() -> int:
    try:
        v = int(load_config().get("poll_seconds", DEFAULT_POLL_S))
    except (TypeError, ValueError):
        v = DEFAULT_POLL_S
    return max(30, v)

def webhook_url() -> str:
    return str(load_config().get("webhook_url") or "").strip()

def acct_list(source: str) -> list:
    """Enabled accounts for one source, from config.json (legacy env fallback)."""
    if SAMPLE_MODE:
        return [{"name": "main"}]
    cfg = load_config()
    srcs = cfg.get("sources") if isinstance(cfg.get("sources"), dict) else {}
    accounts = srcs.get(source)
    if not (isinstance(accounts, list) and accounts) and source in ("vinted", "postnl"):
        leg = legacy_env_account(source)
        if leg:
            accounts = [leg]
            if not log_warned["legacy"]:
                log(f"config.json has no [{source}] accounts — using legacy env "
                    f"(POSTNL_EMAIL/USER_AGENT). Migrate to config.json when easy.")
                log_warned["legacy"] = True
    seen, out = set(), []
    for a in accounts or []:
        if not isinstance(a, dict):
            continue
        name = str(a.get("name") or "main")
        if not re.fullmatch(r"[A-Za-z0-9_.-]{1,24}", name):
            log(f"[{source}] skipping account with bad name: {name!r}")
            continue
        if name in seen:
            log(f"[{source}] duplicate account name {name!r} — keeping the first")
            continue
        seen.add(name)
        out.append(a)
    return out

log_warned = {"legacy": False}

def legacy_env_account(source):
    """Pre-config installs: single account from the old env names."""
    if source == "postnl" and os.environ.get("POSTNL_EMAIL", "").strip():
        return {"name": "main",
                "email": os.environ["POSTNL_EMAIL"].strip(),
                "password": os.environ.get("POSTNL_PASSWORD", "").strip()}
    if source == "vinted" and os.environ.get("USER_AGENT", "").strip():
        return {"name": "main",
                "user_agent": os.environ["USER_AGENT"].strip(),
                "domain": os.environ.get("VINTED_DOMAIN", "www.vinted.nl"),
                "cookies_file": "cookies.json"}
    return None

# ------------------------- storage -------------------------

def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

NEW_SCHEMA = """
CREATE TABLE IF NOT EXISTS transactions (
    source   TEXT NOT NULL DEFAULT 'vinted',
    account  TEXT NOT NULL DEFAULT 'main',
    order_id TEXT NOT NULL,
    seen_at  REAL NOT NULL,
    state    TEXT,
    data     TEXT NOT NULL,
    PRIMARY KEY (source, account, order_id)
);
CREATE TABLE IF NOT EXISTS events (
    ts       REAL NOT NULL,
    source   TEXT NOT NULL DEFAULT 'vinted',
    account  TEXT NOT NULL DEFAULT 'main',
    order_id TEXT NOT NULL,
    kind     TEXT NOT NULL,
    detail   TEXT
);
"""

def _rebuild_transactions(c, with_source, with_account):
    """Rebuild transactions into the current (source, account, order_id) shape."""
    log("migrating transactions schema (rebuilding table)")
    c.executescript("ALTER TABLE transactions RENAME TO transactions_old;")
    c.executescript(NEW_SCHEMA_TRANSACTIONS)
    sel = [("source" if with_source else "'vinted'"), ("account" if with_account else "'main'")]
    c.execute(f"INSERT OR REPLACE INTO transactions (source, account, order_id, seen_at, state, data) "
              f"SELECT {sel[0]}, {sel[1]}, order_id, seen_at, state, data FROM transactions_old")
    c.executescript("DROP TABLE transactions_old;")

NEW_SCHEMA_TRANSACTIONS = """
CREATE TABLE transactions (
    source   TEXT NOT NULL DEFAULT 'vinted',
    account  TEXT NOT NULL DEFAULT 'main',
    order_id TEXT NOT NULL,
    seen_at  REAL NOT NULL,
    state    TEXT,
    data     TEXT NOT NULL,
    PRIMARY KEY (source, account, order_id)
);
"""

def init_db():
    with db() as c:
        cols = {r[1] for r in c.execute("PRAGMA table_info(transactions)")}
        if {"source", "account", "order_id"} <= cols:
            # shape already current — just ensure it exists
            c.executescript(NEW_SCHEMA)
        elif "source" in cols:
            _rebuild_transactions(c, with_source=True, with_account=False)
        elif "order_id" in cols:
            _rebuild_transactions(c, with_source=False, with_account=False)
        c.executescript(NEW_SCHEMA)
        ev_cols = {r[1] for r in c.execute("PRAGMA table_info(events)")}
        if ev_cols and "source" not in ev_cols:
            c.execute("ALTER TABLE events ADD COLUMN source TEXT NOT NULL DEFAULT 'vinted'")
        if ev_cols and "account" not in ev_cols:
            c.execute("ALTER TABLE events ADD COLUMN account TEXT NOT NULL DEFAULT 'main'")

def log(msg):
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}"
    print(line, flush=True)
    try:
        with open(LOGPATH, "a") as f:
            f.write(line + "\n")
    except OSError:
        pass

def save_snapshot(summary):
    tmp = STATEFILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(summary, f, indent=1)
    os.replace(tmp, STATEFILE)

def post_webhook(payload):
    url = webhook_url()
    if not url or not url.startswith("http"):
        return
    try:
        req = urllib.request.Request(url, data=json.dumps(payload).encode(),
                                     headers={"Content-Type": "application/json",
                                              "User-Agent": "parcel-tracker/1.0"})
        urllib.request.urlopen(req, timeout=15)
        log(f"webhook delivered: {payload.get('kind')}")
    except Exception as e:
        log(f"webhook failed: {e}")

# ------------------------- cookies (per account file) -------------------------

_cookie_cache: dict[str, dict] = {}   # path -> {"mtime", "raw"}
COOKIE_LOCK = threading.Lock()

def load_cookies(path):
    """Accepts: {"name":"value",...} | {"cookies":{...}} | raw 'a=b; c=d' string."""
    with COOKIE_LOCK:
        st = _cookie_cache.get(path, {})
        try:
            mtime = os.stat(path).st_mtime
        except OSError:
            return st.get("raw")           # file missing; keep last known
        if st.get("mtime") == mtime and st.get("raw") is not None:
            return st["raw"]
        try:
            txt = open(path).read().strip()
        except OSError:
            return st.get("raw")
        if txt.startswith("{"):
            d = json.loads(txt)
            pairs = d.get("cookies", d)
            raw = "; ".join(f"{k}={v}" for k, v in pairs.items())
        else:
            raw = txt                      # raw header string
        _cookie_cache[path] = {"mtime": mtime, "raw": raw}
        log(f"cookies reloaded for {os.path.basename(path)} ({len(raw)} chars)")
        return raw

# ------------------------- fetch -------------------------

BROWSER_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8,application/json",
    "accept-language": "en-GB,en;q=0.9,nl-NL;q=0.8",   # cookie pair: match the issuing browser
    "cache-control": "no-cache",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
}

def api_get(path, base, ua, cookie_raw=None, referer=None):
    url = f"{base}{path}"
    headers = {"user-agent": ua or "Mozilla/5.0",
               "referer": referer or f"{base}/",
               "x-requested-with": "XMLHttpRequest"}
    if cookie_raw:
        headers["cookie"] = cookie_raw
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read().decode())
    except HTTPError as e:
        body = e.read().decode("utf-8", "replace")
        return e.code, {"__error__": body}
    except (URLError, TimeoutError, json.JSONDecodeError) as e:
        return 0, {"__error__": str(e)}

def is_datadome_block(code, body):
    return code == 403 or "datadome" in str(body.get("__error__", "")).lower() or \
        re.search(r'captch|challenge', str(body.get("__error__", "")), re.I) and code in (403, 412)

# ------------------------- parsing -------------------------

DEADLINE_KEY_RE = re.compile(r'(expir|until_|_until$|_until\b|deadline|pickup_at|pick_up_at)', re.I)
PICKUP_HINT_RE  = re.compile(r'(pick[_ ]?up|parcel[_ ]shop|locker|punt\b|point)', re.I)

def walk_json(node, path="", out=None):
    if out is None:
        out = []
    if isinstance(node, dict):
        for k, v in node.items():
            p = f"{path}.{k}" if path else k
            if isinstance(v, str) and DEADLINE_KEY_RE.search(k) and re.search(r'\d|t|T', v):
                out.append((p, k, v))
            elif isinstance(v, (str, int, float)) and PICKUP_HINT_RE.search(k):
                out.append((p, k, v))
            walk_json(v, p, out)
    elif isinstance(node, list):
        for i, v in enumerate(node):
            walk_json(v, f"{path}[{i}]", out)
    return out

def order_list(payload):
    """Find the actual order array in an escrow/transactions response (None = try legacy)."""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("escrow_orders", "orders", "transactions", "data"):
            v = payload.get(key)
            if isinstance(v, list):
                return v
    return None

def parse_transactions(payload):
    tx = order_list(payload)
    if tx is None:
        tx = payload.get("transactions") if isinstance(payload.get("transactions"), list) else list(payload.values())
    if not isinstance(tx, list):
        tx = []
    parsed = []
    for t in tx:
        if not isinstance(t, dict):
            continue
        order_id = str(t.get("id") or t.get("order_id") or t.get("order_number") or "")
        if not order_id and isinstance(t.get("item"), dict):
            order_id = str(t["item"].get("id", ""))
        item = t.get("item") if isinstance(t.get("item"), dict) else {}
        details = walk_json(t)
        # escrow-orders shape: item titles may hide under items[] / lines[]
        nested_title = ""
        for key in ("items", "lines", "escrow_order_items"):
            arr = t.get(key)
            if isinstance(arr, list) and arr:
                first = arr[0]
                if isinstance(first, dict):
                    first = first.get("item") if isinstance(first.get("item"), dict) else first
                    nested_title = first.get("title") or first.get("item_title") or ""
                    break
        shipment = t.get("shipment") if isinstance(t.get("shipment"), dict) else {}
        parcel = t.get("parcel") if isinstance(t.get("parcel"), dict) else {}
        state = t.get("state") or t.get("status") or shipment.get("status") or parcel.get("status") or ""
        info = {"order_id": order_id,
                "title": item.get("title") or nested_title or t.get("title") or "",
                "state": state,
                "updated_at": t.get("updated_at") or t.get("created_at"),
                "pickup": {}, "deadline": None, "raw": t}
        for p, k, v in details:
            if DEADLINE_KEY_RE.search(k):
                info["deadline"] = f"{k}: {v}"
            elif PICKUP_HINT_RE.search(k) and isinstance(v, str) and len(v) < 120:
                info["pickup"][k] = v
        if isinstance(t.get("pick_up_point"), dict):
            pp = t["pick_up_point"]
            info["pickup"] = {**info["pickup"],
                              "name": pp.get("name") or "", "address": pp.get("address") or "",
                              "city": pp.get("city") or ""}
        if isinstance(t.get("shipping_option"), dict):
            so = t["shipping_option"]
            for kk in ("pick_up_point", "pickup_point"):
                if isinstance(so.get(kk), dict) and not info["pickup"]:
                    pp = so[kk]
                    info["pickup"] = {"name": pp.get("name") or "", "address": pp.get("address") or "",
                                      "city": pp.get("city") or ""}
        parsed.append(info)
    return parsed

# ------------------------- poll loop -------------------------

def fetch_state(acct):
    """returns (ok, data) — data carries user id + parsed transactions."""
    domain = acct.get("domain") or "www.vinted.nl"
    base = f"https://{domain}"
    ua = acct.get("user_agent") or os.environ.get("USER_AGENT", "")
    name = acct.get("name") or "main"
    cookie_raw = None
    if not SAMPLE_MODE:
        cookie_path = os.path.join(DATA_DIR, acct.get("cookies_file") or "cookies.json")
        cookie_raw = load_cookies(cookie_path)
        if not cookie_raw:
            return False, {"error": f"cookies file missing/empty ({cookie_path})",
                           "hint": "copy cookies into data/ per config.json"}
    code, body = api_get("/api/v2/users/current", base, ua, cookie_raw)
    if is_datadome_block(code, body):
        return False, {"error": "datadome_challenge"}
    if code != 200 or not isinstance(body.get("user", body), dict):
        return False, {"error": f"users/current HTTP {code}",
                       "hint": "session cookie stale or wrong domain cookies"}
    user = body.get("user") or body
    uid = user.get("id")
    if not uid:
        return False, {"error": "no user id in /users/current", "body_keys": list(body.keys())}

    # Purchased orders live on the escrow API host (api.<domain>), current-user
    # scoped (no uid in the URL). Referer matches the my_orders page.
    api_host = re.sub(r"^www\.", "", domain)
    escrow_base = f"https://api.{api_host}"
    code, txp = api_get("/escrow-orders/api/v2/current-user/escrow-orders?page=1&per_page=20&type=purchased&status=in_progress",
                        escrow_base, ua, cookie_raw, referer=f"{base}/my_orders?order_type=purchased")
    if is_datadome_block(code, txp):
        return False, {"error": "datadome_challenge"}
    try:
        rawfile = os.path.join(DATA_DIR, f"txraw_{name}.json")
        open(rawfile, "w").write(json.dumps({"http": code, "orders": txp if code == 200 else str(txp), }, indent=1))
    except OSError:
        pass
    if code != 200:
        return False, {"error": f"escrow-orders HTTP {code}",
                       "hint": "raw body dumped to txraw_%s.json — endpoint/auth/header mismatch" % name}
    parsed = parse_transactions(txp)
    for t in parsed:
        t.setdefault("track", None)
    return True, {"user_id": uid, "transactions": parsed}

def fetch_vinted(acct) -> list:
    if SAMPLE_MODE:
        path = os.path.join(os.path.dirname(__file__) or ".", "sample", "sample.json")
        txs = parse_transactions(json.load(open(path))["transactions"])
        for t in txs:
            t["source"] = "vinted"; t["account"] = acct.get("name") or "main"
            t.setdefault("role", "in"); t.setdefault("track", None)
        return txs
    ok, data = fetch_state(acct)
    if not ok:
        raise RuntimeError(data.get("error", "unknown"))
    for t in data["transactions"]:
        t["source"] = "vinted"
        t["account"] = acct.get("name") or "main"
        t.setdefault("role", "in")
        t.setdefault("track", None)
    return data["transactions"]

# ------------------------- postnl source -------------------------

def postnl_token_path(account):
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", account or "main")
    return os.path.join(DATA_DIR, f"postnl_token_{safe}.json")

def _load_postnl_token(path):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None

def _save_postnl_token(path, tok):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(tok, f)
    os.replace(tmp, path)

def fetch_postnl(acct) -> list:
    name = acct.get("name") or "main"
    if SAMPLE_MODE:
        path = os.path.join(os.path.dirname(__file__) or ".", "sample", "postnl.json")
    else:
        if not (acct.get("email") and acct.get("password")):
            raise RuntimeError("postnl account misconfigured: email/password needed")
        tokpath = postnl_token_path(name)
        c = postnl_mod.Client()
        tok = _load_postnl_token(tokpath)
        if not tok or tok.get("expires_at", 0) - 30 < time.time():
            fresh = None
            if tok and tok.get("refresh_token"):
                try:
                    fresh = postnl_mod.refresh(c, tok["refresh_token"])
                    if not fresh.get("refresh_token"):
                        fresh["refresh_token"] = tok["refresh_token"]
                except postnl_mod.PostNLError:
                    fresh = None
            if not fresh:
                fresh = postnl_mod.login(acct["email"], acct["password"])
            _save_postnl_token(tokpath, fresh)
            tok = fresh
        sh = postnl_mod.shipments(c, tok["access_token"])
        out = []
        for role, listkey in (("in", "receiver"), ("out", "sender")):
            for s in sh.get(listkey, []):
                tt = {} if s.get("delivered") else postnl_mod.track_and_trace(
                    c, tok["access_token"], s.get("key") or "")
                n = postnl_mod.parse_shipment(s, tt, role)
                if n:
                    n["account"] = name
                    out.append(n)
        return out
    txs = json.load(open(path))
    for t in txs:
        t["account"] = name
    return txs

def active_sources():
    """One entry per configured account: (source, account, fetch, key)."""
    out = []
    for acct in acct_list("vinted"):
        name = acct.get("name") or "main"
        out.append(("vinted", name, lambda a=acct: fetch_vinted(a), f"vinted/{name}"))
    for acct in acct_list("postnl"):
        name = acct.get("name") or "main"
        out.append(("postnl", name, lambda a=acct: fetch_postnl(a), f"postnl/{name}"))
    return out

def poll_once():
    seen = time.time()
    sources_state = {}
    parsed_all = []
    changes = []
    with db() as c:
        old = {r["source"] + "|" + r["account"] + "|" + r["order_id"]: r["state"]
               for r in c.execute("SELECT source, account, order_id, state FROM transactions")}
    for source, account, fetch, key in active_sources():
        try:
            txs = fetch()
            sources_state[key] = {"ok": True, "count": len(txs)}
        except postnl_mod.InvalidCredentials as e:
            sources_state[key] = {"ok": False, "error": "bad_credentials", "detail": str(e)[:120]}
            log("PostNL rejected credentials — ACTION: fix the account's email/password in config.json")
            continue
        except Exception as e:
            sources_state[key] = {"ok": False, "error": f"{type(e).__name__}: {str(e)[:140]}"}
            log(f"source {key} failed: {sources_state[key]['error']}")
            if "datadome" in str(type(e).__name__) + str(e):
                log("ACTION: copy fresh cookies (session + datadome) into the account's cookies file")
            continue
        with db() as c:
            for t in txs:
                oid = t["order_id"] or f"anon-{(t.get('title') or '')[:20]}"
                prev = old.get(source + "|" + account + "|" + oid)
                c.execute("INSERT OR REPLACE INTO transactions (source, account, order_id, seen_at, state, data) VALUES (?,?,?,?,?,?)",
                          (source, account, oid, seen, t.get("state"), store_row(t)))
                if prev is None:
                    continue                      # first sighting: not a change event
                if prev != (t.get("state") or ""):
                    c.execute("INSERT INTO events (ts, source, account, order_id, kind, detail) VALUES (?,?,?,?,?,?)",
                              (seen, source, account, oid, "state", f"{prev} -> {t.get('state')}"))
                    changes.append(f"[{key}] {oid}: {prev} -> {t.get('state')}")
        parsed_all.extend(txs)
    slim = [{"source": t["source"], "account": t.get("account", "main"),
             "order_id": t["order_id"], "role": t.get("role", "in"),
             "title": t.get("title"), "state": t.get("state"), "pickup": t.get("pickup", {}),
             "deadline": t.get("deadline"), "track": t.get("track"),
             "updated_at": t.get("updated_at")} for t in parsed_all]
    save_snapshot({"ts": seen, "sources": sources_state, "transactions": slim,
                   "changes": changes})
    for ch in changes:
        log(f"change: {ch}")
        post_webhook({"source": "parcel-tracker", "kind": "change", "detail": ch,
                      "ts": seen})
    log("poll ok: %d parcels, %d changes; %s" % (len(parsed_all), len(changes),
        ", ".join(f"{n}:{'ok' if v['ok'] else v['error']}" for n, v in sources_state.items()) or "no sources"))

def poller_loop():
    if SAMPLE_MODE:
        return
    while True:
        try:
            poll_once()
        except Exception as e:
            log(f"poll exception: {e}")
        time.sleep(poll_seconds())

# ------------------------- web UI -------------------------

STYLE = """/* vinted tracker ui */
:root { color-scheme: dark; }
body { background:#0d1117; color:#e6edf3; font:15px/1.45 -apple-system,Segoe UI,Roboto,sans-serif;
       margin:0; padding:24px; max-width:900px; margin-inline:auto; }
h1 { font-size:1.35em; letter-spacing:.3px; display:flex; gap:10px; align-items:center; }
h1 .dot { width:9px;height:9px;border-radius:50%;background:#3fb950; box-shadow:0 0 6px #3fb9;}
h1 .dot.stale { background:#d29922; box-shadow:0 0 6px #d29922; }
h2 { font-size:.95em; color:#8b949e; text-transform:uppercase; letter-spacing:.14em; margin:26px 0 8px; }
table { width:100%; border-collapse:collapse; background:#161b22; border-radius:8px; overflow:hidden; }
th { text-align:left; font-size:.72em; color:#8b949e; text-transform:uppercase; letter-spacing:.1em;
     padding:9px 12px; border-bottom:1px solid #30363d; }
td { padding:10px 12px; border-bottom:1px solid #21262d; vertical-align:top; }
tr:last-child td { border-bottom:none; }
.pill { display:inline-block; padding:2px 9px; border-radius:20px; font-size:.8em; font-weight:600; }
.pill.wait { background:rgba(210,153,34,.15); color:#e3b341; }
.pill.done { background:rgba(63,185,80,.15); color:#3fb950; }
.pill.transit { background:rgba(56,139,253,.15); color:#58a6ff; }
.pill.other { background:rgba(139,148,158,.15); color:#8b949e; }
.src { color:#8b949e; font-size:.78em; border:1px solid #30363d; border-radius:12px; padding:1px 8px; }
.srcline { margin-left:16px; font-size:.62em; color:#8b949e; font-weight:400; }
.rdot { display:inline-block; width:8px; height:8px; border-radius:50%; background:#3fb950; margin-right:5px; }
.rdot.bad { background:#f85149; }
.muted { color:#8b949e; font-size:.85em; }
.deadline { color:#e3b341; font-weight:600; }
.deadline.soon { color:#f85149; }
.refreshed { color:#8b949e; font-size:.78em; margin-top:10px; }
a { color:#58a6ff; }

"""

JS_COUNTDOWN = """
function tick() {
  document.querySelectorAll('[data-until]').forEach(el => {
    const until = new Date(el.dataset.until);
    const days = (until - Date.now()) / 86400000;
    if (isNaN(until)) { el.textContent = el.dataset.show || ''; return; }
    if (days < 0) { el.textContent = 'expired'; el.className='deadline soon'; return; }
    const h = Math.floor(days * 24);
    el.textContent = el.dataset.show + ' — ' + (h < 48 ? h + 'u left' : Math.round(days) + 'd left');
    if (h < 48) el.className = 'deadline soon';
  });
}
setInterval(tick, 60000); tick();
"""

def p_class(state):
    s = (state or "").lower()
    if any(x in s for x in ("bezorgd", "afgeleverd", "ontvangen", "complete")): return "done"
    if ("pick" in s and ("up" in s or "ready" in s)) or "klaar" in s or "punt" in s: return "wait"
    if any(x in s for x in ("received", "picked", "success")): return "done"
    if any(x in s for x in ("transit", "shipped", "sending", "onderweg")): return "transit"
    return "other"

def is_waiting(state, role):
    s = (state or "").lower()
    return role == "in" and s and any(x in s for x in
        ("pick up", "pickup", "pick-up", "ready to", "wait", "pending", "klaar"))

def store_row(info):
    """JSON for the transactions.data column — the normalized dict itself."""
    return json.dumps(info)

def db_rows(con):
    """All parcels, newest first. Handles pre-multi-source rows (re-parsed)."""
    cols = [r[1] for r in con.execute("PRAGMA table_info(transactions)")]
    if "account" in cols:
        rows = con.execute("SELECT source, account, order_id, state, seen_at, data FROM transactions ORDER BY seen_at DESC").fetchall()
    else:
        rows = [{"source": r["source"], "account": "main", "order_id": r["order_id"],
                 "state": r["state"], "seen_at": r["seen_at"], "data": r["data"]}
                for r in con.execute("SELECT source, order_id, state, seen_at, data FROM transactions ORDER BY seen_at DESC")]
    out = []
    for r in rows:
        try:
            d = json.loads(r["data"])
        except json.JSONDecodeError:
            continue
        if not isinstance(d, dict) or "source" not in d:
            # legacy row (raw vinted tx): re-derive
            info = parse_transactions([d.get("raw", d) if isinstance(d, dict) else d])
            if not info:
                continue
            info[0]["state"] = r["state"]
            info[0]["order_id"] = r["order_id"]
            info[0]["source"] = "vinted"
            info[0]["account"] = r.get("account", "main")
            info[0].setdefault("role", "in")
            d = info[0]
        else:
            d["state"] = r["state"]
        d["seen_at"] = r["seen_at"]
        out.append(d)
    return out

def read_snapshot_sources():
    try:
        with open(STATEFILE) as f:
            return (json.load(f) or {}).get("sources", {})
    except Exception:
        return {}

def until_display(value):
    """Return (iso_or_None, plain_text) for a deadline field."""
    s = str(value or "").strip()
    if re.match(r'^\d{4}-\d{2}-\d{2}', s):
        return s, s[:10] + "…"
    tail = s.split(":", 1)[-1].strip().strip('"')
    if re.match(r'^\d{4}-\d{2}-\d{2}', tail):
        return tail, tail[:10] + "…"
    return None, s

def render_transactions(rows, heading, note=""):
    out = f"<h2>{heading}</h2>"
    if not rows:
        out += f'<p class="muted">{note or "none"}</p>'
        return out
    out += "<table><tr><th>Item</th><th>Status</th><th>Where</th><th>Until</th></tr>"
    for t in rows:
        until_iso, until_plain = until_display(t.get("deadline"))
        until_attr = f' data-until="{until_iso}" data-show="{until_iso[:10]}"' if until_iso else ""
        title = t.get("title") or ""
        if t.get("track"):
            title = f"<a href='{t['track']}'>{title}</a>"
        src = (t.get("source") or "vinted")
        acct = (t.get("account") or "").strip()
        if acct and acct != "main":
            src = f"{src}/{acct}"
        out += (f"<tr><td>{title}<div class='muted'>{t.get('order_id','')}</div></td>"
                f"<td><span class='pill {p_class(t.get('state'))}'>{t.get('state') or '?'}</span></td>"
                f"<td class='muted'>{'; '.join(str(v) for v in (t.get('pickup') or {}).values() if v) or '—'}</td>"
                f"<td{until_attr}>{until_plain or '—'}</td>"
                f"<td><span class='src'>{src}</span></td></tr>")
    return out + "</table>"

def render_history(con):
    if "account" in [r[1] for r in con.execute("PRAGMA table_info(events)")]:
        rows = con.execute("SELECT ts, source, account, order_id, kind, detail FROM events ORDER BY ts DESC LIMIT 100").fetchall()
    else:
        rows = con.execute("SELECT ts, 'vinted' AS source, 'main' AS account, order_id, kind, detail FROM events ORDER BY ts DESC LIMIT 100").fetchall()
    out = "<h2>History</h2>"
    if not rows:
        return out + '<p class="muted">no events recorded yet</p>'
    out += "<table><tr><th>When</th><th>Order</th><th>Change</th></tr>"
    for r in rows:
        when = time.strftime("%Y-%m-%d %H:%M", time.localtime(r["ts"]))
        name = r["source"] if r["account"] in ("main", "") else f"{r['source']}/{r['account']}"
        out += f"<tr><td class='muted'>{when}</td><td class='muted'>[{name}] {r['order_id']}</td><td>{r['detail'] or r['kind']}</td></tr>"
    return out + "</table>"

def build_page():
    with db() as con:
        rows = db_rows(con)
        last_seen = max((t["seen_at"] for t in rows), default=None)
        hist = render_history(con)
    srcs = read_snapshot_sources()
    src_dots = ""
    for name, st in srcs.items():
        cls = "" if st.get("ok") else "bad"
        label = name if st.get("ok") else f"{name} ⚠ {st.get('error','')[:28]}"
        src_dots += f"<span class='srcline'><span class='rdot {cls}'></span>{label}</span>"
    fresh = [t for t in rows if time.time() - t["seen_at"] < poll_seconds() * 3]
    waiting = [t for t in fresh if is_waiting(t.get("state"), t.get("role", "in"))]
    others  = [t for t in fresh if t not in waiting]
    stale = bool(last_seen and (time.time() - last_seen) > poll_seconds() * 3)
    dot = "stale" if stale else ""
    if not rows:
        others_note = "nothing yet"
    else:
        others_note = ""
    body = (f"<h1><span class='dot {dot}'></span> Parcels {src_dots}</h1>"
            + render_transactions(waiting, "Awaiting pick-up", others_note or "nothing waiting")
            + render_transactions(others, "Everything else", "nothing yet")
            + hist
            + ("<div class='refreshed'>last poll: "
               + (time.strftime("%H:%M", time.localtime(last_seen)) if last_seen else "never")
               + "</div>"))
    return ("<!doctype html><html><head><meta charset=utf-8>"
            '<meta name=viewport content="width=device-width,initial-scale=1">'
            '<meta http-equiv="refresh" content="60">'
            f"<title>Parcels</title><style>{STYLE}</style>"
            f"<script>{JS_COUNTDOWN}</script></head><body>{body}</body></html>")

class Handler(BaseHTTPRequestHandler):
    def _send(self, code, ctype, body, cache=False):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        if not cache:
            self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body if isinstance(body, bytes) else body.encode())

    def do_GET(self):
        path = self.path.split("?")[0]
        if path == "/":
            self._send(200, "text/html; charset=utf-8", build_page())
        elif path == "/healthz":
            self._send(200, "application/json", json.dumps({"ok": True, "sample": SAMPLE_MODE}))
        elif path == "/api/overview.json":
            with db() as c:
                out = db_rows(c)
            snap = read_snapshot_sources()
            self._send(200, "application/json", json.dumps({
                "transactions": out, "sources": snap}))
        elif path == "/api/history.json":
            with db() as c:
                if "account" in [r[1] for r in c.execute("PRAGMA table_info(events)")]:
                    ev = [dict(r) for r in c.execute("SELECT ts, source, account, order_id, kind, detail FROM events ORDER BY ts DESC LIMIT 200")]
                else:
                    ev = [dict(r) for r in c.execute("SELECT ts, source, 'main' AS account, order_id, kind, detail FROM events ORDER BY ts DESC LIMIT 200")]
            self._send(200, "application/json", json.dumps(ev))
        else:
            self._send(404, "text/html", "<h1>404</h1>")

    def log_message(self, fmt, *args):   # silence request spam
        pass

# ------------------------- entry -------------------------

def seed_sample():
    init_db()
    parsed = []
    vpath = os.path.join(os.path.dirname(__file__) or ".", "sample", "sample.json")
    vinted = parse_transactions(json.load(open(vpath))["transactions"])
    for t in vinted:
        t["source"] = "vinted"; t["account"] = "main"
        t.setdefault("role", "in")
    parsed.extend(vinted)
    ppath = os.path.join(os.path.dirname(__file__) or ".", "sample", "postnl.json")
    postnl_tx = json.load(open(ppath))
    for t in postnl_tx:
        t["account"] = t.get("account") or "main"
    parsed.extend(postnl_tx)
    sources_state = {"vinted/main": {"ok": True, "count": len(vinted)},
                     "postnl/main": {"ok": True, "count": len(postnl_tx)}}
    seen = time.time()
    with db() as con:
        for t in parsed:
            con.execute("INSERT OR REPLACE INTO transactions (source, account, order_id, seen_at, state, data) VALUES (?,?,?,?,?,?)",
                        (t["source"], t["account"], t["order_id"], seen, t["state"], store_row(t)))
    save_snapshot({"ts": seen, "sources": sources_state,
                   "transactions": parsed, "changes": []})
    log("sample data seeded")

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    init_db()
    if SAMPLE_MODE:
        seed_sample()
    else:
        n = sum(len(acct_list(s)) for s in ("vinted", "postnl"))
        if load_config() and not n:
            log("config.json has no enabled accounts — dashboard will be empty until accounts are added")
    threading.Thread(target=poller_loop, daemon=True).start()
    web_port = int(load_config().get("web_port") or DEFAULT_WEB_PORT)
    srv = ThreadingHTTPServer(("0.0.0.0", web_port), Handler)
    log(f"serving on :{web_port} (sample={SAMPLE_MODE})")
    srv.serve_forever()

if __name__ == "__main__":
    main()