#!/usr/bin/env python3
# Vinted parcel tracker — small poller + web UI, stdlib only
# =========================================================
# Runs on a home-network box (docker1.lan). Polls Vinted's authenticated
# API from the SAME residential IP the browser session was created on
# (DataDome is IP+UA+cookie-fingerprint scored), stores everything in a
# local SQLite db, serves a dashboard at the web port, and emits an
# optional generic webhook on change.
#
# Cookie input: /data/cookies.json — {"name": "value"} pairs copied from
# the browser (session token + the "datadome" clearance cookie). The file
# is reloaded live (mtime check); editing it needs no container restart.
#
# Run modes:
#   python3 tracker.py            # normal operation (env-configured)
#   python3 tracker.py --sample   # serve built-in sample data, no network
#
# Required env (normal mode):
#   USER_AGENT     - the exact User-Agent of the browser the cookies came from
# Optional env:
#   VINTED_DOMAIN  - default www.vinted.nl
#   POLL_SECONDS   - default 900 (15 min)
#   WEB_PORT       - default 8080
#   WEBHOOK_URL    - optional; POST JSON on every detected change
#   DATA_DIR       - default /data

import json, os, re, sqlite3, sys, threading, time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError, URLError

DATA_DIR   = os.environ.get("DATA_DIR", "/data")
DOMAIN     = os.environ.get("VINTED_DOMAIN", "www.vinted.nl")
BASE       = "https://" + DOMAIN
POLL_S     = int(os.environ.get("POLL_SECONDS", "900"))
WEB_PORT   = int(os.environ.get("WEB_PORT", "8080"))
USER_AGENT = os.environ.get("USER_AGENT", "")
WEBHOOK    = os.environ.get("WEBHOOK_URL", "").strip()
DB_PATH    = os.path.join(DATA_DIR, "tracker.db")
COOKIES    = os.path.join(DATA_DIR, "cookies.json")
STATEFILE  = os.path.join(DATA_DIR, "snapshot.json")
LOGPATH    = os.path.join(DATA_DIR, "poll.log")

SAMPLE_MODE = "--sample" in sys.argv or os.environ.get("SAMPLE") == "1"

# ------------------------- storage -------------------------

def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def init_db():
    with db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS transactions (
            order_id TEXT PRIMARY KEY,
            seen_at  REAL NOT NULL,
            state    TEXT,
            data     TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS events (
            ts       REAL NOT NULL,
            order_id TEXT NOT NULL,
            kind     TEXT NOT NULL,
            detail   TEXT
        );
        """)

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
    if not WEBHOOK or not WEBHOOK.startswith("http"):
        return
    try:
        req = urllib.request.Request(WEBHOOK, data=json.dumps(payload).encode(),
                                     headers={"Content-Type": "application/json",
                                              "User-Agent": "vinted-tracker/1.0"})
        urllib.request.urlopen(req, timeout=15)
        log(f"webhook delivered: {payload.get('kind')}")
    except Exception as e:
        log(f"webhook failed: {e}")

# ------------------------- cookies -------------------------

_cookie_cache = {"mtime": None, "raw": None}
COOKIE_LOCK = threading.Lock()

def load_cookies():
    """Accepts: {"name":"value",...} | {"cookies":{...}} | raw 'a=b; c=d' string."""
    if SAMPLE_MODE:
        return {"sample": "1"}
    with COOKIE_LOCK:
        try:
            mtime = os.stat(COOKIES).st_mtime
        except OSError:
            return _cookie_cache["raw"]    # file missing; keep last known
        if _cookie_cache["mtime"] == mtime and _cookie_cache["raw"] is not None:
            return _cookie_cache["raw"]
        try:
            txt = open(COOKIES).read().strip()
        except OSError:
            return _cookie_cache["raw"]
        if txt.startswith("{"):
            d = json.loads(txt)
            pairs = d.get("cookies", d)
            raw = "; ".join(f"{k}={v}" for k, v in pairs.items())
        else:
            raw = txt                      # raw header string
        _cookie_cache.update(mtime=mtime, raw=raw)
        log(f"cookies reloaded ({len(raw)} chars)")
        return raw

def cookies_missing() -> list:
    raw = load_cookies()
    names = [m.group(2) for m in re.finditer(r'(?:(^|;)\s*|;)([A-Za-z0-9_.-]+)=', raw or "")]
    missing = []
    if ("datadome=" + ";") not in (f"{raw};" if raw else ""):
        missing.append("datadome")
    return missing

# ------------------------- fetch -------------------------

BROWSER_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8,application/json",
    "accept-language": "en-GB,en;q=0.9,nl-NL;q=0.8",   # cookie pair: match the issuing browser
    "cache-control": "no-cache",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
}

def api_get(path):
    url = f"{BASE}{path}"
    headers = {"user-agent": USER_AGENT or "Mozilla/5.0",
               "referer": f"{BASE}/",
               "x-requested-with": "XMLHttpRequest"}
    if not SAMPLE_MODE:
        raw = load_cookies()
        if raw:
            headers["cookie"] = raw
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

def parse_transactions(payload):
    if isinstance(payload, list):
        tx = payload
    else:
        tx = payload.get("transactions") if isinstance(payload.get("transactions"), list) else list(payload.values())
    if not isinstance(tx, list):
        tx = []
    parsed = []
    for t in tx:
        if not isinstance(t, dict):
            continue
        order_id = str(t.get("id") or t.get("order_id") or "")
        if not order_id and isinstance(t.get("item"), dict):
            order_id = str(t["item"].get("id", ""))
        item = t.get("item") if isinstance(t.get("item"), dict) else {}
        details = walk_json(t)
        info = {"order_id": order_id,
                "title": item.get("title") or t.get("title") or "",
                "state": t.get("state") or t.get("status") or "",
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

def fetch_state():
    """returns (ok, data) — data carries user id + parsed transactions."""
    code, body = api_get("/api/v2/users/current")
    if is_datadome_block(code, body):
        return False, {"error": "datadome_challenge"}
    if code != 200 or not isinstance(body.get("user", body), dict):
        return False, {"error": f"users/current HTTP {code}",
                       "hint": "session cookie stale or wrong domain cookies"}
    user = body.get("user") or body
    uid = user.get("id")
    if not uid:
        return False, {"error": "no user id in /users/current", "body_keys": list(body.keys())}

    code, txp = api_get(f"/api/v2/users/{uid}/transactions?role=buyer&page=1&per_page=50")
    if is_datadome_block(code, txp):
        return False, {"error": "datadome_challenge"}
    if code != 200:
        return False, {"error": f"transactions HTTP {code}",
                       "hint": "endpoint shape differs — inspect the logged raw response"}
    with db() as c:
        open(COOKIES.replace("cookies.json", "txraw_last.json"), "w").write(json.dumps(txp, indent=1))
    parsed = parse_transactions(txp)
    return True, {"user_id": uid, "transactions": parsed}

def poll_once():
    ok, data = fetch_state()
    if not ok:
        log(f"poll failed: {data.get('error')}")
        if data.get("error") == "datadome_challenge":
            log("ACTION: copy fresh cookies (session + datadome) into cookies.json")
        return
    seen = time.time()
    changes = []
    parsed = data.get("transactions", [])
    with db() as c:
        old = {row["order_id"]: row for row in c.execute("SELECT order_id, state FROM transactions")}
        for t in parsed:
            oid = t["order_id"] or f"anon-{t['title'][:20]}"
            prev = old.get(oid)
            c.execute("INSERT OR REPLACE INTO transactions (order_id, seen_at, state, data) VALUES (?,?,?,?)",
                      (oid, seen, t["state"], json.dumps(t["raw"])))
            if prev is None:
                continue                      # first sighting: not a change event
            if prev["state"] != t["state"]:
                c.execute("INSERT INTO events (ts, order_id, kind, detail) VALUES (?,?,?,?)",
                          (seen, oid, "state", f"{prev['state']} -> {t['state']}"))
                changes.append(f"{oid}: {prev['state']} -> {t['state']}")
    save_snapshot({"ts": seen,
                   "user_id": data.get("user_id"),
                   "transactions": [{"order_id": t["order_id"], "title": t["title"], "state": t["state"],
                                     "pickup": t["pickup"], "deadline": t["deadline"],
                                     "updated_at": t["updated_at"]} for t in parsed],
                   "changes": changes})
    for ch in changes:
        log(f"change: {ch}")
        post_webhook({"source": "vinted-tracker", "kind": "change", "detail": ch,
                      "ts": seen})
    log(f"poll ok: {len(parsed)} transactions, {len(changes)} changes")

def poller_loop():
    if SAMPLE_MODE:
        return
    while True:
        try:
            poll_once()
        except Exception as e:
            log(f"poll exception: {e}")
        time.sleep(POLL_S)

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
    if "pick" in s and ("up" in s or "ready" in s): return "wait"
    if any(x in s for x in ("received", "picked", "complete", "success")): return "done"
    if any(x in s for x in ("transit", "shipped", "sending")): return "transit"
    return "other"

def render_transactions(rows, heading, note=""):
    out = f"<h2>{heading}</h2>"
    if not rows:
        out += f'<p class="muted">{note or "none"}</p>'
        return out
    out += "<table><tr><th>Item</th><th>Status</th><th>Pick-up point</th><th>Until</th></tr>"
    for t in rows:
        until_plain = ""
        until_attr = ""
        if t.get("deadline"):
            # deadline currently stored raw-ish ("key: value"); split value out if it is ISO
            raw = t["deadline"].split(":", 1)[-1].strip().strip('"')
            if re.match(r'^\d{4}-\d{2}-\d{2}', raw):
                until_attr = f' data-until="{raw}" data-show="{raw[:10]}"'
                until_plain = raw[:10] + "…"
            else:
                until_plain = t["deadline"]
        out += (f"<tr><td>{t.get('title','')}<div class='muted'>{t.get('order_id','')}</div></td>"
                f"<td><span class='pill {p_class(t.get('state'))}'>{t.get('state') or '?'}</span></td>"
                f"<td class='muted'>{'; '.join(t.get('pickup',{}).values()) or '—'}</td>"
                f"<td{until_attr}>{until_plain or '—'}</td></tr>")
    return out + "</table>"

def render_history(con):
    rows = con.execute("SELECT ts, order_id, kind, detail FROM events ORDER BY ts DESC LIMIT 100").fetchall()
    out = "<h2>History</h2>"
    if not rows:
        return out + '<p class="muted">no events recorded yet</p>'
    out += "<table><tr><th>When</th><th>Order</th><th>Change</th></tr>"
    for r in rows:
        when = time.strftime("%Y-%m-%d %H:%M", time.localtime(r["ts"]))
        out += f"<tr><td class='muted'>{when}</td><td class='muted'>{r['order_id']}</td><td>{r['detail'] or r['kind']}</td></tr>"
    return out + "</table>"

def build_page():
    with db() as con:
        rows = con.execute("SELECT order_id, state, seen_at, data FROM transactions GROUP BY order_id ORDER BY seen_at DESC").fetchall()
        last_seen = max((r["seen_at"] for r in rows), default=None)
        parsed_all = []
        for r in rows:
            d = json.loads(r["data"])
            info = parse_transactions([d.get("raw", d)])
            if info:
                info[0]["state"] = r["state"]
                info[0]["order_id"] = r["order_id"]
                parsed_all.append(info[0])
        waiting = [t for t in parsed_all if t["state"] and
                   (("pick" in t["state"].lower() and ("up" in t["state"].lower() or "wait" in t["state"].lower()))
                    or "wait" in t["state"].lower() or "pending" in t["state"].lower())]
        others = [t for t in parsed_all if t not in waiting]
        hist = render_history(con)
    stale = bool(last_seen and (time.time() - last_seen) > POLL_S * 3)
    dot = "stale" if stale else ""
    body = (f"<h1><span class='dot {dot}'></span> Vinted parcels</h1>"
            + render_transactions(waiting, "Awaiting pick-up", "nothing waiting")
            + render_transactions(others, "Everything else")
            + hist
            + ("<div class='refreshed'>last poll: " + (time.strftime("%H:%M", time.localtime(last_seen)) if last_seen else "never") + "</div>"))
    return ("<!doctype html><html><head><meta charset=utf-8>"
            '<meta name=viewport content="width=device-width,initial-scale=1">'
            '<meta http-equiv="refresh" content="60">'
            f"<title>Vinted parcels</title><style>{STYLE}</style>"
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
                rows = c.execute("SELECT order_id, state, seen_at, data FROM transactions GROUP BY order_id ORDER BY seen_at DESC").fetchall()
            out = []
            for r in rows:
                d = json.loads(r["data"])
                info = parse_transactions([d.get("raw", d)])
                if info:
                    info[0]["state"] = r["state"]
                    out.append(info[0])
            self._send(200, "application/json", json.dumps({"transactions": out}))
        elif path == "/api/history.json":
            with db() as c:
                ev = [dict(r) for r in c.execute("SELECT ts, order_id, kind, detail FROM events ORDER BY ts DESC LIMIT 200")]
            self._send(200, "application/json", json.dumps(ev))
        else:
            self._send(404, "text/html", "<h1>404</h1>")

    def log_message(self, fmt, *args):   # silence request spam
        pass

# ------------------------- entry -------------------------

def seed_sample():
    init_db()
    path = os.path.join(os.path.dirname(__file__) or ".", "sample", "sample.json")
    payload = json.load(open(path))
    parsed = parse_transactions(payload["transactions"])
    with db() as con:
        for t in parsed:
            con.execute("INSERT OR REPLACE INTO transactions (order_id, seen_at, state, data) VALUES (?,?,?,?)",
                        (t["order_id"], time.time(), t["state"], json.dumps(t["raw"])))
    save_snapshot({"ts": time.time(), "transactions": parsed})
    log("sample data seeded")

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    init_db()
    if SAMPLE_MODE:
        seed_sample()
    threading.Thread(target=poller_loop, daemon=True).start()
    srv = ThreadingHTTPServer(("0.0.0.0", WEB_PORT), Handler)
    log(f"serving on :{WEB_PORT} (sample={SAMPLE_MODE})")
    srv.serve_forever()

if __name__ == "__main__":
    main()