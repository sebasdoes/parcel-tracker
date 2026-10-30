#!/usr/bin/env python3
"""Smoke test for the tracker in sample mode (used by CI + local checks).

Starts the server as a real child process (Popen), probes the HTTP endpoints,
then shuts it down. tracker.py --sample serves on :8080 in the foreground.
Sample data covers both sources (vinted + postnl).
"""
import json, os, shutil, subprocess, sys, time, urllib.request, urllib.error

BASE = "http://127.0.0.1:8080"

# fresh data dir every run: seeded rows must never leak between runs (the sample
# DB used to keep rows from earlier, bigger fixtures and flip the row counts)
if os.path.isdir(".data"):
    shutil.rmtree(".data")

server = subprocess.Popen(
    [sys.executable, "tracker.py", "--sample"],
    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
)


def get(path, timeout=5):
    return urllib.request.urlopen(f"{BASE}{path}", timeout=timeout)


def wait_ready(seconds=15.0):
    """Poll /healthz until the server answers ok:true, or give up."""
    deadline = time.time() + seconds
    while time.time() < deadline:
        if server.poll() is not None:          # server died early
            print("SERVER OUTPUT:\n" + (server.stdout.read() if server.stdout else ""), flush=True)
            return False
        try:
            d = json.loads(get("/healthz").read().decode())
            if d.get("ok"):
                return True
        except Exception:
            pass
        time.sleep(0.3)
    return False


checks, notes = [], []
try:
    if not wait_ready():
        checks.append(("server ready (healthz)", False))
        notes.append("server did not answer /healthz in time")
    else:
        checks.append(("server ready (healthz)", True))
        try:
            p = get("/")
            html = p.read().decode()
            checks.append(("/ ok", p.status == 200 and "Parcels" in html
                           and "Awaiting pick-up" in html))
        except Exception as e:
            checks.append(("/ ok", False))
            notes.append(f"/: {e}")
        try:
            j = get("/api/overview.json")
            d = json.loads(j.read().decode())
            srcs = [t.get("source") for t in d["transactions"]]
            checks.append(("/api/overview.json ok",
                           j.status == 200
                           and srcs.count("vinted") == 3
                           and srcs.count("postnl") == 3
                           and srcs.count("bol") == 3
                           and set(d.get("sources", {})) == {"vinted/main", "postnl/main", "bol/main"}))
        except Exception as e:
            checks.append(("/api/overview.json ok", False))
            notes.append(f"/api/overview.json: {e}")
        try:
            h = get("/api/history.json")
            checks.append(("/api/history.json ok", h.status == 200))
        except Exception as e:
            checks.append(("/api/history.json ok", False))
            notes.append(f"/api/history.json: {e}")
finally:
    server.terminate()
    try:
        server.wait(timeout=5)
    except subprocess.TimeoutExpired:
        server.kill()

for name, ok in checks:
    print(f"PASS: {name}" if ok else f"FAIL: {name}")
for n in notes:
    print(f"  note: {n}")
if not (checks and all(ok for _, ok in checks)):
    sys.exit(1)

# ---------------- config/account CRUD against a fresh non-sample instance ----------------
import os as _os
import shutil as _shutil
import tempfile as _tempfile

_tmp = _tempfile.mkdtemp(prefix="parcelsmoke-")
_env = {**_os.environ,
        "DATA_DIR": _tmp,
        "DB_PATH": _tmp + "/t.db",
        "STATEFILE": _tmp + "/s.json",
        "CONFIG_PATH": _tmp + "/config.json",
        "WEB_PORT": "8086"}
BASE2 = "http://127.0.0.1:8086"


def get2(path, timeout=8):
    return urllib.request.urlopen(BASE2 + path, timeout=timeout)


def post2(path, body, timeout=8):
    req = urllib.request.Request(BASE2 + path, method="POST",
                                 headers={"content-type": "application/json"},
                                 data=json.dumps(body).encode())
    try:
        return urllib.request.urlopen(req, timeout=timeout)
    except urllib.error.HTTPError as e:
        return e   # 4xx responses still carry our json error body


srv2 = subprocess.Popen([sys.executable, "tracker.py"], env=_env,
                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


def wait_ready2(seconds=15.0):
    deadline = time.time() + seconds
    while time.time() < deadline:
        if srv2.poll() is not None:
            print("SERVER2 OUTPUT:\n" + (srv2.stdout.read() or ""), flush=True)
            return False
        try:
            if json.loads(get2("/healthz").read().decode()).get("ok"):
                return True
        except Exception:
            pass
        time.sleep(0.3)
    return False


cc = {}
try:
    if not wait_ready2():
        cc["config server ready"] = False
    else:
        cc["config server ready"] = True
        cc["config page ok"] = get2("/config").status == 200 and b"cfgbox" in get2("/config").read()
        r1 = json.loads(post2("/api/accounts", {
            "op": "save", "source": "vinted",
            "fields": {"name": "kim", "cookie": "SECRETCOOKIEVALUEabcdef0123456789", "user_agent": "UA"},
        }).read().decode())
        cc["account save ok"] = r1.get("ok") is True
        accs = json.loads(get2("/api/accounts.json").read().decode())
        blob = json.dumps(accs)
        cookie_shown = [v.get("fields", {}).get("cookie", "")
                        for s in accs.get("shop", {}).values() for v in s]
        cc["secrets masked"] = ("SECRETCOOKIEVALUE" not in blob) and bool(cookie_shown)
        r2 = json.loads(post2("/api/accounts", {
            "op": "delete", "source": "vinted", "name": "kim"}).read().decode())
        accs2 = json.loads(get2("/api/accounts.json").read().decode())
        cc["account delete ok"] = r2.get("ok") is True and not accs2.get("shop", {}).get("vinted")
        r3 = json.loads(post2("/api/accounts", {
            "op": "save", "source": "amazon", "fields": {"name": "x", "cookie": "c"}}).read().decode())
        cc["planned source rejected"] = r3.get("ok") is False
finally:
    srv2.terminate()
    _shutil.rmtree(_tmp, ignore_errors=True)

fails = [k for k, v in cc.items() if not v]
for k, v in cc.items():
    print(("PASS: " if v else "FAIL: ") + k)
if fails:
    sys.exit(1)
print("smoke OK (dashboard + config)")
