#!/usr/bin/env python3
"""Smoke test for the tracker in sample mode (used by CI + local checks).

Starts the server as a real child process (Popen), probes the HTTP endpoints,
then shuts it down. tracker.py --sample serves on :8080 in the foreground.
Sample data covers both sources (vinted + postnl).
"""
import json, subprocess, sys, time, urllib.request

BASE = "http://127.0.0.1:8080"

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
                           and set(d.get("sources", {})) == {"vinted/main", "postnl/main"}))
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
sys.exit(0 if checks and all(ok for _, ok in checks) else 1)