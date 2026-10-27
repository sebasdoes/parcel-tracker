#!/usr/bin/env python3
"""Smoke test for the tracker in sample mode (used by CI + local checks)."""
import json, subprocess, sys, time, urllib.request

r = subprocess.run([sys.executable, "tracker.py", "--sample"], capture_output=True, text=True, timeout=20)
# probe the running server
def get(path):
    return urllib.request.urlopen(f"http://127.0.0.1:8080{path}", timeout=10)

checks = []
try:
    p = get("/")
    html = p.read().decode()
    checks.append(("/ ok", p.status == 200 and "Vinted parcels" in html))
except Exception as e:
    checks.append(("/ ok", False, str(e)))
try:
    j = get("/api/overview.json")
    d = json.loads(j.read().decode())
    checks.append(("/api/overview.json ok", j.status == 200 and len(d["transactions"]) == 3))
except Exception as e:
    checks.append(("/api/overview.json ok", False, str(e)))
try:
    h = get("/api/history.json")
    checks.append(("/api/history.json ok", h.status == 200))
except Exception as e:
    checks.append(("/api/history.json ok", False, str(e)))

for name, ok in checks:
    print(f"{'PASS' if ok is True else 'FAIL'}: {name}" + (f" ({ok[2]})" if not ok and isinstance(ok, tuple) else ""))
sys.exit(0 if all(c[1] is True for c in checks) else 1)