# Vinted parcel tracker

Small poller + dashboard for Vinted packages received via pick-up points:
what's waiting, where, and until when. Stdlib-only Python (no browser inside),
~25 MB alpine image, single SQLite file, optional change webhook.

Inspired by the bambu-downloader pattern: poll → store → show.

## How it works

```
Vinted API (www.vinted.nl)  ◄─── /api/v2/users/current + /users/{id}/transactions?
        ▲                            polled every POLL_SECONDS from inside the
        │                            home LAN (residential IP, browser cookies)
   tracker container ──► /data:  cookies.json (input)  ·  tracker.db (SQLite)
                                 snapshot.json (latest) · poll.log
        
dashboard http://docker1.lan:8085/  — parcels, pickup point, deadline, history
Hermes reads /data/snapshot.json over SSH — ask it anything in chat, anytime.
```

## Setup

### 1. Cookies (your part, ~60 seconds, once every few weeks)

Log in to Vinted in your browser, open DevTools → **Application → Cookies →
https://www.vinted.nl** and copy **all cookies** into `data/cookies.json`:

```json
{
  "access_token_web_session": "eyJhbGciOi...long...",
  "datadome": "long-value...",
  "other": "any other cookies worth copying"
}
```

A pasted raw header string (`a=1; b=2`) also works — the file accepts either
shape.

**Also set `USER_AGENT`** (env) to the *exact* User-Agent of that browser
(DevTools → Console → `navigator.userAgent`). DataDome scores cookie
consistency against the IP and UA; a mismatched UA raises challenge risk.

The container **watches the file's mtime** — refresh = overwrite the file, no
restart.

### 2. Compose (docker1.lan)

```yaml
services:
  vinted-tracker:
    image: ghcr.io/sebasdoes/vinted-parcel-tracker:latest
    container_name: vinted-tracker
    restart: unless-stopped
    ports:
      - "8085:8080"
    environment:
      - USER_AGENT=${USER_AGENT}   # from .env next to the compose file
      - POLL_SECONDS=900
      - VINTED_DOMAIN=www.vinted.nl
    volumes:
      - ./data:/data:Z             # SELinux label — CoreOS podman needs :Z
```

`echo 'USER_AGENT=Mozilla/5.0 ...' > .env` next to the compose file, then
`docker compose up -d`. Dashboard: `http://docker1.lan:8085/`.

### 3. First-run endpoint discovery

The first successful poll writes the **raw API response** to
`data/txraw_last.json`. If field names in the UI look off (Vinted's payload is
reverse-engineered; `pick_up_point` and `pickup_expires_at` are guesses), the
raw file is the ground truth to adjust the parser against.

## Ops

| Symptom | Meaning | Fix |
|---|---|---|
| `poll failed: datadome_challenge` in poll.log | DataDome cookie invalidated | Re-copy fresh cookies → next poll recovers |
| `poll failed: transactions HTTP 401/…` | session JWT expired | Same: re-copy cookies |
| dashboard dot yellow | tracker stale (>3 poll intervals quiet) | check container logs |
| no dashboard | container down / port clash | `docker logs vinted-tracker` |

Webhook (optional): `WEBHOOK_URL` receives a POST
`{"source":"vinted-tracker","kind":"change","detail":"..."}` on every detected
transaction-state change — point it at ntfy/matrix/anything.

## What is stored

- `tracker.db` — `transactions` (latest state per order, raw payload preserved) + `events` (state changes, appended)
- `snapshot.json` — latest normalized overview (what Hermes reads)
- Nothing leaves the box except the optional webhook you configure.