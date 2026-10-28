# Parcel tracker

Small poller + dashboard for your parcels, from every source that's onboarded
today: **Vinted** (browser-cookie session) and **PostNL** (account login,
headless; also picks up AliExpress parcels riding the NL PostNL leg).
What's waiting, where, and until when. Stdlib-only Python (no browser inside),
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

### 1. `data/config.json` — the single configuration file

Everything is configured in one JSON file (hot-reloaded by mtime — edits need
no container restart). Copy `config.example.json` to `data/config.json` and
edit:

```json
{
  "poll_seconds": 900,
  "webhook_url": "",
  "sources": {
    "vinted": [
      {"name": "main",
       "user_agent": "Mozilla/5.0 ...exact UA of that browser...",
       "domain": "www.vinted.nl",
       "cookies_file": "cookies.json"}
    ],
    "postnl": [
      {"name": "main", "email": "you@example.nl", "password": "***"}
    ]
  }
}
```

**Multiple accounts are supported and normal:** every source takes a *list* of
accounts. Each needs a unique `name` (letters/digits/`.`, `-`, `_`); the token
or cookie state is per account (`postnl_token_<name>.json`,
cookies/txraw files per account). Drop an account out of the list and its rows
simply stop refreshing.

### 2. Vinted cookies (your part, ~60 seconds, once every few weeks)

Log in to Vinted in your browser, open DevTools → **Application → Cookies →
https://www.vinted.nl** and copy **all cookies** into the file named by
`cookies_file` (default `data/cookies.json`):

```json
{
  "access_token_web_session": "eyJhbGciOi...long...",
  "datadome": "long-value...",
  "other": "any other cookies worth copying"
}
```

A pasted raw header string (`a=1; b=2`) also works — the file accepts either
shape. `user_agent` **must** be the *exact* User-Agent of that browser
(DevTools → Console → `navigator.userAgent`). DataDome scores cookie
consistency against IP and UA; a mismatched UA raises challenge risk.

### 3. Compose (docker1.lan)

```yaml
services:
  parcel-tracker:
    image: ghcr.io/sebasdoes/parcel-tracker:latest
    container_name: parcel-tracker
    restart: unless-stopped
    ports:
      - "8085:8080"
    volumes:
      - ./data:/data:Z             # SELinux label — CoreOS podman needs :Z
```

No env vars needed — `config.json` in the mounted volume carries all
configuration (legacy env names still work while `config.json` is absent, for
existing installs). Then `podman compose up -d`. Dashboard:
`http://docker1.lan:8085/`.

### 3b. PostNL source

Just list the account under `sources.postnl`. Headless login (PKCE flow
against login.postnl.nl, same as the community HA integration), tokens
auto-refreshed and stored per-account; the password is used only when both
refresh and login-retry are needed. Pulls all incoming **and** outgoing
shipments — no tracking codes to paste. A bonus: AliExpress parcels whose NL
leg travels via PostNL show up here automatically.

**Not onboarded (yet): AliExpress order auto-discovery.** AliExpress signs
every API call (mtop) and some endpoints demand extra anti-bot tokens —
brittle by design. Parked until there's time to babysit that integration;
Cainiao tracking-by-number works anonymously if we ever want the manual path.

| Symptom | Meaning | Fix |
|---|---|---|
| `PostNL rejected credentials` in poll.log | wrong email/password | fix the account in config.json |
| postnl ⚠ transient error | login hiccup (recaptcha/rate limit) or API blip | retries next poll — persistent failures: check container logs |

### 4. First-run endpoint discovery

The first successful poll writes the **raw API response** to
`data/txraw_<account>.json`. If field names in the UI look off (Vinted's
payload is reverse-engineered; `pick_up_point` and `pickup_expires_at` are
guesses), the raw file is the ground truth to adjust the parser against.

## Ops

| Symptom | Meaning | Fix |
|---|---|---|
| `poll failed: datadome_challenge` in poll.log | DataDome cookie invalidated | Re-copy fresh cookies → next poll recovers |
| `poll failed: transactions HTTP 401/…` | session JWT expired | Same: re-copy cookies |
| dashboard dot yellow | tracker stale (>3 poll intervals quiet) | check container logs |
| no dashboard | container down / port clash | `docker logs parcel-tracker` |

Webhook (optional): `webhook_url` in config.json receives a POST
`{"source":"parcel-tracker","kind":"change","detail":"..."}` on every detected
state change — point it at ntfy/matrix/anything.

## What is stored

- `tracker.db` — `transactions` keyed `(source, account, order_id)`, normalized dict preserved + `events` (state changes, appended)
- `snapshot.json` — latest normalized overview + per-source/account health (what Hermes reads)
- `postnl_token_<account>.json` — PostNL access/refresh tokens (per account; created when that account polls)
- Nothing leaves the box except the optional webhook you configure.