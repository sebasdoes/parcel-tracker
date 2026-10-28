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
    image: ghcr.io/sebasdoes/parcel-tracker:latest
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

### 2b. PostNL source (optional — add to the same compose)

```yaml
    environment:
      - POSTNL_EMAIL=${POSTNL_EMAIL}        # jouw.postnl.nl / app email
      - POSTNL_PASSWORD=${POSTNL_PASSWORD}
```

Headless login (PKCE flow against login.postnl.nl, same as the community HA
integration), tokens auto-refreshed and stored in `data/postnl_token.json`;
password is used only when both refresh and login-retry are needed. Pulls all
incoming **and** outgoing shipments for the account — no tracking codes to
paste. A bonus: AliExpress parcels whose NL leg travels via PostNL show up
here automatically.

**Not onboarded (yet): AliExpress order auto-discovery.** AliExpress signs
every API call (mtop) and some endpoints demand extra anti-bot tokens —
brittle by design. Parked until there's time to babysit that integration;
Cainiao tracking-by-number works anonymously if we ever want the manual path.

| Symptom | Meaning | Fix |
|---|---|---|
| `PostNL rejected credentials` in poll.log | wrong email/password | fix the env values |
| postnl ⚠ transient error | login hiccup (recaptcha/rate limit) or API blip | retries next poll — persistent failures: check container logs |

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

- `tracker.db` — `transactions` (latest state per order, normalized dict preserved) + `events` (state changes, appended)
- `snapshot.json` — latest normalized overview + per-source health (what Hermes reads)
- `postnl_token.json` — PostNL access/refresh tokens (created when the PostNL source is on)
- Nothing leaves the box except the optional webhook you configure.