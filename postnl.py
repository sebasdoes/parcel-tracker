#!/usr/bin/env python3
"""PostNL consumer-API source for the parcel tracker — stdlib only.

Ported from the community Home Assistant integration
(ha-parcel-integrations/ha-postnl, itself derived from arjenbos/ha-postnl).
Auth is the same flow HA runs server-side (no browser):

  1. GET  https://login.postnl.nl/<tenant>/login/authorize      (OIDC PKCE)
  2. POST .../widget/traditional_signin.jsonp                   (email+password)
  3. GET  .../widget/get_result.jsonp                           (capture token)
  4. POST .../auth-ui/v2/token-url                              (auth code)
  5. GET  authorize redirect                                    (no redirects)
  6. POST .../login/token                                       (tokens)

Data calls (Bearer token from step 6):
  POST https://jouw.postnl.nl/account/api/graphql               (trackedShipments)
  GET  https://jouw.postnl.nl/track-and-trace/api/trackAndTrace/<key>   (status+window)

Env: POSTNL_EMAIL, POSTNL_PASSWORD. Tokens live in DATA_DIR/postnl_token.json.
"""

import base64, hashlib, json, re, secrets, time
import urllib.parse, urllib.request
from http.cookiejar import CookieJar
from urllib.error import HTTPError

_TENANT = "101112a0-4a0f-4bbb-8176-2f1b2d370d7c"
_OIDC_CLIENT_ID = "bd9f1610-b56d-4e05-a09b-f696f05ddade"
_CAPTURE_CLIENT_ID = "dkyxkt9x888ye422mawmf769yfm9y44j"
_REDIRECT_URI = "https://www.postnl.nl/"
_SCOPE = "openid poa-profiles-api offline_access"
_FLOW_VERSION = "20250910094830574377"
_BASE = "https://login.postnl.nl"
_GRAPHQL = "https://jouw.postnl.nl/account/api/graphql"
_TT_BASE = "https://jouw.postnl.nl/track-and-trace/"

# The login flow is the website's own login widget — a plain desktop Chrome UA.
_LOGIN_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
             "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36")


class InvalidCredentials(Exception):
    """PostNL rejected the credentials outright — fix the env, not a retry."""


class PostNLError(Exception):
    """Transient login/API failure — retry on the next poll."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Client:
    """Cookie-aware HTTP client for the PostNL consumer API."""

    def __init__(self):
        jar = CookieJar()
        common = [urllib.request.HTTPCookieProcessor(jar)]
        self.get_opener = urllib.request.build_opener(*common)          # follows redirects
        self.raw_opener = urllib.request.build_opener(*common, _NoRedirect())  # 302s surface

    def _req(self, opener, req, timeout=30):
        """Return (url, status, headers, body_text); follows redirects via opener."""
        try:
            with opener.open(req, timeout=timeout) as r:
                return r.geturl(), r.status, r.headers, r.read().decode()
        except HTTPError as e:
            body = e.read().decode("utf-8", "replace")
            return None, e.code, e.headers, body
        except Exception as e:
            raise _net_or_generic(e)

    # GET (redirects followed)
    def get(self, url, headers=None, timeout=30):
        u, code, h, body = self._req(self.get_opener,
                                     urllib.request.Request(url, headers=headers or {}),
                                     timeout)
        return code, u, body

    # POST form (redirects followed)
    def post(self, url, data, headers=None, timeout=30):
        h = {"content-type": "application/x-www-form-urlencoded"}
        h.update(headers or {})
        u, code, rh, body = self._req(self.get_opener,
                                      urllib.request.Request(url, data=urllib.parse.urlencode(data).encode(),
                                                             headers=h), timeout)
        return code, u, body

    # GET/POST where we must read the 302 Location + body ourselves
    def raw(self, req, timeout=30):
        return self._req(self.raw_opener, req, timeout)


def _net_or_generic(e):
    name = type(e).__name__
    if "ssl" in str(e).lower() or "timed out" in str(e).lower() or "refused" in str(e).lower():
        return PostNLError(f"network: {name}: {e}")
    return PostNLError(f"{name}: {e}")


def _headers(extra: dict | None = None) -> dict:
    h = {"user-agent": _LOGIN_UA}
    h.update(extra or {})
    return h


def _js_value(body: str, key: str):
    m = re.search(rf"{re.escape(key)}\s*['\"]([^'\"]+)['\"]", body)
    return m.group(1) if m else None


def _json_value(body: str, key: str):
    m = re.search(rf'"{re.escape(key)}"\s*:\s*"([^"]+)"', body)
    return m.group(1) if m else None


def _pkce_pair():
    verifier = base64.urlsafe_b64encode(secrets.token_bytes(96)).rstrip(b"=").decode()
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    return verifier, challenge


def _token_request(c: Client, data: dict) -> dict:
    _, code, body = c.post(f"{_BASE}/{_TENANT}/login/token", data,
                           headers={"referer": f"{_BASE}/"})
    if not isinstance(body, str) or not body.strip().startswith("{"):
        raise PostNLError(f"login/token did not answer JSON ({code}): {str(body)[:150]}")
    tok = json.loads(body)
    if "access_token" not in tok:
        raise PostNLError(f"token request failed: {tok.get('error_description') or tok.get('error','unknown')}")
    return {"access_token": tok["access_token"],
            "refresh_token": tok.get("refresh_token"),
            "expires_in": tok.get("expires_in", 3600),
            "expires_at": time.time() + tok.get("expires_in", 3600),
            "token_type": tok.get("token_type", "Bearer")}


def login(email: str, password: str) -> dict:
    """Full PKCE + Capture login flow using email + password. Returns token dict."""
    c = Client()
    verifier, challenge = _pkce_pair()
    state = base64.urlsafe_b64encode(secrets.token_bytes(24)).rstrip(b"=").decode()

    # --- step 1: the OIDC authorize endpoint (establishes session cookies) ---
    authorize = urllib.parse.urlencode({
        "client_id": _OIDC_CLIENT_ID, "response_type": "code", "scope": _SCOPE,
        "redirect_uri": _REDIRECT_URI, "state": state, "nonce": state,
        "code_challenge": challenge, "code_challenge_method": "S256",
    })
    code_, login_url, page = c.get(f"{_BASE}/{_TENANT}/login/authorize?{authorize}")
    if code_ != 200 or not login_url:
        raise PostNLError(f"authorize HTTP {code_}")

    csrf = _js_value(page, "aicCsrf:")
    if not csrf:
        raise PostNLError("could not find CSRF token in the login page (challenge page?)")

    # --- step 2: submit credentials to the Capture widget ---
    txid = base64.urlsafe_b64encode(secrets.token_bytes(30)).rstrip(b"=").decode()
    s_code, _, _ = c.post(
        f"{_BASE}/widget/traditional_signin.jsonp", {
            "utf8": "✓",
            "signInEmailAddress": email,
            "currentPassword": password,
            "capture_screen": "signIn",
            "js_version": "d445bf4",
            "capture_transactionId": txid,
            "form": "signInForm",
            "flow": "standard",
            "client_id": _CAPTURE_CLIENT_ID,
            "redirect_uri": f"{login_url}&socialRedirect=True",
            "response_type": "token",
            "flow_version": _FLOW_VERSION,
            "settings_version": "",
            "locale": "en-US",
            "recaptchaVersion": "2",
        }, headers={"origin": _BASE, "referer": login_url})
    if s_code not in (200,):
        raise PostNLError(f"signin HTTP {s_code}")

    # --- step 3: poll for the Capture result ---
    result_url = (f"{_BASE}/widget/get_result.jsonp?"
                  + urllib.parse.urlencode({"transactionId": txid,
                                            "cache": str(int(time.time() * 1000))}))
    r_code, _, result_body = c.get(result_url)
    capture_token = _json_value(result_body, "accessToken")
    if not capture_token:
        raise InvalidCredentials(
            f"capture did not return an access token — check credentials ({str(result_body)[:150]})")

    # --- step 4: exchange capture token for an OIDC auth code (302 expected) ---
    token_url = f"{_BASE}/{_TENANT}/auth-ui/v2/token-url?" + urllib.parse.urlparse(login_url).query
    req = urllib.request.Request(token_url, data=urllib.parse.urlencode({
        "screen": "signIn", "authenticated": "True", "registering": "False",
        "accessToken": capture_token, "_csrf_token": csrf,
    }).encode(), headers=_headers({"content-type": "application/x-www-form-urlencoded",
                                   "origin": _BASE, "referer": login_url}))
    _, code_, rh, body = c.raw(req)
    auth_url = (rh.get("Location") if rh else None)
    if not auth_url:
        # sometimes the server wants a second POST acknowledging the loginSuccess screen
        existing = _js_value(body, "existingToken:")
        screen = _js_value(body, "screenToRender:")
        csrf2 = _js_value(body, "aicCsrf:")
        if screen != "loginSuccess" or not existing or not csrf2:
            raise PostNLError(f"hosted login did not reach loginSuccess (screen={screen!r})")
        req = urllib.request.Request(token_url, data=urllib.parse.urlencode({
            "screen": "loginSuccess", "accessToken": existing, "_csrf_token": csrf2,
        }).encode(), headers=_headers({"content-type": "application/x-www-form-urlencoded",
                                       "origin": _BASE, "referer": token_url}))
        _, code_, rh, body = c.raw(req)
        auth_url = rh.get("Location") if rh else None
    if not auth_url:
        raise PostNLError("hosted login did not return an authorize redirect (step 4)")

    # --- step 5: follow the authorize redirect (no redirects) and take the code ---
    _, code_, rh, _ = c.raw(urllib.request.Request(auth_url, headers=_headers()))
    location = str(rh.get("Location") or "") if rh else ""
    q = urllib.parse.parse_qs(urllib.parse.urlparse(location).query)
    auth_code, rstate = q.get("code", [None])[0], q.get("state", [None])[0]
    if not auth_code:
        raise PostNLError("authorize redirect did not return a code")
    if rstate != state:
        raise PostNLError("OIDC state mismatch")

    # --- step 6: exchange the code for tokens ---
    return _token_request(c, {
        "grant_type": "authorization_code", "client_id": _OIDC_CLIENT_ID,
        "code": auth_code, "redirect_uri": _REDIRECT_URI, "code_verifier": verifier,
    })


def refresh(c: Client, refresh_token: str) -> dict:
    return _token_request(c, {"grant_type": "refresh_token",
                              "client_id": _OIDC_CLIENT_ID, "refresh_token": refresh_token})


# ------------------------- data fetching -------------------------

SHIPMENTS_QUERY = """
query {
  trackedShipments {
    receiverShipments { key creationDateTime title barcode delivered
      deliveredTimeStamp deliveryWindowFrom deliveryWindowTo deliveryWindowType
      detailsUrl shipmentType receiverTitle deliveryAddressType
      sourceAccountId sourceDisplayName __typename }
    senderShipments { key creationDateTime title barcode delivered
      deliveredTimeStamp deliveryWindowFrom deliveryWindowTo deliveryWindowType
      detailsUrl shipmentType receiverTitle deliveryAddressType
      sourceAccountId sourceDisplayName __typename }
    __typename
  }
}
"""


def _bearer_json(c: Client, url, token, payload=None):
    headers = {"authorization": f"Bearer {token}",
               "user-agent": _LOGIN_UA,
               "accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode()
        headers["content-type"] = "application/json"
        req = urllib.request.Request(url, data=data, headers=headers)
    else:
        req = urllib.request.Request(url, headers=headers)
    _, code, _, body = c.raw(req)      # raw: never follow redirects on API calls
    return code, body


def shipments(c: Client, token) -> dict:
    """Return {'receiver': [...], 'sender': [...]} GraphQL shipments."""
    code, body = _bearer_json(c, _GRAPHQL, token, {"query": SHIPMENTS_QUERY})
    try:
        d = json.loads(body)
    except json.JSONDecodeError:
        raise PostNLError(f"graphql answered non-JSON ({code}): {str(body)[:150]}")
    if code != 200 or d.get("errors"):
        raise PostNLError(f"graphql HTTP {code}: {json.dumps(d.get('errors') or body)[:200]}")
    ts = (d.get("data") or {}).get("trackedShipments") or {}
    return {"receiver": list(ts.get("receiverShipments") or []),
            "sender": list(ts.get("senderShipments") or [])}


def track_and_trace(c: Client, token, key: str) -> dict:
    code, body = _bearer_json(c, _TT_BASE + f"api/trackAndTrace/{key}?language=nl", token)
    if code != 200:
        return {}
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return {}


def parse_shipment(s: dict, tt: dict, role: str = "in") -> dict | None:
    """Normalize a GraphQL shipment + T&T details into the dashboard shape."""
    barcode = s.get("barcode")
    if not barcode:
        return None
    colli = (tt.get("colli") or {}).get(barcode, {})
    route = colli.get("routeInformation") or {}
    window_from = (route.get("plannedDeliveryTimeWindow") or {}).get("startDateTime") \
        or (colli.get("eta") or {}).get("start") or s.get("deliveryWindowFrom")
    window_to = (route.get("plannedDeliveryTimeWindow") or {}).get("endDateTime") \
        or (colli.get("eta") or {}).get("end") or s.get("deliveryWindowTo")
    status = (colli.get("statusPhase") or {}).get("message") or \
        ("Pakket is bezorgd" if s.get("delivered") else "")
    return {
        "source": "postnl",
        "order_id": barcode,
        "role": role,
        "title": (s.get("title") or s.get("receiverTitle") or "").strip()
                 or f"PostNL {s.get('shipmentType') or 'pakket'}",
        "state": status or ("bezorgd" if s.get("delivered") else "onderweg"),
        "updated_at": s.get("creationDateTime"),
        "pickup": {"type": s["deliveryAddressType"]} if s.get("deliveryAddressType") else {},
        "deadline": window_to or window_from,
        "track": s.get("detailsUrl"),
        "raw": {**s, "tt_status": status},
    }