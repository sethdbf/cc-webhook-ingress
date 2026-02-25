import os
import json
import hmac
import hashlib
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI, Request, Header, HTTPException
import psycopg2
from psycopg2.extras import Json

APP_NAME = "cc-webhook-ingress"

DATABASE_URL = os.environ.get("DATABASE_URL", "")
CC_WEBHOOK_SECRET = os.environ.get("CC_WEBHOOK_SECRET", "")
VERIFY_SIGNATURE = os.environ.get("VERIFY_SIGNATURE", "false").lower() == "true"

# Routing config for fan-out. Example:
# [
#   {"name":"zapier_all","match_prefix":[""],"url":"https://hooks.zapier.com/hooks/catch/XXX/YYY/"},
#   {"name":"zapier_payments","match_prefix":["payment."],"url":"https://hooks.zapier.com/hooks/catch/AAA/BBB/"}
# ]
ROUTES_JSON = os.environ.get("ROUTES_JSON", "[]")

# Optional: forward everything to one place during transition
ZAPIER_FALLBACK_URL = os.environ.get("ZAPIER_FALLBACK_URL", "")

app = FastAPI(title=APP_NAME)


def get_conn():
    if not DATABASE_URL:
        return None
    # Render Postgres typically requires SSL
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def init_db():
    conn = get_conn()
    if not conn:
        return
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS cc_events (
                        id SERIAL PRIMARY KEY,
                        received_at TIMESTAMP DEFAULT NOW(),
                        event_id TEXT UNIQUE,
                        event_type TEXT,
                        payload JSONB NOT NULL
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS cc_event_deliveries (
                        id SERIAL PRIMARY KEY,
                        created_at TIMESTAMP DEFAULT NOW(),
                        event_id TEXT,
                        route_name TEXT,
                        target_url TEXT,
                        status_code INT,
                        ok BOOLEAN,
                        response_body TEXT
                    );
                    """
                )
    finally:
        conn.close()


@app.on_event("startup")
def startup_event():
    init_db()


def safe_routes(routes_json: str) -> List[Dict[str, Any]]:
    try:
        parsed = json.loads(routes_json)
        return parsed if isinstance(parsed, list) else []
    except Exception:
        return []


def extract_event_id(payload: Dict[str, Any]) -> str:
    for k in ["id", "event_id", "uuid", "notification_id"]:
        v = payload.get(k)
        if v:
            return str(v)

    # fall back: stable hash of payload
    raw = json.dumps(payload, sort_keys=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def extract_event_type(payload: Dict[str, Any]) -> str:
    for k in ["event_type", "type", "event", "name"]:
        v = payload.get(k)
        if v:
            return str(v)
    return "unknown"


def matches(event_type: str, route: Dict[str, Any]) -> bool:
    prefixes = route.get("match_prefix", [""])
    if not isinstance(prefixes, list) or not prefixes:
        prefixes = [""]
    for p in prefixes:
        if p == "" or event_type.startswith(p):
            return True
    return False


def verify_hmac_sha256(raw_body: bytes, signature: str) -> bool:
    """
    NOTE: Providers differ on signature scheme + header name.
    We implement a generic HMAC-SHA256(hex) over the raw body.

    When you confirm Currencycloud's signature header + scheme,
    update this function accordingly.
    """
    if not CC_WEBHOOK_SECRET:
        return False
    mac = hmac.new(CC_WEBHOOK_SECRET.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(mac, signature)


def persist_event(event_id: str, event_type: str, payload: Dict[str, Any]) -> bool:
    """
    Returns True if inserted (new event).
    Returns False if duplicate (already exists).
    """
    conn = get_conn()
    if not conn:
        # allow running without DB while developing; treat as new
        return True
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO cc_events (event_id, event_type, payload)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (event_id) DO NOTHING;
                    """,
                    (event_id, event_type, Json(payload)),
                )
                return cur.rowcount == 1
    finally:
        conn.close()


def persist_delivery(event_id: str, route_name: str, url: str, result: Dict[str, Any]) -> None:
    conn = get_conn()
    if not conn:
        return
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO cc_event_deliveries
                    (event_id, route_name, target_url, status_code, ok, response_body)
                    VALUES (%s, %s, %s, %s, %s, %s);
                    """,
                    (
                        event_id,
                        route_name,
                        url,
                        result.get("status_code"),
                        result.get("ok"),
                        result.get("body"),
                    ),
                )
    finally:
        conn.close()


async def forward(url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.post(url, json=payload)
            return {
                "ok": resp.status_code < 300,
                "status_code": resp.status_code,
                "body": resp.text[:2000],
            }
        except Exception as e:
            return {"ok": False, "status_code": None, "body": f"error: {str(e)[:2000]}"}


@app.get("/health")
def health():
    return {"ok": True, "service": APP_NAME}


@app.post("/webhooks/currencycloud")
async def currencycloud_webhook(
    request: Request,
    x_signature: Optional[str] = Header(default=None),
):
    raw = await request.body()

    if VERIFY_SIGNATURE:
        if not x_signature or not verify_hmac_sha256(raw, x_signature):
            raise HTTPException(status_code=401, detail="invalid signature")

    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid json")

    if not isinstance(payload, dict):
        payload = {"data": payload}

    event_id = extract_event_id(payload)
    event_type = extract_event_type(payload)

    is_new = persist_event(event_id, event_type, payload)
    if not is_new:
        # duplicate delivery (retries, etc): ack quickly, do not re-forward
        return {"ok": True, "duplicate": True, "event_id": event_id, "event_type": event_type}

    routes = safe_routes(ROUTES_JSON)
    matched = [r for r in routes if matches(event_type, r)]

    # Optional fallback to keep existing Zapier flow working
    if ZAPIER_FALLBACK_URL:
        already = any(r.get("url") == ZAPIER_FALLBACK_URL for r in matched)
        if not already:
            matched.append({"name": "zapier_fallback", "url": ZAPIER_FALLBACK_URL, "match_prefix": [""]})

    deliveries = []
    for r in matched:
        name = r.get("name", "route")
        url = r.get("url")
        if not url:
            continue
        result = await forward(url, payload)
        persist_delivery(event_id, name, url, result)
        deliveries.append({"route": name, **result})

    return {"ok": True, "event_id": event_id, "event_type": event_type, "deliveries": deliveries}
