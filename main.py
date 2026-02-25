import os
import json
import hashlib
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List

import httpx
from fastapi import FastAPI, Request
import psycopg2
from psycopg2.extras import Json

APP_NAME = "cc-webhook-ingress"

DATABASE_URL = os.environ.get("DATABASE_URL", "")
ROUTES_JSON = os.environ.get("ROUTES_JSON", "[]")

app = FastAPI(title=APP_NAME)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_json(payload: Dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def get_conn():
    if not DATABASE_URL:
        return None
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def init_db():
    conn = get_conn()
    if not conn:
        return
    try:
        with conn:
            with conn.cursor() as cur:
                # Base table
                cur.execute("""
                CREATE TABLE IF NOT EXISTS cc_events (
                    id SERIAL PRIMARY KEY,
                    received_at TIMESTAMP DEFAULT NOW(),
                    event_id TEXT UNIQUE,
                    event_type TEXT,
                    payload JSONB NOT NULL
                );
                """)

                # Minimal trio columns (add if missing)
                cur.execute("ALTER TABLE cc_events ADD COLUMN IF NOT EXISTS ingress_trace_id TEXT;")
                cur.execute("ALTER TABLE cc_events ADD COLUMN IF NOT EXISTS ingress_received_at TIMESTAMPTZ;")
                cur.execute("ALTER TABLE cc_events ADD COLUMN IF NOT EXISTS ingress_payload_sha256 TEXT;")
    finally:
        conn.close()


@app.on_event("startup")
def startup():
    init_db()


@app.get("/health")
def health():
    return {"ok": True, "service": APP_NAME}


def extract_event_id(payload: Dict[str, Any]) -> str:
    # Currencycloud native: body.id
    if isinstance(payload.get("body"), dict):
        body_id = payload["body"].get("id")
        if body_id:
            return str(body_id)

    # fallback: top-level id
    if payload.get("id"):
        return str(payload["id"])

    # last resort: hash
    return sha256_json(payload)


def extract_event_type(payload: Dict[str, Any]) -> str:
    # Currencycloud native: header.notification_type
    if isinstance(payload.get("header"), dict):
        nt = payload["header"].get("notification_type")
        if nt:
            return str(nt)

    # fallback: top-level event_type
    if payload.get("event_type"):
        return str(payload["event_type"])

    return "unknown"


def load_routes() -> List[Dict[str, Any]]:
    try:
        parsed = json.loads(ROUTES_JSON)
        return parsed if isinstance(parsed, list) else []
    except Exception:
        return []


def match_route(event_type: str, route: Dict[str, Any]) -> bool:
    prefixes = route.get("match_prefix", [""])
    if not isinstance(prefixes, list) or not prefixes:
        prefixes = [""]

    for p in prefixes:
        if p == "" or event_type.startswith(p):
            return True
    return False


@app.post("/webhooks/currencycloud")
async def currencycloud_webhook(request: Request):
    payload = await request.json()

    event_id = extract_event_id(payload)
    event_type = extract_event_type(payload)

    # Minimal trio
    trace_id = str(uuid.uuid4())
    received_at = utc_now_iso()
    payload_hash = sha256_json(payload)

    # Persist + dedupe
    conn = get_conn()
    if conn:
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO cc_events (
                            event_id, event_type, payload,
                            ingress_trace_id, ingress_received_at, ingress_payload_sha256
                        )
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (event_id) DO NOTHING;
                        """,
                        (event_id, event_type, Json(payload), trace_id, received_at, payload_hash),
                    )
                    if cur.rowcount == 0:
                        return {
                            "ok": True,
                            "duplicate": True,
                            "event_id": event_id,
                            "event_type": event_type,
                            "ingress_trace_id": trace_id,
                            "ingress_received_at": received_at,
                            "ingress_payload_sha256": payload_hash,
                            "deliveries": []
                        }
        finally:
            conn.close()

    # Forward wrapped payload
    outbound = {
        "ingress": {
            "trace_id": trace_id,
            "received_at": received_at,
            "payload_sha256": payload_hash,
            "event_id": event_id,
            "event_type": event_type,
            "source": "currencycloud"
        },
        "cc": payload
    }

    deliveries = []
    routes = load_routes()

    for r in routes:
        if match_route(event_type, r):
            name = r.get("name", "route")
            url = r.get("url")
            if not url:
                continue

            try:
                async with httpx.AsyncClient(timeout=10.0) as client:
                    resp = await client.post(url, json=outbound)

                deliveries.append({
                    "route": name,
                    "ok": resp.status_code < 300,
                    "status_code": resp.status_code,
                    "body": resp.text[:1000]
                })
            except Exception as e:
                deliveries.append({
                    "route": name,
                    "ok": False,
                    "error": str(e)[:1000]
                })

    return {
        "ok": True,
        "event_id": event_id,
        "event_type": event_type,
        "ingress_trace_id": trace_id,
        "ingress_received_at": received_at,
        "ingress_payload_sha256": payload_hash,
        "deliveries": deliveries
    }
