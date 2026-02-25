import os
import json
import hashlib
from typing import Any, Dict, List

import httpx
from fastapi import FastAPI, Request
import psycopg2
from psycopg2.extras import Json

APP_NAME = "cc-webhook-ingress"

DATABASE_URL = os.environ.get("DATABASE_URL", "")
ROUTES_JSON = os.environ.get("ROUTES_JSON", "[]")

app = FastAPI(title=APP_NAME)


def get_conn():
    if not DATABASE_URL:
        return None
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def init_db():
    conn = get_conn()
    if not conn:
        return
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS cc_events (
                id SERIAL PRIMARY KEY,
                received_at TIMESTAMP DEFAULT NOW(),
                event_id TEXT UNIQUE,
                event_type TEXT,
                payload JSONB NOT NULL
            );
            """)
    conn.close()


@app.on_event("startup")
def startup():
    init_db()


@app.get("/health")
def health():
    return {"ok": True, "service": APP_NAME}


def extract_event_id(payload: Dict[str, Any]) -> str:
    # Currencycloud format
    if isinstance(payload.get("body"), dict):
        body_id = payload["body"].get("id")
        if body_id:
            return str(body_id)

    # fallback
    if payload.get("id"):
        return str(payload["id"])

    return hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode()
    ).hexdigest()


def extract_event_type(payload: Dict[str, Any]) -> str:
    # Currencycloud format
    if isinstance(payload.get("header"), dict):
        nt = payload["header"].get("notification_type")
        if nt:
            return str(nt)

    # fallback
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
    for p in prefixes:
        if p == "" or event_type.startswith(p):
            return True
    return False


@app.post("/webhooks/currencycloud")
async def currencycloud_webhook(request: Request):
    payload = await request.json()

    event_id = extract_event_id(payload)
    event_type = extract_event_type(payload)

    # Deduplicate
    conn = get_conn()
    if conn:
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
                    if cur.rowcount == 0:
                        return {
                            "ok": True,
                            "duplicate": True,
                            "event_id": event_id,
                            "event_type": event_type,
                            "deliveries": []
                        }
        finally:
            conn.close()

    deliveries = []
    routes = load_routes()

    for r in routes:
        if match_route(event_type, r):
            try:
                async with httpx.AsyncClient(timeout=10.0) as client:
                    resp = await client.post(r["url"], json=payload)
                deliveries.append({
                    "route": r.get("name"),
                    "ok": resp.status_code < 300,
                    "status_code": resp.status_code,
                    "body": resp.text[:1000]
                })
            except Exception as e:
                deliveries.append({
                    "route": r.get("name"),
                    "ok": False,
                    "error": str(e)
                })

    return {
        "ok": True,
        "event_id": event_id,
        "event_type": event_type,
        "deliveries": deliveries
    }
