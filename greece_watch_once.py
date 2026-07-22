"""Greece Watch, single-run variant for GitHub Actions (2026-07-22).

Same detection logic as the original greece_watch.py (a local, long-running worker in
a different, private project) but adapted for a stateless, ephemeral CI runner: one
poll cycle per invocation, no OS-level PID lock (GitHub Actions' own `concurrency:`
key in the workflow prevents overlapping runs instead), state persisted by committing
the SQLite file back to the repo after each run.

Discovery: Polymarket's own tag_slug=greece filter on gamma-api.polymarket.com/events
(confirmed live to match exactly what a human sees browsing polymarket.com/tag/greece)
plus a bounded, TITLE-ONLY keyword fallback ("greek"/"greece") for any future market
that might not get the tag applied. Title-only, not description-inclusive: a live
smoke test found "Greece" appearing merely inside a long alphabetical country-eligibility
list in some unrelated markets' descriptions, which is not the same as being about
Greece.

Notification: a plain-text push to ntfy.sh (free, no-signup). Requires an explicit
`Content-Type: text/plain; charset=utf-8` header -- without it, ntfy misreads a UTF-8
body containing Greek text/emoji as a binary file attachment instead of a message
(confirmed by a failed manual test before this was added). The topic name is read
from the NTFY_TOPIC environment variable (a GitHub Actions secret), never committed
to this public repo in plain text.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import sqlite3
import sys
from datetime import datetime, timezone

import httpx

GAMMA = "https://gamma-api.polymarket.com"
DB_PATH = "greece_watch.sqlite3"
HTTP_TIMEOUT = 20.0
PRICE_SWING_THRESHOLD = 0.20
KEYWORD_FALLBACK_PAGES = 3

_GREECE_KEYWORD_RE = re.compile(r"\bgreek\b|\bgreece\b", re.I)

SCHEMA = """
CREATE TABLE IF NOT EXISTS tracked_events (
    event_id      TEXT PRIMARY KEY,
    slug          TEXT NOT NULL,
    title         TEXT NOT NULL,
    discovered_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS tracked_markets (
    condition_id             TEXT PRIMARY KEY,
    event_id                 TEXT NOT NULL,
    question                 TEXT NOT NULL,
    label                    TEXT NOT NULL,
    yes_price                REAL,
    last_notified_yes_price  REAL,
    discovered_at            TEXT NOT NULL,
    updated_at               TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS notifications (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           TEXT NOT NULL,
    kind         TEXT NOT NULL,
    event_id     TEXT,
    condition_id TEXT,
    message      TEXT NOT NULL,
    delivered    INTEGER NOT NULL DEFAULT 0
);
"""


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def open_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    return conn


def get_yes_price(market: dict) -> float | None:
    try:
        outcomes = json.loads(market.get("outcomes") or "[]")
        prices = json.loads(market.get("outcomePrices") or "[]")
        idx = outcomes.index("Yes")
        return float(prices[idx])
    except (ValueError, IndexError, TypeError, json.JSONDecodeError):
        return None


def market_label(market: dict) -> str:
    return market.get("groupItemTitle") or market.get("question") or "?"


def is_greece_related(event: dict) -> bool:
    return bool(_GREECE_KEYWORD_RE.search(event.get("title", "")))


async def discover_greece_events(client: httpx.AsyncClient) -> list[dict]:
    out: list[dict] = []
    seen: set = set()

    r = await client.get(f"{GAMMA}/events", params={
        "active": "true", "closed": "false", "limit": 100, "tag_slug": "greece"})
    for ev in r.json():
        eid = ev.get("id")
        if eid and eid not in seen:
            seen.add(eid)
            out.append(ev)

    for page in range(KEYWORD_FALLBACK_PAGES):
        r = await client.get(f"{GAMMA}/events", params={
            "active": "true", "closed": "false", "limit": 100, "offset": page * 100,
            "order": "volume24hr", "ascending": "false"})
        events = r.json()
        if not events:
            break
        for ev in events:
            eid = ev.get("id")
            if eid and eid not in seen and is_greece_related(ev):
                seen.add(eid)
                out.append(ev)
        if len(events) < 100:
            break

    return out


def build_ntfy_notifier(topic: str):
    async def _notify(message: str) -> None:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            resp = await client.post(
                f"https://ntfy.sh/{topic}",
                content=message.encode("utf-8"),
                headers={"Title": "Polymarket Greece Watch",
                         "Content-Type": "text/plain; charset=utf-8"})
            resp.raise_for_status()
    return _notify


async def _record_and_notify(conn, notify_fn, ts, kind, event_id, condition_id, message) -> bool:
    try:
        await notify_fn(message)
        delivered = 1
    except Exception as exc:  # noqa: BLE001
        delivered = 0
        print(f"  notify failed: {type(exc).__name__}: {exc}", flush=True)
    conn.execute(
        "INSERT INTO notifications (ts, kind, event_id, condition_id, message, delivered) "
        "VALUES (?,?,?,?,?,?)", (ts, kind, event_id, condition_id, message, delivered))
    conn.commit()
    return bool(delivered)


async def poll_once(client, conn, notify_fn) -> list[dict]:
    sent: list[dict] = []
    ts = now_iso()
    events = await discover_greece_events(client)

    for ev in events:
        event_id = str(ev.get("id"))
        title = ev.get("title") or ev.get("slug") or event_id
        slug = ev.get("slug") or ""
        is_new_event = conn.execute(
            "SELECT 1 FROM tracked_events WHERE event_id=?", (event_id,)).fetchone() is None

        if is_new_event:
            conn.execute(
                "INSERT OR IGNORE INTO tracked_events (event_id, slug, title, discovered_at) "
                "VALUES (?,?,?,?)", (event_id, slug, title, ts))
            conn.commit()
            message = f"\U0001F1EC\U0001F1F7 Νέο Greece market: {title}\nhttps://polymarket.com/event/{slug}"
            delivered = await _record_and_notify(conn, notify_fn, ts, "NEW_EVENT", event_id, None, message)
            sent.append({"kind": "NEW_EVENT", "delivered": delivered, "message": message})

        for m in ev.get("markets", []):
            cid = m.get("conditionId")
            if not cid or not m.get("active") or m.get("closed"):
                continue
            price = get_yes_price(m)
            label = market_label(m)
            row = conn.execute("SELECT * FROM tracked_markets WHERE condition_id=?", (cid,)).fetchone()

            if row is None:
                conn.execute(
                    """INSERT INTO tracked_markets (condition_id, event_id, question, label,
                       yes_price, last_notified_yes_price, discovered_at, updated_at)
                       VALUES (?,?,?,?,?,?,?,?)""",
                    (cid, event_id, m.get("question", ""), label, price, price, ts, ts))
                conn.commit()
                if not is_new_event and price is not None:
                    message = (f"\U0001F1EC\U0001F1F7 Νέα επιλογή "
                               f"στο '{title}': {label} (Yes: {price:.0%})")
                    delivered = await _record_and_notify(conn, notify_fn, ts, "NEW_CANDIDATE", event_id, cid, message)
                    sent.append({"kind": "NEW_CANDIDATE", "delivered": delivered, "message": message})
                continue

            if price is None:
                continue
            conn.execute("UPDATE tracked_markets SET yes_price=?, updated_at=? WHERE condition_id=?",
                         (price, ts, cid))
            conn.commit()

            last_notified = row["last_notified_yes_price"]
            if last_notified is not None and abs(price - last_notified) >= PRICE_SWING_THRESHOLD:
                direction = "ανέβηκε \U0001F4C8" if price > last_notified else "έπεσε \U0001F4C9"
                message = (f"\U0001F1EC\U0001F1F7 Μεγάλη κίνηση "
                           f"στο '{title}': {label} (Yes) {direction} "
                           f"{last_notified:.0%} → {price:.0%}")
                delivered = await _record_and_notify(conn, notify_fn, ts, "PRICE_SWING", event_id, cid, message)
                if delivered:
                    conn.execute("UPDATE tracked_markets SET last_notified_yes_price=? WHERE condition_id=?",
                                 (price, cid))
                    conn.commit()
                sent.append({"kind": "PRICE_SWING", "delivered": delivered, "message": message})

    return sent


async def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    topic = os.environ.get("NTFY_TOPIC")
    if not topic:
        print("NTFY_TOPIC env var not set", file=sys.stderr)
        return 1

    conn = open_db(DB_PATH)
    notify_fn = build_ntfy_notifier(topic)
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        sent = await poll_once(client, conn, notify_fn)
    conn.close()

    print(f"{len(sent)} notification(s) this run: {[s['kind'] for s in sent]}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
