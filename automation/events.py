"""
Real-time event broadcasting for the automation engine.
Uses SSE (Server-Sent Events) — one queue per connected client.
"""
import asyncio
import json
from datetime import datetime

_subscribers: list[asyncio.Queue] = []


async def emit(event_type: str, **data):
    """Broadcast an event to all connected SSE clients."""
    payload = json.dumps({"type": event_type, "time": datetime.utcnow().isoformat(), **data})
    dead = []
    for q in _subscribers:
        try:
            q.put_nowait(payload)
        except asyncio.QueueFull:
            dead.append(q)
    for q in dead:
        _subscribers.remove(q)


def subscribe() -> asyncio.Queue:
    q = asyncio.Queue(maxsize=200)
    _subscribers.append(q)
    return q


def unsubscribe(q: asyncio.Queue):
    if q in _subscribers:
        _subscribers.remove(q)
