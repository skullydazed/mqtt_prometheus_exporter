"""Persistent metric store backed by JsonBackedDict."""

from __future__ import annotations

import logging
import time
from datetime import datetime

from json_backed_dict import JsonBackedDict

from .config import STORE_PATH

log = logging.getLogger(__name__)


def make_metric_key(name: str, labels: dict[str, str]) -> str:
    """Build the canonical prometheus-style key used for store deduplication.

    Labels are sorted alphabetically to ensure uniqueness regardless of
    insertion order.
    """
    if not labels:
        return name
    label_str = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
    return f"{name}{{{label_str}}}"


def init_store(path: str) -> JsonBackedDict:
    """Load or create the JBD store, then garbage-collect expired metrics."""
    s = JsonBackedDict(path)

    if "start_time" not in s:
        s["start_time"] = datetime.now()
        s["last_write"] = datetime.now()
        s["message_count"] = 0
        s["metrics"] = {}

    # GC: remove metrics that have exceeded their TTL
    now = time.time()
    to_delete = [
        key for key, meta in s["metrics"].items()
        if meta["ttl"] != -1 and (now - meta["ts"]) > meta["ttl"]
    ]
    for key in to_delete:
        del s["metrics"][key]
        log.debug("GC: expired metric %s", key)

    return s


def write_metric(name: str, labels: dict[str, str], value: float, ttl: int) -> None:
    """Write or update a single gauge metric in the module-level store."""
    key = make_metric_key(name, labels)
    store["metrics"][key] = {
        "name": name,
        "labels": labels,
        "ts": time.time(),
        "ttl": ttl,
        "value": float(value),
    }
    store["last_write"] = datetime.now()
    store["message_count"] = store["message_count"] + 1


store: JsonBackedDict = init_store(STORE_PATH)
