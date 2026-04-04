import logging
import time
from datetime import datetime

from json_backed_dict import JsonBackedDict as JBD

from .config import STORE_PATH, TTL_DEFAULT
from .helpers import make_metric_key

store = JBD(STORE_PATH)
store["start_time"] = datetime.now()
store["last_write"] = datetime.now()
store["message_count"] = 0

if "metrics" not in store:
    store["metrics"] = {}
    logging.info("Initialized new store at %s", STORE_PATH)


def gc_store(jbd) -> int:
    """Remove expired metrics from jbd['metrics']. Returns count removed."""
    _now = time.time()
    _expired = 0
    for _k, _v in jbd["metrics"].items():
        if _v.get("ttl", -1) != -1 and (_now - _v.get("ts", _now)) > _v["ttl"]:
            del jbd["metrics"][_k]
            _expired += 1
    if _expired:
        logging.info("GC: removed %d expired metrics on startup", _expired)
    return _expired


# Garbage-collect expired metrics on startup
gc_store(store)


def store_metric(name: str, labels: dict, value: float, ttl: int | None = None) -> None:
    if ttl is None:
        ttl = TTL_DEFAULT
    key = make_metric_key(name, labels)
    store["metrics"][key] = {
        "name": name,
        "labels": dict(labels),
        "ts": time.time(),
        "ttl": ttl,
        "value": float(value),
    }
    store["last_write"] = datetime.now()
    store["message_count"] += 1
    logging.debug('Wrote metric to store: %s: %s', key, store["metrics"][key])
