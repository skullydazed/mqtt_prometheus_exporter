"""JBD-backed store: initialization, GC, and metric write helpers."""

import logging
import time
from datetime import datetime

from json_backed_dict import JsonBackedDict

from .config import STORE_PATH, TTL_DEFAULT

log = logging.getLogger(__name__)


def make_metric_key(name: str, labels: dict) -> str:
    """Build the canonical, alphabetically-sorted metric key string."""
    if labels:
        label_str = ','.join(f'{k}="{v}"' for k, v in sorted(labels.items()))
        return f'{name}{{{label_str}}}'
    return name


def celsius_to_fahrenheit(celsius: float) -> float:
    """Convert a Celsius temperature to Fahrenheit."""
    return celsius * 9.0 / 5.0 + 32.0


def coerce_bool(value: object) -> int:
    """Coerce truthy/falsey string (or numeric) values to 0 or 1.

    Accepted (case-insensitive): 0/1, yes/no, on/off, true/false.
    Raises ValueError for unrecognised inputs.
    """
    if isinstance(value, (int, float)):
        return int(bool(value))
    s = str(value).strip().lower()
    if s in ('1', 'yes', 'on', 'true'):
        return 1
    if s in ('0', 'no', 'off', 'false'):
        return 0
    raise ValueError(f'Cannot coerce {value!r} to bool')


def store_metric(
    store: JsonBackedDict,
    name: str,
    labels: dict,
    value: float,
    ttl: int = TTL_DEFAULT,
) -> None:
    """Write a single gauge metric into the JBD store."""
    key = make_metric_key(name, labels)
    store['metrics'][key] = {
        'name': name,
        'labels': labels,
        'ts': time.time(),
        'ttl': ttl,
        'value': float(value),
    }
    store['last_write'] = datetime.now()
    store['message_count'] = store.get('message_count', 0) + 1


def init_store(path: str = STORE_PATH) -> JsonBackedDict:
    """Initialise (or load) the JBD store, then GC expired entries."""
    initial: dict = {
        'start_time': datetime.now(),
        'last_write': datetime.now(),
        'message_count': 0,
        'metrics': {},
    }
    store = JsonBackedDict(path, initial=initial)

    # Backfill any missing top-level keys (store existed before these were added)
    for key, default in (
        ('metrics', {}),
        ('message_count', 0),
        ('start_time', datetime.now()),
        ('last_write', datetime.now()),
    ):
        if key not in store:
            store[key] = default

    # GC metrics that have exceeded their TTL
    now = time.time()
    expired = [k for k, v in store['metrics'].items() if v['ttl'] != -1 and (now - v['ts']) > v['ttl']]
    for k in expired:
        log.info('GC: removing expired metric %s', k)
        del store['metrics'][k]

    return store
