"""Custom Prometheus Collector that reads live data from the JBD store."""

import time

from json_backed_dict import JsonBackedDict
from prometheus_client.core import GaugeMetricFamily
from prometheus_client.registry import CollectorRegistry


class MQTTCollector:
    """Yields Prometheus gauge metrics from the JBD store.

    Metrics whose TTL has elapsed are silently skipped (they will be GC'd on
    the next process start).  Metrics with ``ttl == -1`` never expire.
    """

    def __init__(self, store: JsonBackedDict) -> None:
        self._store = store

    def collect(self):  # type: ignore[override]
        now = time.time()
        families: dict[str, GaugeMetricFamily] = {}

        for _key, entry in self._store['metrics'].items():
            if entry['ttl'] != -1 and (now - entry['ts']) > entry['ttl']:
                continue

            name = entry['name']
            labels = entry['labels']
            label_keys = sorted(labels.keys())

            if name not in families:
                families[name] = GaugeMetricFamily(name, name, labels=label_keys)

            families[name].add_metric([str(labels[k]) for k in label_keys], entry['value'])

        yield from families.values()


def make_registry(store: JsonBackedDict) -> CollectorRegistry:
    """Create an isolated CollectorRegistry containing only our collector."""
    registry = CollectorRegistry()
    registry.register(MQTTCollector(store))
    return registry
