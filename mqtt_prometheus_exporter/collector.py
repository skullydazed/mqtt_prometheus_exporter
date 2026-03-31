"""Custom Prometheus Collector that reads live data from the JBD store."""

import time
from collections import defaultdict

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

        # Iterate over a snapshot copy produced by JBD's iteration proxy.
        # Group samples by base metric name so we emit one GaugeMetricFamily
        # per name with potentially many label-sets.
        families: dict[str, GaugeMetricFamily] = {}
        label_keys_by_family: dict[str, list[str]] = {}

        # Collect all valid samples first so we can build label key lists
        samples: list[tuple[str, dict, float]] = []
        for _key, entry in self._store['metrics'].items():
            ttl = entry['ttl']
            ts = entry['ts']
            if ttl != -1 and (now - ts) > ttl:
                continue
            samples.append((entry['name'], dict(entry['labels']), entry['value']))

        # Build consistent label-key lists per metric name using all samples
        label_keys_map: dict[str, list[str]] = defaultdict(list)
        seen_labels: dict[str, set] = defaultdict(set)
        for name, labels, _value in samples:
            for lk in sorted(labels.keys()):
                if lk not in seen_labels[name]:
                    seen_labels[name].add(lk)
                    label_keys_map[name].append(lk)

        # Create GaugeMetricFamily objects
        for name, labels, value in samples:
            if name not in families:
                label_keys = label_keys_map[name]
                families[name] = GaugeMetricFamily(name, name, labels=label_keys)
                label_keys_by_family[name] = label_keys
            label_values = [str(labels.get(k, '')) for k in label_keys_by_family[name]]
            families[name].add_metric(label_values, value)

        yield from families.values()


def make_registry(store: JsonBackedDict) -> CollectorRegistry:
    """Create an isolated CollectorRegistry containing only our collector."""
    registry = CollectorRegistry()
    registry.register(MQTTCollector(store))
    return registry
