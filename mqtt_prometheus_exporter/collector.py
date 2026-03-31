"""Custom Prometheus collector that reads from the metric store."""

from __future__ import annotations

import time

from prometheus_client.metrics_core import GaugeMetricFamily

from . import store as _store_mod


class MQTTCollector:
    """Reads from the module-level store and yields Prometheus metrics."""

    def collect(self):  # noqa: ANN201
        """Yield GaugeMetricFamily objects for all non-expired metrics in the store."""
        now = time.time()
        grouped: dict[str, list[tuple[dict[str, str], float]]] = {}
        for _key, meta in _store_mod.store["metrics"].items():
            ttl = meta["ttl"]
            if ttl != -1 and (now - meta["ts"]) > ttl:
                continue
            grouped.setdefault(meta["name"], []).append((dict(meta["labels"]), meta["value"]))

        for name, samples in grouped.items():
            all_label_names: list[str] = []
            for labels, _ in samples:
                for k in labels:
                    if k not in all_label_names:
                        all_label_names.append(k)

            g = GaugeMetricFamily(name, name, labels=all_label_names)
            for labels, value in samples:
                g.add_metric([labels.get(k, "") for k in all_label_names], value)
            yield g
