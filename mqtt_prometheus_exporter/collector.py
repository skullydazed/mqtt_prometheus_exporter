import logging
import time

from prometheus_client import CollectorRegistry
from prometheus_client.core import GaugeMetricFamily

from .store import store


class MQTTCollector:
    def collect(self):
        now = time.time()
        families: dict[str, tuple[tuple[str, ...], list[tuple[list[str], float]]]] = {}

        for _key, m in store["metrics"].items():
            ttl = m.get("ttl", -1)
            if ttl != -1 and (now - m.get("ts", now)) > ttl:
                continue

            name = m["name"]
            label_names = tuple(sorted(m["labels"].keys()))
            if name not in families:
                families[name] = (label_names, [])
            stored_label_names = families[name][0]
            if label_names != stored_label_names:
                logging.warning(
                    "label mismatch for metric %r: expected %s, got %s — skipping entry",
                    name,
                    stored_label_names,
                    label_names,
                )
                continue
            label_values = [str(m["labels"].get(k, "")) for k in stored_label_names]
            families[name][1].append((label_values, m["value"]))

        for name, (label_names, entries) in families.items():
            g = GaugeMetricFamily(name, f"MQTT metric {name}", labels=list(label_names))
            for label_values, value in entries:
                g.add_metric(label_values, value)
            yield g


registry = CollectorRegistry()
registry.register(MQTTCollector())
