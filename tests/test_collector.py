"""Tests for the custom Prometheus collector."""

import time

import pytest
from prometheus_client import generate_latest

from mqtt_prometheus_exporter.collector import make_registry
from mqtt_prometheus_exporter.store import init_store, store_metric


@pytest.fixture
def store(tmp_path):
    return init_store(str(tmp_path / 'store.json'))


class TestMQTTCollector:
    def test_emits_stored_metric(self, store):
        store_metric(store, 'my_gauge', {'label': 'a'}, 42.0, ttl=300)
        registry = make_registry(store)
        output = generate_latest(registry).decode()
        assert 'my_gauge' in output
        assert '42.0' in output

    def test_filters_expired_metric(self, store):
        store['metrics']['old_one'] = {
            'name': 'old_one',
            'labels': {},
            'ts': time.time() - 9999,
            'ttl': 300,
            'value': 1.0,
        }
        registry = make_registry(store)
        output = generate_latest(registry).decode()
        assert 'old_one' not in output

    def test_never_expire_metric_always_emitted(self, store):
        store['metrics']['immortal'] = {
            'name': 'immortal',
            'labels': {},
            'ts': time.time() - 999999,
            'ttl': -1,
            'value': 7.0,
        }
        registry = make_registry(store)
        output = generate_latest(registry).decode()
        assert 'immortal' in output

    def test_label_values_present(self, store):
        store_metric(store, 'room_temp', {'room': 'kitchen'}, 21.0, ttl=300)
        registry = make_registry(store)
        output = generate_latest(registry).decode()
        assert 'kitchen' in output

    def test_multiple_label_sets_same_metric(self, store):
        store_metric(store, 'ping_avg', {'destination': '8.8.8.8'}, 1.0, ttl=300)
        store_metric(store, 'ping_avg', {'destination': '1.1.1.1'}, 2.0, ttl=300)
        registry = make_registry(store)
        output = generate_latest(registry).decode()
        assert '8.8.8.8' in output
        assert '1.1.1.1' in output
        # Should appear as one metric family
        assert output.count('# HELP ping_avg') == 1
