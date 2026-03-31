"""Tests for store utilities: make_metric_key, coerce_bool, celsius_to_fahrenheit."""

import math
import time

import pytest

from mqtt_prometheus_exporter.store import (
    celsius_to_fahrenheit,
    coerce_bool,
    init_store,
    make_metric_key,
    store_metric,
)


class TestMakeMetricKey:
    def test_no_labels(self):
        assert make_metric_key('foo', {}) == 'foo'

    def test_single_label(self):
        assert make_metric_key('temp', {'room': 'kitchen'}) == 'temp{room="kitchen"}'

    def test_labels_sorted_alphabetically(self):
        key = make_metric_key('temp', {'z': 'last', 'a': 'first'})
        assert key == 'temp{a="first",z="last"}'

    def test_multiple_labels(self):
        key = make_metric_key('ping_avg', {'destination': '8.8.8.8'})
        assert key == 'ping_avg{destination="8.8.8.8"}'


class TestCoerceBool:
    @pytest.mark.parametrize('value', ['1', 'yes', 'on', 'true', 'YES', 'ON', 'True', 'TRUE', 1, 1.0])
    def test_truthy(self, value):
        assert coerce_bool(value) == 1

    @pytest.mark.parametrize('value', ['0', 'no', 'off', 'false', 'NO', 'OFF', 'False', 'FALSE', 0, 0.0])
    def test_falsey(self, value):
        assert coerce_bool(value) == 0

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            coerce_bool('maybe')

    def test_invalid_number_string(self):
        with pytest.raises(ValueError):
            coerce_bool('2')


class TestCelsiusToFahrenheit:
    def test_freezing(self):
        assert math.isclose(celsius_to_fahrenheit(0.0), 32.0)

    def test_boiling(self):
        assert math.isclose(celsius_to_fahrenheit(100.0), 212.0)

    def test_body_temp(self):
        assert math.isclose(celsius_to_fahrenheit(37.0), 98.6, rel_tol=1e-4)

    def test_negative(self):
        assert math.isclose(celsius_to_fahrenheit(-40.0), -40.0)


class TestStoreInit:
    def test_creates_fresh_store(self, tmp_path):
        path = str(tmp_path / 'store.json')
        store = init_store(path)
        assert 'metrics' in store
        assert 'message_count' in store
        assert store['message_count'] == 0

    def test_gcs_expired_metrics(self, tmp_path):
        path = str(tmp_path / 'store.json')
        store = init_store(path)
        # Manually insert an expired metric
        store['metrics']['old_metric'] = {
            'name': 'old_metric',
            'labels': {},
            'ts': time.time() - 1000,
            'ttl': 300,
            'value': 1.0,
        }
        # Re-init should GC it
        store2 = init_store(path)
        assert 'old_metric' not in store2['metrics']

    def test_preserves_unexpired_metrics(self, tmp_path):
        path = str(tmp_path / 'store.json')
        store = init_store(path)
        store['metrics']['fresh_metric'] = {
            'name': 'fresh_metric',
            'labels': {},
            'ts': time.time(),
            'ttl': 300,
            'value': 42.0,
        }
        store2 = init_store(path)
        assert 'fresh_metric' in store2['metrics']

    def test_never_expire_metrics_kept(self, tmp_path):
        path = str(tmp_path / 'store.json')
        store = init_store(path)
        store['metrics']['immortal'] = {
            'name': 'immortal',
            'labels': {},
            'ts': time.time() - 999999,
            'ttl': -1,
            'value': 1.0,
        }
        store2 = init_store(path)
        assert 'immortal' in store2['metrics']


class TestStoreMetric:
    def test_stores_value(self, tmp_path):
        store = init_store(str(tmp_path / 'store.json'))
        store_metric(store, 'my_gauge', {'label': 'val'}, 3.14, ttl=300)
        key = 'my_gauge{label="val"}'
        assert key in store['metrics']
        entry = store['metrics'][key]
        assert math.isclose(entry['value'], 3.14)
        assert entry['name'] == 'my_gauge'
        assert entry['labels'] == {'label': 'val'}
        assert entry['ttl'] == 300

    def test_increments_message_count(self, tmp_path):
        store = init_store(str(tmp_path / 'store.json'))
        assert store['message_count'] == 0
        store_metric(store, 'gauge', {}, 1.0, ttl=300)
        assert store['message_count'] == 1
        store_metric(store, 'gauge', {}, 2.0, ttl=300)
        assert store['message_count'] == 2
