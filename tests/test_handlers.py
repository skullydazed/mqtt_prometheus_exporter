"""Tests for MQTT message handlers."""

import json
import math
from types import SimpleNamespace

import pytest

from mqtt_prometheus_exporter.handlers import (
    handle_ping,
    handle_rtl433,
    handle_weather,
    handle_zigbee2mqtt,
)
from mqtt_prometheus_exporter.store import init_store


def _msg(topic: str, payload: bytes) -> object:
    return SimpleNamespace(topic=topic, payload=payload)


@pytest.fixture
def store(tmp_path):
    return init_store(str(tmp_path / 'store.json'))


class TestHandlePing:
    def test_basic(self, store):
        payload = json.dumps({
            'last_1_min': {'min': 1.0, 'avg': 2.0, 'max': 3.0, 'percent_dropped': 0.0},
        }).encode()
        handle_ping(store, _msg('ping/8.8.8.8', payload))
        metrics = dict(store['metrics'])
        assert 'ping_avg{destination="8.8.8.8"}' in metrics
        assert math.isclose(metrics['ping_avg{destination="8.8.8.8"}']['value'], 2.0)
        assert 'ping_min{destination="8.8.8.8"}' in metrics
        assert 'ping_max{destination="8.8.8.8"}' in metrics
        assert 'ping_percent_dropped{destination="8.8.8.8"}' in metrics

    def test_skips_status(self, store):
        handle_ping(store, _msg('ping/status', b'{}'))
        assert len(store['metrics']) == 0

    def test_nested_topic(self, store):
        payload = json.dumps({
            'last_1_min': {'min': 0.0, 'avg': 1.0, 'max': 2.0, 'percent_dropped': 0.0},
        }).encode()
        handle_ping(store, _msg('ping/some/nested/host', payload))
        assert 'ping_avg{destination="some/nested/host"}' in store['metrics']

    def test_bad_json_logs_warning(self, store, caplog):
        import logging
        with caplog.at_level(logging.WARNING):
            handle_ping(store, _msg('ping/8.8.8.8', b'not-json'))
        assert len(store['metrics']) == 0
        assert caplog.records

    def test_missing_key_logs_warning(self, store, caplog):
        import logging
        with caplog.at_level(logging.WARNING):
            handle_ping(store, _msg('ping/8.8.8.8', json.dumps({'other': 1}).encode()))
        assert len(store['metrics']) == 0
        assert caplog.records


class TestHandleRtl433:
    def test_7part_temperature(self, store):
        handle_rtl433(store, _msg('rtl_433/home/devices/Tower/A/42/temperature_C', b'21.5'))
        metrics = dict(store['metrics'])
        assert 'rtl433_temperature_C{channel="A",model="Tower",sensor="shed"}' in metrics
        assert math.isclose(metrics['rtl433_temperature_C{channel="A",model="Tower",sensor="shed"}']['value'], 21.5)
        # Fahrenheit variant auto-created
        assert 'rtl433_temperature_F{channel="A",model="Tower",sensor="shed"}' in metrics
        f_val = metrics['rtl433_temperature_F{channel="A",model="Tower",sensor="shed"}']['value']
        assert math.isclose(f_val, 21.5 * 9 / 5 + 32)

    def test_6part_defaults_channel_main(self, store):
        handle_rtl433(store, _msg('rtl_433/home/devices/Tower/111/temperature_C', b'20.0'))
        metrics = dict(store['metrics'])
        assert 'rtl433_temperature_C{channel="main",model="Tower",sensor="outdoor"}' in metrics

    def test_unknown_id_uses_raw(self, store):
        handle_rtl433(store, _msg('rtl_433/home/devices/Tower/A/9999/temperature_C', b'15.0'))
        metrics = dict(store['metrics'])
        assert 'rtl433_temperature_C{channel="A",model="Tower",sensor="9999"}' in metrics

    def test_non_forwarded_field_skipped(self, store):
        handle_rtl433(store, _msg('rtl_433/home/devices/Tower/A/42/temperature_F', b'70.0'))
        assert len(store['metrics']) == 0

    def test_unknown_field_skipped(self, store):
        handle_rtl433(store, _msg('rtl_433/home/devices/Tower/A/42/not_a_field', b'1'))
        assert len(store['metrics']) == 0

    def test_too_few_parts_skipped(self, store):
        handle_rtl433(store, _msg('rtl_433/home/devices/Tower', b'1'))
        assert len(store['metrics']) == 0

    def test_wrong_third_part_skipped(self, store):
        handle_rtl433(store, _msg('rtl_433/home/sensors/Tower/A/42/temperature_C', b'20.0'))
        assert len(store['metrics']) == 0

    def test_battery_ok_bool(self, store):
        handle_rtl433(store, _msg('rtl_433/home/devices/Tower/A/42/battery_ok', b'1'))
        metrics = dict(store['metrics'])
        assert 'rtl433_battery_ok{channel="A",model="Tower",sensor="shed"}' in metrics
        assert math.isclose(metrics['rtl433_battery_ok{channel="A",model="Tower",sensor="shed"}']['value'], 1.0)

    def test_humidity_forwarded(self, store):
        handle_rtl433(store, _msg('rtl_433/home/devices/Tower/A/42/humidity', b'65.0'))
        assert 'rtl433_humidity{channel="A",model="Tower",sensor="shed"}' in store['metrics']


class TestHandleZigbee2mqtt:
    def test_temperature_produces_c_and_f(self, store):
        payload = json.dumps({'temperature': 22.5}).encode()
        handle_zigbee2mqtt(store, _msg('zigbee2mqtt/bedroom', payload))
        metrics = dict(store['metrics'])
        assert 'zigbee2mqtt_temperature_C{device="bedroom"}' in metrics
        assert math.isclose(metrics['zigbee2mqtt_temperature_C{device="bedroom"}']['value'], 22.5)
        assert 'zigbee2mqtt_temperature_F{device="bedroom"}' in metrics
        assert math.isclose(metrics['zigbee2mqtt_temperature_F{device="bedroom"}']['value'], 22.5 * 9 / 5 + 32)

    def test_bool_field_coerced(self, store):
        payload = json.dumps({'battery_low': 'yes'}).encode()
        handle_zigbee2mqtt(store, _msg('zigbee2mqtt/bedroom', payload))
        assert math.isclose(store['metrics']['zigbee2mqtt_battery_low{device="bedroom"}']['value'], 1.0)

    def test_linkquality_never_expires(self, store):
        payload = json.dumps({'linkquality': 100}).encode()
        handle_zigbee2mqtt(store, _msg('zigbee2mqtt/bedroom', payload))
        assert store['metrics']['zigbee2mqtt_linkquality{device="bedroom"}']['ttl'] == -1

    def test_brightness_ttl_3600(self, store):
        payload = json.dumps({'brightness': 200}).encode()
        handle_zigbee2mqtt(store, _msg('zigbee2mqtt/living_room', payload))
        assert store['metrics']['zigbee2mqtt_brightness{device="living_room"}']['ttl'] == 3600

    def test_unknown_fields_ignored(self, store):
        payload = json.dumps({'unknown_field': 42}).encode()
        handle_zigbee2mqtt(store, _msg('zigbee2mqtt/bedroom', payload))
        assert len(store['metrics']) == 0

    def test_bad_json(self, store, caplog):
        import logging
        with caplog.at_level(logging.WARNING):
            handle_zigbee2mqtt(store, _msg('zigbee2mqtt/bedroom', b'not-json'))
        assert len(store['metrics']) == 0
        assert caplog.records


class TestHandleWeather:
    def test_daily_index_0_is_today(self, store):
        handle_weather(store, _msg('weather/daily/0/temp_max', b'25.0'))
        assert 'weather_today_temp_max' in store['metrics']

    def test_daily_index_1_is_tomorrow(self, store):
        handle_weather(store, _msg('weather/daily/1/temp_min', b'18.0'))
        assert 'weather_tomorrow_temp_min' in store['metrics']

    def test_daily_other_indices_skipped(self, store):
        handle_weather(store, _msg('weather/daily/2/temp', b'20.0'))
        assert len(store['metrics']) == 0

    def test_resolution_dt_skipped(self, store):
        handle_weather(store, _msg('weather/dt/0/temp', b'20.0'))
        assert len(store['metrics']) == 0

    def test_non_numeric_skipped(self, store):
        handle_weather(store, _msg('weather/daily/0/icon', b'cloudy'))
        assert len(store['metrics']) == 0

    def test_minutely_precipitation_accumulates(self, store):
        import mqtt_prometheus_exporter.handlers as h
        h._minutely_precip_accumulator = 0.0
        handle_weather(store, _msg('weather/minutely/0/precipitation', b'1.0'))
        handle_weather(store, _msg('weather/minutely/1/precipitation', b'2.0'))
        handle_weather(store, _msg('weather/minutely/2/precipitation', b'0.5'))
        assert math.isclose(store['metrics']['weather_precipitation_next_hour']['value'], 3.5)

    def test_minutely_resets_on_index_0(self, store):
        import mqtt_prometheus_exporter.handlers as h
        h._minutely_precip_accumulator = 0.0
        handle_weather(store, _msg('weather/minutely/0/precipitation', b'5.0'))
        handle_weather(store, _msg('weather/minutely/1/precipitation', b'1.0'))
        # Now index 0 again — should reset
        handle_weather(store, _msg('weather/minutely/0/precipitation', b'2.0'))
        handle_weather(store, _msg('weather/minutely/1/precipitation', b'3.0'))
        assert math.isclose(store['metrics']['weather_precipitation_next_hour']['value'], 5.0)

    def test_weather_ttl_is_3600(self, store):
        handle_weather(store, _msg('weather/daily/0/temp', b'20.0'))
        assert store['metrics']['weather_today_temp']['ttl'] == 3600
