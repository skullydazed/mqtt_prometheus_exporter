"""Tests for mqtt_prometheus_exporter handlers."""

from __future__ import annotations

import json
import time
from unittest.mock import MagicMock

import pytest

# We need to import the module under test
import mqtt_prometheus_exporter as mpe

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def store(tmp_path):
    """Return a fresh in-memory-backed JBD store for each test."""
    path = str(tmp_path / "store.json")
    return mpe.init_store(path)


def make_msg(topic: str, payload: str | bytes | dict) -> MagicMock:
    """Build a mock GourdMessage."""
    msg = MagicMock()
    msg.topic = topic
    if isinstance(payload, dict):
        raw = json.dumps(payload).encode()
    elif isinstance(payload, str):
        raw = payload.encode()
    else:
        raw = payload

    msg.payload = raw.decode("utf-8").strip()

    # json property mirrors GourdMessage behaviour
    if msg.payload.startswith("{") and msg.payload.endswith("}"):
        try:
            msg.json = json.loads(msg.payload)
        except Exception:
            msg.json = {}
    else:
        msg.json = {}
    return msg


# ---------------------------------------------------------------------------
# Helper tests
# ---------------------------------------------------------------------------


def test_celsius_to_fahrenheit():
    assert mpe.celsius_to_fahrenheit(0) == 32.0
    assert mpe.celsius_to_fahrenheit(100) == 212.0
    assert abs(mpe.celsius_to_fahrenheit(-40) - (-40.0)) < 1e-9


def test_coerce_bool_truthy():
    for val in ("1", "yes", "YES", "on", "ON", "true", "True", True, 1):
        assert mpe.coerce_bool(val) == 1


def test_coerce_bool_falsy():
    for val in ("0", "no", "NO", "off", "OFF", "false", "False", False, 0):
        assert mpe.coerce_bool(val) == 0


def test_coerce_bool_invalid():
    assert mpe.coerce_bool("maybe") is None
    assert mpe.coerce_bool(2) is None


def test_make_metric_key_sorts_labels():
    key = mpe.make_metric_key("my_metric", {"z": "1", "a": "2"})
    assert key == 'my_metric{a="2",z="1"}'


def test_make_metric_key_no_labels():
    assert mpe.make_metric_key("my_metric", {}) == "my_metric"


# ---------------------------------------------------------------------------
# Store initialisation / GC
# ---------------------------------------------------------------------------


def test_init_store_creates_structure(tmp_path):
    path = str(tmp_path / "store.json")
    store = mpe.init_store(path)
    assert "start_time" in store
    assert "last_write" in store
    assert "message_count" in store
    assert "metrics" in store
    assert store["metrics"] == {}


def test_init_store_gc_expired(tmp_path):
    path = str(tmp_path / "store.json")
    store = mpe.init_store(path)
    # Write a metric that's already expired
    old_ts = time.time() - 1000
    store["metrics"]["old_metric"] = {
        "name": "old_metric",
        "labels": {},
        "ts": old_ts,
        "ttl": 300,
        "value": 1.0,
    }
    # A never-expiring metric
    store["metrics"]["forever_metric"] = {
        "name": "forever_metric",
        "labels": {},
        "ts": old_ts,
        "ttl": -1,
        "value": 2.0,
    }

    # Re-initialise the store (simulates restart with GC)
    store2 = mpe.init_store(path)
    assert "old_metric" not in store2["metrics"]
    assert "forever_metric" in store2["metrics"]


# ---------------------------------------------------------------------------
# Ping handler
# ---------------------------------------------------------------------------


def test_ping_normal(store):
    payload = {"last_1_min": {"min": 1.0, "avg": 2.5, "max": 4.0, "percent_dropped": 0.0}}
    msg = make_msg("ping/8.8.8.8", payload)
    mpe.handle_ping(msg, store)

    metrics = store["metrics"]
    assert 'ping_avg{destination="8.8.8.8"}' in metrics
    assert metrics['ping_avg{destination="8.8.8.8"}']['value'] == 2.5
    assert 'ping_min{destination="8.8.8.8"}' in metrics
    assert 'ping_max{destination="8.8.8.8"}' in metrics
    assert 'ping_percent_dropped{destination="8.8.8.8"}' in metrics


def test_ping_skip_status(store):
    msg = make_msg("ping/status", json.dumps({"last_1_min": {"min": 0}}))
    mpe.handle_ping(msg, store)
    assert store["metrics"] == {}


def test_ping_nested_destination(store):
    payload = {"last_1_min": {"min": 1.0, "avg": 2.0, "max": 3.0, "percent_dropped": 0.0}}
    msg = make_msg("ping/some/nested/host", payload)
    mpe.handle_ping(msg, store)
    assert 'ping_avg{destination="some/nested/host"}' in store["metrics"]


def test_ping_bad_json(store):
    msg = make_msg("ping/8.8.8.8", "not json")
    mpe.handle_ping(msg, store)
    assert store["metrics"] == {}


def test_ping_missing_keys(store):
    msg = make_msg("ping/8.8.8.8", {"other": "data"})
    mpe.handle_ping(msg, store)
    assert store["metrics"] == {}


# ---------------------------------------------------------------------------
# RTL_433 handler
# ---------------------------------------------------------------------------


def test_rtl433_7part_temperature(store):
    msg = make_msg("rtl_433/mygateway/devices/Tower/A/42/temperature_C", "21.5")
    mpe.handle_rtl433(msg, store)

    metrics = store["metrics"]
    # shed is the friendly name for channel=A, id=42
    c_key = 'rtl433_temperature_C{channel="A",model="Tower",sensor="shed"}'
    f_key = 'rtl433_temperature_F{channel="A",model="Tower",sensor="shed"}'
    assert c_key in metrics
    assert abs(metrics[c_key]["value"] - 21.5) < 1e-6
    assert f_key in metrics
    assert abs(metrics[f_key]["value"] - mpe.celsius_to_fahrenheit(21.5)) < 1e-6


def test_rtl433_6part_defaults_main_channel(store):
    # 6 parts → channel defaults to 'main', id 111 → 'outdoor'
    msg = make_msg("rtl_433/mygateway/devices/GT-WT02/111/humidity", "55.0")
    mpe.handle_rtl433(msg, store)

    metrics = store["metrics"]
    key = 'rtl433_humidity{channel="main",model="GT-WT02",sensor="outdoor"}'
    assert key in metrics
    assert metrics[key]["value"] == 55.0


def test_rtl433_skip_non_forward_field(store):
    msg = make_msg("rtl_433/gw/devices/Tower/A/42/channel", "A")
    mpe.handle_rtl433(msg, store)
    assert store["metrics"] == {}


def test_rtl433_skip_unknown_field(store):
    msg = make_msg("rtl_433/gw/devices/Tower/A/42/unknown_field", "1")
    mpe.handle_rtl433(msg, store)
    assert store["metrics"] == {}


def test_rtl433_skip_fewer_than_6_parts(store):
    msg = make_msg("rtl_433/gw/devices/Tower", "1")
    mpe.handle_rtl433(msg, store)
    assert store["metrics"] == {}


def test_rtl433_skip_wrong_3rd_element(store):
    msg = make_msg("rtl_433/gw/sensors/Tower/A/42/temperature_C", "20.0")
    mpe.handle_rtl433(msg, store)
    assert store["metrics"] == {}


def test_rtl433_battery_ok_bool(store):
    msg = make_msg("rtl_433/gw/devices/Tower/A/42/battery_ok", "1")
    mpe.handle_rtl433(msg, store)
    key = 'rtl433_battery_ok{channel="A",model="Tower",sensor="shed"}'
    assert key in store["metrics"]
    assert store["metrics"][key]["value"] == 1.0


def test_rtl433_unknown_sensor_uses_raw_id(store):
    msg = make_msg("rtl_433/gw/devices/Tower/A/9999/humidity", "70.0")
    mpe.handle_rtl433(msg, store)
    key = 'rtl433_humidity{channel="A",model="Tower",sensor="9999"}'
    assert key in store["metrics"]


# ---------------------------------------------------------------------------
# Zigbee2MQTT handler
# ---------------------------------------------------------------------------


def test_zigbee_temperature(store):
    msg = make_msg("zigbee2mqtt/bedroom_sensor", {"temperature": 22.0})
    mpe.handle_zigbee2mqtt(msg, store)

    metrics = store["metrics"]
    c_key = 'zigbee2mqtt_temperature_C{device="bedroom_sensor"}'
    f_key = 'zigbee2mqtt_temperature_F{device="bedroom_sensor"}'
    assert c_key in metrics
    assert metrics[c_key]["value"] == 22.0
    assert f_key in metrics
    assert abs(metrics[f_key]["value"] - mpe.celsius_to_fahrenheit(22.0)) < 1e-6


def test_zigbee_bool_fields(store):
    msg = make_msg("zigbee2mqtt/sensor1", {"battery_low": "yes", "occupancy": "off", "tamper": False})
    mpe.handle_zigbee2mqtt(msg, store)

    metrics = store["metrics"]
    assert metrics['zigbee2mqtt_battery_low{device="sensor1"}']['value'] == 1.0
    assert metrics['zigbee2mqtt_occupancy{device="sensor1"}']['value'] == 0.0
    assert metrics['zigbee2mqtt_tamper{device="sensor1"}']['value'] == 0.0


def test_zigbee_ttls(store):
    msg = make_msg(
        "zigbee2mqtt/s1",
        {
            "battery": 95.0,
            "battery_low": False,
            "brightness": 200.0,
            "linkquality": 80.0,
            "tamper": False,
            "voltage": 3.1,
        },
    )
    mpe.handle_zigbee2mqtt(msg, store)

    metrics = store["metrics"]
    assert metrics['zigbee2mqtt_battery{device="s1"}']['ttl'] == mpe.TTL_DEFAULT
    assert metrics['zigbee2mqtt_battery_low{device="s1"}']['ttl'] == -1
    assert metrics['zigbee2mqtt_brightness{device="s1"}']['ttl'] == 3600
    assert metrics['zigbee2mqtt_linkquality{device="s1"}']['ttl'] == -1
    assert metrics['zigbee2mqtt_tamper{device="s1"}']['ttl'] == -1
    assert metrics['zigbee2mqtt_voltage{device="s1"}']['ttl'] == mpe.TTL_DEFAULT


def test_zigbee_non_json(store):
    msg = make_msg("zigbee2mqtt/sensor1", "not json")
    mpe.handle_zigbee2mqtt(msg, store)
    assert store["metrics"] == {}


# ---------------------------------------------------------------------------
# Weather handler
# ---------------------------------------------------------------------------


def test_weather_daily_today(store):
    msg = make_msg("weather/daily/0/temp_max", "25.0")
    mpe.handle_weather(msg, store)
    assert "weather_today_temp_max" in store["metrics"]
    assert store["metrics"]["weather_today_temp_max"]["value"] == 25.0


def test_weather_daily_tomorrow(store):
    msg = make_msg("weather/daily/1/temp_max", "20.0")
    mpe.handle_weather(msg, store)
    assert "weather_tomorrow_temp_max" in store["metrics"]


def test_weather_daily_skip_other_index(store):
    msg = make_msg("weather/daily/2/temp_max", "18.0")
    mpe.handle_weather(msg, store)
    assert store["metrics"] == {}


def test_weather_dt_skipped(store):
    msg = make_msg("weather/dt/0/value", "12345")
    mpe.handle_weather(msg, store)
    assert store["metrics"] == {}


def test_weather_non_numeric_skipped(store):
    msg = make_msg("weather/daily/0/desc", "sunny")
    mpe.handle_weather(msg, store)
    assert store["metrics"] == {}


def test_weather_minutely_precipitation_accumulates(store, monkeypatch):
    # Reset accumulator before test
    mpe._minutely_precip_accumulator.clear()

    msg0 = make_msg("weather/minutely/0/precipitation", "0.1")
    mpe.handle_weather(msg0, store)
    assert abs(store["metrics"]["weather_precipitation_next_hour"]["value"] - 0.1) < 1e-6

    msg1 = make_msg("weather/minutely/1/precipitation", "0.2")
    mpe.handle_weather(msg1, store)
    assert abs(store["metrics"]["weather_precipitation_next_hour"]["value"] - 0.3) < 1e-6

    # Index 0 resets accumulator
    msg0b = make_msg("weather/minutely/0/precipitation", "0.5")
    mpe.handle_weather(msg0b, store)
    assert abs(store["metrics"]["weather_precipitation_next_hour"]["value"] - 0.5) < 1e-6


# ---------------------------------------------------------------------------
# Custom collector
# ---------------------------------------------------------------------------


def test_collector_filters_expired(store):
    now = time.time()
    store["metrics"]["live{l=\"v\"}"] = {
        "name": "live",
        "labels": {"l": "v"},
        "ts": now,
        "ttl": 300,
        "value": 42.0,
    }
    store["metrics"]["dead{l=\"v\"}"] = {
        "name": "dead",
        "labels": {"l": "v"},
        "ts": now - 1000,
        "ttl": 300,
        "value": 99.0,
    }

    collector = mpe.MQTTCollector(store)
    results = list(collector.collect())
    names = [f.name for f in results]
    assert "live" in names
    assert "dead" not in names


def test_collector_never_expire(store):
    old_ts = time.time() - 999999
    store["metrics"]["forever"] = {
        "name": "forever",
        "labels": {},
        "ts": old_ts,
        "ttl": -1,
        "value": 7.0,
    }
    collector = mpe.MQTTCollector(store)
    results = list(collector.collect())
    names = [f.name for f in results]
    assert "forever" in names
