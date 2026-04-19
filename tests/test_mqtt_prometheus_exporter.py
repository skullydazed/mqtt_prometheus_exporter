"""Tests for mqtt_prometheus_exporter.

Handlers are tested by calling them with a fake message and asserting on
the store. Pure helpers are tested directly.
"""

import json
import os
import tempfile
import time
import types
from datetime import timedelta

import pytest

# Point STORE_PATH at a temp file so the real JBD is used without touching
# any production state.
os.environ["STORE_PATH"] = tempfile.mktemp(suffix=".json")  # ty: ignore[deprecated]

from mqtt_prometheus_exporter.collector import MQTTCollector
from mqtt_prometheus_exporter.handlers.ping import handle_ping
from mqtt_prometheus_exporter.handlers.rtl433 import handle_rtl433
from mqtt_prometheus_exporter.handlers.weather import handle_weather
from mqtt_prometheus_exporter.handlers.zigbee import handle_zigbee
from mqtt_prometheus_exporter.helpers import celsius_to_fahrenheit, make_metric_key, parse_bool
from mqtt_prometheus_exporter.store import gc_store, store

# ---------------------------------------------------------------------------
# Test utilities
# ---------------------------------------------------------------------------


def msg(topic, payload):
    m = types.SimpleNamespace()
    m.topic = topic
    m.payload = payload
    return m


def stored(name, **labels):
    """Return the stored value for a metric, or None if absent."""
    key = make_metric_key(name, labels)
    return store["metrics"].get(key, {}).get("value")


def stored_ttl(name, **labels):
    key = make_metric_key(name, labels)
    return store["metrics"].get(key, {}).get("ttl")


@pytest.fixture(autouse=True)
def clear_store():
    store["metrics"] = {}
    handle_weather(msg("weather/minutely/0/precipitation", "0.0"))
    store["metrics"] = {}
    yield
    store["metrics"] = {}
    handle_weather(msg("weather/minutely/0/precipitation", "0.0"))
    store["metrics"] = {}


# ---------------------------------------------------------------------------
# celsius_to_fahrenheit
# ---------------------------------------------------------------------------


def test_c_to_f_freezing():
    assert celsius_to_fahrenheit(0) == pytest.approx(32.0)


def test_c_to_f_boiling():
    assert celsius_to_fahrenheit(100) == pytest.approx(212.0)


def test_c_to_f_body():
    assert celsius_to_fahrenheit(37) == pytest.approx(98.6)


# ---------------------------------------------------------------------------
# parse_bool
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("v", ["1", "yes", "on", "true", "YES", "True", "ON"])
def test_parse_bool_truthy(v):
    assert parse_bool(v) == 1.0


@pytest.mark.parametrize("v", ["0", "no", "off", "false", "NO", "False", "OFF"])
def test_parse_bool_falsy(v):
    assert parse_bool(v) == 0.0


def test_parse_bool_integer_truthy():
    assert parse_bool(1) == 1.0


def test_parse_bool_integer_falsy():
    assert parse_bool(0) == 0.0


def test_parse_bool_invalid():
    with pytest.raises(ValueError):
        parse_bool("maybe")


# ---------------------------------------------------------------------------
# make_metric_key
# ---------------------------------------------------------------------------


def test_make_metric_key_sorted():
    key = make_metric_key("temp_C", {"room": "kitchen", "source": "zwave"})
    assert key == 'temp_C{room="kitchen",source="zwave"}'


def test_make_metric_key_alphabetical():
    key = make_metric_key("m", {"z": "last", "a": "first"})
    assert key == 'm{a="first",z="last"}'


def test_make_metric_key_no_labels():
    key = make_metric_key("weather_now", {})
    assert key == "weather_now"


# ---------------------------------------------------------------------------
# handle_ping
# ---------------------------------------------------------------------------


def test_ping_stores_all_stats():
    handle_ping(
        msg(
            "ping/8.8.8.8",
            json.dumps({"last_1_min": {"min": 1.1, "avg": 2.2, "max": 3.3, "percent_dropped": 0.0}}),
        )
    )
    assert stored("ping_min", destination="8.8.8.8") == pytest.approx(1.1)
    assert stored("ping_avg", destination="8.8.8.8") == pytest.approx(2.2)
    assert stored("ping_max", destination="8.8.8.8") == pytest.approx(3.3)
    assert stored("ping_percent_dropped", destination="8.8.8.8") == pytest.approx(0.0)


def test_ping_nested_destination():
    handle_ping(msg("ping/some/nested/host", json.dumps({"last_1_min": {"avg": 1.0}})))
    assert stored("ping_avg", destination="some/nested/host") == pytest.approx(1.0)


def test_ping_status_not_stored():
    handle_ping(msg("ping/status", "{}"))
    assert store["metrics"] == {}


def test_ping_bad_json_not_stored():
    handle_ping(msg("ping/8.8.8.8", "not json"))
    assert store["metrics"] == {}


def test_ping_missing_last_1_min_not_stored():
    handle_ping(msg("ping/8.8.8.8", json.dumps({"other": 1})))
    assert store["metrics"] == {}


def test_ping_partial_stats():
    handle_ping(msg("ping/host", json.dumps({"last_1_min": {"avg": 5.0}})))
    assert stored("ping_avg", destination="host") == pytest.approx(5.0)
    assert stored("ping_min", destination="host") is None


# ---------------------------------------------------------------------------
# handle_rtl433
# ---------------------------------------------------------------------------


def test_rtl433_temperature_stores_both_scales():
    handle_rtl433(msg("rtl_433/myhost/devices/Tower/A/42/temperature_C", "21.5"))
    assert stored("rtl433_temperature_C", model="Tower", channel="A", sensor="shed") == pytest.approx(21.5)
    assert stored("rtl433_temperature_F", model="Tower", channel="A", sensor="shed") == pytest.approx(
        celsius_to_fahrenheit(21.5)
    )


def test_rtl433_6part_uses_main_channel():
    handle_rtl433(msg("rtl_433/myhost/devices/Nexus-TH/111/humidity", "65.0"))
    assert stored("rtl433_humidity", model="Nexus-TH", channel="main", sensor="outdoor") == pytest.approx(65.0)


def test_rtl433_bool_field():
    handle_rtl433(msg("rtl_433/h/devices/GT-WT02/A/42/battery_ok", "1"))
    assert stored("rtl433_battery_ok", model="GT-WT02", channel="A", sensor="shed") == 1.0


def test_rtl433_unknown_sensor_id_used_as_name():
    handle_rtl433(msg("rtl_433/h/devices/Tower/B/9999/humidity", "55.0"))
    assert stored("rtl433_humidity", model="Tower", channel="B", sensor="9999") == pytest.approx(55.0)


def test_rtl433_unknown_field_not_stored():
    handle_rtl433(msg("rtl_433/h/devices/Tower/A/42/time", "2024-01-01"))
    assert store["metrics"] == {}


def test_rtl433_too_few_parts_not_stored():
    handle_rtl433(msg("rtl_433/host/devices/Tower", "1"))
    assert store["metrics"] == {}


def test_rtl433_not_devices_not_stored():
    handle_rtl433(msg("rtl_433/host/status/Tower/A/42/humidity", "50"))
    assert store["metrics"] == {}


def test_rtl433_8part_depth_not_stored():
    handle_rtl433(msg("rtl_433/host/devices/Tower/A/42/extra/temperature_C", "21.0"))
    assert store["metrics"] == {}


# ---------------------------------------------------------------------------
# handle_zigbee
# ---------------------------------------------------------------------------


def test_zigbee_temperature_stores_both_scales():
    handle_zigbee(msg("zigbee2mqtt/bedroom_sensor", json.dumps({"temperature": 22.0})))
    assert stored("zigbee2mqtt_temperature_C", device="bedroom_sensor") == pytest.approx(22.0)
    assert stored("zigbee2mqtt_temperature_F", device="bedroom_sensor") == pytest.approx(celsius_to_fahrenheit(22.0))


def test_zigbee_device_label():
    handle_zigbee(msg("zigbee2mqtt/my_device", json.dumps({"battery": 85})))
    assert stored("zigbee2mqtt_battery", device="my_device") == pytest.approx(85.0)


def test_zigbee_bool_field():
    handle_zigbee(msg("zigbee2mqtt/sensor", json.dumps({"occupancy": True})))
    assert stored("zigbee2mqtt_occupancy", device="sensor") == 1.0


def test_zigbee_battery_low_ttl_never():
    handle_zigbee(msg("zigbee2mqtt/sensor", json.dumps({"battery_low": False})))
    assert stored_ttl("zigbee2mqtt_battery_low", device="sensor") == -1


def test_zigbee_brightness_ttl():
    handle_zigbee(msg("zigbee2mqtt/light", json.dumps({"brightness": 200})))
    assert stored_ttl("zigbee2mqtt_brightness", device="light") == 3600


def test_zigbee_bridge_topic_not_stored():
    handle_zigbee(msg("zigbee2mqtt/bridge/info", json.dumps({"state": "online"})))
    assert store["metrics"] == {}


def test_zigbee_slash_device_name():
    handle_zigbee(msg("zigbee2mqtt/bedroom/humidity_temp", json.dumps({"temperature": 22.0})))
    assert stored("zigbee2mqtt_temperature_C", device="bedroom/humidity_temp") == pytest.approx(22.0)


def test_zigbee_unknown_fields_not_stored():
    handle_zigbee(msg("zigbee2mqtt/sensor", json.dumps({"unknown_field": 42})))
    assert store["metrics"] == {}


def test_zigbee_bad_json_not_stored():
    handle_zigbee(msg("zigbee2mqtt/sensor", "not json"))
    assert store["metrics"] == {}


# ---------------------------------------------------------------------------
# handle_weather
# ---------------------------------------------------------------------------


def test_weather_minutely_accumulation():
    handle_weather(msg("weather/minutely/0/precipitation", "0.1"))
    handle_weather(msg("weather/minutely/1/precipitation", "0.2"))
    handle_weather(msg("weather/minutely/2/precipitation", "0.0"))
    assert stored("weather_precipitation_next_hour") == pytest.approx(0.3)


def test_weather_minutely_resets_on_index_0():
    handle_weather(msg("weather/minutely/0/precipitation", "5.0"))
    handle_weather(msg("weather/minutely/1/precipitation", "3.0"))
    handle_weather(msg("weather/minutely/0/precipitation", "1.0"))
    assert stored("weather_precipitation_next_hour") == pytest.approx(1.0)


def test_weather_daily_humidity():
    handle_weather(msg("weather/daily/0/humidity", "65"))
    assert stored("weather_daily_humidity_pct", daily="0") == pytest.approx(65.0)


def test_weather_daily_humidity_day3():
    handle_weather(msg("weather/daily/3/wind_speed", "4.2"))
    assert stored("weather_daily_wind_speed_ms", daily="3") == pytest.approx(4.2)


def test_weather_daily_dew_point_celsius():
    handle_weather(msg("weather/daily/1/dew_point_C", "12.5"))
    assert stored("weather_daily_dew_point_C", daily="1") == pytest.approx(12.5)


def test_weather_daily_sunrise():
    handle_weather(msg("weather/daily/0/sunrise", "1712030400"))
    assert stored("weather_daily_sunrise_ts", daily="0") == pytest.approx(1712030400.0)


def test_weather_daily_index_7():
    handle_weather(msg("weather/daily/7/humidity", "50"))
    assert stored("weather_daily_humidity_pct", daily="7") == pytest.approx(50.0)


def test_weather_daily_unknown_field_not_stored():
    handle_weather(msg("weather/daily/0/dt", "1712030400"))
    assert store["metrics"] == {}


def test_weather_daily_5part_temp_celsius():
    handle_weather(msg("weather/daily/0/temp/morn_C", "14.0"))
    assert stored("weather_daily_temp_C", daily="0", period="morn") == pytest.approx(14.0)


def test_weather_daily_5part_temp_fahrenheit():
    handle_weather(msg("weather/daily/0/temp/max_F", "75.2"))
    assert stored("weather_daily_temp_F", daily="0", period="max") == pytest.approx(75.2)


def test_weather_daily_5part_feels_like_celsius():
    handle_weather(msg("weather/daily/2/feels_like/night_C", "10.0"))
    assert stored("weather_daily_feels_like_C", daily="2", period="night") == pytest.approx(10.0)


def test_weather_daily_5part_bare_period_not_stored():
    handle_weather(msg("weather/daily/0/temp/morn", "14.0"))
    assert store["metrics"] == {}


def test_weather_daily_5part_unrecognized_part4_not_stored():
    handle_weather(msg("weather/daily/0/summary/morn_C", "14.0"))
    assert store["metrics"] == {}


def test_weather_6part_topic_not_stored():
    handle_weather(msg("weather/daily/0/temp/max/extra", "1.0"))
    assert store["metrics"] == {}


def test_weather_dt_not_stored():
    handle_weather(msg("weather/dt/0/timestamp", "1234567890"))
    assert store["metrics"] == {}


def test_weather_non_numeric_not_stored():
    handle_weather(msg("weather/daily/0/description", "Sunny"))
    assert store["metrics"] == {}


def test_weather_wrong_topic_not_stored():
    handle_weather(msg("weather/daily/0", "1.0"))
    assert store["metrics"] == {}


def test_weather_minutely_non_integer_index_not_stored():
    handle_weather(msg("weather/minutely/abc/precipitation", "0.5"))
    assert store["metrics"] == {}


def test_weather_current_humidity():
    handle_weather(msg("weather/current/humidity", "86"))
    assert stored("weather_current_humidity_pct") == pytest.approx(86.0)


def test_weather_current_temp_celsius():
    handle_weather(msg("weather/current/temp_C", "14.16"))
    assert stored("weather_current_temp_C") == pytest.approx(14.16)


def test_weather_current_sunrise():
    handle_weather(msg("weather/current/sunrise", "1712030400"))
    assert stored("weather_current_sunrise_ts") == pytest.approx(1712030400.0)


def test_weather_current_unknown_field_not_stored():
    handle_weather(msg("weather/current/dt", "1712030400"))
    assert store["metrics"] == {}


def test_weather_current_4part_not_stored():
    handle_weather(msg("weather/current/extra/field", "1.0"))
    assert store["metrics"] == {}


def test_weather_hourly_humidity():
    handle_weather(msg("weather/hourly/0/humidity", "80"))
    assert stored("weather_hourly_humidity_pct", hourly="0") == pytest.approx(80.0)


def test_weather_hourly_temp_celsius():
    handle_weather(msg("weather/hourly/6/temp_C", "18.5"))
    assert stored("weather_hourly_temp_C", hourly="6") == pytest.approx(18.5)


def test_weather_hourly_unknown_field_not_stored():
    handle_weather(msg("weather/hourly/0/dt", "1712030400"))
    assert store["metrics"] == {}


def test_weather_hourly_3part_not_stored():
    handle_weather(msg("weather/hourly/0", "1.0"))
    assert store["metrics"] == {}


# ---------------------------------------------------------------------------
# gc_store
# ---------------------------------------------------------------------------


def test_gc_removes_expired_metrics():
    now = time.time()
    store["metrics"] = {
        'fresh{label="a"}': {"name": "fresh", "labels": {}, "ts": now, "ttl": 300, "value": 1.0},
        'expired{label="b"}': {"name": "expired", "labels": {}, "ts": now - 400, "ttl": 300, "value": 2.0},
    }
    removed = gc_store(store)
    assert removed == 1
    assert 'fresh{label="a"}' in store["metrics"]
    assert 'expired{label="b"}' not in store["metrics"]


def test_gc_leaves_permanent_metrics():
    now = time.time()
    store["metrics"] = {
        'perm{label="a"}': {"name": "perm", "labels": {}, "ts": now - 999_999, "ttl": -1, "value": 5.0},
    }
    removed = gc_store(store)
    assert removed == 0
    assert 'perm{label="a"}' in store["metrics"]


def test_gc_empty_store():
    assert gc_store(store) == 0


def test_gc_all_expired():
    now = time.time()
    store["metrics"] = {
        "a{}": {"name": "a", "labels": {}, "ts": now - 100, "ttl": 10, "value": 1.0},
        "b{}": {"name": "b", "labels": {}, "ts": now - 200, "ttl": 10, "value": 2.0},
    }
    removed = gc_store(store)
    assert removed == 2
    assert store["metrics"] == {}


# ---------------------------------------------------------------------------
# store write controls
# ---------------------------------------------------------------------------


def test_store_auto_writes_disabled():
    assert store.write_enabled is False


def test_store_excludes_debug_keys_from_write_triggers():
    store_path = str(store._path)
    original_enabled = store.write_enabled
    original_debug_values = {
        "message_count": store["message_count"],
        "start_time": store["start_time"],
        "last_write": store["last_write"],
    }
    try:
        store.write_enabled = True
        store.save()
        with open(store_path) as f:
            before = f.read()

        store["message_count"] = original_debug_values["message_count"] + 1
        store["start_time"] = original_debug_values["start_time"] + timedelta(seconds=1)
        store["last_write"] = original_debug_values["last_write"] + timedelta(seconds=1)
        with open(store_path) as f:
            after_debug_updates = f.read()
        assert after_debug_updates == before

        store["metrics"]["write_probe"] = {
            "name": "write_probe",
            "labels": {},
            "ts": time.time(),
            "ttl": 60,
            "value": 1.0,
        }
        with open(store_path) as f:
            after_metric = f.read()
        assert after_metric != before
    finally:
        store["metrics"].pop("write_probe", None)
        for key, value in original_debug_values.items():
            store[key] = value
        store.write_enabled = original_enabled
        store.save()


# ---------------------------------------------------------------------------
# MQTTCollector.collect()
# ---------------------------------------------------------------------------


def test_collector_filters_expired_groups_by_name():
    now = time.time()
    store["metrics"] = {
        'm{label="a"}': {"name": "m", "labels": {"label": "a"}, "ts": now, "ttl": 300, "value": 1.0},
        'm{label="b"}': {"name": "m", "labels": {"label": "b"}, "ts": now, "ttl": 300, "value": 2.0},
        'expired{label="x"}': {"name": "expired", "labels": {"label": "x"}, "ts": now - 400, "ttl": 300, "value": 9.0},
        'permanent{label="y"}': {
            "name": "permanent",
            "labels": {"label": "y"},
            "ts": now - 100_000,
            "ttl": -1,
            "value": 5.0,
        },
    }
    families = list(MQTTCollector().collect())
    names = {f.name for f in families}
    assert "m" in names
    assert "permanent" in names
    assert "expired" not in names
    m_family = next(f for f in families if f.name == "m")
    assert len(m_family.samples) == 2
    assert {s.value for s in m_family.samples} == {1.0, 2.0}
