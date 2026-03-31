"""MQTT message handlers and Gourd application."""

from __future__ import annotations

import logging
import threading

from gourd import Gourd

from .config import MQTT_CLIENT_ID, MQTT_HOST, MQTT_PASS, MQTT_PORT, MQTT_USER, TTL_DEFAULT
from .store import write_metric

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# RTL_433 lookup tables
# ---------------------------------------------------------------------------

# (channel, numeric_id) -> friendly_name
RTL433_SENSOR_MAP: dict[tuple[str, int], str] = {
    ("A", 42): "shed",
    ("A", 7054): "garden",
    ("A", 9181): "backdoor",
    ("1", 23): "neighbor_temp1",
    ("1", 136): "neighbor_soil1",
    ("3", 68): "neighbor_soil2",
    ("main", 111): "outdoor",
}

# Fields that should be forwarded; bool fields need coercion
RTL433_FIELD_REGISTRY: dict[str, dict] = {
    "battery_ok": {"type": "bool", "forward": True},
    "humidity": {"type": "float", "forward": True},
    "moisture": {"type": "int", "forward": True},
    "rain_mm": {"type": "float", "forward": True},
    "strike_count": {"type": "int", "forward": True},
    "storm_dist": {"type": "int", "forward": True},
    "temperature_C": {"type": "float", "forward": True},
    "wind_avg_km_h": {"type": "float", "forward": True},
    "wind_avg_m_s": {"type": "float", "forward": True},
    "wind_dir_deg": {"type": "float", "forward": True},
    "wind_max_m_s": {"type": "float", "forward": True},
    "light_lux": {"type": "float", "forward": True},
    "uv": {"type": "float", "forward": True},
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def celsius_to_fahrenheit(c: float) -> float:
    return c * 9 / 5 + 32


def coerce_bool(value: object) -> int | None:
    """Coerce various truthy/falsy representations to 0 or 1.

    Accepts: 0/1, yes/no, on/off, true/false (case-insensitive).
    Returns None when the value is unrecognised.
    """
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        if value in (0, 1):
            return value
        return None
    s = str(value).lower().strip()
    if s in ("1", "true", "yes", "on"):
        return 1
    if s in ("0", "false", "no", "off"):
        return 0
    return None


# ---------------------------------------------------------------------------
# Gourd app
# ---------------------------------------------------------------------------

app = Gourd(
    MQTT_CLIENT_ID,
    mqtt_host=MQTT_HOST,
    mqtt_port=MQTT_PORT,
    username=MQTT_USER,
    password=MQTT_PASS,
    status_enabled=False,
    log_mqtt=False,
)

# ---------------------------------------------------------------------------
# MQTT handlers
# ---------------------------------------------------------------------------


@app.subscribe("ping/#")
def handle_ping(msg) -> None:
    """Handle messages on ``ping/#``."""
    parts = msg.topic.split("/", 1)
    if len(parts) < 2 or parts[1] == "status":
        return
    destination = parts[1]
    try:
        last_1_min = msg.json["last_1_min"]
        for stat in ("min", "avg", "max", "percent_dropped"):
            write_metric(f"ping_{stat}", {"destination": destination}, float(last_1_min[stat]), TTL_DEFAULT)
    except (KeyError, TypeError, ValueError) as exc:
        log.warning("ping handler: could not parse message on %s: %s", msg.topic, exc)


@app.subscribe("rtl_433/#")
def handle_rtl433(msg) -> None:
    """Handle messages on ``rtl_433/#``."""
    parts = msg.topic.split("/")
    if len(parts) not in (6, 7) or parts[2] != "devices":
        return

    if len(parts) == 7:
        model, channel, sensor_id_raw, field = parts[3], parts[4], parts[5], parts[6]
    else:
        model, channel, sensor_id_raw, field = parts[3], "main", parts[4], parts[5]

    field_info = RTL433_FIELD_REGISTRY.get(field)
    if not field_info or not field_info["forward"]:
        return

    try:
        sensor_id_num = int(sensor_id_raw)
    except ValueError:
        sensor_id_num = None

    if sensor_id_num is not None:
        sensor_name = RTL433_SENSOR_MAP.get((channel, sensor_id_num), sensor_id_raw)
    else:
        sensor_name = sensor_id_raw

    raw_payload = msg.payload.strip()
    try:
        value = float(raw_payload)
    except ValueError:
        log.warning("rtl_433 handler: non-numeric payload on %s: %s", msg.topic, raw_payload)
        return

    if field_info["type"] == "bool":
        coerced = coerce_bool(raw_payload)
        if coerced is None:
            log.warning("rtl_433 handler: unrecognised bool value on %s: %s", msg.topic, raw_payload)
            return
        value = float(coerced)

    labels = {"model": model, "channel": channel, "sensor": str(sensor_name)}
    write_metric(f"rtl433_{field}", labels, value, TTL_DEFAULT)
    if field == "temperature_C":
        write_metric("rtl433_temperature_F", labels, celsius_to_fahrenheit(value), TTL_DEFAULT)


@app.subscribe("zigbee2mqtt/#")
def handle_zigbee2mqtt(msg) -> None:
    """Handle messages on ``zigbee2mqtt/<device>``."""
    parts = msg.topic.split("/", 1)
    if len(parts) < 2 or not parts[1]:
        log.warning("zigbee2mqtt handler: malformed topic %s", msg.topic)
        return

    device = parts[1]
    data = msg.json
    if not data:
        log.warning("zigbee2mqtt handler: empty/non-JSON payload on %s", msg.topic)
        return

    field_defs = [
        ("battery", "battery", "float", TTL_DEFAULT),
        ("battery_low", "battery_low", "bool", -1),
        ("brightness", "brightness", "float", 3600),
        ("humidity", "humidity", "float", TTL_DEFAULT),
        ("linkquality", "linkquality", "float", -1),
        ("occupancy", "occupancy", "bool", TTL_DEFAULT),
        ("tamper", "tamper", "bool", -1),
        ("temperature", "temperature", "temp_c", TTL_DEFAULT),
        ("voltage", "voltage", "float", TTL_DEFAULT),
    ]

    for json_key, metric_suffix, field_type, ttl in field_defs:
        if json_key not in data:
            continue

        raw = data[json_key]

        if field_type == "bool":
            coerced = coerce_bool(raw)
            if coerced is None:
                log.warning("zigbee2mqtt handler: unrecognised bool %s=%r on %s", json_key, raw, msg.topic)
                continue
            write_metric(f"zigbee2mqtt_{metric_suffix}", {"device": device}, float(coerced), ttl)

        elif field_type == "temp_c":
            try:
                c_value = float(raw)
            except (TypeError, ValueError):
                log.warning("zigbee2mqtt handler: non-numeric temperature on %s: %r", msg.topic, raw)
                continue
            write_metric("zigbee2mqtt_temperature_C", {"device": device}, c_value, ttl)
            write_metric("zigbee2mqtt_temperature_F", {"device": device}, celsius_to_fahrenheit(c_value), ttl)

        else:
            try:
                write_metric(f"zigbee2mqtt_{metric_suffix}", {"device": device}, float(raw), ttl)
            except (TypeError, ValueError):
                log.warning("zigbee2mqtt handler: non-numeric %s on %s: %r", json_key, msg.topic, raw)


# Accumulator for minutely precipitation; lives in memory, not in the store.
_minutely_precip_accumulator: dict[str, float] = {}
_minutely_precip_lock = threading.Lock()


@app.subscribe("weather/#")
def handle_weather(msg) -> None:
    """Handle messages on ``weather/<resolution>/<index>/<metric>``."""
    parts = msg.topic.split("/")
    if len(parts) != 4:
        return

    _, resolution, index_str, metric = parts
    if resolution == "dt":
        return

    raw = msg.payload.strip()
    try:
        value = float(raw)
    except (ValueError, TypeError):
        return

    weather_ttl = 3600

    if resolution == "minutely" and metric == "precipitation":
        with _minutely_precip_lock:
            if index_str == "0":
                _minutely_precip_accumulator.clear()
                _minutely_precip_accumulator["__sum__"] = value
            else:
                _minutely_precip_accumulator["__sum__"] = _minutely_precip_accumulator.get("__sum__", 0.0) + value
            write_metric("weather_precipitation_next_hour", {}, _minutely_precip_accumulator["__sum__"], weather_ttl)
        return

    if resolution == "daily":
        if index_str == "0":
            period = "today"
        elif index_str == "1":
            period = "tomorrow"
        else:
            return
        write_metric(f"weather_{period}_{metric}", {}, value, weather_ttl)
        return

    write_metric(f"weather_{resolution}_{index_str}_{metric}", {}, value, weather_ttl)
