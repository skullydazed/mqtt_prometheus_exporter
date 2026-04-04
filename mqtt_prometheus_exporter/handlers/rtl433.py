import logging

from ..app import app
from ..helpers import celsius_to_fahrenheit, parse_bool
from ..store import store_metric

log = logging.getLogger(__name__)

RTL_SENSOR_IDS: dict[tuple[str, str], str] = {
    ("A", "42"): "shed",
    ("A", "7054"): "garden",
    ("A", "9181"): "backdoor",
    ("1", "23"): "neighbor_temp1",
    ("1", "136"): "neighbor_soil1",
    ("3", "68"): "neighbor_soil2",
    ("main", "111"): "outdoor",
}

# Only fields with forward=yes
RTL_FIELD_TYPES: dict[str, str] = {
    "battery_ok": "bool",
    "humidity": "float",
    "moisture": "int",
    "rain_mm": "float",
    "strike_count": "int",
    "storm_dist": "int",
    "temperature_C": "float",
    "wind_avg_km_h": "float",
    "wind_avg_m_s": "float",
    "wind_dir_deg": "float",
    "wind_max_m_s": "float",
    "light_lux": "float",
    "uv": "float",
}


@app.subscribe("rtl_433/#")
def handle_rtl433(message):
    parts = message.topic.split("/")
    if len(parts) < 6:
        log.warning("rtl_433: unrecognized topic structure: %s", message.topic)
        return
    if parts[2] != "devices":
        log.warning("rtl_433: unexpected topic segment (expected 'devices'): %s", message.topic)
        return

    field = parts[-1]
    if field not in RTL_FIELD_TYPES:
        return  # silent skip

    if len(parts) == 7:
        model, channel, sensor_id = parts[3], parts[4], parts[5]
    elif len(parts) == 6:
        model, channel, sensor_id = parts[3], "main", parts[4]
    else:
        log.warning("rtl_433: unrecognized topic depth: %s", message.topic)
        return

    try:
        if RTL_FIELD_TYPES[field] == "bool":
            value = parse_bool(message.payload.strip())
        else:
            value = float(message.payload.strip())
    except (ValueError, TypeError):
        log.warning("rtl_433: cannot parse %s=%r from %s", field, message.payload, message.topic)
        return

    labels = {"model": model, "channel": channel, "sensor": RTL_SENSOR_IDS.get((channel, sensor_id), sensor_id)}
    store_metric(f"rtl433_{field}", labels, value)
    if field == "temperature_C":
        store_metric("rtl433_temperature_F", labels, celsius_to_fahrenheit(value))
