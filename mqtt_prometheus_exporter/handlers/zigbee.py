import json
import logging

from ..app import app
from ..helpers import celsius_to_fahrenheit, parse_bool
from ..store import store_metric

log = logging.getLogger(__name__)

# (field_type, ttl)  None ttl → TTL_DEFAULT
ZIGBEE_FIELDS: dict[str, tuple[str, int | None]] = {
    "battery": ("float", None),
    "battery_low": ("bool", -1),
    "brightness": ("float", 3600),
    "humidity": ("float", None),
    "linkquality": ("float", -1),
    "occupancy": ("bool", None),
    "tamper": ("bool", -1),
    "temperature": ("float", None),
    "voltage": ("float", None),
}


@app.subscribe("zigbee2mqtt/#")
def handle_zigbee(message):
    parts = message.topic.split("/", 2)
    if len(parts) != 2:
        return  # skip bridge/subtopics

    device = parts[1]

    try:
        data = json.loads(message.payload)
    except (ValueError, TypeError):
        log.warning("zigbee2mqtt: invalid JSON from %s", message.topic)
        return

    if not isinstance(data, dict):
        log.warning("zigbee2mqtt: payload is not a JSON object in %s", message.topic)
        return

    labels = {"device": device}
    for field, (ftype, ttl) in ZIGBEE_FIELDS.items():
        if field not in data:
            continue
        try:
            value = parse_bool(data[field]) if ftype == "bool" else float(data[field])
        except (ValueError, TypeError):
            log.warning("zigbee2mqtt: cannot parse %s=%r from %s", field, data[field], message.topic)
            continue

        if field == "temperature":
            store_metric("zigbee2mqtt_temperature_C", labels, value, ttl)
            store_metric("zigbee2mqtt_temperature_F", labels, celsius_to_fahrenheit(value), ttl)
        else:
            store_metric(f"zigbee2mqtt_{field}", labels, value, ttl)
