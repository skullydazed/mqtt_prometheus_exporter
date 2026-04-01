"""MQTT message handlers for each subscribed topic pattern."""

import json
import logging

from gourd import Gourd
from json_backed_dict import JsonBackedDict

from .config import MQTT_CLIENT_ID, MQTT_HOST, MQTT_PASS, MQTT_PORT, MQTT_USER, TTL_DEFAULT
from .store import celsius_to_fahrenheit, coerce_bool, gc_store, init_store, store_metric

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# RTL_433 configuration
# ---------------------------------------------------------------------------

# Sensor ID map: {channel: {numeric_id: friendly_name}}
RTL433_SENSOR_IDS: dict[str, dict[int, str]] = {
    'A': {42: 'shed', 7054: 'garden', 9181: 'backdoor'},
    '1': {23: 'neighbor_temp1', 136: 'neighbor_soil1'},
    '3': {68: 'neighbor_soil2'},
    'main': {111: 'outdoor'},
}

# Field registry: {field: {'forward': bool, 'type': str}}
RTL433_FIELDS: dict[str, dict] = {
    'active':        {'forward': False, 'type': 'bool'},
    'battery_ok':    {'forward': True,  'type': 'bool'},
    'button':        {'forward': False, 'type': 'bool'},
    'channel':       {'forward': False, 'type': 'str'},
    'exception':     {'forward': False, 'type': 'bool'},
    'humidity':      {'forward': True,  'type': 'float'},
    'id':            {'forward': False, 'type': 'int'},
    'mic':           {'forward': False, 'type': 'str'},
    'moisture':      {'forward': True,  'type': 'int'},
    'rain_mm':       {'forward': True,  'type': 'float'},
    'raw_msg':       {'forward': False, 'type': 'str'},
    'rfi':           {'forward': False, 'type': 'bool'},
    'sequence_num':  {'forward': False, 'type': 'int'},
    'strike_count':  {'forward': True,  'type': 'int'},
    'storm_dist':    {'forward': True,  'type': 'int'},
    'temperature_C': {'forward': True,  'type': 'float'},
    'temperature_F': {'forward': False, 'type': 'float'},
    'time':          {'forward': False, 'type': 'datetime'},
    'transmit':      {'forward': False, 'type': 'str'},
    'wind_avg_km_h': {'forward': True,  'type': 'float'},
    'wind_avg_m_s':  {'forward': True,  'type': 'float'},
    'wind_dir_deg':  {'forward': True,  'type': 'float'},
    'wind_max_m_s':  {'forward': True,  'type': 'float'},
    'light_lux':     {'forward': True,  'type': 'float'},
    'uv':            {'forward': True,  'type': 'float'},
}

# ---------------------------------------------------------------------------
# Zigbee2MQTT field configuration
# ---------------------------------------------------------------------------

# Fields that should be coerced to bool (0/1)
ZIGBEE_BOOL_FIELDS = {'battery_low', 'occupancy', 'tamper'}

# Field → TTL override (None means use TTL_DEFAULT)
ZIGBEE_FIELD_TTLS: dict[str, int] = {
    'battery':      TTL_DEFAULT,
    'battery_low':  -1,
    'brightness':   3600,
    'humidity':     TTL_DEFAULT,
    'linkquality':  -1,
    'occupancy':    TTL_DEFAULT,
    'tamper':       -1,
    'temperature':  TTL_DEFAULT,  # handled specially → temperature_C/_F
    'voltage':      TTL_DEFAULT,
}

WEATHER_TTL = 3600

store: JsonBackedDict = init_store()
gc_store(store)
app = Gourd(
    MQTT_CLIENT_ID,
    mqtt_host=MQTT_HOST,
    mqtt_port=MQTT_PORT,
    username=MQTT_USER,
    password=MQTT_PASS,
)
_minutely_precip_accumulator: float = 0.0


@app.subscribe('ping/#')
def handle_ping(msg) -> None:
    """Handle messages on ``ping/#``."""
    if msg.topic == 'ping/status':
        return

    _, destination = msg.topic.split('/', 1)

    try:
        data = json.loads(msg.payload)
        last_1_min = data['last_1_min']
        stats = {
            'min':             last_1_min['min'],
            'avg':             last_1_min['avg'],
            'max':             last_1_min['max'],
            'percent_dropped': last_1_min['percent_dropped'],
        }
    except (json.JSONDecodeError, KeyError, TypeError) as exc:
        log.warning('ping: failed to parse %s payload: %s', msg.topic, exc)
        return

    for stat, value in stats.items():
        store_metric(
            store,
            name=f'ping_{stat}',
            labels={'destination': destination},
            value=float(value),
            ttl=TTL_DEFAULT,
        )


@app.subscribe('rtl_433/#')
def handle_rtl433(msg) -> None:
    """Handle messages on ``rtl_433/#``."""
    parts = msg.topic.split('/')

    if len(parts) < 6 or parts[2] != 'devices':
        return  # silently skip malformed topics

    field = parts[-1]

    if len(parts) == 7:
        # rtl_433/<source>/devices/<model>/<channel>/<id>/<field>
        model = parts[3]
        channel = str(parts[4])
        sensor_id_raw = parts[5]
    elif len(parts) == 6:
        # rtl_433/<source>/devices/<model>/<id>/<field>
        model = parts[3]
        channel = 'main'
        sensor_id_raw = parts[4]
    else:
        return

    # Check field registry
    field_info = RTL433_FIELDS.get(field)
    if field_info is None or not field_info['forward']:
        return

    # Resolve sensor friendly name
    try:
        numeric_id = int(sensor_id_raw)
    except ValueError:
        numeric_id = None

    sensor: str
    if numeric_id is not None and channel in RTL433_SENSOR_IDS and numeric_id in RTL433_SENSOR_IDS[channel]:
        sensor = RTL433_SENSOR_IDS[channel][numeric_id]
    else:
        sensor = sensor_id_raw

    labels = {'model': model, 'channel': channel, 'sensor': sensor}

    # Parse and coerce the payload value
    try:
        raw_value = msg.payload.decode('utf-8').strip()
        field_type = field_info['type']
        if field_type == 'bool':
            value = float(coerce_bool(raw_value))
        else:
            value = float(raw_value)
    except (ValueError, UnicodeDecodeError) as exc:
        log.warning('rtl_433: failed to parse %s payload %r: %s', msg.topic, msg.payload, exc)
        return

    store_metric(store, name=f'rtl433_{field}', labels=labels, value=value, ttl=TTL_DEFAULT)

    # Also export temperature_F for temperature_C fields
    if field == 'temperature_C':
        store_metric(
            store,
            name='rtl433_temperature_F',
            labels=labels,
            value=celsius_to_fahrenheit(value),
            ttl=TTL_DEFAULT,
        )


@app.subscribe('zigbee2mqtt/#')
def handle_zigbee2mqtt(msg) -> None:
    """Handle messages on ``zigbee2mqtt/<device>``."""
    # msg.topic is e.g. "zigbee2mqtt/bedroom_sensor"
    parts = msg.topic.split('/', 1)
    if len(parts) < 2:
        log.warning('zigbee2mqtt: unexpected topic format: %s', msg.topic)
        return
    device = parts[1]

    try:
        data = json.loads(msg.payload)
    except json.JSONDecodeError as exc:
        log.warning('zigbee2mqtt: failed to parse %s payload: %s', msg.topic, exc)
        return

    if not isinstance(data, dict):
        log.warning('zigbee2mqtt: non-dict payload for %s', msg.topic)
        return

    for field, ttl_default in ZIGBEE_FIELD_TTLS.items():
        if field not in data:
            continue
        raw = data[field]
        ttl = ttl_default

        if field == 'temperature':
            # Export as temperature_C and temperature_F
            try:
                temp_c = float(raw)
            except (ValueError, TypeError):
                log.warning('zigbee2mqtt: non-numeric temperature in %s', msg.topic)
                continue
            store_metric(store, 'zigbee2mqtt_temperature_C', {'device': device}, temp_c, ttl)
            store_metric(store, 'zigbee2mqtt_temperature_F', {'device': device}, celsius_to_fahrenheit(temp_c), ttl)
        elif field in ZIGBEE_BOOL_FIELDS:
            try:
                value = float(coerce_bool(raw))
            except ValueError:
                log.warning('zigbee2mqtt: cannot coerce %r to bool for field %s in %s', raw, field, msg.topic)
                continue
            store_metric(store, f'zigbee2mqtt_{field}', {'device': device}, value, ttl)
        else:
            try:
                value = float(raw)
            except (ValueError, TypeError):
                log.warning('zigbee2mqtt: non-numeric value for field %s in %s', field, msg.topic)
                continue
            store_metric(store, f'zigbee2mqtt_{field}', {'device': device}, value, ttl)


@app.subscribe('weather/#')
def handle_weather(msg) -> None:
    """Handle messages on ``weather/<resolution>/<index>/<metric>``."""
    global _minutely_precip_accumulator

    parts = msg.topic.split('/')
    if len(parts) < 4:
        log.warning('weather: unexpected topic format: %s', msg.topic)
        return

    resolution = parts[1]
    index_str = parts[2]
    metric = parts[3]

    if resolution == 'dt':
        return

    # Parse numeric payload; skip non-numeric silently
    try:
        raw = msg.payload.decode('utf-8').strip()
        value = float(raw)
    except (ValueError, UnicodeDecodeError):
        return

    if resolution == 'minutely' and metric == 'precipitation':
        try:
            index = int(index_str)
        except ValueError:
            return
        if index == 0:
            _minutely_precip_accumulator = 0.0
        _minutely_precip_accumulator += value
        store_metric(store, 'weather_precipitation_next_hour', {}, _minutely_precip_accumulator, WEATHER_TTL)

    elif resolution == 'daily':
        try:
            index = int(index_str)
        except ValueError:
            return
        if index == 0:
            prefix = 'today'
        elif index == 1:
            prefix = 'tomorrow'
        else:
            return
        store_metric(store, f'weather_{prefix}_{metric}', {}, value, WEATHER_TTL)

    else:
        # Other resolutions: store as-is under weather_<resolution>_<index>_<metric>
        store_metric(store, f'weather_{resolution}_{index_str}_{metric}', {}, value, WEATHER_TTL)
