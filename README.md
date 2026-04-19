# mqtt_prometheus_exporter

Bridges MQTT topics to a Prometheus `/metrics` endpoint. Subscribes to several MQTT topic families, stores metric values with TTLs, and exposes them as gauges via a threaded HTTP server.

## Requirements

- Python 3.11+
- An MQTT broker (e.g. Mosquitto)

## Installation

```bash
pip install -e .
```

## Running

```bash
gourd mqtt_prometheus_exporter:app
```

The HTTP server starts on `http://127.0.0.1:5023/metrics` by default.

## Configuration

All settings are environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `MQTT_CLIENT_ID` | `mqtt_prometheus_exporter` | MQTT client identifier |
| `MQTT_HOST` | `localhost` | MQTT broker hostname |
| `MQTT_PORT` | `1883` | MQTT broker port |
| `MQTT_USER` | *(empty)* | MQTT username |
| `MQTT_PASS` | *(empty)* | MQTT password |
| `STORE_PATH` | `store.json` | Path to persistent metric store |
| `TTL_DEFAULT` | `300` | Default metric TTL in seconds |
| `STORE_WRITE_INTERVAL` | `300` | Interval in seconds for periodic store writes (must be `>= 1.0`) |
| `HTTP_HOST` | `127.0.0.1` | HTTP server bind address |
| `HTTP_PORT` | `5023` | HTTP server port |

## MQTT Topic Handlers

### `ping/#`

Reads ping stats from `ping2mqtt`. Payload is JSON with a `last_1_min` object containing `min`, `avg`, `max`, and `percent_dropped`. Skips `ping/status`.

```
ping_avg{destination="8.8.8.8"}
ping_min{destination="8.8.8.8"}
ping_max{destination="8.8.8.8"}
ping_percent_dropped{destination="8.8.8.8"}
```

### `rtl_433/#`

Reads sensor data from an RTL-SDR gateway. Topics follow the pattern `rtl_433/<source>/devices/<model>/[<channel>/]<id>/<field>`. Only whitelisted fields are forwarded. Sensor IDs are mapped to friendly names.

```
rtl433_temperature_C{model="Nexus-TH",channel="A",sensor="shed"}
rtl433_temperature_F{model="Nexus-TH",channel="A",sensor="shed"}
rtl433_humidity{model="Nexus-TH",channel="A",sensor="shed"}
rtl433_battery_ok{model="Nexus-TH",channel="A",sensor="shed"}
```

### `zigbee2mqtt/<device>`

Reads JSON payloads from Zigbee2MQTT. Extracts battery, humidity, temperature (both °C and °F), occupancy, brightness, linkquality, tamper, and voltage.

```
zigbee2mqtt_temperature_C{device="bedroom_sensor"}
zigbee2mqtt_temperature_F{device="bedroom_sensor"}
zigbee2mqtt_battery{device="bedroom_sensor"}
zigbee2mqtt_occupancy{device="motion_sensor"}
```

### `weather/<resolution>/<index>/<metric>`

Reads weather data from `openweathermaps2mqtt`. Accumulates minutely precipitation across a 60-minute rolling window; surfaces today's and tomorrow's daily forecasts.

```
weather_precipitation_next_hour
weather_today_temp
weather_tomorrow_temp
```

## Metric Store

Metrics are persisted in a JSON file (default: `store.json`). Each metric has a TTL; expired metrics are filtered from `/metrics` responses and garbage-collected at startup. Set `ttl = -1` for metrics that should never expire (e.g. `linkquality`, `tamper`).

## Development

```bash
pip install -e ".[dev]"
pytest
ruff check .
ty check
```
