# Requirements: mqtt_prometheus_exporter

## Overview

A Python application that bridges MQTT and Prometheus. It subscribes to MQTT topics using Gourd, tracks metric values with TTLs, and exposes them via a ThreadingHTTPServer `/metrics` endpoint in Prometheus format.

## Pypi Dependencies

- `gourd` — MQTT subscription framework. Do not read the source code! Only read documentation or you will get confused.
    - https://gourd.clueboard.co/
- `prometheus_client` — Prometheus metric types and exposition format
- `json-backed-dict` — (`JBD`) thread-safe iteration-safe persistent store with datetime.date, datetime,time, datetime.datetime, and datetime.timedelta 2-way serialization support
    - https://pypi.org/project/json-backed-dict/

DO NOT HALLUCINATE URLS. Use referenced URLs from trusted sources, or if you have to web search for them.

## Framework

Use gourd as the primary framework for this software. Read the documentation, you will be misled if you attempt to read the source. This is the doc site: https://gourd.clueboard.co/ 

Use python's built-in ThreadingHTTPServer for exposing the /metrics endpoint. 

Gourd should be used in the normal way, by launching `gourd app_module:app`. This means gourd controls the main thread, and a separate thread is spawned for http.

## Store Schema

Backed by a `JBD` instance. Initialized on startup if the file does not exist. Garbage collected on startup to remove metrics older than their TTL. All metrics stored as gauge.

```python
{
    'start_time': datetime,  # debugging field
    'last_write': datetime,  # debugging field
    'message_count': int,    # debugging field
    'metrics': {
        # key is still the full prometheus string, used only for uniqueness
        # e.g. 'temperature_C{room="kitchen",source="zwave"}'
        # Labels should normalized to alphabetical order for dedupe purposes
        'metric_name{label="value",...}': {
            'name':   str,    # base metric name, e.g. 'temperature_C'
            'labels': dict,   # e.g. {'room': 'kitchen', 'source': 'zwave'}
            'ts':     float,  # time.time() at last update
            'ttl':    int,    # seconds; -1 = never expire
            'value':  float
        }
    }
}
```

## Configuration

Defined as module-level constants (not a config file):

```python
MQTT_CLIENT_ID = os.environ.get('MQTT_CLIENT_ID', 'mqtt_prometheus_exporter')
MQTT_HOST = os.environ.get('MQTT_HOST', 'localhost')
MQTT_PORT = int(os.environ.get('MQTT_PORT', '1883'))
MQTT_USER = os.environ.get('MQTT_USER', '')
MQTT_PASS = os.environ.get('MQTT_PASS', '')
STORE_PATH = os.environ.get('STORE_PATH', 'store.json')
TTL_DEFAULT = int(os.environ.get('TTL_DEFAULT', '300'))
HTTP_HOST = os.environ.get('HTTP_HOST', '127.0.0.1')
HTTP_PORT = int(os.environ.get('HTTP_PORT', '5023'))
```

## Temperature Handling

When handling temperature, all temps on the MQTT bus are in Celsius. Sometimes they are explicitly labeled with `_C` on the end, sometimes not. We always export names with `_C` on the end, mutating when necessary.

All temperatures also get converted to Farenheit and exported with `_F` at the end of the name.

## Boolean Handling

Boolean fields need to accept several forms of "truthy" and "falsey" values, all case insensitive: 0/1, yes/no, on/off, true/false

When processed they should be coerced to 0/1.

## Input Sources

All TTLs use TTL_DEFAULT unless specified. All metrics are stored as gauge.

### `ping/#`

- Skip `ping/status`
- Split topic on the first `/` only; everything after is the `destination` label (e.g. `ping/8.8.8.8` → `8.8.8.8`, `ping/some/nested/host` → `some/nested/host`)
- Parse JSON payload; read the `last_1_min` sub-object
- For each of `min`, `avg`, `max`, `percent_dropped`, store as a separate metric
- On parse error or missing keys, log a warning and skip the message
- **TTL:** `TTL_DEFAULT`
- **Destination:** `ping_<stat>{destination="<remainder>"}` e.g. `ping_avg{destination="8.8.8.8"}`

### Input Source: `rtl_433/#`

Listens for messages from the `rtl_433` gateway and processes them based on topic length.

**Topic Extraction Rules:**
- Split the topic by `/`. 
- Discard if the topic has fewer than 6 parts, or if the 3rd element (index 2) is not `'devices'`.
- The **Field** (metric) is always the last element in the topic.

**Topic Mapping:**
- **7-part topic:** `rtl_433/<source>/devices/<model>/<channel>/<id>/<field>`
- **6-part topic:** `rtl_433/<source>/devices/<model>/<id>/<field>` (Default `<channel>` to `'main'`)

**Processing Steps:**
1. Check `<field>` against the **Field Registry**. If it's missing or not marked `forward: yes`, skip silently.
2. Lookup `<id>` in the **Sensor ID Map** using both the extracted `<channel>` and the `<id>`. If unknown, fallback to the numeric `<id>` as a string.
3. If `<field>` is `temperature_C`, also calculate the Fahrenheit equivalent on the fly.
4. **Destination:** Store in the JBD store with the key:  
   `rtl433_<field>{model="<model>",channel="<channel>",sensor="<friendly_or_numeric_id>"}`

#### Sensor ID Map (RTL_433)

To avoid ID collisions across different channels, lookups are scoped by Channel first, then by Numeric ID.

| Channel | Numeric ID | Friendly Name |
| :--- | :--- | :--- |
| `A` | 42 | shed |
| `A` | 7054 | garden |
| `A` | 9181 | backdoor |
| `1` | 23 | neighbor_temp1 |
| `1` | 136 | neighbor_soil1 |
| `3` | 68 | neighbor_soil2 |
| `main` | 111 | outdoor |

#### Field Registry (RTL_433)

| Field | Type | Forward | Devices |
| :--- | :--- | :--- | :--- |
| `active` | bool→0/1 | no | 6045M |
| `battery_ok` | bool→0/1 | **yes** | Tower, 6045M, 5-in-1, Nexus-TH, GT-WT02, Springfield-Soil |
| `button` | bool→0/1 | no | Nexus-TH, GT-WT02, Springfield-Soil |
| `channel` | str | no | Tower, 6045M, 5-in-1, Nexus-TH, GT-WT02, Springfield-Soil |
| `exception` | bool→0/1 | no | Nexus-TH, GT-WT02, Springfield-Soil |
| `humidity` | float | **yes** | Tower, 6045M, 5-in-1, Nexus-TH, GT-WT02 |
| `id` | int | no | Tower, 6045M, 5-in-1, Nexus-TH, GT-WT02, Springfield-Soil |
| `mic` | str | no | Tower, 6045M, 5-in-1, GT-WT02 |
| `moisture` | int | **yes** | Springfield-Soil |
| `rain_mm` | float | **yes** | 5-in-1 |
| `raw_msg` | str | no | 6045M |
| `rfi` | bool→0/1 | no | 6045M |
| `sequence_num` | int | no | 5-in-1 |
| `strike_count` | int | **yes** | 6045M |
| `storm_dist` | int | **yes** | 6045M |
| `temperature_C` | float | **yes** | Tower, 6045M, 5-in-1, Nexus-TH, GT-WT02, Springfield-Soil |
| `temperature_F` | float | no | Tower, 6045M, 5-in-1, Nexus-TH, GT-WT02, Springfield-Soil |
| `time` | datetime | no | Tower, 6045M, 5-in-1, Nexus-TH, GT-WT02, Springfield-Soil |
| `transmit` | str | no | Springfield-Soil |
| `wind_avg_km_h` | float | **yes** | 5-in-1 |
| `wind_avg_m_s` | float | **yes** | Cotech-367959 |
| `wind_dir_deg` | float | **yes** | 5-in-1, Cotech-367959 |
| `wind_max_m_s` | float | **yes** | Cotech-367959 |
| `light_lux` | float | **yes** | Cotech-367959 |
| `uv` | float | **yes** | Cotech-367959 |

### `zigbee2mqtt/<device>` (JSON payload)

| Field | Type | TTL |
|-------|------|-----|
| `battery` | float | default |
| `battery_low` | bool→0/1 | -1 (never) |
| `brightness` | float | 3600 |
| `humidity` | float | default |
| `linkquality` | float | -1 |
| `occupancy` | bool→0/1 | default |
| `tamper` | bool→0/1 | -1 |
| `temperature` -> `temperature_C`, `temperature_F` | float °C + °F variant | default |
| `voltage` | float | default |

- **Destination:** `zigbee2mqtt_<field>{device="<topic-suffix>"}` e.g. `zigbee2mqtt_temperature_C{device="bedroom_sensor"}`

### `weather/<resolution>/<index>/<metric>`

- Weather TTL: 3600 seconds
- Skip `resolution == 'dt'`
- `minutely/precipitation`: accumulate all indices and sum; emit as single metric. Reset the accumulator when index 0 comes in. Accumulator lives in memory, not the JBD.
- `daily` index 0 → `today`, index 1 → `tomorrow`; skip others
- Only emit numeric payloads; skip non-numeric silently
- **Destination:** `weather_precipitation_next_hour`, `weather_today_<metric>`, `weather_tomorrow_<metric>`

## Custom Prometheus Collector

You will need to write a custom Collector to map the Store Schema to the format that prometheus expects. Treat the Store as the source of truth. If you use iteration on JBDs it will create a copy under the hood, avoiding the problem of mutation breaking iteration. This means you don't need to make a copy of the JBD yourself. 

Filter out metrics that are too old per their TTL. Let them persist in the datastructure, they'll be GC'd at the next startup.

You'll need to suppress prometheus_client's default process/platform collectors, or generate_latest() will include them alongside your custom metrics. Call REGISTRY.unregister(PROCESS_COLLECTOR) etc. at startup, or use a separate CollectorRegistry.

## Shutdown Sequencing

Signal handlers for `SIGTERM` and `SIGINT` should set a shared `threading.Event` (e.g. `stop_event`). All loops and blocking calls should check or respect this event.

Shutdown must proceed in this order:

1. **Stop accepting MQTT messages** — call gourd's stop/disconnect method so no new messages enter the pipeline.
2. **Drain in-flight handlers** — gourd dispatches callbacks on its own threads; wait briefly (e.g. a short `join` or `sleep`) to let any currently-executing handlers finish writing to the JBD before proceeding.
3. **Stop the HTTP server** — call `ThreadingHTTPServer.shutdown()`. This blocks until the current request (if any) completes

A `threading.Event` is the preferred coordination primitive — avoid `time.sleep`-based polling in the signal handler itself. The signal handler should only set the event; the main thread does the actual teardown sequence.

Do not call `sys.exit()` directly from a signal handler or a thread — set the event, let the main thread unwind cleanly.

## Non-Functional Requirements

- Log a warning for any unrecognized MQTT topic or unparseable payload
- Use pytest, ruff, and ty for validation
- Do not read gourd's source code or I will cancel your work.
- Do not explore ~/home_automation except for this current directory, ~/home_automation/mqtt_prometheus_exporter
