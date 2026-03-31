"""mqtt_prometheus_exporter — bridges MQTT and Prometheus."""

from __future__ import annotations

import logging
import os
import signal
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from gourd import Gourd
from json_backed_dict import JsonBackedDict
from prometheus_client import GC_COLLECTOR, PLATFORM_COLLECTOR, PROCESS_COLLECTOR, REGISTRY, generate_latest
from prometheus_client.metrics_core import GaugeMetricFamily

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MQTT_CLIENT_ID = os.environ.get("MQTT_CLIENT_ID", "mqtt_prometheus_exporter")
MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USER = os.environ.get("MQTT_USER", "")
MQTT_PASS = os.environ.get("MQTT_PASS", "")
STORE_PATH = os.environ.get("STORE_PATH", "store.json")
TTL_DEFAULT = int(os.environ.get("TTL_DEFAULT", "300"))
HTTP_HOST = os.environ.get("HTTP_HOST", "127.0.0.1")
HTTP_PORT = int(os.environ.get("HTTP_PORT", "5023"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
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


def make_metric_key(name: str, labels: dict[str, str]) -> str:
    """Build the canonical prometheus-style key used for store deduplication.

    Labels are sorted alphabetically to ensure uniqueness regardless of
    insertion order.
    """
    if not labels:
        return name
    label_str = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
    return f"{name}{{{label_str}}}"


# ---------------------------------------------------------------------------
# Store management
# ---------------------------------------------------------------------------


def init_store(path: str) -> JsonBackedDict:
    """Load or create the JBD store, then garbage-collect expired metrics."""
    store = JsonBackedDict(path)

    if "start_time" not in store:
        store["start_time"] = datetime.now()
        store["last_write"] = datetime.now()
        store["message_count"] = 0
        store["metrics"] = {}

    # GC: remove metrics that have exceeded their TTL
    now = time.time()
    metrics = store["metrics"]
    to_delete = []
    for key, meta in metrics.items():
        ttl = meta["ttl"]
        if ttl != -1 and (now - meta["ts"]) > ttl:
            to_delete.append(key)
    for key in to_delete:
        del store["metrics"][key]
        log.debug("GC: expired metric %s", key)

    return store


def write_metric(
    store: JsonBackedDict,
    name: str,
    labels: dict[str, str],
    value: float,
    ttl: int,
) -> None:
    """Write or update a single gauge metric in the JBD store."""
    key = make_metric_key(name, labels)
    store["metrics"][key] = {
        "name": name,
        "labels": labels,
        "ts": time.time(),
        "ttl": ttl,
        "value": float(value),
    }
    store["last_write"] = datetime.now()
    store["message_count"] = store["message_count"] + 1


# ---------------------------------------------------------------------------
# Custom Prometheus Collector
# ---------------------------------------------------------------------------


class MQTTCollector:
    """Reads from the JBD store and yields Prometheus metrics."""

    def __init__(self, store: JsonBackedDict) -> None:
        self._store = store

    def collect(self):  # noqa: ANN201
        now = time.time()
        # Iterating the JBD creates an internal copy, so mutation is safe.
        grouped: dict[str, list[tuple[dict[str, str], float]]] = {}
        for _key, meta in self._store["metrics"].items():
            ttl = meta["ttl"]
            if ttl != -1 and (now - meta["ts"]) > ttl:
                continue
            name = meta["name"]
            grouped.setdefault(name, []).append((dict(meta["labels"]), meta["value"]))

        for name, samples in grouped.items():
            # Collect the full set of label names for this metric
            all_label_names: list[str] = []
            for labels, _ in samples:
                for k in labels:
                    if k not in all_label_names:
                        all_label_names.append(k)

            g = GaugeMetricFamily(name, name, labels=all_label_names)
            for labels, value in samples:
                label_values = [labels.get(k, "") for k in all_label_names]
                g.add_metric(label_values, value)
            yield g


# ---------------------------------------------------------------------------
# MQTT message handlers
# ---------------------------------------------------------------------------


def handle_ping(msg, store: JsonBackedDict) -> None:
    """Handle messages on ``ping/#``."""
    topic: str = msg.topic
    parts = topic.split("/", 1)
    if len(parts) < 2:
        return
    destination = parts[1]

    if destination == "status":
        return

    try:
        data = msg.json
        last_1_min = data["last_1_min"]
        for stat in ("min", "avg", "max", "percent_dropped"):
            value = last_1_min[stat]
            name = f"ping_{stat}"
            write_metric(store, name, {"destination": destination}, float(value), TTL_DEFAULT)
    except (KeyError, TypeError, ValueError) as exc:
        log.warning("ping handler: could not parse message on %s: %s", topic, exc)


def handle_rtl433(msg, store: JsonBackedDict) -> None:
    """Handle messages on ``rtl_433/#``."""
    topic: str = msg.topic
    parts = topic.split("/")

    # Must have 6 or 7 parts, and index 2 must be 'devices'
    if len(parts) not in (6, 7):
        return
    if parts[2] != "devices":
        return

    source = parts[1]  # noqa: F841
    if len(parts) == 7:
        model = parts[3]
        channel = parts[4]
        sensor_id_raw = parts[5]
        field = parts[6]
    else:  # 6 parts
        model = parts[3]
        channel = "main"
        sensor_id_raw = parts[4]
        field = parts[5]

    # Check field registry
    field_info = RTL433_FIELD_REGISTRY.get(field)
    if not field_info or not field_info["forward"]:
        return

    # Parse the numeric sensor ID
    try:
        sensor_id_num = int(sensor_id_raw)
    except ValueError:
        sensor_id_num = None

    # Friendly name lookup
    if sensor_id_num is not None:
        sensor_name = RTL433_SENSOR_MAP.get((channel, sensor_id_num), sensor_id_raw)
    else:
        sensor_name = sensor_id_raw

    # Parse payload value
    raw_payload = msg.payload.strip()
    try:
        value = float(raw_payload)
    except ValueError:
        log.warning("rtl_433 handler: non-numeric payload on %s: %s", topic, raw_payload)
        return

    # Apply bool coercion if needed
    if field_info["type"] == "bool":
        coerced = coerce_bool(raw_payload)
        if coerced is None:
            log.warning("rtl_433 handler: unrecognised bool value on %s: %s", topic, raw_payload)
            return
        value = float(coerced)

    labels = {"model": model, "channel": channel, "sensor": str(sensor_name)}
    write_metric(store, f"rtl433_{field}", labels, value, TTL_DEFAULT)

    # Auto-generate Fahrenheit for temperature_C
    if field == "temperature_C":
        fahrenheit = celsius_to_fahrenheit(value)
        write_metric(store, "rtl433_temperature_F", labels, fahrenheit, TTL_DEFAULT)


def handle_zigbee2mqtt(msg, store: JsonBackedDict) -> None:
    """Handle messages on ``zigbee2mqtt/<device>``."""
    topic: str = msg.topic
    parts = topic.split("/", 1)
    if len(parts) < 2 or not parts[1]:
        log.warning("zigbee2mqtt handler: malformed topic %s", topic)
        return
    device = parts[1]

    data = msg.json
    if not data:
        log.warning("zigbee2mqtt handler: empty/non-JSON payload on %s", topic)
        return

    # Field definitions: (json_key, metric_suffix, type, ttl)
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
                log.warning("zigbee2mqtt handler: unrecognised bool %s=%r on %s", json_key, raw, topic)
                continue
            value = float(coerced)
            write_metric(store, f"zigbee2mqtt_{metric_suffix}", {"device": device}, value, ttl)

        elif field_type == "temp_c":
            try:
                c_value = float(raw)
            except (TypeError, ValueError):
                log.warning("zigbee2mqtt handler: non-numeric temperature on %s: %r", topic, raw)
                continue
            write_metric(store, "zigbee2mqtt_temperature_C", {"device": device}, c_value, ttl)
            write_metric(store, "zigbee2mqtt_temperature_F", {"device": device}, celsius_to_fahrenheit(c_value), ttl)

        else:  # float
            try:
                value = float(raw)
            except (TypeError, ValueError):
                log.warning("zigbee2mqtt handler: non-numeric %s on %s: %r", json_key, topic, raw)
                continue
            write_metric(store, f"zigbee2mqtt_{metric_suffix}", {"device": device}, value, ttl)


# Accumulator for minutely precipitation; lives in memory, not in the JBD.
_minutely_precip_accumulator: dict[str, float] = {}
_minutely_precip_lock = threading.Lock()


def handle_weather(msg, store: JsonBackedDict) -> None:
    """Handle messages on ``weather/<resolution>/<index>/<metric>``."""
    topic: str = msg.topic
    parts = topic.split("/")

    # Expected: weather/<resolution>/<index>/<metric>
    if len(parts) != 4:
        return

    _, resolution, index_str, metric = parts

    if resolution == "dt":
        return

    # Only numeric payloads
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
            total = _minutely_precip_accumulator["__sum__"]
            write_metric(store, "weather_precipitation_next_hour", {}, total, weather_ttl)
        return

    if resolution == "daily":
        if index_str == "0":
            period = "today"
        elif index_str == "1":
            period = "tomorrow"
        else:
            return
        write_metric(store, f"weather_{period}_{metric}", {}, value, weather_ttl)
        return

    # Other resolutions: emit directly (but skip minutely non-precipitation above)
    write_metric(store, f"weather_{resolution}_{index_str}_{metric}", {}, value, weather_ttl)


# ---------------------------------------------------------------------------
# HTTP server
# ---------------------------------------------------------------------------


def make_metrics_handler() -> type[BaseHTTPRequestHandler]:
    """Return a request handler class that serves /metrics.

    Metrics are read from the module-level ``store`` via the ``MQTTCollector``
    registered in ``REGISTRY`` at module import time.
    """

    class MetricsHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path == "/metrics":
                output = generate_latest(REGISTRY)
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                self.send_header("Content-Length", str(len(output)))
                self.end_headers()
                self.wfile.write(output)
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, format: str, *args: object) -> None:  # type: ignore[override]
            log.debug("HTTP %s", format % args)

    return MetricsHandler


# ---------------------------------------------------------------------------
# Module-level app and store
# ---------------------------------------------------------------------------

# Suppress default prometheus collectors
for _collector in (PROCESS_COLLECTOR, PLATFORM_COLLECTOR, GC_COLLECTOR):
    try:
        REGISTRY.unregister(_collector)
    except Exception:
        pass

store = init_store(STORE_PATH)
REGISTRY.register(MQTTCollector(store))

app = Gourd(
    MQTT_CLIENT_ID,
    mqtt_host=MQTT_HOST,
    mqtt_port=MQTT_PORT,
    username=MQTT_USER,
    password=MQTT_PASS,
    status_enabled=False,
    log_mqtt=False,
)


@app.subscribe("ping/#")
def on_ping(msg):
    handle_ping(msg, store)


@app.subscribe("rtl_433/#")
def on_rtl433(msg):
    handle_rtl433(msg, store)


@app.subscribe("zigbee2mqtt/#")
def on_zigbee(msg):
    handle_zigbee2mqtt(msg, store)


@app.subscribe("weather/#")
def on_weather(msg):
    handle_weather(msg, store)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    # Shutdown coordination
    stop_event = threading.Event()

    def _signal_handler(signum: int, frame: object) -> None:
        log.info("Received signal %s, shutting down…", signum)
        stop_event.set()

    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    # Start MQTT in background thread
    app.loop_start()

    # Start HTTP server in background thread
    handler_class = make_metrics_handler()
    http_server = ThreadingHTTPServer((HTTP_HOST, HTTP_PORT), handler_class)
    http_thread = threading.Thread(target=http_server.serve_forever, daemon=True, name="http-server")
    http_thread.start()
    log.info("HTTP /metrics available at http://%s:%d/metrics", HTTP_HOST, HTTP_PORT)

    # Wait for shutdown signal
    stop_event.wait()

    # Shutdown sequence:
    # 1. Stop MQTT
    app.loop_stop()
    # 2. Drain in-flight handlers
    time.sleep(0.5)
    # 3. Stop HTTP server
    http_server.shutdown()
    log.info("Shutdown complete.")


if __name__ == "__main__":
    main()
