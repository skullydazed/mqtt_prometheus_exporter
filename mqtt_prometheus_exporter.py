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
# Store
# ---------------------------------------------------------------------------


def init_store(path: str) -> JsonBackedDict:
    """Load or create the JBD store, then garbage-collect expired metrics."""
    s = JsonBackedDict(path)

    if "start_time" not in s:
        s["start_time"] = datetime.now()
        s["last_write"] = datetime.now()
        s["message_count"] = 0
        s["metrics"] = {}

    # GC: remove metrics that have exceeded their TTL
    now = time.time()
    to_delete = [
        key for key, meta in s["metrics"].items()
        if meta["ttl"] != -1 and (now - meta["ts"]) > meta["ttl"]
    ]
    for key in to_delete:
        del s["metrics"][key]
        log.debug("GC: expired metric %s", key)

    return s


def write_metric(name: str, labels: dict[str, str], value: float, ttl: int) -> None:
    """Write or update a single gauge metric in the module-level store."""
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
# Prometheus collector
# ---------------------------------------------------------------------------


class MQTTCollector:
    """Reads from the module-level store and yields Prometheus metrics."""

    def collect(self):  # noqa: ANN201
        now = time.time()
        grouped: dict[str, list[tuple[dict[str, str], float]]] = {}
        for _key, meta in store["metrics"].items():
            ttl = meta["ttl"]
            if ttl != -1 and (now - meta["ts"]) > ttl:
                continue
            grouped.setdefault(meta["name"], []).append((dict(meta["labels"]), meta["value"]))

        for name, samples in grouped.items():
            all_label_names: list[str] = []
            for labels, _ in samples:
                for k in labels:
                    if k not in all_label_names:
                        all_label_names.append(k)

            g = GaugeMetricFamily(name, name, labels=all_label_names)
            for labels, value in samples:
                g.add_metric([labels.get(k, "") for k in all_label_names], value)
            yield g


# ---------------------------------------------------------------------------
# Module-level store and Prometheus setup
# ---------------------------------------------------------------------------

for _collector in (PROCESS_COLLECTOR, PLATFORM_COLLECTOR, GC_COLLECTOR):
    try:
        REGISTRY.unregister(_collector)
    except Exception:
        pass

store = init_store(STORE_PATH)
REGISTRY.register(MQTTCollector())

# ---------------------------------------------------------------------------
# Gourd app and MQTT handlers
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


# ---------------------------------------------------------------------------
# HTTP server
# ---------------------------------------------------------------------------


class MetricsHandler(BaseHTTPRequestHandler):
    """Serves ``/metrics`` in Prometheus text exposition format."""

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


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    stop_event = threading.Event()

    def _signal_handler(signum: int, frame: object) -> None:
        log.info("Received signal %s, shutting down…", signum)
        stop_event.set()

    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    app.loop_start()

    http_server = ThreadingHTTPServer((HTTP_HOST, HTTP_PORT), MetricsHandler)
    threading.Thread(target=http_server.serve_forever, daemon=True, name="http-server").start()
    log.info("HTTP /metrics available at http://%s:%d/metrics", HTTP_HOST, HTTP_PORT)

    stop_event.wait()

    app.loop_stop()
    time.sleep(0.5)
    http_server.shutdown()
    log.info("Shutdown complete.")


if __name__ == "__main__":
    main()
