"""mqtt_prometheus_exporter — bridges MQTT topics to Prometheus metrics.

Start with: ``gourd mqtt_prometheus_exporter:app``

Gourd imports this package and calls ``app.run_forever()``, owning the main
thread.  The Prometheus HTTP server runs in a separate daemon thread.
"""

import logging
import signal

from gourd import Gourd
from prometheus_client import GC_COLLECTOR, PLATFORM_COLLECTOR, PROCESS_COLLECTOR, REGISTRY

from .collector import make_registry
from .config import (
    HTTP_HOST,
    HTTP_PORT,
    MQTT_CLIENT_ID,
    MQTT_HOST,
    MQTT_PASS,
    MQTT_PORT,
    MQTT_USER,
)
from .handlers import handle_ping, handle_rtl433, handle_weather, handle_zigbee2mqtt, store
from .server import start_server

log = logging.getLogger(__name__)

REGISTRY.unregister(PROCESS_COLLECTOR)
REGISTRY.unregister(PLATFORM_COLLECTOR)
REGISTRY.unregister(GC_COLLECTOR)

registry = make_registry(store)
http_server, _http_thread = start_server(HTTP_HOST, HTTP_PORT, registry)

app = Gourd(
    MQTT_CLIENT_ID,
    mqtt_host=MQTT_HOST,
    mqtt_port=MQTT_PORT,
    username=MQTT_USER,
    password=MQTT_PASS,
)


# SIGTERM is not handled by paho by default; stop the loop cleanly.
def _on_sigterm(signum: int, frame: object) -> None:
    app.loop_stop()


signal.signal(signal.SIGTERM, _on_sigterm)

app.subscribe('ping/#')(handle_ping)
app.subscribe('rtl_433/#')(handle_rtl433)
app.subscribe('zigbee2mqtt/#')(handle_zigbee2mqtt)
app.subscribe('weather/#')(handle_weather)
