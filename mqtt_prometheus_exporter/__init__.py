"""mqtt_prometheus_exporter — bridges MQTT topics to Prometheus metrics.

Start with: ``gourd mqtt_prometheus_exporter:app``

Gourd imports this package and calls ``app.run_forever()``, owning the main
thread.  The Prometheus HTTP server runs in a separate daemon thread.
"""

import logging
import signal
from threading import Thread

from prometheus_client import GC_COLLECTOR, PLATFORM_COLLECTOR, PROCESS_COLLECTOR, REGISTRY

from .collector import make_registry
from .config import HTTP_HOST, HTTP_PORT
from .handlers import app, store
from .metrics_http import make_server

log = logging.getLogger(__name__)

REGISTRY.unregister(PROCESS_COLLECTOR)
REGISTRY.unregister(PLATFORM_COLLECTOR)
REGISTRY.unregister(GC_COLLECTOR)

registry = make_registry(store)
_metrics_server = make_server(HTTP_HOST, HTTP_PORT, registry)
Thread(target=_metrics_server.serve_forever, daemon=True, name='metrics-http').start()
log.info('Metrics HTTP server listening on http://%s:%s/metrics', HTTP_HOST, HTTP_PORT)


# SIGTERM is not handled by paho by default; stop the loop cleanly.
def _on_sigterm(signum: int, frame: object) -> None:
    app.loop_stop()


signal.signal(signal.SIGTERM, _on_sigterm)
