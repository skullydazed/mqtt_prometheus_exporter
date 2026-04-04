"""MQTT → Prometheus exporter.

Subscribes to MQTT topics via Gourd, stores gauge metrics with TTLs in a
JBD-backed store, and exposes /metrics via ThreadingHTTPServer.

Launch with: gourd mqtt_prometheus_exporter:app
"""

import logging
import signal
import threading
import time

from .app import app
from .config import HTTP_HOST, HTTP_PORT
from .http_server import httpd

stop_event = threading.Event()


def _signal_handler(sig, frame):
    logging.info("Received signal %s, initiating shutdown...", sig)
    stop_event.set()


signal.signal(signal.SIGTERM, _signal_handler)
signal.signal(signal.SIGINT, _signal_handler)


def _shutdown_worker():
    stop_event.wait()
    logging.info("Shutdown: stopping MQTT...")
    app.loop_stop()
    time.sleep(0.5)  # drain in-flight handlers
    logging.info("Shutdown: stopping HTTP server...")
    httpd.shutdown()
    logging.info("Shutdown complete")


_http_thread = threading.Thread(target=httpd.serve_forever, name="http", daemon=True)
_http_thread.start()
logging.info("HTTP metrics server started on %s:%d", HTTP_HOST, HTTP_PORT)

_shutdown_thread = threading.Thread(target=_shutdown_worker, name="shutdown", daemon=False)
_shutdown_thread.start()
