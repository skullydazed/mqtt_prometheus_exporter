"""Entry point: signal handling and server startup."""

from __future__ import annotations

import logging
import signal
import threading
import time
from http.server import ThreadingHTTPServer

from .config import HTTP_HOST, HTTP_PORT
from .handlers import app
from .server import MetricsHandler

log = logging.getLogger(__name__)


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
