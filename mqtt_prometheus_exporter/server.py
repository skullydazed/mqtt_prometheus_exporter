"""ThreadingHTTPServer exposing the /metrics endpoint."""

import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from prometheus_client.registry import CollectorRegistry

log = logging.getLogger(__name__)


class MetricsHandler(BaseHTTPRequestHandler):
    """Minimal HTTP handler that serves Prometheus metrics at /metrics."""

    registry: CollectorRegistry  # set as class variable before use

    def do_GET(self) -> None:  # noqa: N802
        if self.path != '/metrics':
            self.send_response(404)
            self.end_headers()
            return

        output = generate_latest(self.registry)
        self.send_response(200)
        self.send_header('Content-Type', CONTENT_TYPE_LATEST)
        self.send_header('Content-Length', str(len(output)))
        self.end_headers()
        self.wfile.write(output)

    def log_message(self, fmt: str, *args: object) -> None:
        log.debug(fmt, *args)


def make_server(host: str, port: int, registry: CollectorRegistry) -> ThreadingHTTPServer:
    """Create a ThreadingHTTPServer bound to *host*:*port*."""

    class _Handler(MetricsHandler):
        pass

    _Handler.registry = registry
    server = ThreadingHTTPServer((host, port), _Handler)
    return server


def start_server(host: str, port: int, registry: CollectorRegistry) -> tuple[ThreadingHTTPServer, Thread]:
    """Start the metrics HTTP server in a daemon thread.

    Returns the server and the thread so the caller can call
    ``server.shutdown()`` later.
    """
    server = make_server(host, port, registry)
    thread = Thread(target=server.serve_forever, daemon=True, name='metrics-http')
    thread.start()
    log.info('Metrics HTTP server listening on http://%s:%s/metrics', host, port)
    return server, thread
