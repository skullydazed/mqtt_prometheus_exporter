"""HTTP server for the Prometheus /metrics endpoint."""

from __future__ import annotations

import logging
from http.server import BaseHTTPRequestHandler

from prometheus_client import REGISTRY, generate_latest

log = logging.getLogger(__name__)


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
