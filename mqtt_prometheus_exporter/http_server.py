import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from .app import app
from .collector import registry
from .config import HTTP_HOST, HTTP_PORT


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/metrics":
            data = generate_latest(registry)
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.end_headers()
            self.wfile.write(data)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):  # noqa: A002
        pass  # suppress HTTP access logs


@app.thread()
def serve_metrics():
    httpd = ThreadingHTTPServer((HTTP_HOST, HTTP_PORT), MetricsHandler)
    logging.info("HTTP metrics server started on %s:%d", HTTP_HOST, HTTP_PORT)
    httpd.serve_forever()
