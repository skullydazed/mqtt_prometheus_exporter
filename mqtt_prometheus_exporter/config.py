"""Configuration constants loaded from environment variables."""

from __future__ import annotations

import logging
import os

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
