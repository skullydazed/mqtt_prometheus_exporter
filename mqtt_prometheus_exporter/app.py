from gourd import Gourd

from .config import MQTT_CLIENT_ID, MQTT_HOST, MQTT_PASS, MQTT_PORT, MQTT_USER

app = Gourd(app_name=MQTT_CLIENT_ID, mqtt_host=MQTT_HOST, mqtt_port=MQTT_PORT, username=MQTT_USER, password=MQTT_PASS)
