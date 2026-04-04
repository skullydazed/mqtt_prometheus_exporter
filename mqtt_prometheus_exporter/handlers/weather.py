import logging
import threading

from ..app import app
from ..store import store_metric

log = logging.getLogger(__name__)

_minutely_precip: dict[int, float] = {}
_minutely_precip_lock = threading.Lock()


@app.subscribe("weather/#")
def handle_weather(message):
    parts = message.topic.split("/")
    if len(parts) != 4:
        return

    _, resolution, index, metric = parts

    if resolution == "dt":
        return

    try:
        value = float(message.payload.strip())
    except (ValueError, TypeError):
        return  # silent skip non-numeric

    ttl = 3600

    if resolution == "minutely":
        if metric != "precipitation":
            return
        try:
            idx = int(index)
        except ValueError:
            log.warning("minutely: non-integer index %r, skipping", index)
            return
        with _minutely_precip_lock:
            if idx == 0:
                _minutely_precip.clear()
            _minutely_precip[idx] = value
            total = sum(_minutely_precip.values())
        store_metric("weather_precipitation_next_hour", {}, total, ttl)
        return

    if resolution == "daily":
        if index == "0":
            prefix = "today"
        elif index == "1":
            prefix = "tomorrow"
        else:
            return
        store_metric(f"weather_{prefix}_{metric}", {}, value, ttl)
        return
