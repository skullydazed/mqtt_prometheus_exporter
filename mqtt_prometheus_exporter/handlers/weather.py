import logging
import threading

from ..app import app
from ..store import store_metric

log = logging.getLogger(__name__)

_minutely_precip: dict[int, float] = {}
_minutely_precip_lock = threading.Lock()

_DAILY_METRIC_MAP = {
    "humidity": "weather_humidity_pct",
    "pressure": "weather_pressure_hpa",
    "wind_speed": "weather_wind_speed_ms",
    "wind_gust": "weather_wind_gust_ms",
    "wind_deg": "weather_wind_deg",
    "dew_point_C": "weather_dew_point_celsius",
    "dew_point_F": "weather_dew_point_fahrenheit",
    "uvi": "weather_uvi",
    "pop": "weather_pop",
    "rain": "weather_rain_mm",
    "snow": "weather_snow_mm",
    "clouds": "weather_clouds_pct",
    "sunrise": "weather_sunrise_ts",
    "sunset": "weather_sunset_ts",
    "moonrise": "weather_moonrise_ts",
    "moonset": "weather_moonset_ts",
    "moon_phase": "weather_moon_phase",
}

_TEMP_METRICS = {
    ("temp", "_C"): "weather_temp_celsius",
    ("temp", "_F"): "weather_temp_fahrenheit",
    ("feels_like", "_C"): "weather_feels_like_celsius",
    ("feels_like", "_F"): "weather_feels_like_fahrenheit",
}


@app.subscribe("weather/#")
def handle_weather(message):
    parts = message.topic.split("/")

    if len(parts) < 3:
        return

    resolution = parts[1]

    if resolution == "dt":
        return

    try:
        value = float(message.payload.strip())
    except (ValueError, TypeError):
        return  # silent skip non-numeric

    ttl = 3600

    if resolution == "minutely":
        if len(parts) != 4 or parts[3] != "precipitation":
            return
        try:
            idx = int(parts[2])
        except ValueError:
            log.warning("minutely: non-integer index %r, skipping", parts[2])
            return
        with _minutely_precip_lock:
            if idx == 0:
                _minutely_precip.clear()
            _minutely_precip[idx] = value
            total = sum(_minutely_precip.values())
        store_metric("weather_precipitation_next_hour", {}, total, ttl)
        return

    if resolution == "daily":
        if len(parts) == 4:
            # 4-part topic: weather/daily/{n}/{metric}
            index, metric = parts[2], parts[3]
            prom_name = _DAILY_METRIC_MAP.get(metric)
            if prom_name is None:
                return
            store_metric(prom_name, {"day": index}, value, ttl)
        elif len(parts) == 5:
            # 5-part topic: weather/daily/{n}/{part4}/{part5}
            index, part4, part5 = parts[2], parts[3], parts[4]
            if part5.endswith("_C"):
                unit = "_C"
                period = part5[:-2]
            elif part5.endswith("_F"):
                unit = "_F"
                period = part5[:-2]
            else:
                return  # bare period path, no unit suffix
            prom_name = _TEMP_METRICS.get((part4, unit))
            if prom_name is None:
                return
            store_metric(prom_name, {"day": index, "period": period}, value, ttl)
        return
