import logging
import threading

from ..app import app
from ..store import store_metric

log = logging.getLogger(__name__)

_minutely_precip: dict[int, float] = {}
_minutely_precip_lock = threading.Lock()

_WEATHER_SUFFIXES = {
    "humidity": "_humidity_pct",
    "pressure": "_pressure_hpa",
    "wind_speed": "_wind_speed_ms",
    "wind_gust": "_wind_gust_ms",
    "wind_deg": "_wind_deg",
    "dew_point_C": "_dew_point_C",
    "dew_point_F": "_dew_point_F",
    "temp_C": "_temp_C",
    "temp_F": "_temp_F",
    "feels_like_C": "_feels_like_C",
    "feels_like_F": "_feels_like_F",
    "uvi": "_uvi",
    "clouds": "_clouds_pct",
    "visibility": "_visibility_m",
    "pop": "_pop",
    "rain": "_rain_mm",
    "snow": "_snow_mm",
    "sunrise": "_sunrise_ts",
    "sunset": "_sunset_ts",
    "moonrise": "_moonrise_ts",
    "moonset": "_moonset_ts",
    "moon_phase": "_moon_phase",
}

# Field names that nest a sub-period (e.g. weather/daily/0/temp/morn_C)
_TEMP_TYPES = {"temp", "feels_like"}


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

    # Infer structure from topic shape: numeric parts[2] → indexed resolution
    try:
        index = parts[2]
        int(index)
        indexed = True
    except ValueError:
        indexed = False

    if indexed:
        if len(parts) == 4:
            suffix = _WEATHER_SUFFIXES.get(parts[3])
            if suffix is None:
                return
            store_metric(f"weather_{resolution}{suffix}", {resolution: index}, value, ttl)
        elif len(parts) == 5:
            temp_type, part5 = parts[3], parts[4]
            if temp_type not in _TEMP_TYPES:
                return
            if not (part5.endswith("_C") or part5.endswith("_F")):
                return
            unit, period = part5[-2:], part5[:-2]
            store_metric(f"weather_{resolution}_{temp_type}{unit}", {resolution: index, "period": period}, value, ttl)
    else:
        # scalar resolution (e.g. current): weather/{resolution}/{field}
        if len(parts) != 3:
            return
        suffix = _WEATHER_SUFFIXES.get(parts[2])
        if suffix is None:
            return
        store_metric(f"weather_{resolution}{suffix}", {}, value, ttl)
