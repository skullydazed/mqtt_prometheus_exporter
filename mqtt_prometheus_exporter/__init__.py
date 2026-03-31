"""mqtt_prometheus_exporter — bridges MQTT and Prometheus."""

# noqa: F401
from prometheus_client import GC_COLLECTOR, PLATFORM_COLLECTOR, PROCESS_COLLECTOR, REGISTRY

from .__main__ import main as main
from .collector import MQTTCollector as MQTTCollector
from .config import TTL_DEFAULT as TTL_DEFAULT
from .handlers import (
    app as app,
)
from .handlers import (
    celsius_to_fahrenheit as celsius_to_fahrenheit,
)
from .handlers import (
    coerce_bool as coerce_bool,
)
from .handlers import (
    handle_ping as handle_ping,
)
from .handlers import (
    handle_rtl433 as handle_rtl433,
)
from .handlers import (
    handle_weather as handle_weather,
)
from .handlers import (
    handle_zigbee2mqtt as handle_zigbee2mqtt,
)
from .store import init_store as init_store
from .store import make_metric_key as make_metric_key

for _c in (PROCESS_COLLECTOR, PLATFORM_COLLECTOR, GC_COLLECTOR):
    try:
        REGISTRY.unregister(_c)
    except Exception:
        pass

REGISTRY.register(MQTTCollector())
