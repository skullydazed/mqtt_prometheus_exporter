import json
import logging

from ..app import app
from ..store import store_metric

log = logging.getLogger(__name__)


@app.subscribe("ping/#")
def handle_ping(message):
    parts = message.topic.split("/", 1)
    if len(parts) < 2:
        log.warning("ping: unrecognized topic structure: %s", message.topic)
        return
    remainder = parts[1]
    if remainder == "status":
        return

    try:
        data = json.loads(message.payload)
    except (ValueError, TypeError):
        log.warning("ping: failed to parse JSON from %s", message.topic)
        return

    last_1_min = data.get("last_1_min")
    if not isinstance(last_1_min, dict):
        log.warning("ping: missing last_1_min in %s", message.topic)
        return

    for stat in ("min", "avg", "max", "percent_dropped"):
        if stat not in last_1_min:
            continue
        try:
            value = float(last_1_min[stat])
        except (ValueError, TypeError):
            log.warning("ping: cannot parse %s=%r in %s", stat, last_1_min[stat], message.topic)
            continue
        store_metric(f"ping_{stat}", {"destination": remainder}, value)
