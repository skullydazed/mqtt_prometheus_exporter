import logging
import time

from .app import app
from .config import STORE_WRITE_INTERVAL
from .store import store


@app.thread()
def flush_store():
    interval = max(STORE_WRITE_INTERVAL, 1.0)
    if interval != STORE_WRITE_INTERVAL:
        logging.warning("STORE_WRITE_INTERVAL must be >= 1.0; using %.1f", interval)

    logging.info("Periodic store writer started (interval=%.1fs)", interval)
    while True:
        time.sleep(interval)
        try:
            store.save()
        except Exception:
            logging.exception("Periodic store save failed")
