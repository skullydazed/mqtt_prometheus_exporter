import logging
import time

from .app import app
from .config import STORE_WRITE_INTERVAL
from .store import store


@app.thread()
def flush_store():
    interval = STORE_WRITE_INTERVAL
    logging.info("Periodic store writer started (interval=%.1fs)", interval)
    while True:
        time.sleep(interval)
        try:
            store.save()
        except Exception:
            logging.exception("Periodic store save failed")
