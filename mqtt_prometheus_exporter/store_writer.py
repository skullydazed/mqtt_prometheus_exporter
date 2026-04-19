import logging
import time

from .app import app
from .config import STORE_WRITE_INTERVAL
from .store import store


@app.thread()
def flush_store():
    logging.info("Periodic store writer started (interval=%ss)", STORE_WRITE_INTERVAL)
    while True:
        try:
            store.save()
        except Exception:
            logging.exception("Periodic store save failed")
        time.sleep(STORE_WRITE_INTERVAL)
