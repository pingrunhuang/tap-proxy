from __future__ import annotations

import locale
import os
import signal
import threading

from dotenv import load_dotenv
from loguru import logger

from config import Settings
from logger import configure_logger
from order_store import PostgresOrderStore
from proxy import TapProxy
from tap_session import NativeTapSession


def configure_native_locale() -> None:
    """Force the locale supported by the legacy TAP Linux C++ SDK."""
    os.environ["LANG"] = "C"
    os.environ["LC_ALL"] = "C"
    locale.setlocale(locale.LC_ALL, "C")


def main() -> int:
    load_dotenv()
    configure_native_locale()
    configure_logger()
    settings = Settings.from_env()
    settings.validate(require_tap=True)
    order_store = PostgresOrderStore(
        settings.database_url,
        min_size=settings.database_pool_min_size,
        max_size=settings.database_pool_max_size,
        connect_timeout_seconds=settings.database_connect_timeout_seconds,
    )
    proxy = TapProxy(settings)
    proxy.set_session(
        NativeTapSession(
            settings,
            proxy.enqueue_publish,
            order_store=order_store,
        )
    )
    shutdown = threading.Event()

    def request_shutdown(signum: int, _frame: object) -> None:
        logger.info("Received signal {}; shutting down", signum)
        shutdown.set()

    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)

    try:
        proxy.start()
        if not proxy.session.connect(settings.connect_timeout_seconds):
            logger.warning(
                "TAP readiness timeout; proxy remains online while the native "
                "session continues connecting/reconnecting"
            )
        else:
            logger.info("TAP proxy is ready")
        shutdown.wait()
        return 0
    except Exception:
        logger.exception("TAP proxy terminated unexpectedly")
        return 1
    finally:
        proxy.stop()


if __name__ == "__main__":
    raise SystemExit(main())
