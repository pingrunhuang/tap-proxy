from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

TRUE_VALUES = frozenset({"1", "true", "yes", "on", "y"})


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    return default if value is None else value.strip().lower() in TRUE_VALUES


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    return default if value is None else int(value)


def _float_env(name: str, default: float) -> float:
    value = os.getenv(name)
    return default if value is None else float(value)


def _symbols_env(name: str) -> list[str]:
    return [item.strip() for item in os.getenv(name, "").split(",") if item.strip()]


@dataclass(slots=True)
class Settings:
    md_host: str = ""
    md_port: int = 0
    md_user_id: str = ""
    md_password: str = ""
    md_auth_code: str = ""
    td_host: str = ""
    td_port: int = 0
    td_user_id: str = ""
    td_password: str = ""
    td_auth_code: str = ""
    client_id: str = ""
    client_location: str = "CN"
    enable_md: bool = True
    initial_symbols: list[str] | None = None
    tap_data_path: Path = Path("flow/tap")
    tap_timezone: str = "Asia/Shanghai"
    connect_timeout_seconds: float = 20.0
    query_timeout_seconds: float = 10.0
    reconnect_max_attempts: int = 5
    reconnect_initial_delay_seconds: float = 2.0
    reconnect_max_delay_seconds: float = 60.0
    database_url: str = (
        "postgresql://tap_proxy:tap_proxy@127.0.0.1:5432/tap_proxy"
    )
    database_pool_min_size: int = 1
    database_pool_max_size: int = 5
    database_connect_timeout_seconds: float = 10.0
    zmq_bind_host: str = "0.0.0.0"
    zmq_pub_port: int = 5575
    zmq_rep_port: int = 5576
    publish_queue_size: int = 100_000

    @classmethod
    def from_env(cls) -> "Settings":
        shared_host = os.getenv("TAP_HOST", "")
        shared_port = _int_env("TAP_PORT", 0)
        shared_user_id = os.getenv("TAP_USER_ID", "")
        shared_password = os.getenv("TAP_PASSWORD", "")
        shared_auth_code = os.getenv("TAP_AUTH_CODE", "")
        return cls(
            md_host=os.getenv("TAP_MD_HOST", shared_host),
            md_port=_int_env("TAP_MD_PORT", shared_port),
            md_user_id=os.getenv("TAP_MD_USER_ID", shared_user_id),
            md_password=os.getenv("TAP_MD_PASSWORD", shared_password),
            md_auth_code=os.getenv("TAP_MD_AUTH_CODE", shared_auth_code),
            td_host=os.getenv("TAP_TD_HOST", shared_host),
            td_port=_int_env("TAP_TD_PORT", shared_port),
            td_user_id=os.getenv("TAP_TD_USER_ID", shared_user_id),
            td_password=os.getenv("TAP_TD_PASSWORD", shared_password),
            td_auth_code=os.getenv("TAP_TD_AUTH_CODE", shared_auth_code),
            client_id=os.getenv("TAP_CLIENT_ID", ""),
            client_location=os.getenv("TAP_CLIENT_LOCATION", "CN"),
            enable_md=_bool_env("TAP_ENABLE_MD", True),
            initial_symbols=_symbols_env("TAP_SYMBOLS"),
            tap_data_path=Path(os.getenv("TAP_DATA_PATH", "flow/tap")),
            tap_timezone=os.getenv("TAP_TIMEZONE", "Asia/Shanghai"),
            connect_timeout_seconds=_float_env("TAP_CONNECT_TIMEOUT_SECONDS", 20.0),
            query_timeout_seconds=_float_env("TAP_QUERY_TIMEOUT_SECONDS", 10.0),
            reconnect_max_attempts=_int_env("TAP_RECONNECT_MAX_ATTEMPTS", 5),
            reconnect_initial_delay_seconds=_float_env(
                "TAP_RECONNECT_INITIAL_DELAY_SECONDS",
                2.0,
            ),
            reconnect_max_delay_seconds=_float_env(
                "TAP_RECONNECT_MAX_DELAY_SECONDS",
                60.0,
            ),
            database_url=os.getenv(
                "DATABASE_URL",
                "postgresql://tap_proxy:tap_proxy@127.0.0.1:5432/tap_proxy",
            ),
            database_pool_min_size=_int_env("DATABASE_POOL_MIN_SIZE", 1),
            database_pool_max_size=_int_env("DATABASE_POOL_MAX_SIZE", 5),
            database_connect_timeout_seconds=_float_env(
                "DATABASE_CONNECT_TIMEOUT_SECONDS",
                10.0,
            ),
            zmq_bind_host=os.getenv("ZMQ_BIND_HOST", "0.0.0.0"),
            zmq_pub_port=_int_env("ZMQ_PUB_PORT", 5575),
            zmq_rep_port=_int_env("ZMQ_REP_PORT", 5576),
            publish_queue_size=_int_env("ZMQ_PUBLISH_QUEUE_SIZE", 100_000),
        )

    def validate(self, *, require_tap: bool = False) -> None:
        if not self.zmq_bind_host.strip():
            raise ValueError("ZMQ_BIND_HOST must not be empty")
        for name, value in (
            ("ZMQ_PUB_PORT", self.zmq_pub_port),
            ("ZMQ_REP_PORT", self.zmq_rep_port),
        ):
            if not 0 <= value <= 65535:
                raise ValueError(f"{name} must be between 0 and 65535")
        if self.zmq_pub_port and self.zmq_pub_port == self.zmq_rep_port:
            raise ValueError("ZMQ_PUB_PORT and ZMQ_REP_PORT must be different")
        if self.publish_queue_size < 1:
            raise ValueError("ZMQ_PUBLISH_QUEUE_SIZE must be at least 1")
        if self.connect_timeout_seconds <= 0:
            raise ValueError("TAP_CONNECT_TIMEOUT_SECONDS must be positive")
        if self.query_timeout_seconds <= 0:
            raise ValueError("TAP_QUERY_TIMEOUT_SECONDS must be positive")
        if self.reconnect_max_attempts < -1:
            raise ValueError("TAP_RECONNECT_MAX_ATTEMPTS must be -1 or greater")
        if self.reconnect_initial_delay_seconds < 0:
            raise ValueError(
                "TAP_RECONNECT_INITIAL_DELAY_SECONDS must not be negative"
            )
        if self.reconnect_max_delay_seconds < self.reconnect_initial_delay_seconds:
            raise ValueError(
                "TAP_RECONNECT_MAX_DELAY_SECONDS must be greater than or equal to "
                "TAP_RECONNECT_INITIAL_DELAY_SECONDS"
            )
        if not self.tap_timezone.strip():
            raise ValueError("TAP_TIMEZONE must not be empty")
        if not self.database_url.startswith(("postgresql://", "postgres://")):
            raise ValueError("DATABASE_URL must be a PostgreSQL connection URL")
        if self.database_pool_min_size < 1:
            raise ValueError("DATABASE_POOL_MIN_SIZE must be at least 1")
        if self.database_pool_max_size < self.database_pool_min_size:
            raise ValueError(
                "DATABASE_POOL_MAX_SIZE must be >= DATABASE_POOL_MIN_SIZE"
            )
        if self.database_connect_timeout_seconds <= 0:
            raise ValueError("DATABASE_CONNECT_TIMEOUT_SECONDS must be positive")
        if not self.enable_md and self.initial_symbols:
            raise ValueError("TAP_SYMBOLS must be empty when TAP_ENABLE_MD=false")
        if require_tap:
            required = {
                "TAP_TD_HOST": self.td_host,
                "TAP_TD_PORT": self.td_port,
                "TAP_TD_USER_ID": self.td_user_id,
                "TAP_TD_PASSWORD": self.td_password,
                "TAP_TD_AUTH_CODE": self.td_auth_code,
            }
            if self.enable_md:
                required.update({
                    "TAP_MD_HOST": self.md_host,
                    "TAP_MD_PORT": self.md_port,
                    "TAP_MD_USER_ID": self.md_user_id,
                    "TAP_MD_PASSWORD": self.md_password,
                    "TAP_MD_AUTH_CODE": self.md_auth_code,
                })
            missing = [
                name
                for name, value in required.items()
                if value in ("", 0)
            ]
            if missing:
                raise ValueError(
                    f"Missing required TAP configuration: {', '.join(missing)}"
                )
