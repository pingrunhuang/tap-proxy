from __future__ import annotations

import json
import threading
from typing import Any, Protocol

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool


class OrderStore(Protocol):
    persistent: bool

    def reserve(
        self,
        *,
        client_id: str,
        strategy_id: str,
        client_order_id: str,
        symbol: str,
        offset: str,
        payload: dict[str, Any],
    ) -> bool: ...

    def get(
        self,
        client_id: str,
        strategy_id: str,
        client_order_id: str,
    ) -> dict[str, Any] | None: ...

    def find_by_native(self, tap_client_order_no: str) -> dict[str, Any] | None: ...

    def find_by_order_no(
        self,
        tap_order_no: str,
        tap_server_flag: str = "",
    ) -> dict[str, Any] | None: ...

    def set_native(
        self,
        *,
        client_id: str,
        strategy_id: str,
        client_order_id: str,
        tap_client_order_no: str,
    ) -> None: ...

    def update_tap(
        self,
        *,
        tap_client_order_no: str,
        tap_order_no: str,
        tap_server_flag: str,
        status: str,
    ) -> None: ...

    def mark_failed(
        self,
        *,
        client_id: str,
        strategy_id: str,
        client_order_id: str,
        message: str,
    ) -> None: ...

    def list(self) -> list[dict[str, Any]]: ...

    def record_trade(self, payload: dict[str, Any]) -> bool: ...

    def list_trades(
        self,
        client_id: str,
        strategy_id: str,
        *,
        after_id: int = 0,
        limit: int = 500,
    ) -> dict[str, Any]: ...

    def is_healthy(self) -> bool: ...

    def close(self) -> None: ...


class MemoryOrderStore:
    """Non-persistent store for unit tests and transport-only development."""

    persistent = False

    def __init__(self) -> None:
        self._records: dict[tuple[str, str, str], dict[str, Any]] = {}
        self._trades: list[dict[str, Any]] = []
        self._trade_event_ids: set[str] = set()
        self._lock = threading.RLock()

    @staticmethod
    def _key(
        client_id: str,
        strategy_id: str,
        client_order_id: str,
    ) -> tuple[str, str, str]:
        return client_id, strategy_id, client_order_id

    def reserve(
        self,
        *,
        client_id: str,
        strategy_id: str,
        client_order_id: str,
        symbol: str,
        offset: str,
        payload: dict[str, Any],
    ) -> bool:
        key = self._key(client_id, strategy_id, client_order_id)
        with self._lock:
            if key in self._records:
                return False
            self._records[key] = {
                "client_id": client_id,
                "strategy_id": strategy_id,
                "client_order_id": client_order_id,
                "symbol": symbol,
                "offset": offset,
                "tap_client_order_no": "",
                "tap_order_no": "",
                "tap_server_flag": "",
                "status": "PENDING_SUBMIT",
                "status_message": "",
                "payload": dict(payload),
            }
            return True

    def get(
        self,
        client_id: str,
        strategy_id: str,
        client_order_id: str,
    ) -> dict[str, Any] | None:
        with self._lock:
            record = self._records.get(
                self._key(client_id, strategy_id, client_order_id)
            )
            return None if record is None else dict(record)

    def find_by_native(self, tap_client_order_no: str) -> dict[str, Any] | None:
        with self._lock:
            return self._find("tap_client_order_no", tap_client_order_no)

    def find_by_order_no(
        self,
        tap_order_no: str,
        tap_server_flag: str = "",
    ) -> dict[str, Any] | None:
        with self._lock:
            for record in self._records.values():
                if record.get("tap_order_no") != tap_order_no:
                    continue
                if (
                    tap_server_flag
                    and record.get("tap_server_flag") != tap_server_flag
                ):
                    continue
                return dict(record)
            return None

    def _find(self, field: str, value: str) -> dict[str, Any] | None:
        if not value:
            return None
        for record in self._records.values():
            if record.get(field) == value:
                return dict(record)
        return None

    def set_native(
        self,
        *,
        client_id: str,
        strategy_id: str,
        client_order_id: str,
        tap_client_order_no: str,
    ) -> None:
        with self._lock:
            record = self._records[
                self._key(client_id, strategy_id, client_order_id)
            ]
            record["tap_client_order_no"] = tap_client_order_no
            record["status"] = "ACCEPTED"

    def update_tap(
        self,
        *,
        tap_client_order_no: str,
        tap_order_no: str,
        tap_server_flag: str,
        status: str,
    ) -> None:
        with self._lock:
            record = self._find("tap_client_order_no", tap_client_order_no)
            if record is None:
                return
            key = self._key(
                record["client_id"],
                record["strategy_id"],
                record["client_order_id"],
            )
            target = self._records[key]
            target["tap_order_no"] = tap_order_no or target["tap_order_no"]
            target["tap_server_flag"] = (
                tap_server_flag or target["tap_server_flag"]
            )
            target["status"] = status

    def mark_failed(
        self,
        *,
        client_id: str,
        strategy_id: str,
        client_order_id: str,
        message: str,
    ) -> None:
        with self._lock:
            record = self._records[
                self._key(client_id, strategy_id, client_order_id)
            ]
            record["status"] = "SUBMIT_FAILED"
            record["status_message"] = message

    def list(self) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(record) for record in self._records.values()]

    def record_trade(self, payload: dict[str, Any]) -> bool:
        event_id = str(payload.get("event_id") or "").strip()
        if not event_id:
            raise ValueError("trade payload requires event_id")
        with self._lock:
            if event_id in self._trade_event_ids:
                return False
            self._trade_event_ids.add(event_id)
            self._trades.append(
                {"id": len(self._trades) + 1, "payload": dict(payload)}
            )
            return True

    def list_trades(
        self,
        client_id: str,
        strategy_id: str,
        *,
        after_id: int = 0,
        limit: int = 500,
    ) -> dict[str, Any]:
        page_size = min(max(int(limit), 1), 1000)
        cursor = max(int(after_id), 0)
        with self._lock:
            matching = [
                row
                for row in self._trades
                if row["id"] > cursor
                and str(row["payload"].get("client_id") or "") == client_id
                and str(row["payload"].get("strategy_id") or "") == strategy_id
            ]
            page = matching[:page_size]
            return {
                "trades": [dict(row["payload"]) for row in page],
                "next_after_id": int(page[-1]["id"]) if page else cursor,
                "has_more": len(matching) > page_size,
            }

    def is_healthy(self) -> bool:
        return True

    def close(self) -> None:
        return None


class PostgresOrderStore:
    """Persistent TAP ownership and native-order identifier mapping."""

    persistent = True

    def __init__(
        self,
        database_url: str,
        *,
        min_size: int = 1,
        max_size: int = 5,
        connect_timeout_seconds: float = 10.0,
    ) -> None:
        self._pool = ConnectionPool(
            conninfo=database_url,
            min_size=min_size,
            max_size=max_size,
            timeout=connect_timeout_seconds,
            kwargs={"autocommit": True, "row_factory": dict_row},
            open=False,
        )
        self._pool.open(wait=True, timeout=connect_timeout_seconds)
        self._initialize_schema()

    def _initialize_schema(self) -> None:
        with self._pool.connection() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS tap_orders (
                    id BIGSERIAL PRIMARY KEY,
                    client_id TEXT NOT NULL,
                    strategy_id TEXT NOT NULL,
                    client_order_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    "offset" TEXT NOT NULL,
                    tap_client_order_no TEXT,
                    tap_order_no TEXT,
                    tap_server_flag TEXT,
                    status TEXT NOT NULL,
                    status_message TEXT NOT NULL DEFAULT '',
                    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (client_id, strategy_id, client_order_id)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS tap_trades (
                    id BIGSERIAL PRIMARY KEY,
                    event_id TEXT NOT NULL UNIQUE,
                    client_id TEXT NOT NULL DEFAULT '',
                    strategy_id TEXT NOT NULL DEFAULT '',
                    account_id TEXT NOT NULL DEFAULT '',
                    trading_day TEXT NOT NULL DEFAULT '',
                    trade_id TEXT NOT NULL DEFAULT '',
                    payload JSONB NOT NULL,
                    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_tap_trades_owner_cursor "
                "ON tap_trades(client_id, strategy_id, id)"
            )
            connection.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_tap_orders_client_order_no
                ON tap_orders(tap_client_order_no)
                WHERE tap_client_order_no IS NOT NULL
                """
            )
            connection.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_tap_orders_order_key
                ON tap_orders(tap_order_no, tap_server_flag)
                WHERE tap_order_no IS NOT NULL
                  AND tap_server_flag IS NOT NULL
                """
            )

    def reserve(
        self,
        *,
        client_id: str,
        strategy_id: str,
        client_order_id: str,
        symbol: str,
        offset: str,
        payload: dict[str, Any],
    ) -> bool:
        with self._pool.connection() as connection:
            row = connection.execute(
                """
                INSERT INTO tap_orders
                    (client_id, strategy_id, client_order_id, symbol, "offset",
                     status, payload)
                VALUES (%s, %s, %s, %s, %s, 'PENDING_SUBMIT', %s::jsonb)
                ON CONFLICT (client_id, strategy_id, client_order_id) DO NOTHING
                RETURNING id
                """,
                (
                    client_id,
                    strategy_id,
                    client_order_id,
                    symbol,
                    offset,
                    json.dumps(payload, ensure_ascii=False, sort_keys=True),
                ),
            ).fetchone()
            return row is not None

    def get(
        self,
        client_id: str,
        strategy_id: str,
        client_order_id: str,
    ) -> dict[str, Any] | None:
        with self._pool.connection() as connection:
            return connection.execute(
                """
                SELECT * FROM tap_orders
                WHERE client_id=%s AND strategy_id=%s AND client_order_id=%s
                """,
                (client_id, strategy_id, client_order_id),
            ).fetchone()

    def find_by_native(self, tap_client_order_no: str) -> dict[str, Any] | None:
        if not tap_client_order_no:
            return None
        with self._pool.connection() as connection:
            return connection.execute(
                """
                SELECT * FROM tap_orders
                WHERE tap_client_order_no=%s
                ORDER BY updated_at DESC LIMIT 1
                """,
                (tap_client_order_no,),
            ).fetchone()

    def find_by_order_no(
        self,
        tap_order_no: str,
        tap_server_flag: str = "",
    ) -> dict[str, Any] | None:
        if not tap_order_no:
            return None
        with self._pool.connection() as connection:
            return connection.execute(
                """
                SELECT * FROM tap_orders
                WHERE tap_order_no=%s
                  AND (%s='' OR tap_server_flag=%s)
                ORDER BY updated_at DESC LIMIT 1
                """,
                (tap_order_no, tap_server_flag, tap_server_flag),
            ).fetchone()

    def set_native(
        self,
        *,
        client_id: str,
        strategy_id: str,
        client_order_id: str,
        tap_client_order_no: str,
    ) -> None:
        with self._pool.connection() as connection:
            connection.execute(
                """
                UPDATE tap_orders
                SET tap_client_order_no=%s, status='ACCEPTED', updated_at=NOW()
                WHERE client_id=%s AND strategy_id=%s AND client_order_id=%s
                """,
                (
                    tap_client_order_no,
                    client_id,
                    strategy_id,
                    client_order_id,
                ),
            )

    def update_tap(
        self,
        *,
        tap_client_order_no: str,
        tap_order_no: str,
        tap_server_flag: str,
        status: str,
    ) -> None:
        if not tap_client_order_no:
            return
        with self._pool.connection() as connection:
            connection.execute(
                """
                UPDATE tap_orders
                SET tap_order_no=COALESCE(NULLIF(%s, ''), tap_order_no),
                    tap_server_flag=COALESCE(NULLIF(%s, ''), tap_server_flag),
                    status=%s,
                    updated_at=NOW()
                WHERE tap_client_order_no=%s
                """,
                (
                    tap_order_no,
                    tap_server_flag,
                    status,
                    tap_client_order_no,
                ),
            )

    def mark_failed(
        self,
        *,
        client_id: str,
        strategy_id: str,
        client_order_id: str,
        message: str,
    ) -> None:
        with self._pool.connection() as connection:
            connection.execute(
                """
                UPDATE tap_orders
                SET status='SUBMIT_FAILED', status_message=%s, updated_at=NOW()
                WHERE client_id=%s AND strategy_id=%s AND client_order_id=%s
                """,
                (message, client_id, strategy_id, client_order_id),
            )

    def list(self) -> list[dict[str, Any]]:
        with self._pool.connection() as connection:
            return connection.execute(
                "SELECT * FROM tap_orders ORDER BY updated_at ASC"
            ).fetchall()

    def record_trade(self, payload: dict[str, Any]) -> bool:
        event_id = str(payload.get("event_id") or "").strip()
        if not event_id:
            raise ValueError("trade payload requires event_id")
        with self._pool.connection() as connection:
            row = connection.execute(
                """
                INSERT INTO tap_trades(
                    event_id, client_id, strategy_id, account_id,
                    trading_day, trade_id, payload
                ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (event_id) DO NOTHING
                RETURNING id
                """,
                (
                    event_id,
                    str(payload.get("client_id") or ""),
                    str(payload.get("strategy_id") or ""),
                    str(payload.get("account_id") or ""),
                    str(payload.get("trading_day") or ""),
                    str(payload.get("trade_id") or ""),
                    json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str),
                ),
            ).fetchone()
            return row is not None

    def list_trades(
        self,
        client_id: str,
        strategy_id: str,
        *,
        after_id: int = 0,
        limit: int = 500,
    ) -> dict[str, Any]:
        page_size = min(max(int(limit), 1), 1000)
        cursor = max(int(after_id), 0)
        with self._pool.connection() as connection:
            rows = connection.execute(
                """
                SELECT id, payload
                FROM tap_trades
                WHERE client_id=%s AND strategy_id=%s AND id>%s
                ORDER BY id ASC
                LIMIT %s
                """,
                (client_id, strategy_id, cursor, page_size + 1),
            ).fetchall()
        has_more = len(rows) > page_size
        page = rows[:page_size]
        return {
            "trades": [row["payload"] for row in page],
            "next_after_id": int(page[-1]["id"]) if page else cursor,
            "has_more": has_more,
        }

    def is_healthy(self) -> bool:
        try:
            with self._pool.connection() as connection:
                row = connection.execute("SELECT 1 AS ok").fetchone()
                return bool(row and row["ok"] == 1)
        except Exception:
            return False

    def close(self) -> None:
        self._pool.close()
