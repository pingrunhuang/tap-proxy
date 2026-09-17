from __future__ import annotations

import hashlib
import json
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Protocol
from zoneinfo import ZoneInfo

from loguru import logger

from config import Settings
from order_store import MemoryOrderStore, OrderStore
from protocol import (
    Direction,
    Event,
    OrderStatus,
    TapSymbol,
    market_data_topic,
)


try:
    from vnpy_tap.api import (
        APILOGLEVEL_NONE,
        APIYNFLAG_NO,
        MdApi as _NativeMdApi,
        TdApi as _NativeTdApi,
        tap_td_constant,
    )

    NATIVE_TAP_AVAILABLE = True
    NATIVE_TAP_IMPORT_ERROR = ""
except (ImportError, OSError) as exc:
    APILOGLEVEL_NONE = 0
    APIYNFLAG_NO = "N"
    _NativeMdApi = object
    _NativeTdApi = object
    NATIVE_TAP_AVAILABLE = False
    NATIVE_TAP_IMPORT_ERROR = str(exc)

    class _FallbackTdConstants:
        TAPI_SIDE_BUY = "B"
        TAPI_SIDE_SELL = "S"
        TAPI_ORDER_TYPE_LIMIT = "2"
        TAPI_ORDER_STATE_SUBMIT = "0"
        TAPI_ORDER_STATE_ACCEPT = "1"
        TAPI_ORDER_STATE_QUEUED = "2"
        TAPI_ORDER_STATE_PARTFINISHED = "3"
        TAPI_ORDER_STATE_FINISHED = "4"
        TAPI_ORDER_STATE_CANCELED = "5"
        TAPI_ORDER_STATE_LEFTDELETED = "6"
        TAPI_ORDER_STATE_FAIL = "9"

    tap_td_constant = _FallbackTdConstants()


PublishCallback = Callable[[str, str, dict[str, Any]], None]
ApiFactory = Callable[["NativeTapSession"], Any]

SUCCESS = 0
LAST_FLAGS = frozenset({"Y", 1, True})


def _constant(name: str, fallback: str) -> str:
    return str(getattr(tap_td_constant, name, fallback))


SIDE_BUY = _constant("TAPI_SIDE_BUY", "B")
SIDE_SELL = _constant("TAPI_SIDE_SELL", "S")
ORDER_TYPE_LIMIT = _constant("TAPI_ORDER_TYPE_LIMIT", "2")

DIRECTION_TAP_TO_PROTOCOL = {
    SIDE_BUY: Direction.BUY.value,
    SIDE_SELL: Direction.SELL.value,
}
DIRECTION_PROTOCOL_TO_TAP = {
    Direction.BUY.value: SIDE_BUY,
    Direction.SELL.value: SIDE_SELL,
}
STATUS_TAP_TO_PROTOCOL = {
    _constant("TAPI_ORDER_STATE_SUBMIT", "0"): OrderStatus.SUBMITTED.value,
    _constant("TAPI_ORDER_STATE_ACCEPT", "1"): OrderStatus.SUBMITTED.value,
    _constant("TAPI_ORDER_STATE_QUEUED", "2"): OrderStatus.SUBMITTED.value,
    _constant("TAPI_ORDER_STATE_PARTFINISHED", "3"): OrderStatus.PARTTRADED.value,
    _constant("TAPI_ORDER_STATE_FINISHED", "4"): OrderStatus.TRADED.value,
    _constant("TAPI_ORDER_STATE_CANCELED", "5"): OrderStatus.CANCELLED.value,
    _constant("TAPI_ORDER_STATE_LEFTDELETED", "6"): OrderStatus.CANCELLED.value,
    _constant("TAPI_ORDER_STATE_FAIL", "9"): OrderStatus.REJECTED.value,
}


def _trading_day_from_timestamp(value: Any) -> str:
    text = str(value or "").strip()
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:10].replace("-", "")
    if (
        len(text) >= 8
        and text[:8].isdigit()
        and 1900 <= int(text[:4]) <= 2199
    ):
        return text[:8]
    if len(text) >= 6 and text[:6].isdigit():
        try:
            return datetime.strptime(text[:6], "%y%m%d").strftime("%Y%m%d")
        except ValueError:
            return ""
    return ""


def _trade_event_id(
    *,
    gateway_name: str,
    account_id: str,
    trading_day: str,
    exchange: str,
    trade_id: str,
    fallback: dict[str, Any],
) -> str:
    """Return a stable identifier for one native fill."""
    identity: dict[str, Any] = {
        "gateway_name": gateway_name,
        "account_id": account_id,
        "trading_day": trading_day,
        "exchange": exchange,
        "trade_id": trade_id,
    }
    if not trade_id:
        identity["fallback"] = fallback
    encoded = json.dumps(
        identity,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return f"trade:{gateway_name.lower()}:{hashlib.sha256(encoded).hexdigest()}"


class TapUnavailableError(RuntimeError):
    pass


class TapNotReadyError(RuntimeError):
    pass


@dataclass(slots=True)
class OrderIdentity:
    client_id: str
    strategy_id: str
    client_order_id: str
    symbol: str


class TapSession(Protocol):
    def connect(self, timeout: float | None = None) -> bool: ...

    def is_ready(self) -> bool: ...

    def status(self) -> dict[str, Any]: ...

    def subscribe_market_data(self, symbols: list[str]) -> None: ...

    def unsubscribe_market_data(self, symbols: list[str]) -> None: ...

    def query_account(self, max_age_seconds: float | None = None) -> dict[str, Any]: ...

    def query_positions(
        self,
        max_age_seconds: float | None = None,
    ) -> list[dict[str, Any]]: ...

    def query_orders(
        self,
        max_age_seconds: float | None = None,
    ) -> list[dict[str, Any]]: ...

    def query_persisted_trades(
        self,
        client_id: str,
        strategy_id: str,
        *,
        after_id: int = 0,
        limit: int = 500,
    ) -> dict[str, Any]: ...

    def latest_trade_cursor(self, client_id: str, strategy_id: str) -> int: ...

    def query_persisted_orders(
        self,
        client_id: str,
        strategy_id: str,
    ) -> list[dict[str, Any]]: ...

    def place_order(self, request: dict[str, Any]) -> dict[str, Any]: ...

    def cancel_order(self, request: dict[str, Any]) -> dict[str, Any]: ...

    def close(self) -> None: ...


class PendingTapSession:
    """Transport-only placeholder used by focused proxy tests."""

    def connect(self, timeout: float | None = None) -> bool:
        del timeout
        return False

    def is_ready(self) -> bool:
        return False

    def status(self) -> dict[str, Any]:
        return {
            "native_available": False,
            "md_ready": False,
            "td_ready": False,
            "account_ready": False,
            "implementation": "pending_or_test_double",
        }

    def _not_implemented(self) -> None:
        raise NotImplementedError("native TAP session is not configured")

    def subscribe_market_data(self, symbols: list[str]) -> None:
        del symbols
        self._not_implemented()

    def unsubscribe_market_data(self, symbols: list[str]) -> None:
        del symbols
        self._not_implemented()

    def query_account(self, max_age_seconds: float | None = None) -> dict[str, Any]:
        del max_age_seconds
        self._not_implemented()

    def query_positions(
        self,
        max_age_seconds: float | None = None,
    ) -> list[dict[str, Any]]:
        del max_age_seconds
        self._not_implemented()

    def query_orders(
        self,
        max_age_seconds: float | None = None,
    ) -> list[dict[str, Any]]:
        del max_age_seconds
        self._not_implemented()

    def query_persisted_trades(
        self,
        client_id: str,
        strategy_id: str,
        *,
        after_id: int = 0,
        limit: int = 500,
    ) -> dict[str, Any]:
        del client_id, strategy_id, after_id, limit
        self._not_implemented()

    def latest_trade_cursor(self, client_id: str, strategy_id: str) -> int:
        del client_id, strategy_id
        self._not_implemented()

    def query_persisted_orders(
        self,
        client_id: str,
        strategy_id: str,
    ) -> list[dict[str, Any]]:
        del client_id, strategy_id
        self._not_implemented()

    def place_order(self, request: dict[str, Any]) -> dict[str, Any]:
        del request
        self._not_implemented()

    def cancel_order(self, request: dict[str, Any]) -> dict[str, Any]:
        del request
        self._not_implemented()

    def close(self) -> None:
        return None


class TapQuoteApi(_NativeMdApi):
    def __init__(self, session: "NativeTapSession") -> None:
        super().__init__()
        self.session = session

    def onRspLogin(self, errorCode: int, data: dict[str, Any]) -> None:
        self.session.run_callback(
            "md_login",
            self.session.on_md_login,
            errorCode,
            data,
        )

    def onAPIReady(self) -> None:
        self.session.run_callback("md_ready", self.session.on_md_ready)

    def onDisconnect(self, reason: Any) -> None:
        self.session.run_callback(
            "md_disconnect",
            self.session.on_disconnect,
            "MD",
            reason,
        )

    def onRspSubscribeQuote(
        self,
        session: int,
        errorCode: int,
        last: Any,
        data: dict[str, Any],
    ) -> None:
        self.session.run_callback(
            "md_subscribe",
            self.session.on_subscribe_response,
            session,
            errorCode,
            last,
            data,
        )

    def onRtnQuote(self, data: dict[str, Any]) -> None:
        self.session.run_callback("quote", self.session.on_quote, data)


class TapTradeApi(_NativeTdApi):
    def __init__(self, session: "NativeTapSession") -> None:
        super().__init__()
        self.session = session

    def onConnect(self, address: str) -> None:
        self.session.run_callback("td_connect", self.session.on_td_connect, address)

    def onRspLogin(self, errorCode: int, data: dict[str, Any]) -> None:
        self.session.run_callback(
            "td_login",
            self.session.on_td_login,
            errorCode,
            data,
        )

    def onAPIReady(self, code: int) -> None:
        self.session.run_callback("td_ready", self.session.on_td_ready, code)

    def onDisconnect(self, reason: Any) -> None:
        self.session.run_callback(
            "td_disconnect",
            self.session.on_disconnect,
            "TD",
            reason,
        )

    def onRspQryAccount(
        self,
        session: int,
        errorCode: int,
        last: Any,
        data: dict[str, Any],
    ) -> None:
        self.session.run_callback(
            "account_query",
            self.session.on_account_response,
            session,
            errorCode,
            last,
            data,
        )

    def onRspQryFund(
        self,
        session: int,
        errorCode: int,
        last: Any,
        data: dict[str, Any],
    ) -> None:
        logger.debug(
            "OnRspQryFund session={} errorCode={} isLast={} data={}",
            session,
            errorCode,
            last,
            json.dumps(data, default=str, ensure_ascii=False, sort_keys=True)
            if data is not None
            else None,
        )
        currency = data.get("CurrencyNo", "USD")
        if last=="Y" and currency=="USD":
            logger.debug(f"Publishing qualified data: {data}")
            self.session.run_callback(
                "fund_query",
                self.session.on_fund_response,
                session,
                errorCode,
                last,
                data,
            )

    def onRtnFund(self, data: dict[str, Any]) -> None:
        self.session.run_callback("fund", self.session.on_fund, data)

    def onRspQryPositionSummary(
        self,
        session: int,
        errorCode: int,
        last: Any,
        data: dict[str, Any],
    ) -> None:
        self.session.run_callback(
            "position_query",
            self.session.on_position_response,
            session,
            errorCode,
            last,
            data,
        )

    def onRtnPositionSummary(self, data: dict[str, Any]) -> None:
        self.session.run_callback("position", self.session.on_position, data)

    def onRspQryOrder(
        self,
        session: int,
        errorCode: int,
        last: Any,
        data: dict[str, Any],
    ) -> None:
        self.session.run_callback(
            "order_query",
            self.session.on_order_response,
            session,
            errorCode,
            last,
            data,
        )

    def onRtnOrder(self, data: dict[str, Any]) -> None:
        self.session.run_callback("order", self.session.on_order, data)

    def onRspQryFill(
        self,
        session: int,
        errorCode: int,
        last: Any,
        data: dict[str, Any],
    ) -> None:
        self.session.run_callback(
            "fill_query",
            self.session.on_fill_response,
            session,
            errorCode,
            last,
            data,
        )

    def onRtnFill(self, data: dict[str, Any]) -> None:
        self.session.run_callback("fill", self.session.on_fill, data)

    def onRspOrderAction(
        self,
        session: int,
        errorCode: int,
        data: dict[str, Any],
    ) -> None:
        self.session.run_callback(
            "order_action",
            self.session.on_order_action,
            session,
            errorCode,
            data,
        )


class NativeTapSession:
    """Owns one native TAP MD/TD connection and emits protocol dictionaries."""

    def __init__(
        self,
        settings: Settings,
        publish: PublishCallback,
        *,
        md_factory: ApiFactory | None = None,
        td_factory: ApiFactory | None = None,
        native_available: bool | None = None,
        order_store: OrderStore | None = None,
    ) -> None:
        self.settings = settings
        self.publish = publish
        self.native_available = (
            NATIVE_TAP_AVAILABLE
            if native_available is None
            else native_available
        )
        self.native_import_error = NATIVE_TAP_IMPORT_ERROR
        self._md_factory = md_factory or TapQuoteApi
        self._td_factory = td_factory or TapTradeApi
        self.timezone = ZoneInfo(settings.tap_timezone)
        self.order_store = order_store or MemoryOrderStore()

        self.md_api: Any | None = None
        self.td_api: Any | None = None
        self.md_ready_event = threading.Event()
        self.td_ready_event = threading.Event()
        self.account_ready_event = threading.Event()
        self.initial_sync_event = threading.Event()
        self._closing = threading.Event()
        self._connect_lock = threading.Lock()
        self._mapping_lock = threading.RLock()
        self._reconnect_lock = threading.Lock()
        self._reconnect_thread: threading.Thread | None = None
        self.reconnect_attempts = 0
        self.last_error = ""

        self.account_no = ""
        self.active_symbols: set[str] = set(settings.initial_symbols or [])
        self._symbol_by_short: dict[tuple[str, str, str], str] = {}
        for symbol in self.active_symbols:
            self._register_symbol(symbol)

        self._identity_by_native: dict[str, OrderIdentity] = {}
        self._native_by_identity: dict[tuple[str, str, str], str] = {}
        self._order_no_by_native: dict[str, str] = {}
        self._native_by_order_key: dict[tuple[str, str], str] = {}
        self._server_flag_by_native: dict[str, str] = {}
        self._pending_cancels: set[str] = set()
        self._restore_order_mappings()

        self._account_cache: dict[str, Any] = {}
        self._position_cache: dict[tuple[str, str], dict[str, Any]] = {}
        self._order_cache: dict[str, dict[str, Any]] = {}
        self._account_cache_time = 0.0
        self._position_cache_time = 0.0
        self._order_cache_time = 0.0
        self._account_query_event = threading.Event()
        self._position_query_event = threading.Event()
        self._order_query_event = threading.Event()
        self._account_query_lock = threading.Lock()
        self._position_query_lock = threading.Lock()
        self._order_query_lock = threading.Lock()
        self._query_errors: dict[str, str] = {}

    def connect(self, timeout: float | None = None) -> bool:
        if not self.native_available:
            detail = f": {self.native_import_error}" if self.native_import_error else ""
            raise TapUnavailableError(f"vnpy-tap native API is unavailable{detail}")
        if self._closing.is_set():
            return False
        self.settings.validate(require_tap=True)
        timeout_value = timeout or self.settings.connect_timeout_seconds
        deadline = time.monotonic() + timeout_value

        with self._connect_lock:
            if self.is_ready():
                return True
            self._close_native()
            self._reset_connection_state()
            self.settings.tap_data_path.mkdir(parents=True, exist_ok=True)
            self.td_api = self._td_factory(self)
            if self.settings.enable_md:
                self.md_api = self._md_factory(self)
                self._initialize_md()
            self._initialize_td()

        readiness_events = [
            self.td_ready_event,
            self.account_ready_event,
            self.initial_sync_event,
        ]
        if self.settings.enable_md:
            readiness_events.insert(0, self.md_ready_event)
        for event in readiness_events:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not event.wait(remaining):
                self.last_error = "TAP login readiness timeout"
                self.publish_status(self.last_error)
                return False

        self.reconnect_attempts = 0
        self.last_error = ""
        self.publish_status("connected")
        return True

    def _initialize_md(self) -> None:
        assert self.md_api is not None
        path = self._encoded_data_path()
        self.md_api.init()
        self.md_api.setTapQuoteAPIDataPath(path)
        set_log_level = getattr(self.md_api, "setTapQuoteAPILogLevel", None)
        if callable(set_log_level):
            set_log_level(APILOGLEVEL_NONE)
        self.md_api.createTapQuoteAPI(
            {
                "AuthCode": self.settings.md_auth_code,
                "KeyOperationLogPath": path,
            },
            0,
        )
        self.md_api.setHostAddress(self.settings.md_host, self.settings.md_port)
        self.md_api.login(
            {
                "UserNo": self.settings.md_user_id,
                "Password": self.settings.md_password,
                "ISModifyPassword": APIYNFLAG_NO,
                "ISDDA": APIYNFLAG_NO,
            }
        )

    def _initialize_td(self) -> None:
        assert self.td_api is not None
        path = self._encoded_data_path()
        self.td_api.init()
        self.td_api.createITapTradeAPI(
            {
                "AuthCode": self.settings.td_auth_code,
                "KeyOperationLogPath": path,
                "LogLevel": APILOGLEVEL_NONE,
            },
            0,
        )
        self.td_api.setHostAddress(self.settings.td_host, self.settings.td_port)
        self.td_api.login(
            {
                "UserNo": self.settings.td_user_id,
                "Password": self.settings.td_password,
                "ISModifyPassword": APIYNFLAG_NO,
                "NoticeIgnoreFlag": "TAPI_NOTICE_IGNORE_POSITIONPROFIT",
            }
        )

    def _encoded_data_path(self) -> bytes:
        return str(self.settings.tap_data_path.resolve()).encode("GBK")

    def _reset_connection_state(self) -> None:
        self.md_ready_event.clear()
        self.td_ready_event.clear()
        self.account_ready_event.clear()
        self._account_query_event.clear()
        self._position_query_event.clear()
        self._order_query_event.clear()
        self.initial_sync_event.clear()
        self.account_no = ""

    def is_ready(self) -> bool:
        return (
            self.native_available
            and (not self.settings.enable_md or self.md_ready_event.is_set())
            and self.td_ready_event.is_set()
            and self.account_ready_event.is_set()
            and self.initial_sync_event.is_set()
            and not self._closing.is_set()
        )

    def status(self) -> dict[str, Any]:
        return {
            "native_available": self.native_available,
            "md_enabled": self.settings.enable_md,
            "md_ready": self.md_ready_event.is_set(),
            "td_ready": self.td_ready_event.is_set(),
            "account_ready": self.account_ready_event.is_set(),
            "initial_sync_ready": self.initial_sync_event.is_set(),
            "account_id": self.account_no,
            "active_symbols": sorted(self.active_symbols),
            "reconnect_attempts": self.reconnect_attempts,
            "last_error": self.last_error,
            "implementation": "native_tap_session",
            "order_mapping_persistent": self.order_store.persistent,
            "order_store_healthy": self.order_store.is_healthy(),
        }

    def run_callback(
        self,
        name: str,
        callback: Callable[..., None],
        *args: Any,
    ) -> None:
        try:
            callback(*args)
        except Exception as exc:
            logger.exception("TAP callback failed: {}", name)
            self.publish_error(
                "callback_error",
                f"{name}: {exc}",
                retryable=False,
            )

    def on_md_login(self, error_code: int, data: dict[str, Any]) -> None:
        del data
        if error_code != SUCCESS:
            self.last_error = f"MD login failed: {error_code}"
            self.publish_error("md_login_failed", self.last_error, retryable=True)

    def on_md_ready(self) -> None:
        self.md_ready_event.set()
        self.publish_status("MD ready")
        self._subscribe_symbols(sorted(self.active_symbols))

    def on_td_connect(self, address: str) -> None:
        logger.info("TAP TD connected: {}", address)

    def on_td_login(self, error_code: int, data: dict[str, Any]) -> None:
        del data
        if error_code != SUCCESS:
            self.last_error = f"TD login failed: {error_code}"
            self.publish_error("td_login_failed", self.last_error, retryable=True)

    def on_td_ready(self, code: int) -> None:
        self.td_ready_event.set()
        self.publish_status(f"TD ready (notification code: {code})")
        assert self.td_api is not None
        self.td_api.qryAccount({})

    def on_disconnect(self, channel: str, reason: Any) -> None:
        if channel == "MD":
            self.md_ready_event.clear()
        else:
            self.td_ready_event.clear()
            self.account_ready_event.clear()
        self.last_error = f"{channel} disconnected: {reason}"
        self.publish_status(self.last_error)
        if not self._closing.is_set():
            self._schedule_reconnect(self.last_error)

    def on_subscribe_response(
        self,
        session: int,
        error_code: int,
        last: Any,
        data: dict[str, Any],
    ) -> None:
        del session, last
        if error_code != SUCCESS:
            self.publish_error(
                "subscribe_failed",
                f"subscription failed: {error_code}",
                retryable=True,
                details={"data": data},
            )
            return
        if data:
            self.on_quote(data)

    def on_quote(self, data: dict[str, Any]) -> None:
        symbol = self._canonical_from_data(data, "ContractNo1")
        if not symbol:
            return
        local_time = int(time.time() * 1000)
        exchange_time = str(data.get("DateTimeStamp", ""))
        exchange_timestamp = self._parse_timestamp(exchange_time)
        payload = {
            "symbol": symbol,
            "exchange": TapSymbol.parse(symbol).exchange_no,
            "last_price": self._number(data.get("QLastPrice", data.get("MatchPrice"))),
            "volume": self._integer(data.get("QTotalQty", data.get("MatchQty"))),
            "open_interest": self._number(data.get("QPositionQty")),
            "upper_limit": self._number(data.get("QLimitUpPrice")),
            "lower_limit": self._number(data.get("QLimitDownPrice")),
            "bid_price_1": self._first_number(data.get("QBidPrice")),
            "bid_volume_1": self._first_integer(data.get("QBidQty")),
            "ask_price_1": self._first_number(data.get("QAskPrice")),
            "ask_volume_1": self._first_integer(data.get("QAskQty")),
            "exchange_time": exchange_time,
            "exchange_timestamp": exchange_timestamp,
            "local_time": local_time,
        }
        logger.debug(f"on_quote: {payload}")
        self.publish(market_data_topic(symbol), Event.MARKET_DATA.value, payload)

    def on_account_response(
        self,
        session: int,
        error_code: int,
        last: Any,
        data: dict[str, Any],
    ) -> None:
        del session, last
        if error_code != SUCCESS:
            self._query_failed(
                "account",
                error_code,
                self._account_query_event,
                "account",
            )
            return
        if not data:
            return
        self.account_no = str(data["AccountNo"])
        self.account_ready_event.set()
        assert self.td_api is not None
        self.td_api.qryFund({"AccountNo": self.account_no})

    def on_fund_response(
        self,
        session: int,
        error_code: int,
        last: Any,
        data: dict[str, Any],
    ) -> None:
        del session
        if error_code != SUCCESS:
            self._query_failed(
                "fund",
                error_code,
                self._account_query_event,
                "account",
            )
            return
        if data:
            self._update_account(data)
        if self._is_last(last):
            self._account_cache_time = time.monotonic()
            self._account_query_event.set()
            if self.td_api is not None and not self.initial_sync_event.is_set():
                self.td_api.qryPositionSummary({})

    def on_fund(self, data: dict[str, Any]) -> None:
        if data:
            self._update_account(data)

    def _update_account(self, data: dict[str, Any]) -> None:
        account_id = str(data.get("AccountNo", self.account_no))
        balance = self._number(data.get("MarketEquity")) or 0.0
        available = self._number(data.get("Available")) or 0.0
        frozen_value = data.get("FrozenMargin")
        frozen = (
            self._number(frozen_value)
            if frozen_value is not None
            else max(balance - available, 0.0)
        )
        account = {
            "account_id": account_id,
            "balance": balance,
            "frozen": frozen,
            "available": available,
            "currency": str(data.get("CurrencyNo", "")),
        }
        self.account_no = account_id
        self._account_cache = account
        self._account_cache_time = time.monotonic()
        self.publish(f"account.{account_id}", Event.ACCOUNT.value, account)

    def on_position_response(
        self,
        session: int,
        error_code: int,
        last: Any,
        data: dict[str, Any],
    ) -> None:
        del session
        if error_code != SUCCESS:
            self._query_failed(
                "position",
                error_code,
                self._position_query_event,
                "positions",
            )
            return
        if data:
            self._update_position(data)
        if self._is_last(last):
            self._position_cache_time = time.monotonic()
            self._position_query_event.set()
            if self.td_api is not None and not self.initial_sync_event.is_set():
                self.td_api.qryOrder({})

    def on_position(self, data: dict[str, Any]) -> None:
        if data:
            self._update_position(data)

    def _update_position(self, data: dict[str, Any]) -> None:
        symbol = self._canonical_from_data(data, "ContractNo")
        direction = self._direction(data.get("MatchSide"))
        if not symbol or not direction:
            return
        position = {
            "account_id": str(data.get("AccountNo", self.account_no)),
            "symbol": symbol,
            "direction": direction,
            "volume": self._integer(data.get("PositionQty")) or 0,
            "yesterday_volume": self._integer(data.get("HisPositionQty")) or 0,
        }
        self._position_cache[(symbol, direction)] = position
        self.publish(
            f"positions.{position['account_id']}",
            Event.POSITION.value,
            position,
        )

    def on_order_response(
        self,
        session: int,
        error_code: int,
        last: Any,
        data: dict[str, Any],
    ) -> None:
        del session
        if error_code != SUCCESS:
            self._query_failed(
                "order",
                error_code,
                self._order_query_event,
                "orders",
            )
            return
        if data:
            self._update_order(data)
        if self._is_last(last):
            self._order_cache_time = time.monotonic()
            self._order_query_event.set()
            self.last_error = ""
            self.reconnect_attempts = 0
            self.initial_sync_event.set()
            self.publish_status("initial account/position/order sync complete")

    def on_order(self, data: dict[str, Any]) -> None:
        if not data:
            return
        error_code = int(data.get("ErrorCode", SUCCESS) or SUCCESS)
        if error_code != SUCCESS:
            self.publish_error(
                "order_rejected",
                f"TAP rejected order: {error_code}",
                retryable=False,
                details={"tap_client_order_no": data.get("ClientOrderNo", "")},
            )
        self._update_order(data, forced_rejected=error_code != SUCCESS)

    def _update_order(
        self,
        data: dict[str, Any],
        *,
        forced_rejected: bool = False,
    ) -> None:
        native_id = str(data.get("ClientOrderNo", ""))
        order_no = str(data.get("OrderNo", ""))
        server_flag = str(data.get("ServerFlag", ""))
        if native_id and order_no:
            with self._mapping_lock:
                self._order_no_by_native[native_id] = order_no
                self._native_by_order_key[(order_no, server_flag)] = native_id
                self._server_flag_by_native[native_id] = server_flag
        identity = self._identity_by_native.get(native_id)
        if identity is None and native_id:
            record = self.order_store.find_by_native(native_id)
            if record:
                identity = self._hydrate_record(record)
        symbol = self._canonical_from_data(data, "ContractNo")
        direction = self._direction(data.get("OrderSide"))
        if not symbol or not direction:
            return
        status = (
            OrderStatus.REJECTED.value
            if forced_rejected
            else STATUS_TAP_TO_PROTOCOL.get(
                str(data.get("OrderState", "")),
                OrderStatus.UNKNOWN.value,
            )
        )
        if native_id:
            self.order_store.update_tap(
                tap_client_order_no=native_id,
                tap_order_no=order_no,
                tap_server_flag=server_flag,
                status=status,
            )
        account_id = str(data.get("AccountNo", self.account_no))
        order = {
            "account_id": account_id,
            "client_id": identity.client_id if identity else "",
            "strategy_id": identity.strategy_id if identity else "",
            "client_order_id": (
                identity.client_order_id if identity else native_id
            ),
            "symbol": identity.symbol if identity else symbol,
            "direction": direction,
            "price": self._number(data.get("OrderPrice")),
            "volume": self._integer(data.get("OrderQty")) or 0,
            "traded": self._integer(data.get("OrderMatchQty")) or 0,
            "status": status,
            "status_message": str(data.get("ErrorText", "")),
            "tap_client_order_no": native_id,
            "tap_order_no": order_no,
            "tap_server_flag": server_flag,
        }
        self._order_cache[native_id] = order
        self._publish_scoped("orders", account_id, identity, Event.ORDER, order)
        if native_id in self._pending_cancels and order_no:
            self._pending_cancels.discard(native_id)
            self._send_cancel(native_id)

    def on_fill_response(
        self,
        session: int,
        error_code: int,
        last: Any,
        data: dict[str, Any],
    ) -> None:
        del session, last
        if error_code != SUCCESS:
            self.publish_error(
                "fill_query_failed",
                f"fill query failed: {error_code}",
                retryable=True,
            )
            return
        if data:
            self._update_trade(data)

    def on_fill(self, data: dict[str, Any]) -> None:
        if data:
            self._update_trade(data)

    def _update_trade(self, data: dict[str, Any]) -> None:
        order_no = str(data.get("OrderNo", ""))
        server_flag = str(data.get("ServerFlag", ""))
        native_id = self._native_by_order_key.get((order_no, server_flag), "")
        if not native_id and order_no:
            record = self.order_store.find_by_order_no(order_no, server_flag)
            if record:
                identity = self._hydrate_record(record)
                native_id = str(record.get("tap_client_order_no", "") or "")
            else:
                identity = None
        else:
            identity = self._identity_by_native.get(native_id)
        symbol = self._canonical_from_data(data, "ContractNo")
        direction = self._direction(data.get("MatchSide"))
        if not symbol or not direction:
            return
        account_id = str(data.get("AccountNo", self.account_no))
        trade_time = str(data.get("MatchDateTime", ""))
        gateway_name = "TAP"
        trading_day = _trading_day_from_timestamp(trade_time)
        exchange = str(data.get("ExchangeNo", ""))
        trade_id = str(data.get("MatchNo", ""))
        price = self._number(data.get("MatchPrice"))
        volume = self._integer(data.get("MatchQty")) or 0
        trade = {
            "event_id": _trade_event_id(
                gateway_name=gateway_name,
                account_id=account_id,
                trading_day=trading_day,
                exchange=exchange,
                trade_id=trade_id,
                fallback={
                    "order_id": order_no,
                    "client_order_id": (
                        identity.client_order_id if identity else native_id
                    ),
                    "symbol": identity.symbol if identity else symbol,
                    "direction": direction,
                    "price": price,
                    "volume": volume,
                    "trade_time": trade_time,
                },
            ),
            "gateway_name": gateway_name,
            "account_id": account_id,
            "client_id": identity.client_id if identity else "",
            "strategy_id": identity.strategy_id if identity else "",
            "client_order_id": (
                identity.client_order_id if identity else native_id
            ),
            "order_id": order_no,
            "trade_id": trade_id,
            "symbol": identity.symbol if identity else symbol,
            "exchange": exchange,
            "direction": direction,
            "price": price,
            "volume": volume,
            "trade_time": trade_time,
            "trading_day": trading_day,
            "exchange_timestamp": self._parse_timestamp(trade_time),
        }
        try:
            trade_cursor = self.order_store.record_trade(trade)
        except Exception:
            logger.exception(
                "Failed to persist TAP trade before publish event_id={}",
                trade.get("event_id"),
            )
            return
        logger.debug(
            "Persisted TAP trade event_id={} strategy_id={} trade_cursor={}",
            trade.get("event_id"),
            trade.get("strategy_id"),
            trade_cursor,
        )
        if not trade_cursor:
            logger.debug("Skip duplicate TAP trade event_id={}", trade.get("event_id"))
            return
        published_trade = {**trade, "trade_cursor": int(trade_cursor)}
        self._publish_scoped(
            "trades",
            account_id,
            identity,
            Event.TRADE,
            published_trade,
        )

    def on_order_action(
        self,
        session: int,
        error_code: int,
        data: dict[str, Any],
    ) -> None:
        del session
        if error_code != SUCCESS:
            self.publish_error(
                "cancel_failed",
                f"TAP cancel failed: {error_code}",
                retryable=False,
                details={"data": data},
            )

    def subscribe_market_data(self, symbols: list[str]) -> None:
        if not self.settings.enable_md:
            raise RuntimeError("TAP market data is disabled by TAP_ENABLE_MD=false")
        normalized = [TapSymbol.parse(symbol).canonical for symbol in symbols]
        for symbol in normalized:
            self.active_symbols.add(symbol)
            self._register_symbol(symbol)
        if self.md_ready_event.is_set():
            self._subscribe_symbols(normalized)

    def _subscribe_symbols(self, symbols: list[str]) -> None:
        if not symbols or self.md_api is None:
            return
        for symbol in symbols:
            info = TapSymbol.parse(symbol)
            self.md_api.subscribeQuote(self._quote_request(info))

    def unsubscribe_market_data(self, symbols: list[str]) -> None:
        if not self.settings.enable_md:
            raise RuntimeError("TAP market data is disabled by TAP_ENABLE_MD=false")
        normalized = [TapSymbol.parse(symbol).canonical for symbol in symbols]
        for symbol in normalized:
            self.active_symbols.discard(symbol)
        if self.md_api is None or not self.md_ready_event.is_set():
            return
        unsubscribe = getattr(self.md_api, "unsubscribeQuote", None)
        if not callable(unsubscribe):
            unsubscribe = getattr(self.md_api, "unSubscribeQuote", None)
        if not callable(unsubscribe):
            self.publish_error(
                "unsubscribe_unsupported",
                "native TAP API does not expose unsubscribeQuote",
                retryable=False,
            )
            return
        for symbol in normalized:
            unsubscribe(self._quote_request(TapSymbol.parse(symbol)))

    @staticmethod
    def _quote_request(info: TapSymbol) -> dict[str, Any]:
        return {
            "ExchangeNo": info.exchange_no,
            "CommodityType": info.commodity_type,
            "CommodityNo": info.commodity_no,
            "ContractNo1": info.contract_no,
            "CallOrPutFlag1": "N",
            "CallOrPutFlag2": "N",
        }

    def query_account(
        self,
        max_age_seconds: float | None = None,
    ) -> dict[str, Any]:
        self._require_ready()
        if self._cache_fresh(self._account_cache_time, max_age_seconds):
            return dict(self._account_cache)
        with self._account_query_lock:
            self._query_errors.pop("account", None)
            self._account_query_event.clear()
            assert self.td_api is not None
            if self.account_no:
                self.td_api.qryFund({"AccountNo": self.account_no})
            else:
                self.td_api.qryAccount({})
            self._wait_query(self._account_query_event, "account")
            self._raise_query_error("account")
            return dict(self._account_cache)

    def query_positions(
        self,
        max_age_seconds: float | None = None,
    ) -> list[dict[str, Any]]:
        self._require_ready()
        if self._cache_fresh(self._position_cache_time, max_age_seconds):
            return [dict(item) for item in self._position_cache.values()]
        with self._position_query_lock:
            self._query_errors.pop("positions", None)
            self._position_cache.clear()
            self._position_query_event.clear()
            assert self.td_api is not None
            self.td_api.qryPositionSummary({})
            self._wait_query(self._position_query_event, "positions")
            self._raise_query_error("positions")
            return [dict(item) for item in self._position_cache.values()]

    def query_orders(
        self,
        max_age_seconds: float | None = None,
    ) -> list[dict[str, Any]]:
        self._require_ready()
        if self._cache_fresh(self._order_cache_time, max_age_seconds):
            return [dict(item) for item in self._order_cache.values()]
        with self._order_query_lock:
            self._query_errors.pop("orders", None)
            self._order_cache.clear()
            self._order_query_event.clear()
            assert self.td_api is not None
            self.td_api.qryOrder({})
            self._wait_query(self._order_query_event, "orders")
            self._raise_query_error("orders")
            return [dict(item) for item in self._order_cache.values()]

    def query_persisted_trades(
        self,
        client_id: str,
        strategy_id: str,
        *,
        after_id: int = 0,
        limit: int = 500,
    ) -> dict[str, Any]:
        return self.order_store.list_trades(
            client_id,
            strategy_id,
            after_id=after_id,
            limit=limit,
        )

    def latest_trade_cursor(self, client_id: str, strategy_id: str) -> int:
        return self.order_store.latest_trade_cursor(client_id, strategy_id)

    def query_persisted_orders(
        self,
        client_id: str,
        strategy_id: str,
    ) -> list[dict[str, Any]]:
        return [
            row
            for row in self.order_store.list()
            if str(row.get("client_id") or "") == client_id
            and str(row.get("strategy_id") or "") == strategy_id
        ]

    def place_order(self, request: dict[str, Any]) -> dict[str, Any]:
        self._require_ready()
        identity_key = (
            request["client_id"],
            request["strategy_id"],
            request["client_order_id"],
        )
        info = TapSymbol.parse(request["symbol"])
        existing_record = self.order_store.get(*identity_key)
        if existing_record:
            existing_native = str(
                existing_record.get("tap_client_order_no", "") or ""
            )
            if existing_native:
                self._hydrate_record(existing_record)
            return {
                "accepted": True,
                "duplicate": True,
                "client_order_id": request["client_order_id"],
                "tap_client_order_no": existing_native,
                "recovery_required": not bool(existing_native),
                "status": str(existing_record.get("status", "")),
                "message": str(existing_record.get("status_message", "") or ""),
            }

        reserved = self.order_store.reserve(
            client_id=request["client_id"],
            strategy_id=request["strategy_id"],
            client_order_id=request["client_order_id"],
            symbol=info.canonical,
            payload=request,
        )
        if not reserved:
            existing_record = self.order_store.get(*identity_key) or {}
            return {
                "accepted": True,
                "duplicate": True,
                "client_order_id": request["client_order_id"],
                "tap_client_order_no": str(
                    existing_record.get("tap_client_order_no", "") or ""
                ),
                "recovery_required": not bool(
                    existing_record.get("tap_client_order_no")
                ),
                "status": str(existing_record.get("status", "")),
                "message": str(existing_record.get("status_message", "") or ""),
            }

        order_request: dict[str, Any] = {
            "AccountNo": self.account_no,
            "ExchangeNo": info.exchange_no,
            "CommodityType": info.commodity_type,
            "CommodityNo": info.commodity_no,
            "ContractNo": info.contract_no,
            "OrderType": ORDER_TYPE_LIMIT,
            "OrderSide": DIRECTION_PROTOCOL_TO_TAP[request["direction"]],
            "OrderPrice": request["price"],
            "OrderQty": request["volume"],
        }
        if self.settings.client_id:
            order_request["ClientID"] = self.settings.client_id
            order_request["ClientLocationID"] = self.settings.client_location

        assert self.td_api is not None
        try:
            error_id, native_session, raw_order_id = self.td_api.insertOrder(
                order_request
            )
        except Exception as exc:
            self.order_store.mark_failed(
                client_id=request["client_id"],
                strategy_id=request["strategy_id"],
                client_order_id=request["client_order_id"],
                message=str(exc),
            )
            raise
        native_id = self._decode_order_id(raw_order_id)
        if error_id != SUCCESS:
            self.order_store.mark_failed(
                client_id=request["client_id"],
                strategy_id=request["strategy_id"],
                client_order_id=request["client_order_id"],
                message=f"TAP insertOrder failed: {error_id}",
            )
            raise RuntimeError(f"TAP insertOrder failed: {error_id}")
        if not native_id:
            self.order_store.mark_failed(
                client_id=request["client_id"],
                strategy_id=request["strategy_id"],
                client_order_id=request["client_order_id"],
                message="TAP insertOrder returned an empty ClientOrderNo",
            )
            raise RuntimeError("TAP insertOrder returned an empty ClientOrderNo")

        self.order_store.set_native(
            client_id=request["client_id"],
            strategy_id=request["strategy_id"],
            client_order_id=request["client_order_id"],
            tap_client_order_no=native_id,
        )
        identity = OrderIdentity(
            client_id=request["client_id"],
            strategy_id=request["strategy_id"],
            client_order_id=request["client_order_id"],
            symbol=info.canonical,
        )
        with self._mapping_lock:
            self._identity_by_native[native_id] = identity
            self._native_by_identity[identity_key] = native_id
        return {
            "accepted": True,
            "duplicate": False,
            "client_order_id": request["client_order_id"],
            "tap_client_order_no": native_id,
            "tap_session": native_session,
            "message": "",
        }

    def cancel_order(self, request: dict[str, Any]) -> dict[str, Any]:
        self._require_ready()
        identity_key = (
            request["client_id"],
            request["strategy_id"],
            request["client_order_id"],
        )
        native_id = self._native_by_identity.get(identity_key)
        if not native_id:
            record = self.order_store.get(*identity_key)
            if record:
                identity = self._hydrate_record(record)
                native_id = str(record.get("tap_client_order_no", "") or "")
        if not native_id:
            raise RuntimeError(
                "order identity is reserved but TAP ClientOrderNo is unavailable; "
                "manual reconciliation is required"
            )
        if native_id not in self._order_no_by_native:
            self._pending_cancels.add(native_id)
            return {
                "accepted": True,
                "pending": True,
                "client_order_id": request["client_order_id"],
            }
        self._send_cancel(native_id)
        return {
            "accepted": True,
            "pending": False,
            "client_order_id": request["client_order_id"],
        }

    def _send_cancel(self, native_id: str) -> None:
        order_no = self._order_no_by_native[native_id]
        server_flag = self._server_flag_by_native[native_id]
        assert self.td_api is not None
        self.td_api.cancelOrder(
            {
                "OrderNo": order_no,
                "ServerFlag": server_flag,
            }
        )

    def _restore_order_mappings(self) -> None:
        for record in self.order_store.list():
            self._hydrate_record(record)

    def _hydrate_record(self, record: dict[str, Any]) -> OrderIdentity:
        identity = OrderIdentity(
            client_id=str(record["client_id"]),
            strategy_id=str(record["strategy_id"]),
            client_order_id=str(record["client_order_id"]),
            symbol=str(record["symbol"]),
        )
        native_id = str(record.get("tap_client_order_no", "") or "")
        order_no = str(record.get("tap_order_no", "") or "")
        server_flag = str(record.get("tap_server_flag", "") or "")
        identity_key = (
            identity.client_id,
            identity.strategy_id,
            identity.client_order_id,
        )
        with self._mapping_lock:
            if native_id:
                self._identity_by_native[native_id] = identity
                self._native_by_identity[identity_key] = native_id
            if native_id and order_no:
                self._order_no_by_native[native_id] = order_no
                self._native_by_order_key[(order_no, server_flag)] = native_id
                self._server_flag_by_native[native_id] = server_flag
        return identity

    def _require_ready(self) -> None:
        if not self.is_ready():
            raise TapNotReadyError("TAP session is not ready")

    def _wait_query(self, event: threading.Event, name: str) -> None:
        if not event.wait(self.settings.query_timeout_seconds):
            raise TimeoutError(f"TAP {name} query timed out")

    @staticmethod
    def _cache_fresh(timestamp: float, max_age_seconds: float | None) -> bool:
        return (
            max_age_seconds is not None
            and timestamp > 0
            and time.monotonic() - timestamp <= max_age_seconds
        )

    def _query_failed(
        self,
        name: str,
        error_code: int,
        event: threading.Event,
        query_key: str,
    ) -> None:
        self.last_error = f"{name} query failed: {error_code}"
        self._query_errors[query_key] = self.last_error
        self.publish_error(
            f"{name}_query_failed",
            self.last_error,
            retryable=True,
        )
        event.set()

    def _raise_query_error(self, query_key: str) -> None:
        message = self._query_errors.pop(query_key, "")
        if message:
            raise RuntimeError(message)

    def _schedule_reconnect(self, reason: str) -> None:
        if self.settings.reconnect_max_attempts == 0:
            return
        with self._reconnect_lock:
            if self._reconnect_thread and self._reconnect_thread.is_alive():
                return
            self._reconnect_thread = threading.Thread(
                target=self._reconnect_loop,
                args=(reason,),
                name="tap-reconnect",
                daemon=True,
            )
            self._reconnect_thread.start()

    def _reconnect_loop(self, reason: str) -> None:
        while (
            self.settings.reconnect_max_attempts < 0
            or self.reconnect_attempts < self.settings.reconnect_max_attempts
        ):
            self.reconnect_attempts += 1
            delay = min(
                self.settings.reconnect_initial_delay_seconds
                * (2 ** max(0, self.reconnect_attempts - 1)),
                self.settings.reconnect_max_delay_seconds,
            )
            self.publish_status(
                f"reconnect attempt {self.reconnect_attempts} in {delay}s: {reason}"
            )
            if self._closing.wait(delay):
                return
            try:
                if self.connect(self.settings.connect_timeout_seconds):
                    return
            except Exception as exc:
                self.last_error = str(exc)
                logger.exception("TAP reconnect attempt failed")
        self.publish_error(
            "reconnect_exhausted",
            f"TAP reconnect attempts exhausted: {self.reconnect_attempts}",
            retryable=False,
        )

    def publish_status(self, reason: str) -> None:
        logger.info("TAP status: {}", reason)
        self.publish(
            "status.TAP",
            Event.STATUS.value,
            {
                "ready": self.is_ready(),
                "md_enabled": self.settings.enable_md,
                "md_ready": self.md_ready_event.is_set(),
                "td_ready": self.td_ready_event.is_set(),
                "account_ready": self.account_ready_event.is_set(),
                "initial_sync_ready": self.initial_sync_event.is_set(),
                "reason": reason,
                "occurred_at": int(time.time() * 1000),
            },
        )

    def publish_error(
        self,
        code: str,
        message: str,
        *,
        retryable: bool,
        details: dict[str, Any] | None = None,
    ) -> None:
        payload: dict[str, Any] = {
            "code": code,
            "message": message,
            "retryable": retryable,
            "occurred_at": int(time.time() * 1000),
        }
        if details:
            payload["details"] = details
        logger.error("TAP error {}: {}", code, message)
        self.publish("errors.TAP", Event.ERROR.value, payload)

    def _publish_scoped(
        self,
        prefix: str,
        account_id: str,
        identity: OrderIdentity | None,
        event: Event,
        payload: dict[str, Any],
    ) -> None:
        self.publish(f"{prefix}.{account_id}", event.value, payload)
        if identity:
            self.publish(
                f"{prefix}.{account_id}.{identity.strategy_id}",
                event.value,
                payload,
            )

    def _register_symbol(self, symbol: str) -> None:
        info = TapSymbol.parse(symbol)
        self._symbol_by_short[
            (info.exchange_no, info.commodity_no, info.contract_no)
        ] = info.canonical

    def _canonical_from_data(
        self,
        data: dict[str, Any],
        contract_field: str,
    ) -> str:
        exchange = str(data.get("ExchangeNo", "")).upper()
        commodity = str(data.get("CommodityNo", "")).upper()
        contract = str(data.get(contract_field, "")).upper()
        commodity_type = str(data.get("CommodityType", "")).upper()
        if exchange and commodity_type and commodity and contract:
            symbol = TapSymbol(
                exchange,
                commodity_type,
                commodity,
                contract,
            ).canonical
            self._register_symbol(symbol)
            return symbol
        symbol = self._symbol_by_short.get((exchange, commodity, contract), "")
        if not symbol:
            self.publish_error(
                "unknown_symbol",
                f"cannot resolve TAP symbol from {exchange}:{commodity}:{contract}",
                retryable=False,
            )
        return symbol

    def _direction(self, value: Any) -> str:
        direction = DIRECTION_TAP_TO_PROTOCOL.get(str(value), "")
        if not direction:
            self.publish_error(
                "unknown_direction",
                f"unknown TAP side: {value}",
                retryable=False,
            )
        return direction

    def _parse_timestamp(self, value: str) -> int | None:
        if not value:
            return None
        for fmt in (
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%y%m%d%H%M%S.%f",
        ):
            try:
                parsed = datetime.strptime(value, fmt).replace(tzinfo=self.timezone)
                return int(parsed.timestamp() * 1000)
            except ValueError:
                continue
        self.publish_error(
            "timestamp_parse_failed",
            f"unsupported TAP timestamp: {value}",
            retryable=False,
        )
        return None

    def _decode_order_id(self, value: Any) -> str:
        if isinstance(value, bytes):
            raw = value
            if self.settings.client_id:
                prefix = (
                    b"#"
                    + self.settings.client_id.encode()
                    + b"#"
                    + self.settings.client_location.encode()
                    + b"#"
                )
                raw = raw.replace(prefix, b"", 1)
            return raw.decode()
        return str(value)

    @staticmethod
    def _is_last(value: Any) -> bool:
        return value in LAST_FLAGS

    @staticmethod
    def _number(value: Any) -> float | None:
        if value in (None, ""):
            return None
        return float(value)

    @staticmethod
    def _integer(value: Any) -> int | None:
        if value in (None, ""):
            return None
        return int(value)

    @classmethod
    def _first_number(cls, values: Any) -> float | None:
        return cls._number(values[0]) if values else None

    @classmethod
    def _first_integer(cls, values: Any) -> int | None:
        return cls._integer(values[0]) if values else None

    def _close_native(self) -> None:
        for api in (self.md_api, self.td_api):
            if api is None:
                continue
            disconnect = getattr(api, "disconnect", None)
            if callable(disconnect):
                try:
                    disconnect()
                except Exception:
                    logger.exception("Failed to disconnect native TAP API")
            exit_api = getattr(api, "exit", None)
            if callable(exit_api):
                try:
                    exit_api()
                except Exception:
                    logger.exception("Failed to exit native TAP API")
        self.md_api = None
        self.td_api = None

    def close(self) -> None:
        if self._closing.is_set():
            return
        self._closing.set()
        self._close_native()
        for event in (
            self.md_ready_event,
            self.td_ready_event,
            self.account_ready_event,
            self.initial_sync_event,
        ):
            event.clear()
        for event in (
            self._account_query_event,
            self._position_query_event,
            self._order_query_event,
        ):
            event.set()
        reconnect_thread = self._reconnect_thread
        if reconnect_thread and reconnect_thread.is_alive():
            reconnect_thread.join(timeout=2)
        self.order_store.close()
