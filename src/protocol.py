from __future__ import annotations

import math
import time
from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any


SCHEMA_VERSION = 1


class Action(str, Enum):
    PING = "ping"
    STATUS = "status"
    SUBSCRIBE_MARKET_DATA = "subscribe_market_data"
    UNSUBSCRIBE_MARKET_DATA = "unsubscribe_market_data"
    GET_ACCOUNT = "get_account"
    GET_POSITIONS = "get_positions"
    GET_ORDERS = "get_orders"
    GET_TRADES = "get_trades"
    GET_TRADE_CURSOR = "get_trade_cursor"
    PLACE_ORDER = "place_order"
    CANCEL_ORDER = "cancel_order"


class Direction(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderStatus(str, Enum):
    SUBMITTED = "SUBMITTED"
    PARTTRADED = "PARTTRADED"
    TRADED = "TRADED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    UNKNOWN = "UNKNOWN"


class Event(str, Enum):
    MARKET_DATA = "marketdata"
    ORDER = "order"
    TRADE = "trade"
    ACCOUNT = "account"
    POSITION = "position"
    STATUS = "status"
    ERROR = "error"


class ErrorCode(str, Enum):
    INVALID_REQUEST = "invalid_request"
    UNSUPPORTED_ACTION = "unsupported_action"
    NOT_IMPLEMENTED = "not_implemented"
    NOT_READY = "not_ready"
    TIMEOUT = "timeout"
    INTERNAL_ERROR = "internal_error"


@dataclass(frozen=True, slots=True)
class TapSymbol:
    exchange_no: str
    commodity_type: str
    commodity_no: str
    contract_no: str

    @classmethod
    def parse(cls, value: Any) -> "TapSymbol":
        if not isinstance(value, str):
            raise ValueError("symbol must be a string")
        parts = [part.strip().upper() for part in value.split(":")]
        if len(parts) != 4 or any(not part for part in parts):
            raise ValueError(
                "symbol must use ExchangeNo:CommodityType:CommodityNo:ContractNo"
            )
        return cls(*parts)

    @property
    def canonical(self) -> str:
        return ":".join(
            (
                self.exchange_no,
                self.commodity_type,
                self.commodity_no,
                self.contract_no,
            )
        )


class ProtocolError(ValueError):
    def __init__(self, message: str, code: ErrorCode = ErrorCode.INVALID_REQUEST):
        super().__init__(message)
        self.code = code


def parse_action(request: Any) -> Action:
    if not isinstance(request, dict):
        raise ProtocolError("request must be a JSON object")
    value = str(request.get("action", "")).strip().lower()
    if not value:
        raise ProtocolError("Missing required field: action")
    try:
        return Action(value)
    except ValueError as exc:
        raise ProtocolError(
            f"Unsupported action: {value}",
            ErrorCode.UNSUPPORTED_ACTION,
        ) from exc


def require_fields(request: dict[str, Any], *fields: str) -> None:
    missing = [name for name in fields if request.get(name) in (None, "")]
    if missing:
        raise ProtocolError(f"Missing required fields: {', '.join(missing)}")


def normalize_identity(request: dict[str, Any], *fields: str) -> None:
    require_fields(request, *fields)
    for field in fields:
        value = request[field]
        if not isinstance(value, str) or not value.strip():
            raise ProtocolError(f"{field} must be a non-empty string")
        request[field] = value.strip()


def canonical_symbol(value: Any) -> str:
    return TapSymbol.parse(value).canonical


def normalize_symbols(value: Any) -> list[str]:
    if isinstance(value, str):
        values = value.split(",")
    elif isinstance(value, (list, tuple, set)):
        values = value
    else:
        raise ProtocolError("symbols must be a string or an array")
    try:
        return list(dict.fromkeys(canonical_symbol(item) for item in values))
    except ValueError as exc:
        raise ProtocolError(str(exc)) from exc


def validate_request(request: Any) -> tuple[Action, dict[str, Any]]:
    action = parse_action(request)
    assert isinstance(request, dict)
    normalized = dict(request)
    normalized["action"] = action.value

    if action in {Action.SUBSCRIBE_MARKET_DATA, Action.UNSUBSCRIBE_MARKET_DATA}:
        normalize_identity(normalized, "client_id", "strategy_id")
        require_fields(normalized, "symbols")
        normalized["symbols"] = normalize_symbols(normalized["symbols"])
        if not normalized["symbols"]:
            raise ProtocolError("symbols must contain at least one symbol")

    elif action is Action.PLACE_ORDER:
        normalize_identity(
            normalized,
            "client_id",
            "strategy_id",
            "client_order_id",
        )
        require_fields(
            normalized,
            "symbol",
            "direction",
            "price",
            "volume",
        )
        normalized.pop("offset", None)
        normalized["symbol"] = canonical_symbol(normalized["symbol"])
        try:
            if isinstance(normalized["price"], bool):
                raise ValueError("price must be numeric")
            normalized["direction"] = Direction(
                str(normalized["direction"]).upper()
            ).value
            normalized["price"] = float(normalized["price"])
            if isinstance(normalized["volume"], bool):
                raise ValueError("volume must be an integer")
            volume_decimal = Decimal(str(normalized["volume"]))
            if (
                not volume_decimal.is_finite()
                or volume_decimal != volume_decimal.to_integral_value()
            ):
                raise ValueError("volume must be an integer")
            normalized["volume"] = int(volume_decimal)
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ProtocolError(str(exc)) from exc
        if not math.isfinite(normalized["price"]) or normalized["price"] <= 0:
            raise ProtocolError("price must be a positive finite number")
        if normalized["volume"] <= 0:
            raise ProtocolError("volume must be positive")

    elif action is Action.CANCEL_ORDER:
        normalize_identity(
            normalized,
            "client_id",
            "strategy_id",
            "client_order_id",
        )

    elif action in {Action.GET_TRADES, Action.GET_TRADE_CURSOR}:
        normalize_identity(normalized, "client_id", "strategy_id")
        if action is Action.GET_TRADE_CURSOR:
            return action, normalized
        for field, default in (("after_id", 0), ("limit", 500)):
            value = normalized.get(field, default)
            if isinstance(value, bool):
                raise ProtocolError(f"{field} must be an integer")
            try:
                normalized[field] = int(value)
            except (TypeError, ValueError) as exc:
                raise ProtocolError(f"{field} must be an integer") from exc
        if normalized["after_id"] < 0:
            raise ProtocolError("after_id must not be negative")
        if normalized["limit"] <= 0 or normalized["limit"] > 1000:
            raise ProtocolError("limit must be between 1 and 1000")

    elif action in {
        Action.GET_ACCOUNT,
        Action.GET_POSITIONS,
        Action.GET_ORDERS,
    }:
        if action is Action.GET_ORDERS and normalized.get("local_only"):
            normalize_identity(normalized, "client_id", "strategy_id")
        if "force_refresh" in normalized and not isinstance(
            normalized["force_refresh"],
            bool,
        ):
            raise ProtocolError("force_refresh must be a boolean")
        if "max_age_ms" in normalized:
            if isinstance(normalized["max_age_ms"], bool):
                raise ProtocolError("max_age_ms must be an integer")
            try:
                normalized["max_age_ms"] = int(normalized["max_age_ms"])
            except (TypeError, ValueError) as exc:
                raise ProtocolError("max_age_ms must be an integer") from exc
            if normalized["max_age_ms"] < 0:
                raise ProtocolError("max_age_ms must not be negative")

    return action, normalized


def json_value(value: Any) -> Any:
    if is_dataclass(value):
        return json_value(asdict(value))
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_value(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def response_ok(data: Any = None, request_id: str | None = None) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "ok",
        "request_id": request_id,
        "data": json_value(data),
        "error": None,
    }


def response_error(
    message: str,
    request_id: str | None = None,
    *,
    code: ErrorCode = ErrorCode.INVALID_REQUEST,
    retryable: bool = False,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "error",
        "request_id": request_id,
        "data": None,
        "error": {
            "code": code.value,
            "message": message,
            "retryable": retryable,
        },
    }


def event_payload(event: str | Event, data: Any) -> dict[str, Any]:
    event_value = event.value if isinstance(event, Event) else str(event).strip()
    if not event_value:
        raise ValueError("event must not be empty")
    return {
        "schema_version": SCHEMA_VERSION,
        "event": event_value,
        "published_at": int(time.time() * 1000),
        "data": json_value(data),
    }


def market_data_topic(symbol: Any) -> str:
    return f"marketdata.TAP.{canonical_symbol(symbol)}"
