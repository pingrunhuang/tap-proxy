from datetime import datetime, timezone

import pytest

from protocol import (
    Action,
    ErrorCode,
    Event,
    ProtocolError,
    TapSymbol,
    event_payload,
    json_value,
    market_data_topic,
    normalize_symbols,
    response_error,
    validate_request,
)


def test_tap_symbol_is_canonicalized():
    symbol = TapSymbol.parse(" comex : f : gc : 2608 ")
    assert symbol.canonical == "COMEX:F:GC:2608"
    assert market_data_topic(symbol.canonical) == "marketdata.TAP.COMEX:F:GC:2608"


@pytest.mark.parametrize(
    "value",
    ["GC2608", "COMEX:F:GC", "COMEX::GC:2608", "", None],
)
def test_invalid_tap_symbols_are_rejected(value):
    with pytest.raises(ValueError, match="symbol"):
        TapSymbol.parse(value)


def test_symbols_are_deduplicated_after_canonicalization():
    assert normalize_symbols(
        ["COMEX:F:GC:2608", " comex:f:gc:2608 ", "COMEX:F:GC:2610"]
    ) == ["COMEX:F:GC:2608", "COMEX:F:GC:2610"]


def test_place_order_is_normalized_and_validated():
    action, request = validate_request(
        {
            "action": "PLACE_ORDER",
            "client_id": "engine-01",
            "strategy_id": "gc-arb",
            "client_order_id": "gc-arb-1",
            "symbol": "comex:f:gc:2608",
            "direction": "buy",
            "offset": "open",
            "price": "2400.5",
            "volume": "1",
        }
    )
    assert action is Action.PLACE_ORDER
    assert request["symbol"] == "COMEX:F:GC:2608"
    assert request["direction"] == "BUY"
    assert request["offset"] == "OPEN"
    assert request["price"] == 2400.5
    assert request["volume"] == 1


@pytest.mark.parametrize(
    "field,value,message",
    [
        ("price", 0, "price"),
        ("price", float("inf"), "price"),
        ("volume", 0, "volume"),
        ("volume", 1.5, "integer"),
        ("volume", True, "integer"),
        ("price", True, "numeric"),
        ("direction", "HOLD", "HOLD"),
        ("offset", "NONE", "NONE"),
    ],
)
def test_invalid_place_order_values_are_rejected(field, value, message):
    request = {
        "action": "place_order",
        "client_id": "engine-01",
        "strategy_id": "gc-arb",
        "client_order_id": "gc-arb-1",
        "symbol": "COMEX:F:GC:2608",
        "direction": "BUY",
        "offset": "OPEN",
        "price": 2400.5,
        "volume": 1,
    }
    request[field] = value
    with pytest.raises(ProtocolError, match=message):
        validate_request(request)


def test_order_identity_is_required():
    with pytest.raises(ProtocolError, match="client_id"):
        validate_request(
            {
                "action": "place_order",
                "symbol": "COMEX:F:GC:2608",
                "direction": "BUY",
                "offset": "OPEN",
                "price": 2400.5,
                "volume": 1,
            }
        )


def test_order_identity_must_be_a_non_empty_string():
    with pytest.raises(ProtocolError, match="client_id"):
        validate_request(
            {
                "action": "cancel_order",
                "client_id": [],
                "strategy_id": "gc-arb",
                "client_order_id": "order-1",
            }
        )


def test_unknown_action_has_stable_error_code():
    with pytest.raises(ProtocolError) as exc_info:
        validate_request({"action": "launch_missiles"})
    assert exc_info.value.code is ErrorCode.UNSUPPORTED_ACTION


def test_force_refresh_must_be_boolean():
    with pytest.raises(ProtocolError, match="force_refresh"):
        validate_request({"action": "get_positions", "force_refresh": "false"})


def test_get_trades_requires_owner_and_normalizes_pagination():
    action, request = validate_request(
        {
            "action": "get_trades",
            "client_id": "engine",
            "strategy_id": "gc-arb",
            "after_id": "10",
            "limit": "25",
        }
    )
    assert action is Action.GET_TRADES
    assert request["after_id"] == 10
    assert request["limit"] == 25

    with pytest.raises(ProtocolError, match="strategy_id"):
        validate_request({"action": "get_trades", "client_id": "engine"})


def test_event_and_error_envelopes_follow_schema_v1():
    event = event_payload(
        Event.STATUS,
        {"at": datetime(2026, 7, 28, tzinfo=timezone.utc)},
    )
    error = response_error(
        "not ready",
        "req-1",
        code=ErrorCode.NOT_READY,
        retryable=True,
    )
    assert event["schema_version"] == 1
    assert event["event"] == "status"
    assert isinstance(event["published_at"], int)
    assert event["data"]["at"] == "2026-07-28T00:00:00+00:00"
    assert error["error"] == {
        "code": "not_ready",
        "message": "not ready",
        "retryable": True,
    }


def test_non_finite_values_are_safe_for_json():
    assert json_value({"price": float("inf")}) == {"price": None}
