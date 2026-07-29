from __future__ import annotations

import json

import pytest

from market_data_example import (
    command_request,
    decode_pub_message,
    parse_args,
)


def test_parse_args_normalizes_symbol() -> None:
    args = parse_args([" lme:f:ni:3m ", "--count", "1"])

    assert args.symbol == "LME:F:NI:3M"
    assert args.count == 1


def test_command_request_contains_subscription_identity() -> None:
    request = command_request(
        "subscribe_market_data",
        "LME:F:NI:3M",
        "client-1",
        "strategy-1",
    )

    assert request["action"] == "subscribe_market_data"
    assert request["client_id"] == "client-1"
    assert request["strategy_id"] == "strategy-1"
    assert request["symbols"] == ["LME:F:NI:3M"]
    assert request["request_id"].startswith("md-example-")


def test_decode_pub_message() -> None:
    topic, payload = decode_pub_message(
        [
            b"marketdata.TAP.LME:F:NI:3M",
            json.dumps({"event": "marketdata", "data": {"last_price": 15000}}).encode(),
        ]
    )

    assert topic == "marketdata.TAP.LME:F:NI:3M"
    assert payload["data"]["last_price"] == 15000


def test_decode_pub_message_rejects_bad_frame_count() -> None:
    with pytest.raises(ValueError, match="expected 2 PUB frames"):
        decode_pub_message([b"topic-only"])
