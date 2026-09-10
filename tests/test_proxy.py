import time

import pytest
import zmq

from config import Settings
from proxy import TapProxy


class FakeNativeSession:
    def __init__(self):
        self.subscribed = []
        self.unsubscribed = []
        self.query_calls = []
        self.placed = []
        self.cancelled = []

    def connect(self, timeout=None):
        return True

    def is_ready(self):
        return True

    def status(self):
        return {"implementation": "fake_native"}

    def subscribe_market_data(self, symbols):
        self.subscribed.append(symbols)

    def unsubscribe_market_data(self, symbols):
        self.unsubscribed.append(symbols)

    def query_account(self, max_age_seconds=None):
        self.query_calls.append(("account", max_age_seconds))
        return {"account_id": "TAP-ACCOUNT"}

    def query_positions(self, max_age_seconds=None):
        self.query_calls.append(("positions", max_age_seconds))
        return []

    def query_orders(self, max_age_seconds=None):
        self.query_calls.append(("orders", max_age_seconds))
        return []

    def query_persisted_trades(
        self,
        client_id,
        strategy_id,
        *,
        after_id=0,
        limit=500,
    ):
        self.query_calls.append(
            ("trades", client_id, strategy_id, after_id, limit)
        )
        return {
            "trades": [{"event_id": "trade:tap:1"}],
            "next_after_id": 1,
            "has_more": False,
        }

    def latest_trade_cursor(self, client_id, strategy_id):
        self.query_calls.append(("trade_cursor", client_id, strategy_id))
        return 7

    def query_persisted_orders(self, client_id, strategy_id):
        self.query_calls.append(("persisted_orders", client_id, strategy_id))
        return []

    def place_order(self, request):
        self.placed.append(request)
        return {"accepted": True, "client_order_id": request["client_order_id"]}

    def cancel_order(self, request):
        self.cancelled.append(request)
        return {"accepted": True, "client_order_id": request["client_order_id"]}

    def close(self):
        return None


@pytest.fixture
def proxy():
    instance = TapProxy(
        Settings(
            zmq_bind_host="127.0.0.1",
            zmq_pub_port=0,
            zmq_rep_port=0,
        )
    )
    instance.start()
    try:
        yield instance
    finally:
        instance.stop()


def test_ping_and_status_report_transport_but_not_tap_ready(proxy):
    ping = proxy.handle_command({"action": "ping", "request_id": "health-1"})
    status = proxy.handle_command({"action": "status"})
    assert ping["status"] == "ok"
    assert ping["request_id"] == "health-1"
    assert ping["data"]["transport_ready"] is True
    assert ping["data"]["ready"] is False
    assert ping["data"]["md_enabled"] is True
    assert ping["data"]["td_enabled"] is True
    assert status["data"]["pub_port"] == proxy.bound_pub_port
    assert status["data"]["rep_port"] == proxy.bound_rep_port
    assert status["data"]["session"]["implementation"] == "pending_or_test_double"
    assert status["data"]["md_enabled"] is True
    assert status["data"]["td_enabled"] is True


def test_frozen_but_unimplemented_command_is_explicit(proxy):
    response = proxy.handle_command(
        {
            "action": "subscribe_market_data",
            "client_id": "engine-01",
            "strategy_id": "gc-arb",
            "symbols": ["COMEX:F:GC:2608"],
        }
    )
    assert response["status"] == "error"
    assert response["error"]["code"] == "not_implemented"


def test_invalid_and_unknown_commands_have_distinct_errors(proxy):
    missing = proxy.handle_command({})
    unknown = proxy.handle_command({"action": "unknown"})
    assert missing["error"]["code"] == "invalid_request"
    assert unknown["error"]["code"] == "unsupported_action"


def test_real_req_rep_round_trip(proxy):
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.LINGER, 0)
    socket.setsockopt(zmq.RCVTIMEO, 2000)
    socket.connect(f"tcp://127.0.0.1:{proxy.bound_rep_port}")
    try:
        socket.send_json({"action": "ping", "request_id": "roundtrip-1"})
        response = socket.recv_json()
        assert response["status"] == "ok"
        assert response["request_id"] == "roundtrip-1"
    finally:
        socket.close(0)
        context.term()


def test_publish_queue_emits_two_frame_event(proxy):
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.setsockopt(zmq.LINGER, 0)
    socket.setsockopt(zmq.RCVTIMEO, 2000)
    socket.setsockopt_string(zmq.SUBSCRIBE, "status.TAP")
    socket.connect(f"tcp://127.0.0.1:{proxy.bound_pub_port}")
    try:
        # Allow the PUB/SUB subscription handshake to complete.
        time.sleep(0.1)
        proxy.enqueue_publish("status.TAP", "status", {"ready": False})
        topic, payload = socket.recv_multipart()
        assert topic == b"status.TAP"
        assert b'"schema_version": 1' in payload
        assert b'"ready": false' in payload
    finally:
        socket.close(0)
        context.term()


def test_commands_are_dispatched_to_native_session():
    session = FakeNativeSession()
    instance = TapProxy(Settings(), session=session)
    symbol = "COMEX:F:GC:2608"

    first = instance.handle_command(
        {
            "action": "subscribe_market_data",
            "client_id": "engine",
            "strategy_id": "a",
            "symbols": [symbol],
        }
    )
    second = instance.handle_command(
        {
            "action": "subscribe_market_data",
            "client_id": "engine",
            "strategy_id": "b",
            "symbols": [symbol],
        }
    )
    account = instance.handle_command(
        {"action": "get_account", "max_age_ms": 5000}
    )
    positions = instance.handle_command(
        {"action": "get_positions", "force_refresh": True}
    )
    trades = instance.handle_command(
        {
            "action": "get_trades",
            "client_id": "engine",
            "strategy_id": "a",
            "after_id": 0,
            "limit": 25,
        }
    )
    cursor = instance.handle_command(
        {
            "action": "get_trade_cursor",
            "client_id": "engine",
            "strategy_id": "a",
        }
    )
    order_request = {
        "action": "place_order",
        "client_id": "engine",
        "strategy_id": "a",
        "client_order_id": "order-1",
        "symbol": symbol,
        "direction": "BUY",
        "price": 2400.5,
        "volume": 1,
    }
    placed = instance.handle_command(order_request)
    cancelled = instance.handle_command(
        {
            "action": "cancel_order",
            "client_id": "engine",
            "strategy_id": "a",
            "client_order_id": "order-1",
        }
    )

    assert first["data"]["newly_active"] == [symbol]
    assert second["data"]["newly_active"] == []
    assert session.subscribed == [[symbol], []]
    assert account["data"]["account_id"] == "TAP-ACCOUNT"
    assert positions["status"] == "ok"
    assert session.query_calls == [
        ("account", 5.0),
        ("positions", None),
        ("trades", "engine", "a", 0, 25),
        ("trade_cursor", "engine", "a"),
    ]
    assert trades["data"]["trades"][0]["event_id"] == "trade:tap:1"
    assert cursor["data"] == {"cursor": 7}
    assert placed["data"]["accepted"] is True
    assert cancelled["data"]["accepted"] is True
