from pathlib import Path

import pytest

from config import Settings
from order_store import MemoryOrderStore
from tap_session import (
    ORDER_TYPE_LIMIT,
    SIDE_BUY,
    NativeTapSession,
    TapUnavailableError,
    _trading_day_from_timestamp,
)


SYMBOL = "COMEX:F:GC:2608"


def test_trading_day_supports_tap_timestamp_formats():
    assert _trading_day_from_timestamp("2026-07-28 10:31:05.456") == "20260728"
    assert _trading_day_from_timestamp("260728103105.456") == "20260728"


class FakeMdApi:
    def __init__(self, session):
        self.session = session
        self.calls = []
        self.subscriptions = []
        self.unsubscriptions = []
        self.closed = False

    def init(self):
        self.calls.append("init")

    def setTapQuoteAPIDataPath(self, path):
        self.calls.append(("path", path))

    def setTapQuoteAPILogLevel(self, level):
        self.calls.append(("log_level", level))

    def createTapQuoteAPI(self, request, mode):
        self.calls.append(("create", request, mode))

    def setHostAddress(self, host, port):
        self.calls.append(("host", host, port))

    def login(self, request):
        self.calls.append(("login", request))
        self.session.on_md_login(0, {})
        self.session.on_md_ready()

    def subscribeQuote(self, request):
        self.subscriptions.append(request)

    def unsubscribeQuote(self, request):
        self.unsubscriptions.append(request)

    def disconnect(self):
        self.closed = True

    def exit(self):
        self.closed = True


class FakeTdApi:
    def __init__(self, session):
        self.session = session
        self.calls = []
        self.cancel_requests = []
        self.insert_requests = []
        self.closed = False
        self.next_order = 1

    def init(self):
        self.calls.append("init")

    def createITapTradeAPI(self, request, mode):
        self.calls.append(("create", request, mode))

    def setHostAddress(self, host, port):
        self.calls.append(("host", host, port))

    def login(self, request):
        self.calls.append(("login", request))
        self.session.on_td_login(0, {})
        self.session.on_td_ready(0)

    def qryAccount(self, request):
        self.calls.append(("qryAccount", request))
        self.session.on_account_response(
            1,
            0,
            "Y",
            {"AccountNo": "TAP-ACCOUNT"},
        )

    def qryFund(self, request):
        self.calls.append(("qryFund", request))
        self.session.on_fund_response(
            2,
            0,
            "Y",
            {
                "AccountNo": "TAP-ACCOUNT",
                "Balance": 100_000,
                "Available": 97_500,
                "FrozenMargin": 2_500,
                "CurrencyNo": "USD",
            },
        )

    def qryPositionSummary(self, request):
        self.calls.append(("qryPositionSummary", request))
        self.session.on_position_response(
            3,
            0,
            "Y",
            {
                "AccountNo": "TAP-ACCOUNT",
                "ExchangeNo": "COMEX",
                "CommodityType": "F",
                "CommodityNo": "GC",
                "ContractNo": "2608",
                "MatchSide": SIDE_BUY,
                "PositionQty": 2,
                "HisPositionQty": 1,
            },
        )

    def qryOrder(self, request):
        self.calls.append(("qryOrder", request))
        self.session.on_order_response(4, 0, "Y", {})

    def insertOrder(self, request):
        self.insert_requests.append(request)
        native_id = f"NATIVE-{self.next_order}".encode()
        self.next_order += 1
        return 0, 10, native_id

    def cancelOrder(self, request):
        self.cancel_requests.append(request)

    def disconnect(self):
        self.closed = True

    def exit(self):
        self.closed = True


class FakeRejectedTdApi(FakeTdApi):
    def insertOrder(self, request):
        self.insert_requests.append(request)
        return 42, 10, b""


def make_settings(tmp_path: Path) -> Settings:
    return Settings(
        md_host="md.example",
        md_port=10001,
        md_user_id="md-user",
        md_password="md-password",
        md_auth_code="md-auth",
        td_host="td.example",
        td_port=10002,
        td_user_id="td-user",
        td_password="td-password",
        td_auth_code="td-auth",
        initial_symbols=[SYMBOL],
        tap_data_path=tmp_path / "tap-flow",
        query_timeout_seconds=0.5,
        reconnect_max_attempts=0,
    )


@pytest.fixture
def native_session(tmp_path):
    published = []
    session = NativeTapSession(
        make_settings(tmp_path),
        lambda topic, event, data: published.append((topic, event, data)),
        md_factory=FakeMdApi,
        td_factory=FakeTdApi,
        native_available=True,
    )
    try:
        yield session, published
    finally:
        session.close()


def test_connect_runs_native_login_and_initial_snapshot_chain(native_session):
    session, published = native_session

    assert session.connect(0.5)
    assert session.is_ready()
    assert session.status()["initial_sync_ready"] is True
    assert session.account_no == "TAP-ACCOUNT"
    assert session.query_account(60)["available"] == 97_500
    assert session.query_positions(60) == [
        {
            "account_id": "TAP-ACCOUNT",
            "symbol": SYMBOL,
            "direction": "BUY",
            "volume": 2,
            "yesterday_volume": 1,
        }
    ]
    assert any(topic == "account.TAP-ACCOUNT" for topic, _, _ in published)
    assert any(topic == "positions.TAP-ACCOUNT" for topic, _, _ in published)


def test_td_only_connect_skips_md_api(tmp_path):
    config = make_settings(tmp_path)
    config.enable_md = False
    config.initial_symbols = []
    session = NativeTapSession(
        config,
        lambda *_args: None,
        md_factory=lambda _session: pytest.fail("MD API must not be created"),
        td_factory=FakeTdApi,
        native_available=True,
    )
    try:
        assert session.connect(0.5)
        assert session.md_api is None
        assert session.is_ready()
        assert session.status()["md_enabled"] is False
        with pytest.raises(RuntimeError, match="TAP_ENABLE_MD=false"):
            session.subscribe_market_data([SYMBOL])
    finally:
        session.close()


def test_td_ready_notification_code_is_not_treated_as_error(tmp_path):
    class NonzeroReadyTdApi(FakeTdApi):
        def login(self, request):
            self.calls.append(("login", request))
            self.session.on_td_login(0, {})
            self.session.on_td_ready(-4)

    session = NativeTapSession(
        make_settings(tmp_path),
        lambda *_args: None,
        md_factory=FakeMdApi,
        td_factory=NonzeroReadyTdApi,
        native_available=True,
    )
    try:
        assert session.connect(0.5)
        assert session.status()["td_ready"] is True
        assert session.status()["initial_sync_ready"] is True
    finally:
        session.close()


def test_quote_is_converted_to_protocol_event(native_session):
    session, published = native_session
    assert session.connect(0.5)

    session.on_quote(
        {
            "ExchangeNo": "COMEX",
            "CommodityType": "F",
            "CommodityNo": "GC",
            "ContractNo1": "2608",
            "QLastPrice": 2400.5,
            "QTotalQty": 1024,
            "QPositionQty": 50,
            "QLimitUpPrice": 2500,
            "QLimitDownPrice": 2300,
            "QBidPrice": [2400.4],
            "QBidQty": [3],
            "QAskPrice": [2400.6],
            "QAskQty": [4],
            "DateTimeStamp": "2026-07-28 10:30:01.123",
        }
    )

    topic, event, tick = published[-1]
    assert topic == f"marketdata.TAP.{SYMBOL}"
    assert event == "marketdata"
    assert tick["symbol"] == SYMBOL
    assert tick["last_price"] == 2400.5
    assert tick["bid_volume_1"] == 3
    assert isinstance(tick["exchange_timestamp"], int)


def test_subscribe_response_publishes_initial_quote_snapshot(native_session):
    session, published = native_session
    assert session.connect(0.5)
    published.clear()

    session.on_subscribe_response(
        1,
        0,
        "Y",
        {
            "ExchangeNo": "LME",
            "CommodityType": "F",
            "CommodityNo": "NI",
            "ContractNo1": "3M",
            "QLastPrice": 15_000,
            "QTotalQty": 10,
            "QBidPrice": [14_995],
            "QBidQty": [1],
            "QAskPrice": [15_005],
            "QAskQty": [2],
            "DateTimeStamp": "2026-07-29 01:30:00.000",
        },
    )

    topic, event, tick = published[-1]
    assert topic == "marketdata.TAP.LME:F:NI:3M"
    assert event == "marketdata"
    assert tick["last_price"] == 15_000


def test_subscribe_and_unsubscribe_use_canonical_tap_request(native_session):
    session, _ = native_session
    assert session.connect(0.5)
    session.md_api.subscriptions.clear()

    session.subscribe_market_data(["COMEX:F:GC:2610"])
    session.unsubscribe_market_data(["COMEX:F:GC:2610"])

    assert session.md_api.subscriptions == [
        {
            "ExchangeNo": "COMEX",
            "CommodityType": "F",
            "CommodityNo": "GC",
            "ContractNo1": "2610",
            "CallOrPutFlag1": "N",
            "CallOrPutFlag2": "N",
        }
    ]
    assert session.md_api.unsubscriptions[0]["ContractNo1"] == "2610"


def test_place_order_is_idempotent_and_cancel_waits_for_order_mapping(
    native_session,
):
    session, published = native_session
    assert session.connect(0.5)
    request = {
        "client_id": "engine-01",
        "strategy_id": "gc-arb",
        "client_order_id": "gc-arb-1",
        "symbol": SYMBOL,
        "direction": "BUY",
        "price": 2400.5,
        "volume": 1,
    }

    first = session.place_order(request)
    duplicate = session.place_order(request)
    assert first["tap_client_order_no"] == "NATIVE-1"
    assert first["message"] == ""
    assert duplicate["duplicate"] is True
    assert duplicate["message"] == ""
    assert len(session.td_api.insert_requests) == 1
    assert session.td_api.insert_requests[0]["OrderType"] == ORDER_TYPE_LIMIT

    pending = session.cancel_order(request)
    assert pending["pending"] is True
    assert session.td_api.cancel_requests == []

    session.on_order(
        {
            "ErrorCode": 0,
            "AccountNo": "TAP-ACCOUNT",
            "ClientOrderNo": "NATIVE-1",
            "OrderNo": "ORDER-1",
            "ServerFlag": "S",
            "ExchangeNo": "COMEX",
            "CommodityType": "F",
            "CommodityNo": "GC",
            "ContractNo": "2608",
            "OrderSide": SIDE_BUY,
            "OrderPrice": 2400.5,
            "OrderQty": 1,
            "OrderMatchQty": 0,
            "OrderState": "0",
        }
    )

    assert session.td_api.cancel_requests == [
        {"OrderNo": "ORDER-1", "ServerFlag": "S"}
    ]
    account_order_events = [
        item for item in published if item[0] == "orders.TAP-ACCOUNT"
    ]
    strategy_order_events = [
        item for item in published if item[0] == "orders.TAP-ACCOUNT.gc-arb"
    ]
    assert account_order_events[-1][2]["client_order_id"] == "gc-arb-1"
    assert strategy_order_events


def test_failed_order_duplicate_returns_failure_message(tmp_path):
    request = {
        "client_id": "engine-01",
        "strategy_id": "gc-arb",
        "client_order_id": "gc-arb-failed-1",
        "symbol": SYMBOL,
        "direction": "BUY",
        "price": 2400.5,
        "volume": 1,
    }
    session = NativeTapSession(
        make_settings(tmp_path),
        lambda *_args: None,
        md_factory=FakeMdApi,
        td_factory=FakeRejectedTdApi,
        native_available=True,
    )
    try:
        assert session.connect(0.5)
        with pytest.raises(RuntimeError, match="TAP insertOrder failed: 42"):
            session.place_order(request)

        duplicate = session.place_order(request)
        assert duplicate["accepted"] is True
        assert duplicate["duplicate"] is True
        assert duplicate["status"] == "SUBMIT_FAILED"
        assert duplicate["message"] == "TAP insertOrder failed: 42"
    finally:
        session.close()


def test_fill_uses_order_identity_and_publishes_strategy_topic(native_session):
    session, published = native_session
    assert session.connect(0.5)
    request = {
        "client_id": "engine-01",
        "strategy_id": "gc-arb",
        "client_order_id": "gc-arb-2",
        "symbol": SYMBOL,
        "direction": "BUY",
        "price": 2400.5,
        "volume": 1,
    }
    session.place_order(request)
    session.on_order(
        {
            "ErrorCode": 0,
            "AccountNo": "TAP-ACCOUNT",
            "ClientOrderNo": "NATIVE-1",
            "OrderNo": "ORDER-2",
            "ServerFlag": "S",
            "ExchangeNo": "COMEX",
            "CommodityType": "F",
            "CommodityNo": "GC",
            "ContractNo": "2608",
            "OrderSide": SIDE_BUY,
            "OrderPrice": 2400.5,
            "OrderQty": 1,
            "OrderMatchQty": 1,
            "OrderState": "4",
        }
    )
    fill = {
        "AccountNo": "TAP-ACCOUNT",
        "OrderNo": "ORDER-2",
        "MatchNo": "MATCH-1",
        "ExchangeNo": "COMEX",
        "CommodityType": "F",
        "CommodityNo": "GC",
        "ContractNo": "2608",
        "MatchSide": SIDE_BUY,
        "MatchPrice": 2400.5,
        "MatchQty": 1,
        "MatchDateTime": "2026-07-28 10:31:05.456",
    }
    session.on_fill(fill)

    strategy_trades = [
        data
        for topic, event, data in published
        if topic == "trades.TAP-ACCOUNT.gc-arb" and event == "trade"
    ]
    first = strategy_trades[-1]
    assert first["client_order_id"] == "gc-arb-2"
    assert "offset" not in first
    assert first["event_id"].startswith("trade:tap:")
    assert first["gateway_name"] == "TAP"
    assert first["account_id"] == "TAP-ACCOUNT"
    assert first["trading_day"] == "20260728"
    assert first["exchange"] == "COMEX"
    assert first["trade_id"] == "MATCH-1"
    assert first["order_id"] == "ORDER-2"

    session.on_fill(fill)
    session.on_fill({**fill, "MatchNo": "MATCH-2"})
    strategy_trades = [
        data
        for topic, event, data in published
        if topic == "trades.TAP-ACCOUNT.gc-arb" and event == "trade"
    ]
    assert strategy_trades[-2]["event_id"] == first["event_id"]
    assert strategy_trades[-1]["event_id"] != first["event_id"]


def test_persistent_store_restores_idempotency_and_cancel_mapping(tmp_path):
    store = MemoryOrderStore()
    store.persistent = True
    request = {
        "client_id": "engine-01",
        "strategy_id": "gc-arb",
        "client_order_id": "gc-arb-restart-1",
        "symbol": SYMBOL,
        "direction": "BUY",
        "price": 2400.5,
        "volume": 1,
    }
    first = NativeTapSession(
        make_settings(tmp_path),
        lambda *_args: None,
        md_factory=FakeMdApi,
        td_factory=FakeTdApi,
        native_available=True,
        order_store=store,
    )
    assert first.connect(0.5)
    first.place_order(request)
    first.on_order(
        {
            "ErrorCode": 0,
            "AccountNo": "TAP-ACCOUNT",
            "ClientOrderNo": "NATIVE-1",
            "OrderNo": "ORDER-RESTART-1",
            "ServerFlag": "S",
            "ExchangeNo": "COMEX",
            "CommodityType": "F",
            "CommodityNo": "GC",
            "ContractNo": "2608",
            "OrderSide": SIDE_BUY,
            "OrderPrice": 2400.5,
            "OrderQty": 1,
            "OrderMatchQty": 0,
            "OrderState": "0",
        }
    )
    first.close()

    restored = NativeTapSession(
        make_settings(tmp_path),
        lambda *_args: None,
        md_factory=FakeMdApi,
        td_factory=FakeTdApi,
        native_available=True,
        order_store=store,
    )
    try:
        assert restored.connect(0.5)
        duplicate = restored.place_order(request)
        assert duplicate["duplicate"] is True
        assert duplicate["tap_client_order_no"] == "NATIVE-1"
        assert duplicate["message"] == ""
        assert restored.td_api.insert_requests == []
        assert restored.status()["order_mapping_persistent"] is True

        cancelled = restored.cancel_order(request)
        assert cancelled["pending"] is False
        assert restored.td_api.cancel_requests == [
            {"OrderNo": "ORDER-RESTART-1", "ServerFlag": "S"}
        ]
    finally:
        restored.close()


def test_unavailable_native_library_fails_before_connecting(tmp_path):
    session = NativeTapSession(
        make_settings(tmp_path),
        lambda *_args: None,
        native_available=False,
    )
    with pytest.raises(TapUnavailableError, match="unavailable"):
        session.connect(0.1)
