from order_store import MemoryOrderStore


def test_memory_order_store_reserves_and_updates_mapping():
    store = MemoryOrderStore()
    identity = {
        "client_id": "engine",
        "strategy_id": "nickel",
        "client_order_id": "order-1",
    }

    assert store.reserve(
        **identity,
        symbol="LME:F:NI:3M",
        offset="OPEN",
        payload={"direction": "BUY"},
    )
    assert not store.reserve(
        **identity,
        symbol="LME:F:NI:3M",
        offset="OPEN",
        payload={"direction": "BUY"},
    )

    store.set_native(**identity, tap_client_order_no="NATIVE-1")
    store.update_tap(
        tap_client_order_no="NATIVE-1",
        tap_order_no="ORDER-1",
        tap_server_flag="S",
        status="SUBMITTED",
    )

    record = store.get("engine", "nickel", "order-1")
    assert record is not None
    assert record["tap_client_order_no"] == "NATIVE-1"
    assert record["tap_order_no"] == "ORDER-1"
    assert store.find_by_order_no("ORDER-1", "S")["strategy_id"] == "nickel"
    assert store.find_by_order_no("ORDER-1", "OTHER") is None

    assert store.reserve(
        client_id="engine-02",
        strategy_id="copper",
        client_order_id="order-2",
        symbol="LME:F:CU:3M",
        offset="OPEN",
        payload={},
    )
    store.set_native(
        client_id="engine-02",
        strategy_id="copper",
        client_order_id="order-2",
        tap_client_order_no="NATIVE-2",
    )
    store.update_tap(
        tap_client_order_no="NATIVE-2",
        tap_order_no="ORDER-1",
        tap_server_flag="OTHER",
        status="SUBMITTED",
    )
    assert store.find_by_order_no("ORDER-1", "S")["strategy_id"] == "nickel"
    assert store.find_by_order_no("ORDER-1", "OTHER")["strategy_id"] == "copper"


def test_memory_order_store_persists_trades_by_owner_with_cursor():
    store = MemoryOrderStore()
    first = {
        "event_id": "trade:tap:1",
        "client_id": "engine",
        "strategy_id": "gc-arb",
    }
    second = {
        "event_id": "trade:tap:2",
        "client_id": "engine",
        "strategy_id": "other",
    }

    assert store.record_trade(first)
    assert not store.record_trade(first)
    assert store.record_trade(second)

    page = store.list_trades("engine", "gc-arb", after_id=0, limit=1)
    assert page["trades"] == [{**first, "trade_cursor": 1}]
    assert page["next_after_id"] == 1
    assert page["has_more"] is False
    assert store.latest_trade_cursor("engine", "gc-arb") == 1
