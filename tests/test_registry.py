from registry import SubscriptionRegistry


def test_subscriptions_are_reference_counted_by_owner():
    registry = SubscriptionRegistry()
    symbol = "COMEX:F:GC:2608"

    assert registry.subscribe("engine", "a", [symbol]) == [symbol]
    assert registry.subscribe("engine", "b", [symbol]) == []
    assert registry.active_symbols() == [symbol]

    assert registry.unsubscribe("engine", "a", [symbol]) == []
    assert registry.unsubscribe("engine", "b", [symbol]) == [symbol]
    assert registry.active_symbols() == []

