from __future__ import annotations

import threading
from collections import defaultdict


class SubscriptionRegistry:
    """Reference-count market-data subscriptions by client and strategy."""

    def __init__(self) -> None:
        self._owners: dict[str, set[tuple[str, str]]] = defaultdict(set)
        self._lock = threading.Lock()

    def subscribe(
        self,
        client_id: str,
        strategy_id: str,
        symbols: list[str],
    ) -> list[str]:
        owner = (client_id, strategy_id)
        newly_active: list[str] = []
        with self._lock:
            for symbol in symbols:
                owners = self._owners[symbol]
                if not owners:
                    newly_active.append(symbol)
                owners.add(owner)
        return newly_active

    def unsubscribe(
        self,
        client_id: str,
        strategy_id: str,
        symbols: list[str],
    ) -> list[str]:
        owner = (client_id, strategy_id)
        newly_inactive: list[str] = []
        with self._lock:
            for symbol in symbols:
                owners = self._owners.get(symbol)
                if not owners:
                    continue
                owners.discard(owner)
                if not owners:
                    self._owners.pop(symbol, None)
                    newly_inactive.append(symbol)
        return newly_inactive

    def active_symbols(self) -> list[str]:
        with self._lock:
            return list(self._owners)

