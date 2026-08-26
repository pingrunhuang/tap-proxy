from __future__ import annotations

import json
import queue
import threading
from typing import Any

import zmq
from loguru import logger

from config import Settings
from protocol import (
    Action,
    ErrorCode,
    ProtocolError,
    SCHEMA_VERSION,
    event_payload,
    normalize_symbols,
    response_error,
    response_ok,
    validate_request,
)
from registry import SubscriptionRegistry
from tap_session import (
    PendingTapSession,
    TapNotReadyError,
    TapSession,
    TapUnavailableError,
)


class TapProxy:
    def __init__(
        self,
        settings: Settings,
        session: TapSession | None = None,
    ) -> None:
        self.settings = settings
        self.session = session or PendingTapSession()
        self.subscriptions = SubscriptionRegistry()
        if settings.initial_symbols:
            self.subscriptions.subscribe(
                "proxy",
                "startup",
                normalize_symbols(settings.initial_symbols),
            )
        self._publish_queue: queue.Queue[tuple[str, str, dict[str, Any]]] = (
            queue.Queue(maxsize=settings.publish_queue_size)
        )
        self.context: zmq.Context | None = None
        self.pub_socket: zmq.Socket | None = None
        self.rep_socket: zmq.Socket | None = None
        self.active = threading.Event()
        self.bound_pub_port: int | None = None
        self.bound_rep_port: int | None = None
        self._publisher_thread: threading.Thread | None = None
        self._command_thread: threading.Thread | None = None

    def set_session(self, session: TapSession) -> None:
        if self.active.is_set():
            raise RuntimeError("cannot replace TAP session after proxy start")
        self.session = session

    def start(self) -> None:
        if self.active.is_set():
            return
        self.settings.validate()
        self.context = zmq.Context()
        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.setsockopt(zmq.LINGER, 0)
        self.bound_pub_port = self._bind(self.pub_socket, self.settings.zmq_pub_port)

        self.rep_socket = self.context.socket(zmq.REP)
        self.rep_socket.setsockopt(zmq.LINGER, 0)
        self.bound_rep_port = self._bind(self.rep_socket, self.settings.zmq_rep_port)

        self.active.set()
        self._publisher_thread = threading.Thread(
            target=self._publisher_loop,
            name="tap-publisher",
            daemon=True,
        )
        self._command_thread = threading.Thread(
            target=self._command_loop,
            name="tap-commands",
            daemon=True,
        )
        self._publisher_thread.start()
        self._command_thread.start()
        logger.info(
            "TAP proxy transport listening: PUB={}:{} REP={}:{}",
            self.settings.zmq_bind_host,
            self.bound_pub_port,
            self.settings.zmq_bind_host,
            self.bound_rep_port,
        )

    def _bind(self, socket: zmq.Socket, port: int) -> int:
        base_endpoint = f"tcp://{self.settings.zmq_bind_host}"
        if port == 0:
            return socket.bind_to_random_port(base_endpoint)
        socket.bind(f"{base_endpoint}:{port}")
        return port

    def enqueue_publish(
        self,
        topic: str,
        event: str,
        data: dict[str, Any],
    ) -> None:
        if not topic.strip():
            raise ValueError("topic must not be empty")
        try:
            self._publish_queue.put_nowait((topic, event, data))
        except queue.Full:
            logger.error("Publish queue is full; dropping topic={}", topic)

    def handle_command(self, request: Any) -> dict[str, Any]:
        request_id = (
            str(request.get("request_id", "")) or None
            if isinstance(request, dict)
            else None
        )
        try:
            action, normalized = validate_request(request)
            if action is Action.PING:
                session_status = self.session.status()
                return response_ok(
                    {
                        "service": "tap-proxy",
                        "transport_ready": self.active.is_set(),
                        "ready": self.session.is_ready(),
                        "database_ready": bool(
                            session_status.get("order_store_healthy", False)
                        ),
                        "phase": "native_session",
                        "protocol_version": SCHEMA_VERSION,
                        "md_enabled": self.settings.enable_md,
                    },
                    request_id,
                )
            if action is Action.STATUS:
                return response_ok(
                    {
                        "service": "tap-proxy",
                        "transport_ready": self.active.is_set(),
                        "ready": self.session.is_ready(),
                        "phase": "native_session",
                        "protocol_version": SCHEMA_VERSION,
                        "session": self.session.status(),
                        "published_queue_size": self._publish_queue.qsize(),
                        "pub_port": self.bound_pub_port,
                        "rep_port": self.bound_rep_port,
                    },
                    request_id,
                )
            if action is Action.SUBSCRIBE_MARKET_DATA:
                if not self.settings.enable_md:
                    raise RuntimeError("TAP market data is disabled by TAP_ENABLE_MD=false")
                symbols = normalized["symbols"]
                newly_active = self.subscriptions.subscribe(
                    normalized["client_id"],
                    normalized["strategy_id"],
                    symbols,
                )
                try:
                    self.session.subscribe_market_data(newly_active)
                except Exception:
                    self.subscriptions.unsubscribe(
                        normalized["client_id"],
                        normalized["strategy_id"],
                        symbols,
                    )
                    raise
                return response_ok(
                    {
                        "symbols": symbols,
                        "newly_active": newly_active,
                        "active_symbols": self.subscriptions.active_symbols(),
                    },
                    request_id,
                )
            if action is Action.UNSUBSCRIBE_MARKET_DATA:
                if not self.settings.enable_md:
                    raise RuntimeError("TAP market data is disabled by TAP_ENABLE_MD=false")
                symbols = normalized["symbols"]
                newly_inactive = self.subscriptions.unsubscribe(
                    normalized["client_id"],
                    normalized["strategy_id"],
                    symbols,
                )
                try:
                    self.session.unsubscribe_market_data(newly_inactive)
                except Exception:
                    self.subscriptions.subscribe(
                        normalized["client_id"],
                        normalized["strategy_id"],
                        symbols,
                    )
                    raise
                return response_ok(
                    {
                        "symbols": symbols,
                        "newly_inactive": newly_inactive,
                        "active_symbols": self.subscriptions.active_symbols(),
                    },
                    request_id,
                )
            if action is Action.GET_ACCOUNT:
                return response_ok(
                    self.session.query_account(self._max_age_seconds(normalized)),
                    request_id,
                )
            if action is Action.GET_POSITIONS:
                return response_ok(
                    self.session.query_positions(self._max_age_seconds(normalized)),
                    request_id,
                )
            if action is Action.GET_ORDERS:
                if normalized.get("local_only"):
                    return response_ok(
                        self.session.query_persisted_orders(
                            normalized["client_id"],
                            normalized["strategy_id"],
                        ),
                        request_id,
                    )
                return response_ok(
                    self.session.query_orders(self._max_age_seconds(normalized)),
                    request_id,
                )
            if action is Action.GET_TRADES:
                return response_ok(
                    self.session.query_persisted_trades(
                        normalized["client_id"],
                        normalized["strategy_id"],
                        after_id=normalized["after_id"],
                        limit=normalized["limit"],
                    ),
                    request_id,
                )
            if action is Action.GET_TRADE_CURSOR:
                return response_ok(
                    {
                        "cursor": self.session.latest_trade_cursor(
                            normalized["client_id"],
                            normalized["strategy_id"],
                        )
                    },
                    request_id,
                )
            if action is Action.PLACE_ORDER:
                return response_ok(self.session.place_order(normalized), request_id)
            if action is Action.CANCEL_ORDER:
                return response_ok(self.session.cancel_order(normalized), request_id)
            raise ProtocolError(
                f"Unsupported action: {action.value}",
                ErrorCode.UNSUPPORTED_ACTION,
            )
        except ProtocolError as exc:
            return response_error(
                str(exc),
                request_id,
                code=exc.code,
            )
        except NotImplementedError as exc:
            return response_error(
                str(exc),
                request_id,
                code=ErrorCode.NOT_IMPLEMENTED,
            )
        except (TapUnavailableError, TapNotReadyError) as exc:
            return response_error(
                str(exc),
                request_id,
                code=ErrorCode.NOT_READY,
                retryable=True,
            )
        except TimeoutError as exc:
            return response_error(
                str(exc),
                request_id,
                code=ErrorCode.TIMEOUT,
                retryable=True,
            )
        except RuntimeError as exc:
            return response_error(
                str(exc),
                request_id,
                code=ErrorCode.INTERNAL_ERROR,
            )
        except Exception as exc:
            logger.exception("Unexpected command failure")
            return response_error(
                str(exc),
                request_id,
                code=ErrorCode.INTERNAL_ERROR,
                retryable=True,
            )

    @staticmethod
    def _max_age_seconds(request: dict[str, Any]) -> float | None:
        if request.get("force_refresh"):
            return None
        max_age_ms = request.get("max_age_ms")
        return None if max_age_ms is None else float(max_age_ms) / 1000.0

    def _publisher_loop(self) -> None:
        assert self.pub_socket is not None
        while self.active.is_set() or not self._publish_queue.empty():
            try:
                topic, event, data = self._publish_queue.get(timeout=0.2)
            except queue.Empty:
                continue
            try:
                payload = json.dumps(
                    event_payload(event, data),
                    ensure_ascii=False,
                    allow_nan=False,
                ).encode("utf-8")
                self.pub_socket.send_multipart([topic.encode("utf-8"), payload])
            except Exception:
                logger.exception("Failed to publish topic={}", topic)
            finally:
                self._publish_queue.task_done()

    def _command_loop(self) -> None:
        assert self.rep_socket is not None
        poller = zmq.Poller()
        poller.register(self.rep_socket, zmq.POLLIN)
        while self.active.is_set():
            request_id: str | None = None
            try:
                events = dict(poller.poll(250))
                if self.rep_socket not in events:
                    continue
                request = self.rep_socket.recv_json()
                if isinstance(request, dict):
                    request_id = str(request.get("request_id", "")) or None
                response = self.handle_command(request)
            except Exception as exc:
                logger.exception("Command loop error")
                response = response_error(
                    str(exc),
                    request_id,
                    code=ErrorCode.INTERNAL_ERROR,
                    retryable=True,
                )
            try:
                self.rep_socket.send_json(response)
            except zmq.ZMQError:
                if self.active.is_set():
                    logger.exception("Failed to send command response")

    def stop(self) -> None:
        if not self.active.is_set() and self.context is None:
            self.session.close()
            return
        self.active.clear()
        if self._command_thread and self._command_thread.is_alive():
            self._command_thread.join(timeout=2)
        self.session.close()
        if self._publisher_thread and self._publisher_thread.is_alive():
            self._publisher_thread.join(timeout=2)
        for socket in (self.rep_socket, self.pub_socket):
            if socket is not None:
                socket.close(0)
        if self.context is not None:
            self.context.term()
        self.context = None
        self.rep_socket = None
        self.pub_socket = None
