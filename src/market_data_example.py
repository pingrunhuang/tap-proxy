from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from typing import Any, Sequence

import zmq

from protocol import canonical_symbol, market_data_topic


def symbol_argument(value: str) -> str:
    try:
        return canonical_symbol(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must not be negative")
    return parsed


def non_negative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must not be negative")
    return parsed


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Subscribe to one TAP canonical symbol and print ZeroMQ market-data "
            "events."
        )
    )
    parser.add_argument(
        "symbol",
        type=symbol_argument,
        help="ExchangeNo:CommodityType:CommodityNo:ContractNo",
    )
    parser.add_argument(
        "--host",
        default=os.getenv("ZMQ_HOST", "127.0.0.1"),
        help="tap-proxy host (default: ZMQ_HOST or 127.0.0.1)",
    )
    parser.add_argument(
        "--pub-port",
        type=int,
        default=int(os.getenv("ZMQ_PUB_PORT", "5575")),
    )
    parser.add_argument(
        "--rep-port",
        type=int,
        default=int(os.getenv("ZMQ_REP_PORT", "5576")),
    )
    parser.add_argument(
        "--client-id",
        default=os.getenv("MD_CLIENT_ID", "market-data-example"),
    )
    parser.add_argument(
        "--strategy-id",
        default=os.getenv("MD_STRATEGY_ID", "manual"),
    )
    parser.add_argument(
        "--count",
        type=non_negative_int,
        default=0,
        help="exit after N market-data events; 0 means run until Ctrl+C",
    )
    parser.add_argument(
        "--idle-timeout",
        type=non_negative_float,
        default=0.0,
        help="exit if no market data arrives for N seconds; 0 disables timeout",
    )
    parser.add_argument(
        "--command-timeout",
        type=non_negative_float,
        default=5.0,
        help="REP command timeout in seconds",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="pretty-print multi-line JSON instead of one JSON object per line",
    )
    return parser.parse_args(argv)


def command_request(
    action: str,
    symbol: str,
    client_id: str,
    strategy_id: str,
) -> dict[str, Any]:
    return {
        "action": action,
        "request_id": f"md-example-{uuid.uuid4().hex}",
        "client_id": client_id,
        "strategy_id": strategy_id,
        "symbols": [symbol],
    }


def send_command(
    context: zmq.Context,
    endpoint: str,
    request: dict[str, Any],
    timeout_seconds: float,
) -> dict[str, Any]:
    timeout_ms = max(1, int(timeout_seconds * 1000))
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.LINGER, 0)
    socket.setsockopt(zmq.SNDTIMEO, timeout_ms)
    socket.setsockopt(zmq.RCVTIMEO, timeout_ms)
    socket.connect(endpoint)
    try:
        socket.send_json(request)
        response = socket.recv_json()
    except zmq.Again as exc:
        raise TimeoutError(
            f"tap-proxy command timed out after {timeout_seconds:g}s"
        ) from exc
    finally:
        socket.close(0)
    if not isinstance(response, dict):
        raise RuntimeError("tap-proxy returned a non-object response")
    if response.get("status") != "ok":
        error = response.get("error") or {}
        message = error.get("message") or "unknown tap-proxy error"
        raise RuntimeError(str(message))
    return response


def decode_pub_message(frames: list[bytes]) -> tuple[str, dict[str, Any]]:
    if len(frames) != 2:
        raise ValueError(f"expected 2 PUB frames, received {len(frames)}")
    topic = frames[0].decode("utf-8")
    payload = json.loads(frames[1].decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("PUB payload must be a JSON object")
    return topic, payload


def print_event(topic: str, payload: dict[str, Any], *, pretty: bool) -> None:
    output = {"topic": topic, **payload}
    indent = 2 if pretty else None
    print(
        json.dumps(output, ensure_ascii=False, indent=indent, allow_nan=False),
        flush=True,
    )


def run(args: argparse.Namespace) -> int:
    pub_endpoint = f"tcp://{args.host}:{args.pub_port}"
    rep_endpoint = f"tcp://{args.host}:{args.rep_port}"
    topic = market_data_topic(args.symbol)
    context = zmq.Context()
    subscriber = context.socket(zmq.SUB)
    subscriber.setsockopt(zmq.LINGER, 0)
    subscriber.setsockopt_string(zmq.SUBSCRIBE, topic)
    subscriber.setsockopt_string(zmq.SUBSCRIBE, "errors.TAP")
    subscriber.connect(pub_endpoint)
    subscribed = False

    try:
        response = send_command(
            context,
            rep_endpoint,
            command_request(
                "subscribe_market_data",
                args.symbol,
                args.client_id,
                args.strategy_id,
            ),
            args.command_timeout,
        )
        subscribed = True
        print(
            f"subscribed symbol={args.symbol} topic={topic} "
            f"response={response['status']}",
            file=sys.stderr,
            flush=True,
        )

        poller = zmq.Poller()
        poller.register(subscriber, zmq.POLLIN)
        received = 0
        last_market_data = time.monotonic()
        while args.count == 0 or received < args.count:
            events = dict(poller.poll(1000))
            if subscriber not in events:
                if (
                    args.idle_timeout > 0
                    and time.monotonic() - last_market_data >= args.idle_timeout
                ):
                    raise TimeoutError(
                        f"no market data received for {args.idle_timeout:g}s"
                    )
                continue

            received_topic, payload = decode_pub_message(
                subscriber.recv_multipart()
            )
            if received_topic == "errors.TAP":
                print_event(received_topic, payload, pretty=args.pretty)
                continue
            if received_topic != topic:
                continue
            print_event(received_topic, payload, pretty=args.pretty)
            received += 1
            last_market_data = time.monotonic()
        return 0
    except KeyboardInterrupt:
        print("stopped by user", file=sys.stderr)
        return 0
    except (RuntimeError, TimeoutError, ValueError, zmq.ZMQError) as exc:
        print(f"market-data example failed: {exc}", file=sys.stderr)
        return 1
    finally:
        if subscribed:
            try:
                send_command(
                    context,
                    rep_endpoint,
                    command_request(
                        "unsubscribe_market_data",
                        args.symbol,
                        args.client_id,
                        args.strategy_id,
                    ),
                    args.command_timeout,
                )
                print(f"unsubscribed symbol={args.symbol}", file=sys.stderr)
            except Exception as exc:
                print(f"unsubscribe failed: {exc}", file=sys.stderr)
        subscriber.close(0)
        context.term()


def main(argv: Sequence[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
