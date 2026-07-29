from __future__ import annotations

import json
import os
import sys

import zmq


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: python src/command_example.py '<json-request>'")
        return 2
    request = json.loads(sys.argv[1])
    host = os.getenv("ZMQ_HOST", "127.0.0.1")
    port = int(os.getenv("ZMQ_REP_PORT", "5576"))
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.LINGER, 0)
    socket.setsockopt(zmq.RCVTIMEO, 5000)
    socket.connect(f"tcp://{host}:{port}")
    try:
        socket.send_json(request)
        print(json.dumps(socket.recv_json(), ensure_ascii=False, indent=2))
        return 0
    finally:
        socket.close(0)
        context.term()


if __name__ == "__main__":
    raise SystemExit(main())

