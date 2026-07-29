from __future__ import annotations

import argparse
import json
import os
import threading
from pathlib import Path
from typing import Any, Sequence

from dotenv import load_dotenv
from vnpy_tap.api import (
    APILOGLEVEL_NONE,
    APIYNFLAG_NO,
    TdApi,
)

from config import Settings
from main import configure_native_locale


class ContractQueryApi(TdApi):
    def __init__(self, exchange: str, commodity: str) -> None:
        super().__init__()
        self.exchange = exchange
        self.commodity = commodity
        self.done = threading.Event()
        self.error = ""
        self.commodities: list[dict[str, Any]] = []
        self.contracts: list[dict[str, Any]] = []

    def onRspLogin(self, error: int, data: dict[str, Any]) -> None:
        del data
        if error:
            self.error = f"TAP login failed: {error}"
            self.done.set()

    def onAPIReady(self, code: int) -> None:
        del code
        self.qryCommodity()

    def onRspQryCommodity(
        self,
        session: int,
        error: int,
        last: str,
        data: dict[str, Any],
    ) -> None:
        del session
        if error:
            self.error = f"TAP commodity query failed: {error}"
            self.done.set()
            return
        if self._matches(data):
            self.commodities.append(dict(data))
        if last == "Y":
            self.qryContract({})

    def onRspQryContract(
        self,
        session: int,
        error: int,
        last: str,
        data: dict[str, Any],
    ) -> None:
        del session
        if error:
            self.error = f"TAP contract query failed: {error}"
            self.done.set()
            return
        if self._matches(data):
            self.contracts.append(dict(data))
        if last == "Y":
            self.done.set()

    def onDisconnect(self, reason: int) -> None:
        if not self.done.is_set():
            self.error = f"TAP disconnected before query completed: {reason}"
            self.done.set()

    def _matches(self, data: dict[str, Any]) -> bool:
        if not data:
            return False
        return (
            str(data.get("ExchangeNo", "")).upper() == self.exchange
            and str(data.get("CommodityNo", "")).upper() == self.commodity
        )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query the TAP contract catalog without placing orders."
    )
    parser.add_argument("--exchange", default="LME")
    parser.add_argument("--commodity", default="NI")
    parser.add_argument("--timeout", type=float, default=90.0)
    return parser.parse_args(argv)


def selected_fields(data: dict[str, Any]) -> dict[str, Any]:
    names = (
        "ExchangeNo",
        "CommodityType",
        "CommodityNo",
        "CommodityEngName",
        "ContractNo1",
        "ContractNo2",
        "ContractName",
        "ContractType",
        "CallOrPutFlag1",
        "StrikePrice1",
        "ContractExpDate",
        "LastTradeDate",
    )
    return {name: data[name] for name in names if data.get(name) not in (None, "")}


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    load_dotenv()
    configure_native_locale()
    settings = Settings.from_env()
    settings.validate(require_tap=True)

    api = ContractQueryApi(args.exchange.upper(), args.commodity.upper())
    api.init()
    query_path = Path("/tmp/tap-contract-query")
    query_path.mkdir(parents=True, exist_ok=True)
    encoded_path = str(query_path).encode("GBK")
    api.createITapTradeAPI(
        {
            "AuthCode": settings.td_auth_code,
            "KeyOperationLogPath": encoded_path,
            "LogLevel": APILOGLEVEL_NONE,
        },
        0,
    )
    api.setHostAddress(settings.td_host, settings.td_port)
    api.login(
        {
            "UserNo": settings.td_user_id,
            "Password": settings.td_password,
            "ISModifyPassword": APIYNFLAG_NO,
            "NoticeIgnoreFlag": "TAPI_NOTICE_IGNORE_POSITIONPROFIT",
        }
    )

    if not api.done.wait(args.timeout):
        print(
            json.dumps(
                {"status": "error", "error": "contract query timed out"},
                ensure_ascii=False,
            ),
            flush=True,
        )
        os._exit(1)
    if api.error:
        print(
            json.dumps(
                {"status": "error", "error": api.error},
                ensure_ascii=False,
            ),
            flush=True,
        )
        os._exit(1)

    result = {
        "status": "ok",
        "exchange": api.exchange,
        "commodity": api.commodity,
        "commodities": [selected_fields(item) for item in api.commodities],
        "contracts": [selected_fields(item) for item in api.contracts],
    }
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str), flush=True)
    # The legacy SDK can block or crash during teardown under QEMU. This command
    # runs in a disposable process, so bypass native destructors after flushing.
    os._exit(0)


if __name__ == "__main__":
    raise SystemExit(main())
