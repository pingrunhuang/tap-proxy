from __future__ import annotations

import locale
import os

from main import configure_native_locale


def test_configure_native_locale_forces_legacy_c_locale(
    monkeypatch,
) -> None:
    monkeypatch.setenv("LANG", "C.UTF-8")
    monkeypatch.delenv("LC_ALL", raising=False)

    configure_native_locale()

    assert os.environ["LANG"] == "C"
    assert os.environ["LC_ALL"] == "C"
    assert locale.setlocale(locale.LC_ALL) == "C"
