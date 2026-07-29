FROM public.ecr.aws/docker/library/python:3.11-slim-bullseye AS openssl11

FROM ghcr.io/astral-sh/uv:python3.12-bookworm AS locale-builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends locales \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/locale \
    && localedef --no-archive \
        --inputfile=zh_CN \
        --charmap=GB18030 \
        /opt/locale/zh_CN.GB18030

FROM ghcr.io/astral-sh/uv:python3.12-bookworm

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

COPY pyproject.toml uv.lock README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev \
    --no-install-package pyside6 \
    --no-install-package pyside6-addons \
    --no-install-package pyside6-essentials \
    --no-install-package shiboken6

# vnpy-tap 9.4.11 imports this backport without declaring it.
RUN uv pip install "importlib-metadata==8.7.0"

# TAP's bundled Linux SDK is linked against OpenSSL 1.1.
COPY --from=openssl11 /usr/lib/x86_64-linux-gnu/libssl.so.1.1 /usr/lib/x86_64-linux-gnu/
COPY --from=openssl11 /usr/lib/x86_64-linux-gnu/libcrypto.so.1.1 /usr/lib/x86_64-linux-gnu/
COPY --from=locale-builder /opt/locale/zh_CN.GB18030 /usr/lib/locale/zh_CN.GB18030

# The TAP Cython extensions hard-code zh_CN.GB18030 internally.
ENV LANG=C \
    LC_ALL=C

COPY src ./src
RUN mkdir -p flow/tap logs

CMD ["uv", "run", "--no-sync", "python", "src/main.py"]
