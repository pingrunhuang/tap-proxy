FROM public.ecr.aws/docker/library/python:3.11-slim-bullseye AS openssl11

FROM python:3.12-bookworm AS locale-builder

ARG DEBIAN_MIRROR=https://mirrors.tuna.tsinghua.edu.cn

RUN sed -i "s|http://deb.debian.org|${DEBIAN_MIRROR}|g" \
        /etc/apt/sources.list.d/debian.sources \
    && apt-get -o Acquire::Retries=5 -o Acquire::https::Timeout=60 update \
    && apt-get install -y --no-install-recommends locales \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/locale \
    && localedef --no-archive \
        --inputfile=zh_CN \
        --charmap=GB18030 \
        /opt/locale/zh_CN.GB18030

FROM python:3.12-bookworm

ARG UV_VERSION=0.11.32
ARG DEBIAN_MIRROR=https://mirrors.tuna.tsinghua.edu.cn

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_HTTP_RETRIES=5 \
    UV_HTTP_TIMEOUT=120 \
    UV_CONCURRENT_DOWNLOADS=4

RUN sed -i "s|http://deb.debian.org|${DEBIAN_MIRROR}|g" \
        /etc/apt/sources.list.d/debian.sources \
    && apt-get -o Acquire::Retries=5 -o Acquire::https::Timeout=60 update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

RUN curl --retry 5 --retry-all-errors --fail --location --silent --show-error \
        "https://astral.sh/uv/${UV_VERSION}/install.sh" \
        --output /tmp/uv-installer.sh \
    && UV_UNMANAGED_INSTALL=/usr/local/bin sh /tmp/uv-installer.sh \
    && rm /tmp/uv-installer.sh

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
