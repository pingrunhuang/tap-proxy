# TAP ZeroMQ Proxy

独立的 TAP 接入服务。目标是由一个进程持有 TAP MD/TD 会话，通过 ZeroMQ
向多个交易引擎和策略提供行情与交易能力。

当前完成拆分计划的第 1～3 步：

- TAP Proxy v1 协议已经冻结，详见
  [`docs/protocol-v1.md`](docs/protocol-v1.md)。
- ZeroMQ PUB/REP 服务、发布队列、配置、日志和容器骨架已经可运行。
- 原生 TAP MD/TD Session 已接入，负责登录、初始快照、订阅、查询、下单、
  撤单和断线重连。
- TAP 回调已经转换为协议字典，不依赖交易引擎的数据模型。
- v1 的健康检查、行情订阅、查询、下单和撤单命令均已接入 Session。
- macOS 通过假原生 API 运行单元测试；真实 Session 运行在 Linux x86_64
  或 Windows。

第 4 步尚未完成：订单身份和 TAP `OrderNo + ServerFlag` 映射当前只保存在
内存。服务重启后不能恢复旧订单撤单能力，因此暂不应直接用于生产交易。

## 架构

```text
TAP MD/TD Front ----------> TAP native session
                                  |
                                  v
                            tap-proxy
                         PUB:5575  REP:5576
                              |       |
                              +-------+---- trading engines / strategies
```

TAP 原生回调未来只会写入线程安全发布队列。ZeroMQ socket 由各自所属线程
操作，避免跨线程共享 socket。

## macOS 开发

macOS 不安装 `vnpy-tap`，但可以运行全部协议、Proxy 和假原生 API 测试：

```bash
uv sync
uv run pytest -q
```

真实服务应在 Linux x86_64 环境运行。直接在 macOS 执行 `src/main.py` 会
明确报告原生 TAP API 不可用。

## Linux x86_64 本地运行

官方 TAP Linux SDK 仍依赖 OpenSSL 1.1；如果宿主系统只提供 OpenSSL 3，
优先使用下面的 Docker 方式。直接在兼容的 Linux x86_64 主机运行时，还需
为 `vnpy-tap 9.4.11` 补装其未声明的运行时依赖：

```bash
cp .env.example .env
# 填写真实 TAP MD/TD 配置
uv sync
uv pip install importlib-metadata==8.7.0
uv run python src/main.py
```

查询健康状态：

```bash
uv run python src/command_example.py '{"action":"ping","request_id":"health-1"}'
uv run python src/command_example.py '{"action":"status"}'
```

订阅指定合约并持续打印行情：

```bash
uv run python src/market_data_example.py LME:F:NI:3M
```

只接收一条行情，10 秒内没有数据则退出：

```bash
uv run python src/market_data_example.py LME:F:NI:3M \
  --count 1 \
  --idle-timeout 10 \
  --pretty
```

程序会先连接 PUB，再通过 REP 注册订阅；正常退出或按 `Ctrl+C` 时自动发送
退订请求。远程 Proxy 可通过 `--host`、`--pub-port` 和 `--rep-port` 指定。

## Docker

```bash
cp .env.example .env
# 填写真实 TAP MD/TD 配置
docker compose up --build -d
docker compose logs -f
```

在不连接柜台的情况下验证容器能够加载原生模块：

```bash
docker compose build
docker compose run --rm --no-deps tap-proxy \
  uv run --no-sync python -c \
  "import platform; from vnpy_tap.api import MdApi, TdApi; print(f'arch={platform.machine()} vnpy-tap-native=ok')"
```

Apple Silicon Mac 上预期输出
`arch=x86_64 vnpy-tap-native=ok`。首次构建需要下载 vn.py 的较大运行时
依赖，后续构建会复用 Docker 缓存。镜像已经包含 TAP 所需的 OpenSSL 1.1
兼容库。

默认端口刻意避开现有 `ibkr-proxy` 和 `ctp-proxy`：

| 服务 | PUB | REP |
| --- | ---: | ---: |
| IBKR Proxy | 5555 | 5556 |
| CTP Proxy | 5565 | 5566 |
| TAP Proxy | 5575 | 5576 |

## 就绪条件

`ping.data.ready` 只有在以下条件全部成立时才会为 `true`：

- MD API Ready
- TD API Ready
- 已取得 TAP 账户
- 初始资金快照完成
- 初始持仓快照完成
- 初始委托快照完成

断线后立即变回 `false`，重连和初始同步成功后恢复为 `true`。

## 当前安全边界

ZeroMQ 接口尚未实现身份认证，部署时必须只在私有网络开放端口。订单映射
尚未持久化；进程内重复 `client_id + strategy_id + client_order_id` 会被
幂等处理，但重启后的幂等和撤单恢复必须等第 4 步完成。
