# TAP Proxy Protocol v1

## 1. Transport

TAP Proxy 使用两个 ZeroMQ socket：

- PUB：服务端发布异步事件，默认 `tcp://0.0.0.0:5575`
- REP：客户端发送同步命令，默认 `tcp://0.0.0.0:5576`

PUB 消息包含两个 frame：

1. UTF-8 topic
2. UTF-8 JSON payload

REP 每次接收、返回一个 JSON 对象。协议版本号为整数 `1`。

## 2. Canonical symbol

TAP 合约统一使用：

```text
<ExchangeNo>:<CommodityType>:<CommodityNo>:<ContractNo>
```

例如：

```text
COMEX:F:GC:2608
```

四段都不能为空。Proxy 在协议入口去除首尾空格并转换为大写。异步事件、
命令响应和订单映射中不得使用 `GC2608` 这类无法唯一定位 TAP 合约的短代码。

## 3. Command envelope

所有请求必须是 JSON 对象，并包含 `action`。建议客户端为每个请求提供唯一
的 `request_id`：

```json
{
  "action": "ping",
  "request_id": "health-1"
}
```

统一成功响应：

```json
{
  "schema_version": 1,
  "status": "ok",
  "request_id": "health-1",
  "data": {},
  "error": null
}
```

统一失败响应：

```json
{
  "schema_version": 1,
  "status": "error",
  "request_id": "health-1",
  "data": null,
  "error": {
    "code": "invalid_request",
    "message": "Missing required field: action",
    "retryable": false
  }
}
```

错误码：

| code | 含义 |
| --- | --- |
| `invalid_request` | 字段缺失、格式或取值错误 |
| `unsupported_action` | action 不属于协议 v1 |
| `not_implemented` | action 属于 v1，但当前服务阶段尚未实现 |
| `not_ready` | TAP Session 未完成登录或当前不可交易 |
| `timeout` | 柜台或内部操作超时 |
| `internal_error` | 未分类服务端错误 |

## 4. Commands

### Health

```json
{"action":"ping","request_id":"health-1"}
{"action":"status","request_id":"status-1"}
```

`ping.data`：

```json
{
  "service": "tap-proxy",
  "transport_ready": true,
  "ready": false,
  "phase": "native_session",
  "protocol_version": 1
}
```

`ready` 只有在 TAP MD/TD API Ready、账户就绪以及资金、持仓、委托的初始
快照全部完成后才会为 `true`。

`status.data` 还包含：

```json
{
  "session": {
    "native_available": true,
    "md_ready": true,
    "td_ready": true,
    "account_ready": true,
    "initial_sync_ready": true,
    "implementation": "native_tap_session",
    "order_mapping_persistent": true,
    "order_store_healthy": true
  },
  "published_queue_size": 0,
  "pub_port": 5575,
  "rep_port": 5576
}
```

### Market-data subscription

订阅和退订都必须携带客户端及策略身份：

```json
{
  "action": "subscribe_market_data",
  "request_id": "sub-1",
  "client_id": "engine-01",
  "strategy_id": "gc-arb",
  "symbols": ["COMEX:F:GC:2608"]
}
```

```json
{
  "action": "unsubscribe_market_data",
  "request_id": "unsub-1",
  "client_id": "engine-01",
  "strategy_id": "gc-arb",
  "symbols": ["COMEX:F:GC:2608"]
}
```

### Queries

```json
{"action":"get_account","request_id":"account-1"}
{"action":"get_positions","request_id":"positions-1","force_refresh":true}
{"action":"get_orders","request_id":"orders-1","max_age_ms":5000}
{"action":"get_trades","request_id":"trades-1","client_id":"engine-01","strategy_id":"gc-arb","after_id":0,"limit":500}
```

查询响应中的 `data` 分别为账户对象、持仓数组和订单数组。客户端不能只依赖
PUB/SUB 恢复状态；重连后必须调用查询命令获取快照。

成交在发布前持久化到 PostgreSQL。`get_trades` 只返回指定 owner 的记录，
并通过 `next_after_id` 和 `has_more` 分页，供 Engine 恢复断线期间漏收的成交。

查询成功代表本次快照已经完整返回，因此引擎应在处理完响应后产生自己的
账户或持仓同步完成信号，不需要等待额外 PUB topic。

### Place order

```json
{
  "action": "place_order",
  "request_id": "order-request-1",
  "client_id": "engine-01",
  "strategy_id": "gc-arb",
  "client_order_id": "gc-arb-20260728-000001",
  "symbol": "COMEX:F:GC:2608",
  "direction": "BUY",
  "offset": "OPEN",
  "price": 2400.5,
  "volume": 1
}
```

`client_id + strategy_id + client_order_id` 是幂等键，三者均为必填。重复请求
不得导致第二次向 TAP 发单。

下单响应的 `data.message` 返回已持久化的失败原因；成功订单以及没有失败
原因的订单返回空字符串。示例：

```json
{
  "accepted": true,
  "duplicate": true,
  "client_order_id": "gc-arb-20260728-000001",
  "tap_client_order_no": "",
  "recovery_required": true,
  "status": "SUBMIT_FAILED",
  "message": "TAP insertOrder failed: 42"
}
```

`direction`：

- `BUY`
- `SELL`

`offset`：

- `OPEN`
- `CLOSE`
- `CLOSETODAY`
- `CLOSEYESTERDAY`

### Cancel order

```json
{
  "action": "cancel_order",
  "request_id": "cancel-1",
  "client_id": "engine-01",
  "strategy_id": "gc-arb",
  "client_order_id": "gc-arb-20260728-000001"
}
```

撤单必须使用原始订单的完整幂等身份。Proxy 负责将它解析为 TAP
`OrderNo + ServerFlag`。

## 5. PUB event envelope

```json
{
  "schema_version": 1,
  "event": "marketdata",
  "published_at": 1785232800123,
  "data": {}
}
```

`published_at` 是 Proxy 发布时的 Unix epoch 毫秒数。

Topics：

```text
marketdata.TAP.<canonical_symbol>
orders.<account_id>
orders.<account_id>.<strategy_id>
trades.<account_id>
trades.<account_id>.<strategy_id>
account.<account_id>
positions.<account_id>
status.TAP
errors.TAP
```

### Market data

Topic：`marketdata.TAP.<canonical_symbol>`，`event` 为 `marketdata`。

```json
{
  "symbol": "COMEX:F:GC:2608",
  "exchange": "COMEX",
  "last_price": 2400.5,
  "volume": 1024,
  "open_interest": 0,
  "upper_limit": 2500,
  "lower_limit": 2300,
  "bid_price_1": 2400.4,
  "bid_volume_1": 3,
  "ask_price_1": 2400.6,
  "ask_volume_1": 4,
  "exchange_time": "2026-07-28 10:30:01.123",
  "exchange_timestamp": 1785205801123,
  "local_time": 1785205801125
}
```

价格字段为 JSON number；柜台没有提供的价格或数量使用 `null`，不得用
`NaN` 或 `Infinity`。

### Order

Topic：`orders.<account_id>`，有订单归属时同时发布
`orders.<account_id>.<strategy_id>`；`event` 为 `order`。

```json
{
  "account_id": "TAP_ACCOUNT",
  "client_id": "engine-01",
  "strategy_id": "gc-arb",
  "client_order_id": "gc-arb-20260728-000001",
  "symbol": "COMEX:F:GC:2608",
  "direction": "BUY",
  "offset": "OPEN",
  "price": 2400.5,
  "volume": 1,
  "traded": 0,
  "status": "SUBMITTED",
  "status_message": "",
  "tap_client_order_no": "123",
  "tap_order_no": "456",
  "tap_server_flag": "S"
}
```

订单状态枚举：

- `SUBMITTED`
- `PARTTRADED`
- `TRADED`
- `CANCELLED`
- `REJECTED`
- `UNKNOWN`

### Trade

Topic：`trades.<account_id>`，有订单归属时同时发布
`trades.<account_id>.<strategy_id>`；`event` 为 `trade`。

```json
{
  "account_id": "TAP_ACCOUNT",
  "client_id": "engine-01",
  "strategy_id": "gc-arb",
  "client_order_id": "gc-arb-20260728-000001",
  "trade_id": "MATCH-1",
  "symbol": "COMEX:F:GC:2608",
  "direction": "BUY",
  "offset": "OPEN",
  "price": 2400.5,
  "volume": 1,
  "trade_time": "2026-07-28 10:31:05.456",
  "exchange_timestamp": 1785205865456
}
```

### Account

Topic：`account.<account_id>`，`event` 为 `account`。

```json
{
  "account_id": "TAP_ACCOUNT",
  "balance": 100000,
  "frozen": 2500,
  "available": 97500,
  "currency": "USD"
}
```

`get_account.data` 使用同一个对象结构。

### Position

Topic：`positions.<account_id>`，`event` 为 `position`。

```json
{
  "account_id": "TAP_ACCOUNT",
  "symbol": "COMEX:F:GC:2608",
  "direction": "BUY",
  "volume": 2,
  "yesterday_volume": 1
}
```

`get_positions.data` 为这些对象组成的数组。

### Status and error

`status.TAP` 的 `event` 为 `status`：

```json
{
  "ready": false,
  "md_ready": false,
  "td_ready": false,
  "reason": "TD disconnected",
  "occurred_at": 1785205865456
}
```

`errors.TAP` 的 `event` 为 `error`：

```json
{
  "code": "tap_error",
  "message": "TAP error description",
  "retryable": true,
  "occurred_at": 1785205865456
}
```

`marketdata` 事件必须同时保留：

- TAP 原始交易所时间
- 解析后的 `exchange_timestamp`
- Proxy 收到回调的 `local_time`
- Proxy 发布时间 `published_at`

订单、成交、资金和持仓事件必须使用协议枚举字符串，不能透传
`vnpy-tap` 的 Python 常量对象。

## 6. Compatibility

- 新增可选字段属于向后兼容变更，不提升版本。
- 删除字段、修改字段含义、修改枚举或 topic 结构必须提升
  `schema_version`。
- 客户端必须拒绝高于自身支持范围的版本。
- 未知 JSON 字段应被忽略，未知 action 和枚举值必须返回错误。

## 7. Phase-4/5 implementation boundary

v1 action 已全部接入原生 TAP Session。当前实现：

- MD/TD 登录和初始资金、持仓、委托同步
- 动态行情订阅和引用计数
- 资金、持仓和委托查询
- 下单、延迟撤单以及异步订单和成交事件
- PostgreSQL 跨进程重启订单幂等
- TAP `ClientOrderNo`、`OrderNo + ServerFlag` 和策略归属恢复
- 断线状态事件和指数退避重连
- 交易引擎侧 ZeroMQ TAP Gateway；客户端不依赖 `vnpy-tap`

以下数据写入 PostgreSQL：

- `client_id + strategy_id + client_order_id` 幂等键
- TAP `ClientOrderNo`
- TAP `OrderNo + ServerFlag`
- 订单 Offset 和策略归属

因此 Proxy 重启后可恢复旧订单的策略归属和撤单映射。
`status.data.session.order_mapping_persistent` 为 `true`；
`order_store_healthy` 表示当前数据库连接是否可用。

报单前会先创建 `PENDING_SUBMIT` 记录，再调用 TAP 原生 API。若进程在柜台
接受报单后、写回 `ClientOrderNo` 前崩溃，同一幂等键不会自动重放，而是
返回 `recovery_required=true`。运维人员必须先用柜台委托查询结果完成核对，
再决定后续处理，以免产生重复订单。
