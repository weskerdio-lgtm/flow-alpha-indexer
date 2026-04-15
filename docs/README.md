🚀 FlowAlpha 链上实时数据监听中台 

## 一、 项目定位与核心场景 (Business Scenarios)

 **定位** ：一个极致低延迟、高并发的多链（EVM + Solana）数据处理引擎。摒弃笨重的消息中间件，采用极简直连与读写分离架构，为上层 DApp、展示面板和交易机器人提供强一致性的实时数据支撑。

 **核心业务场景（漏斗式过滤高价值数据）** ：

1. **Alpha 资产狙击（极速）：** 实时捕获 Solana（Raydium/Pump.fun）新流动性池与代币发射，毫秒级推送买入信号。
2. **巨鲸动态追踪（深度）：** 过滤噪音，监控 Uniswap V3 上金额 > 10,000 USD 的高胜率“聪明钱”地址，支撑自动化跟单系统。
3. **安全风控熔断（准度）：** 监控黑名单地址互动与 DeFi 协议异常资金流出，实时报警。

---

## 二、 核心技术架构 (Architecture)

系统采用**“端到端全双工、CQRS 读写分离”**架构，利用 Go 的超强并发与 PostgreSQL 的强事务特性打通全链路，并引入 Redis 保障前端秒开体验。

### 底层基建：三大开源引擎缝合

站在巨人的肩膀上，将以下三个顶级开源库重构并融合为系统底盘：

1. **底盘与状态机 (`streamingfast/substreams-sink-sql`)：** 剥离其通用 SQL 驱动，提取核心的 **Firehose gRPC 客户端**与 **Cursor（游标）断点续传**管理逻辑。
2. **并发调度引擎 (`Layr-Labs/chain-indexer`)：** 借鉴其 **Go 协程池 (Worker Pool)** 削峰填谷设计与 EVM 解析流转框架。
3. **极速解码核武器 (`zewebdev1337/solana-swap-go`)：** 作为无状态函数库引入，实现极其复杂的 Solana 指令零拷贝反序列化。

---

## 三、 数据链路设计与落地细节 (ETL & CQRS Design)

在无中间件的直连架构下，链路性能与数据准确性是生死线。

### 1. 极速写入链路 (Write Path: 漏斗过滤 + 微批落盘)

* **读取 (Extract)** ：程序从 PG 的 `cursors` 表读取上次断点，向 Firehose 发起订阅。设置内存 Channel 滑动窗口，防止突发流量引发 OOM。
* **过滤与清洗 (Transform)** ：
* **业务降压** ：只订阅核心合约（如特定 Uniswap Pool）；只反序列化 `Swap/Mint/Burn` 核心动作。
* **内存布隆过滤器** ：只放行命中“目标观察名单”的地址，其余数据在 Go 内存中直接抛弃。
* **微批入库 (Load)** ：在 Go 内核中维护 Buffer Ring，每积攒 2000 条或满 500ms，触发一次 `pgx.CopyFrom` 的高频并发插入。

### 2. 毫秒级分发链路 (Read Path: 读写分离)

为保证前端监控面板的“零延迟”体验，绝对禁止前端高频轮询核心数据库：

* **极热数据 (Redis + WebSocket)** ：Go 引擎在解码出高价值交易后，**双写** Redis (如 ZSet 排行榜)，并通过 `Pub/Sub` 触发 WebSocket 主动向前端页面推送实时跳动数据。
* **温冷报表 (PG 物化视图)** ：针对历史 K 线或 30 天胜率统计，在 PostgreSQL 中建立并发物化视图（Materialized Views），由 Go 定时刷新，供前端秒查。

---

## 四、 攻克硬核难点：链重组 (Reorg) 的终极处理

将区块链分叉回滚的计算压力，全部推给 PostgreSQL 的事务与防冲突机制：

1. **状态机感知** ：Firehose 推送 `StepNew`（新块）与 `StepUndo`（回滚块）。
2. **表设计支撑** ：建立以 `chain_id`, `tx_hash`, `log_index` 为联合主键的 `dex_swaps` 表。异构的协议数据（如 `sqrtPriceX96`）存入 PG 的 **`JSONB`** 字段。
3. **事务级 Upsert 覆盖** ：利用 PG 的 `ON CONFLICT DO UPDATE` 机制。当 Go 引擎收到 `StepUndo` 回滚数据时，直接带上 `is_deleted = TRUE` 标记下发。PG 会自动覆盖原纪录，配合事务保证业务查询的绝对最终一致性。
4. **前端撤销同步** ：Go 引擎同步清理 Redis 脏缓存，并通过 WebSocket 推送 `REVERT_TRADE` 信号，前端页面瞬间剔除错误记录。

---

## 五、 实施落地蓝图 (Execution Plan)

* **第一阶段：跑通基建（破冰期）**
  * **动作** ：Fork 并改造 `substreams-sink-sql`，连通 Firehose。实现单链（Ethereum）、单合约监听。
  * **里程碑** ：跑通 Go 引擎的 `Cursor` 管理与 PG 的基础插入；验证程序中断重启后数据能精准续传。
* **第二阶段：并发攻坚与解码（深水区）**
  * **动作** ：整合 `solana-swap-go`，接入高并发的 Solana Raydium 数据。在 Go 内部实现 Worker Pool，利用 `sync.Pool` 复用对象；实现 `pgx.CopyFrom` 微批入库。
  * **里程碑** ：在 Solana 交易高峰期，Go 内存消费平稳，PG 写入不存在死锁或连接被打满的情况。
* **第三阶段：业务闭环与全栈展示（高光期）**
  * **动作** ：编写带有 `ON CONFLICT` 的事务级 Upsert SQL 处理 Reorg；在 Go 内存中加入布隆过滤器。接入 Redis 双写与 WebSocket 推送服务。
  * **里程碑** ：实现一个实时监控 UI。前端页面能毫秒级闪烁最新巨鲸交易，并在遭遇链上分叉时自动修正报表数据。

---
