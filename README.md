# 🚀 FlowAlpha 极速实时数据抓取流式中台

FlowAlpha 是一套拥有极致低延迟、具备毫秒级反应与链重组容错机制的多链捕获系统引擎。通过采用最轻量原生的 Go 环境与 PostgreSQL 剥除笨重中间件，保障如 Alpha 资产狙击与高胜率地址跟随系统的快闪速度！

## 一、 系统架构理念 (Monorepo & ETL)

本工程采用纯粹业务分层隔离的“单库多应用”代码结构。

```text
flow-alpha-indexer/
├── backend/          # Go 流式服务与 API 集群
│   ├── cmd/
│   │   ├── indexer/  # [执行引擎]: 全自动化 Firehose 监听、过滤与落盘入口
│   │   └── api/      # [请求引擎]: 未来向前端提供 API 报表与 WebSocket 推送
│   └── internal/
│       ├── model/    # [共享模型]: 全局贯通流转的基础数据结构 (如实体 SwapEvent)
│       ├── indexer/  # [专属核心]: 抽离开来的极速 ETL 流水线管道 
│       │   ├── listener/     # [E 层] 拦截 Firehose 封包及 Reorg 重组警报
│       │   ├── transformer/  # [T 层] 基于布隆算法丢弃无用杂波，实现结构规整化
│       │   └── loader/       # [L 层] 执行极致 Pgx 事务高频打库，同步游标防断连
│       └── storage/  # [基础底盘]: Postgres 长连池及等核心组件支持群
├── frontend/         # 基于 Vite + React TS 的全栈监控面板层
└── docs/             # 核心设计手稿及说明文件
```

## 二、 核心特性优势

1. **摒弃笨重 ORM**：拒绝主流 Substream 官方自带通用的臃肿 `sink-sql` 依赖。转而利用原生的 `pgx` 封装“微批推送引擎”，确保最严苛的量化监控低延时读写！
2. **坚不可摧的断点游标 (Cursor)**：即使服务器意外停电引发系统被硬杀，依托于在同一个原子事务中绑定的状态机，系统重启依然能顺理成章找回断点，绝不遗漏或重复。
3. **全双工的分叉/重组自适应 (Reorg)**：无惧极速链 (如 Polygon/Solana) 或者以太坊分叉。内部原生搭载 `BlockUndoSignal` 流转处理，一旦遇到回滚数据即刻自动化修正错误波段。

## 三、 本地极速拉起指南

### 1. 准备基础设施

* 启动 `PostgreSQL` 实例环境。
* 在其内部运行初始化建表代码：
  ```bash
  psql -U your_user -d your_db -f backend/migrations/000001_init_schema.up.sql
  ```
* 访问 [Pinax Network](https://app.pinax.network/) 领取开发者免费的大额 Firehose 测试 Token 凭证。

### 2. 注入运行变量

向 `backend/` 下注入刚建立好的环境参数与连接口令入刚建立好的环境参数与连接口令：

```bash
cp backend/.env.example backend/.env
# 使用编辑器填入对应的 Token 以及 Database URL:
# vi backend/.env
```

### 3. 编排并拉起 Go 强力引擎

```bash
cd backend
go mod tidy
go build -o indexer-engine ./cmd/indexer
./indexer-engine
```

### 4. 加载调试前端面板

```bash
cd frontend
npm install
npm run dev
```

### 5. 自动化测试与功能集验证

为了支持 TDD 用例驱动与沙盒演练，本项目的所有组件都配套了高覆盖率的 `*_test.go`。
特别是对于 **真实网络的抓取连通性测试**，我们单独放置在 `listener` 包内。

运行下述指令即可单独对“真网连结”或者“游标数据库更新”发起沙盒验证（无需启动全服）：
```bash
cd backend

# 测试：全流程各个组件 (Cursor管理、数据清洗、落盘分叉回滚容错)
go test -v ./internal/...

# 测试：真网数据串流读取 (抓取限定的50个区块并熔断)
go test -v ./internal/indexer/listener -run TestLiveNetworkExtraction
```

