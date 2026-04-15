package model

import "time"

// SwapEvent 代表了系统内部流转的标准化交易实体
// 这就是 ETL 中的 "T" 输出的结果模型。今后无论数据源是 Uniswap 还是 Raydium，
// 都会尽力清洗、解构为这个通用格式供入库层 (Loader) 和后期 Web 层共享处理。
type SwapEvent struct {
	ID          string    // 唯一标识
	BlockNumber uint64    // 区块号
	TxHash      string    // 交易 Hash
	Payload     []byte    // 原生 JSON 或特定结构的序列化串（PG 中为 JSONB）
	Timestamp   time.Time // 区块时间
}
