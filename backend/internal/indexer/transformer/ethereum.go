package transformer

import (
	"context"
	"log"
	"time"

	"flow-alpha-indexer/internal/model"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
)

// EthTransformer 针对 Ethereum 的清洗转换器。
// 未来可以非常方便地加入 SolanaTransformer。
type EthTransformer struct {
	// TODO: 在此处可以挂载布隆过滤器、黑白名单缓存预加载等，用于拦截那些噪音交易
}

func NewEthTransformer() *EthTransformer {
	return &EthTransformer{}
}

// Transform 将原生的 gRPC pbsubstreamsrpc 数据解压、过滤和清洗，输出为纯净内部模型数组
func (t *EthTransformer) Transform(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData) ([]model.SwapEvent, error) {
	blockNum := data.Clock.Number
	blockId := data.Clock.Id

	var payload []byte
	
	// 真正的云端去噪过滤机制：只有当底层 Substreams 真的命中了业务规则（比如真有人创建了新池子）
	// MapOutput 才会有值！我们直接拦截这批真实数据，将空垃圾块彻底丢弃。
	if data.Output != nil && data.Output.MapOutput != nil {
		payload = data.Output.MapOutput.Value
		log.Printf("🔥 [深度清洗层] 命中核心价值数据！在区块 #%d (%s) 截取到 %s 类型的装载包，包体大小 %d Byte", 
			blockNum, blockId, data.Output.MapOutput.TypeUrl, len(payload))
	} else {
		// 该区块对于我们无聊透顶，直接丢弃！
		return nil, nil
	}

	events := []model.SwapEvent{
		{
			ID:          blockId, 
			BlockNumber: blockNum,
			TxHash:      "0x_REAL_HASH_WILL_BE_MAPPED_LATER", // 完整的 Proto 结构反解我们留到业务期做
			Payload:     payload, // 塞入绝对纯正的原生链上结果
			Timestamp:   time.Now(),
		},
	}

	return events, nil
}
