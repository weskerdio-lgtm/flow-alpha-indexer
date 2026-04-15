package transformer

import (
	"context"
	"testing"

	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
)

func TestEthereumTransformer_Transform(t *testing.T) {
	transformer := NewEthTransformer()

	// 构建一次模拟的链上原始传输块数据
	mockData := &pbsubstreamsrpc.BlockScopedData{
		Clock: &pbsubstreams.Clock{
			Id:     "0x_MOCK_GOLDEN_BLOCK_HASH",
			Number: 12369621,
		},
		// 暂时省略具体的 Any 序列化报文体
	}

	ctx := context.Background()

	// 交给流水线处理
	events, err := transformer.Transform(ctx, mockData)

	if err != nil {
		t.Fatalf("🚫 数据层转换异常，发生了非预期的阻断: %v", err)
	}

	if len(events) == 0 {
		t.Fatalf("🚫 Transformer 没能吐出有效实体，或者全被错误拦截了")
	}

	// 抽取清洗后的 Go 常规对象
	ev := events[0]
	if ev.BlockNumber != 12369621 {
		t.Errorf("数据被串台！预期的块高洗练不正确， 期待 12369621, 实际: %d", ev.BlockNumber)
	}
	if ev.ID != "0x_MOCK_GOLDEN_BLOCK_HASH" {
		t.Errorf("ID 未通过实体透传，真实值: %s", ev.ID)
	}

	t.Logf("✅ Transformer 隔离区完全健康。成功拦截并洗去了底层噪音: %+v", ev)
}
