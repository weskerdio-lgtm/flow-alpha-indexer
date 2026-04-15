package listener

import (
	"context"
	"flow-alpha-indexer/internal/model"
	"log"

	sink "github.com/streamingfast/substreams-sink"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
)

// Transformer 解耦清洗层，让 Listener 只认接口不认人，方便写纯粹的测试
type Transformer interface {
	Transform(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData) ([]model.SwapEvent, error)
}

// Loader 解耦打盘层，杜绝死锁强绑定 PostgreSQL
type Loader interface {
	Load(ctx context.Context, events []model.SwapEvent, currentCursor string, blockNum uint64) error
	Rollback(ctx context.Context, lastValidBlock uint64, undoCursor string) error
}

// FirehoseListener 实现官方 sink.Handler 的极简提取层 (ETL 中的 Extract)
type FirehoseListener struct {
	transformer Transformer
	loader      Loader
}

func NewFirehoseListener(t Transformer, l Loader) *FirehoseListener {
	return &FirehoseListener{
		transformer: t,
		loader:      l,
	}
}

// HandleBlockScopedData 接收官方 Firehose 发送的每一次实时跳动数据包
func (l *FirehoseListener) HandleBlockScopedData(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *sink.Cursor) error {
	log.Printf("📡 [Listener] 真实网络脉冲到达! 捕获到主网区块: #%d (%s)", data.Clock.Number, data.Clock.Id)

	// Step 1: 送到下一级（清洗层）
	events, err := l.transformer.Transform(ctx, data)
	if err != nil {
		return err
	}

	// 针对无需写入的空块进行降压
	if len(events) == 0 {
		return nil
	}

	// Step 2: 洗出高价值对象后，交给再下一级（存储入库层）
	err = l.loader.Load(ctx, events, cursor.String(), data.Clock.Number)
	if err != nil {
		return err
	}

	return nil
}

// HandleBlockUndo 处理分叉回滚信号
func (l *FirehoseListener) HandleBlockUndoSignal(ctx context.Context, data *pbsubstreamsrpc.BlockUndoSignal, cursor *sink.Cursor) error {
	log.Printf("📡 [Listener] 收到【分叉回滚】告警: LastValidBlock=#%d", data.LastValidBlock.Number)

	// 直接穿透到下游加载器执行数据清洗
	err := l.loader.Rollback(ctx, data.LastValidBlock.Number, cursor.String())
	if err != nil {
		return err
	}

	return nil
}
