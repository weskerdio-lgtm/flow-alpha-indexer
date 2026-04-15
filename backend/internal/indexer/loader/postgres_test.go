package loader

import (
	"context"
	"os"
	"testing"
	"time"

	"flow-alpha-indexer/internal/model"
	"flow-alpha-indexer/internal/storage/cursor"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

func TestPostgresLoader_Rollback(t *testing.T) {
	// 定位至全局配置
	_ = godotenv.Load("../../../../.env")

	dbUrl := os.Getenv("DATABASE_URL")
	if dbUrl == "" {
		t.Skip("⏭️ 未提供全局 DATABASE_URL 参数，跳过本环节对于 Postgres 的并发装载测试。")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbUrl)
	if err != nil {
		t.Fatalf("无法触碰您的 PostgreSQL 集群: %v", err)
	}
	defer pool.Close()

	store := cursor.NewStore(pool, "test_rollback_workflow_01")
	ldr := NewPostgresLoader(pool, store)

	// 构建一个不幸遭到了链分叉影响的数据对象 (比如身处 9999 区块)
	mockEvents := []model.SwapEvent{
		{
			ID:          "mock_tx_hash_reorg",
			BlockNumber: 9999,
			TxHash:      "0x_fake_reorg",
			Payload:     []byte(`{"test": "reorg_safe_guard"}`),
			Timestamp:   time.Now(),
		},
	}

	// 环节 1: 按常规微批入库
	err = ldr.Load(ctx, mockEvents, "C_9999", 9999)
	if err != nil {
		t.Fatalf("🚨 Loader 微批打盘失效: %v", err)
	}

	// 确认库内真实产生了这笔记录
	var count int
	pool.QueryRow(ctx, "SELECT COUNT(*) FROM test_swaps WHERE id=$1", "mock_tx_hash_reorg").Scan(&count)
	if count != 1 {
		t.Fatalf("🚨 [错误] 入库执行报告成功，但 PG 里没检索到 test_swaps 这条数据。")
	}

	// 环节 2: 链上发生网络震荡，Firehose 给你下达追回指令，退到 9998 号块！
	err = ldr.Rollback(ctx, 9998, "C_9998")
	if err != nil {
		t.Fatalf("🚨 [危机] 级联回退命令卡死: %v", err)
	}

	// 验收: 在数据库中查询这笔可怜的交易，它应当随着这阵分叉风被无情且干净地清理
	pool.QueryRow(ctx, "SELECT COUNT(*) FROM test_swaps WHERE id=$1", "mock_tx_hash_reorg").Scan(&count)
	if count != 0 {
		t.Errorf("🚨 链分叉清洗失败！这笔被遗弃的高危交易仍然残留在数据库里影响您的指标。当前残留记录数: %d", count)
	}

	t.Log("✅ 恭喜！Loader 落盘和链分叉修复逻辑双双流转通过！")
}
