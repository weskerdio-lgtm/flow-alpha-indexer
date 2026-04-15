package cursor

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

func TestStoreLifeCycle(t *testing.T) {
	// 在 TDD 测试环节尝试向上层寻找 .env
	_ = godotenv.Load("../../../../.env")

	dbUrl := os.Getenv("DATABASE_URL")
	if dbUrl == "" {
		t.Skip("⏭️ 未提供极速数据库 DATABASE_URL，已跳过 Cursor 落地集成测试。")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbUrl)
	if err != nil {
		t.Fatalf("连通极速数据库池失败: %v", err)
	}
	defer pool.Close()

	storeID := "unit_test_cursor_worker"
	store := NewStore(pool, storeID)

	// Step1: UPSERT 初次插入
	if err := store.SaveCursor(ctx, "C_BLOCK_100", 100); err != nil {
		t.Fatalf("初次原子性 UPSERT 失败: %v", err)
	}

	// Step2: UPSERT 再次覆盖
	if err := store.SaveCursor(ctx, "C_BLOCK_200", 200); err != nil {
		t.Fatalf("由于业务步进覆盖游标失败: %v", err)
	}

	// Step3: 测试安全提取
	val, err := store.GetCursor(ctx)
	if err != nil {
		t.Fatalf("提取最新的断点失败: %v", err)
	}

	if val != "C_BLOCK_200" {
		t.Errorf("提取的游标与系统预期不一致！期待: %s 实际提取: %s", "C_BLOCK_200", val)
	}

	t.Log("✅ Cursor 原子存取保护逻辑验证完美流转！")
}
