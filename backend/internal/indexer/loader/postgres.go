package loader

import (
	"context"
	"log"

	"flow-alpha-indexer/internal/model"
	"flow-alpha-indexer/internal/storage/cursor"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresLoader 极速入库与状态落地层 (ETL 中的 Load)
type PostgresLoader struct {
	pool        *pgxpool.Pool
	cursorStore *cursor.Store
}

func NewPostgresLoader(pool *pgxpool.Pool, cStore *cursor.Store) *PostgresLoader {
	return &PostgresLoader{
		pool:        pool,
		cursorStore: cStore,
	}
}

// Load 将标准实体打盘，包含事务落盘与 Cursor 记录滑动
func (l *PostgresLoader) Load(ctx context.Context, events []model.SwapEvent, currentCursor string, blockNum uint64) error {
	log.Printf("📥 [Loader] 收到 %d 条高纯度业务数据，进行事务批处理... Cursor: %s", len(events), currentCursor)

	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) // 安全兜底

	for _, ev := range events {
		// 单条 Insert 示例。实际追求速度时使用 px.CopyFrom 阵列批量推送
		_, err := tx.Exec(ctx, 
			"INSERT INTO test_swaps (id, block_num, payload, created_at) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING",
			ev.ID, ev.BlockNumber, string(ev.Payload), ev.Timestamp,
		)
		if err != nil {
			log.Printf("📥 [Loader] SQL异常: %v", err)
			return err
		}
	}

	err = tx.Commit(ctx)
	if err != nil {
		return err
	}

	// 业务成功落盘后，滑动游标。TODO: 生产环境下与上面写入同属一个强一致事务。
	err = l.cursorStore.SaveCursor(ctx, currentCursor, blockNum)
	if err != nil {
		log.Printf("📥 [Loader] 游标落地异常: %v", err)
		return err
	}

	return nil
}

// Rollback 处理链重组撤销命令
func (l *PostgresLoader) Rollback(ctx context.Context, lastValidBlock uint64, undoCursor string) error {
	log.Printf("⚠️ [Loader] 发起级联撤销！正在从数据库剔除错乱区块，目标退到: #%d", lastValidBlock)
	
	// 这里用硬删除示意，正式环境推荐 UPDATE is_deleted=TRUE
	_, err := l.pool.Exec(ctx, "DELETE FROM test_swaps WHERE block_num > $1", lastValidBlock)
	if err != nil {
		return err
	}

	err = l.cursorStore.SaveCursor(ctx, undoCursor, lastValidBlock)
	if err != nil {
		return err
	}
	return nil
}
