package cursor

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool    *pgxpool.Pool
	storeID string
}

// NewStore 创建一个绑定游标 ID 的状态存储器
func NewStore(pool *pgxpool.Pool, storeID string) *Store {
	return &Store{
		pool:    pool,
		storeID: storeID,
	}
}

// GetCursor 从 PG 数据库中提取最后停止时的断点
func (s *Store) GetCursor(ctx context.Context) (string, error) {
	var cursor string
	err := s.pool.QueryRow(ctx, "SELECT cursor_val FROM cursors WHERE id = $1", s.storeID).Scan(&cursor)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil // 返回空表示这是第一次启动
		}
		return "", err
	}
	return cursor, nil
}

// SaveCursor 原子化覆盖最新断点。系统中断后可以从这里完美续传。
func (s *Store) SaveCursor(ctx context.Context, cursor string, blockNum uint64) error {
	query := `
		INSERT INTO cursors (id, cursor_val, block_num, updated_at) 
		VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
		ON CONFLICT (id) 
		DO UPDATE SET cursor_val = EXCLUDED.cursor_val, block_num = EXCLUDED.block_num, updated_at = CURRENT_TIMESTAMP;
	`
	_, err := s.pool.Exec(ctx, query, s.storeID, cursor, blockNum)
	return err
}
