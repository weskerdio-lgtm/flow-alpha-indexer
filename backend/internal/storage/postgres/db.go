package postgres

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
)

type DB struct {
	Pool *pgxpool.Pool
}

// ConnectDB 初始化并返回 pgx 连接池
func ConnectDB(ctx context.Context, dbURL string) (*DB, error) {
	config, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		return nil, fmt.Errorf("解析数据库配置失败: %w", err)
	}

	// 专门为极速并发微调的配置：可根据机器性能进一步调整
	config.MaxConns = 20

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("建立连接池失败: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("数据库 ping 失败: %w", err)
	}

	log.Println("成功连接到 PostgreSQL 开发数据库池")
	return &DB{Pool: pool}, nil
}

// Close 用于在主程序退出时释放资源
func (db *DB) Close() {
	if db.Pool != nil {
		db.Pool.Close()
		log.Println("数据库连接池已关闭")
	}
}
