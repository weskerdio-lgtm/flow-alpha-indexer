package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"flow-alpha-indexer/internal/config"
	"flow-alpha-indexer/internal/indexer/listener"
	"flow-alpha-indexer/internal/indexer/loader"
	"flow-alpha-indexer/internal/indexer/transformer"
	"flow-alpha-indexer/internal/storage/cursor"
	"flow-alpha-indexer/internal/storage/postgres"
)

func main() {
	log.Println("🚀 启动 FlowAlpha 极速 ETL 监听引擎 (Indexer) ...")

	// 1. [共享底盘] 加载系统变量配置
	cfg := config.LoadConfig()

	// 2. [共享底盘] 挂载强事务型关系数据库连接池
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := postgres.ConnectDB(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("无法连接数据库: %v", err)
	}
	defer db.Close() // 全局退出时优雅施放

	// 3. [共享底盘] 初始化状态机存储
	const STORE_ID = "ethereum_uniswap_v3_etl"
	cursorStore := cursor.NewStore(db.Pool, STORE_ID)

	startCursorStr, err := cursorStore.GetCursor(ctx)
	if err != nil {
		log.Fatalf("提取断点异常: %v", err)
	}
	if startCursorStr == "" {
		log.Println("📥 初次启动，无历史游标记录。")
	} else {
		log.Printf("📥 成功恢复系统重启前的火线断点游标: [%s]", startCursorStr)
	}

	// 4. [专有流水引擎] 将系统像流水线一样依次组装 (Load -> Transform -> Extract)
	
	// 最后端 (入库)
	pgLoader := loader.NewPostgresLoader(db.Pool, cursorStore)
	
	// 中段 (过滤清洗)
	ethTransformer := transformer.NewEthTransformer()
	
	// 最前线 (网络收发调度机)
	firehoseHandler := listener.NewFirehoseListener(ethTransformer, pgLoader)
	
	// 5. 将第一组装件赋予官方依赖（因为此阶段无 Token 不真实发起调用，只是打印验证关系）
	_ = firehoseHandler

	log.Println("✅ [ETL 组装完成] 数据被隔离在严格单向的 Listener -> Transformer -> Loader 管道流中！准备好面对千军万马的并发。")

	// 捕捉 Ctrl+C 信号保证程序不断气地平稳落库最新的一笔写操作
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	<-sig
	log.Println("收到强杀信号，切断连接池接收并释放常驻内存堆...')")
	cancel()
	log.Println("引擎静止。")
}
