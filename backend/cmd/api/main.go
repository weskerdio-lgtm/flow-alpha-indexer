package main

import (
	"log"
)

// 这个服务未来负责暴露 REST API 查库数据，或者将 Redis 极速热点数据通过 WebSocket 甩向前端页面的。
func main() {
	log.Println("🌐 [Web 服务预留骨架] 此处日后将接入 Gin/Fiber/WebSocket 引擎...")
	log.Println("它会与 Indexer 进程分开部署，但复用相同的 internal/storage 与 internal/model 等核心库")
}
