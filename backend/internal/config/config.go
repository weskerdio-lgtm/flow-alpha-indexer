package config

import (
	"log"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type Config struct {
	DatabaseURL        string
	FirehoseEndpoint   string
	FirehoseAPIToken   string
	StartBlockNum      uint64
	SubstreamsPackage  string // 用于以太坊上直接引入编译好的 .spkg 文件
	SubstreamsModule   string // 如 map_swaps
}

// LoadConfig 从环境变量或 .env 文件中加载启动配置
func LoadConfig() *Config {
	// 尝试加载本地的 .env 文件
	_ = godotenv.Load()

	startBlock, _ := strconv.ParseUint(getEnv("START_BLOCK_NUM", "0"), 10, 64)

	cfg := &Config{
		DatabaseURL:       getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/flow_alpha?sslmode=disable"),
		FirehoseEndpoint:  getEnv("FIREHOSE_ENDPOINT", "eth.substreams.pinax.network:443"),
		FirehoseAPIToken:  getEnv("FIREHOSE_API_TOKEN", ""),
		StartBlockNum:     startBlock,
		SubstreamsPackage: getEnv("SUBSTREAMS_PACKAGE", "https://github.com/streamingfast/substreams-uniswap-v3/releases/download/v0.2.8/substreams.spkg"),
		SubstreamsModule:  getEnv("SUBSTREAMS_MODULE", "map_pools_created"), // 作为测试预设一个模块
	}

	if cfg.FirehoseAPIToken == "" {
		log.Println("WARNING: FIREHOSE_API_TOKEN 未设置，连接正式节点可能失败，请在测试前提供。")
	}

	return cfg
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
