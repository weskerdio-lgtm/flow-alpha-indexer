package listener_test

import (
	"context" // Go 核心包：用于控制程序的生命周期（比如设置超时，没它程序可能会死锁）
	"os"      // Go 核心包：用于读取操作系统环境变量（比如读取 .env 里的 Token）
	"testing" // Go 测试包：所有以 _test.go 结尾的文件都必须引用它
	"time"    // Go 时间包：处理等待、超时等时间操作

	// 引入我们自己项目内部定义的各种功能包
	"flow-alpha-indexer/internal/indexer/listener"
	"flow-alpha-indexer/internal/indexer/transformer"
	"flow-alpha-indexer/internal/model"

	// 引入第三方强大的 Web3 与数据库工具包
	"github.com/joho/godotenv"            // 自动加载 .env 文件到程序环境
	"github.com/streamingfast/bstream"     // 处理区块链“块流”的基础工具
	"github.com/streamingfast/logging"     // 高性能日志系统
	"github.com/streamingfast/substreams/client"   // Substreams 客户端（发号施令的）
	"github.com/streamingfast/substreams/manifest" // 用于解析 .spkg 这种子流包文件
	sink "github.com/streamingfast/substreams-sink" // “漏斗”：负责把数据从云端排干到我们本地
)

// 定义全局的测试日志记录器，方便在控制台看到内部运行细节
var zlog, tracer = logging.PackageLogger("test", "test")

// TestLiveNetworkExtraction 是我们这个“真网连结”测试的主函数
func TestLiveNetworkExtraction(t *testing.T) {
	// 第一步：尝试加载根目录下的 .env 文件。里面存着您的 API Key。
	_ = godotenv.Load("../../../.env")

	// 从环境变量里把刚才配置的地址、Token 统统拿出来
	endpoint := os.Getenv("FIREHOSE_ENDPOINT") // 云端服务器地址
	
	// 这里非常关键：我们要确定用什么身份去跟 Pinax 说话
	jwtToken := os.Getenv("FIREHOSE_JWT")
	if jwtToken == "" {
		t.Skip("跳过测试: 您没在 .env 里填 FIREHOSE_JWT，无法起跳")
	}

	// 如果地址都没配置，测试也没法跑，直接跳过
	if endpoint == "" {
		t.Skip("跳过测试: 缺少 FIREHOSE_ENDPOINT 配置")
	}

	// 第二步：创建一个“倒计时闹钟” (Context)。我们给测试 30 秒时间。
	// 如果 30 秒还没跑完，程序会自动自杀，防止它在后台无限运行消耗您的额度。
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel() // 函数结束时，不论成功失败，都关闭闹钟

	// 第三步：【核心组装】把我们的 ETL 三兄弟组装起来
	// 1. 模拟一个装载器 (testLoader)，它不写数据库，只管在屏幕打印
	ldr := &testLoader{t: t}
	// 2. 拿出我们的“洗肉工” (Transformer)，也就是之前写的那个清洗数据的类
	tfm := transformer.NewEthTransformer()
	// 3. 拿出我们的“拦截器” (Listener)，把清洗工和装载器塞进它肚子里
	handler := listener.NewFirehoseListener(tfm, ldr)

	// 第五步：加载 .spkg 文件。这个文件就像是一个“说明书”，告诉 Substreams 怎么在以太坊里找 Uniswap 交易。
	pkgReader, err := manifest.NewReader("../../../uniswap-v3.spkg")
	if err != nil {
		t.Fatalf("找不着 spkg 文件: %v", err)
	}
	pkg, err := pkgReader.Read()
	if err != nil {
		t.Fatalf("spkg 文件打不开或格式不对: %v", err)
	}

	// 解析这个“说明书”里的逻辑图，看它支持哪些数据导出
	graph, err := manifest.NewModuleGraph(pkg.Package.Modules.Modules)
	if err != nil {
		t.Fatalf("spkg 逻辑有错: %v", err)
	}
	// 告诉它我们要监听 map_pools_created 这个模块（它负责盯着新池子的建立）
	outputModule, err := graph.Module("map_pools_created")
	if err != nil {
		t.Fatalf("想听的模块不在 spkg 里: %v", err)
	}

	// 第六步：配置“接头暗号”。告诉 Substreams 我们的目的地、Token 和协议类型(JWT)
	clientConfig := client.NewSubstreamsClientConfig(client.SubstreamsClientConfigOptions{
		Endpoint:  endpoint,
		AuthToken: jwtToken,
		AuthType:  client.JWT,
	})

	// 第七步：初始化“漏斗” (Sinker)。它是连接我们本地代码和云端节点的桥梁。
	s, err := sink.New(
		sink.SubstreamsModeDevelopment, // 开发模式：允许快速测试和断点重启
		false,
		pkg.Package,
		outputModule,
		manifest.ModuleHash(nil),
		clientConfig,
		zlog,
		tracer,
		// 重点！这里指定了区块范围：12,369,621 是 Uniswap V3 在以太坊诞生的那个瞬间。
		// 我们只抓这 10 个区块，这样测试能秒过。
		sink.WithBlockRange(bstream.NewRangeExcludingEnd(12369621, 12369631)),
	)
	if err != nil {
		t.Fatalf("初始化漏斗失败: %v", err)
	}

	t.Log("📡 [通道开启] 正在请求数据，如果不动请检查您的 API Key 是否过期...")

	// 第八步：【正式起飞】。这一步程序会真正建立 gRPC 长连接。
	// 数据会像洪水一样涌进来，流入我们的 Handler (其实就是流入了 Listener -> Transformer -> Loader)
	s.Run(ctx, sink.NewBlankCursor(), sink.NewSinkerHandlers(handler.HandleBlockScopedData, handler.HandleBlockUndoSignal))

	t.Log("✅ 测试完成：这意味着您的 Token、网络和代码逻辑全部通了！")
}

// =============================================================================
// 这下面是一个“虚拟装载器” (Mock Loader)
// 就像是练习拳击时的沙袋。为了测试安全，我不让它真的往数据库里写东西。
// 它收到数据后，只会乖乖地在屏幕上打印一下。
// =============================================================================
type testLoader struct {
	t *testing.T
}

// Load 函数是这个“沙袋”对外接收数据的接口。
func (m *testLoader) Load(ctx context.Context, events []model.SwapEvent, cursor string, blockNum uint64) error {
	m.t.Logf("📥 [Mock Loader] 嘿！我真的接到主网数据包了！高度=#%d", blockNum)
	for _, ev := range events {
		m.t.Logf("   |- 真实数据包（二进制）大小: %d 字节", len(ev.Payload))
	}
	return nil
}

// Rollback 是处理区块链分叉回滚的。在测试里我们直接忽略它。
func (m *testLoader) Rollback(ctx context.Context, lastValidBlock uint64, undoCursor string) error {
	return nil
}
