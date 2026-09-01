package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"poly-copy/pkg/database"
	"poly-copy/pkg/polymarket"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := database.Connect()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	log.Println("数据库连接成功")

	settlementInterval := 1 * time.Minute
	copyWalletInterval := 1 * time.Minute
	copyWalletLimit := 5
	copyTraderPollInterval := 3 * time.Second
	copyTraderSyncInterval := 1 * time.Minute
	copyMode := "live"
	privateKey := mustGetCfg(ctx, db, "POLYMARKET_PRIVATE_KEY")
	funderAddress := mustGetCfg(ctx, db, "POLYMARKET_PROXY_ADDRESS")

	cfg := polymarket.Config{
		WSURL:               "wss://ws-live-data.polymarket.com",
		GammaMarketsURL:     "https://gamma-api.polymarket.com/markets",
		CLOBURL:             "https://clob.polymarket.com",
		DataAPITradesURL:    "https://data-api.polymarket.com/trades",
		PrivateKey:          privateKey,
		FunderAddress:       funderAddress,
		PythonBin:           "python3",
		LiveOrderScript:     "pkg/polymarket/live_order.py",
		BalanceScript:       "pkg/polymarket/wallet_balance.py",
		MinSize:             0,
		ReconnectDelay:      2 * time.Second,
		SettlementBatchSize: 100,
		CopyMode:            copyMode,
		CopyUSDC:            1,
		MinCopyUSDC:         5,
		CopyTradeLimit:      10,
		MinCopyPrice:        0.05,
		MaxCopyPrice:        0.95,
		SkipUpDownMarkets:   true,
	}

	go polymarket.StartSettlementScanner(ctx, db, cfg, settlementInterval)
	go polymarket.StartCopyWalletSyncer(ctx, db, copyWalletInterval, copyWalletLimit)
	go polymarket.StartCopyTrader(ctx, db, cfg, copyTraderPollInterval, copyTraderSyncInterval)

	if err := polymarket.RunTradeTracker(ctx, db, cfg); err != nil {
		log.Fatal(err)
	}
}

func mustGetCfg(ctx context.Context, db database.QueryRower, key string) string {
	value, err := database.GetCfgName(ctx, db, key)
	if err != nil {
		log.Fatalf("读取配置失败：%s err=%v", key, err)
	}

	value = strings.TrimSpace(value)
	if value == "" {
		log.Fatalf("读取配置失败：%s 为空", key)
	}

	return value
}
