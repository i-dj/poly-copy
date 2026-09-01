package main

import (
	"context"
	"log"
	"os"
	"os/signal"
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

	cfg := polymarket.Config{
		WSURL:               "wss://ws-live-data.polymarket.com",
		GammaMarketsURL:     "https://gamma-api.polymarket.com/markets",
		MinSize:             0,
		ReconnectDelay:      5 * time.Second,
		SettlementBatchSize: 100,
	}

	go polymarket.StartSettlementScanner(ctx, db, cfg, settlementInterval)
	go polymarket.StartCopyWalletSyncer(ctx, db, copyWalletInterval, copyWalletLimit)

	if err := polymarket.RunTradeTracker(ctx, db, cfg); err != nil {
		log.Fatal(err)
	}
}
