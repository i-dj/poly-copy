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

	cfg := polymarket.Config{
		WSURL:               "wss://ws-live-data.polymarket.com",
		GammaMarketsURL:     "https://gamma-api.polymarket.com/markets",
		MinSize:             0,
		ReconnectDelay:      5 * time.Second,
		SettlementInterval:  1 * time.Minute,
		SettlementBatchSize: 100,
	}

	if err := polymarket.RunTradeTracker(ctx, db, cfg); err != nil {
		log.Fatal(err)
	}
}
