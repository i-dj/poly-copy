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
		WSURL:          "wss://ws-live-data.polymarket.com",
		LeaderboardURL: "https://data-api.polymarket.com/v1/leaderboard",
		MinSize:        0,
		PNLCacheTTL:    300 * time.Second,
		PNLTimeout:     8 * time.Second,
		ReconnectDelay: 5 * time.Second,
	}

	if err := polymarket.RunMonitor(ctx, db, cfg); err != nil {
		log.Fatal(err)
	}
}
