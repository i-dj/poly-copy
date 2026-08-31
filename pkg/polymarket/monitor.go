package polymarket

import (
	"context"
	"database/sql"
	"errors"
	"log"
)

func RunMonitor(ctx context.Context, db *sql.DB, cfg Config) error {
	client := NewClient(cfg)
	repo := NewMonitorRepository(db)

	log.Println("启动 Polymarket 全市场成交入库监听")
	log.Printf("WS: %s", cfg.WSURL)
	log.Printf("MIN_SIZE: %v", cfg.MinSize)
	log.Printf("PNL_CACHE_SECONDS: %v", int(cfg.PNLCacheTTL.Seconds()))
	log.Printf("PNL_TIMEOUT: %v", cfg.PNLTimeout.Seconds())
	log.Println("PnL source: official data-api leaderboard DAY/WEEK/MONTH/ALL")

	err := client.ListenAllTrades(ctx, func(ctx context.Context, trade Trade) error {
		row := client.NormalizeTrade(ctx, trade)
		if row.Wallet == "" {
			log.Printf("跳过成交: 缺少 wallet tx=%s market=%s", row.TxHash, row.MarketTitle)
			return nil
		}

		size := SafeFloat(row.Size)
		if size < cfg.MinSize {
			log.Printf("跳过成交: size=%s 小于 MIN_SIZE=%v wallet=%s tx=%s", row.Size, cfg.MinSize, row.Wallet, row.TxHash)
			return nil
		}

		result, err := repo.Upsert(ctx, row)
		if err != nil {
			return err
		}

		log.Printf("成交入库 %s: %s", result, row.LogString())
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	log.Println("已停止")
	return nil
}
