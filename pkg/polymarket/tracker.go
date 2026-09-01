package polymarket

import (
	"context"
	"database/sql"
	"errors"
	"log"
)

func RunTradeTracker(ctx context.Context, db *sql.DB, cfg Config) error {
	client := NewClient(cfg)
	repo := NewTradeRepository(db)

	log.Println("启动 Polymarket 全市场成交盈亏追踪")
	log.Printf("WS: %s", cfg.WSURL)
	log.Printf("MIN_SIZE: %v", cfg.MinSize)

	err := client.ListenAllTrades(ctx, func(ctx context.Context, trade Trade) error {
		row, err := client.NormalizeTrade(ctx, trade)
		if err != nil {
			return err
		}

		if row.Wallet == "" {
			log.Printf("跳过成交: 缺少 wallet tx=%s market=%s", row.TxHash, row.MarketTitle)
			return nil
		}

		if row.AssetID == "" {
			log.Printf("跳过成交: 缺少 asset_id wallet=%s tx=%s market=%s", row.Wallet, row.TxHash, row.MarketTitle)
			return nil
		}

		if row.Side != "BUY" && row.Side != "SELL" {
			log.Printf("跳过成交: 不支持 side=%s wallet=%s tx=%s", row.Side, row.Wallet, row.TxHash)
			return nil
		}

		if row.Size < cfg.MinSize {
			log.Printf("跳过成交: size=%s 小于 MIN_SIZE=%v wallet=%s tx=%s", formatFloat(row.Size, 12), cfg.MinSize, row.Wallet, row.TxHash)
			return nil
		}

		result, err := repo.ProcessTrade(ctx, row)
		if err != nil {
			return err
		}

		log.Printf(
			"成交处理完成 inserted=%t price_updated=%d settled=%d closed=%d realized_pnl=%s remaining_sell=%s %s",
			result.Inserted,
			result.PriceUpdated,
			result.Settled,
			result.Closed,
			formatFloat(result.RealizedPNL, 12),
			formatFloat(result.RemainingSell, 12),
			row.LogString(),
		)
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	log.Println("已停止")
	return nil
}
