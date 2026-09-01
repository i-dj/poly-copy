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

	log.Printf("启动 Polymarket 成交追踪，MIN_SIZE=%v", cfg.MinSize)

	err := client.ListenAllTrades(ctx, func(ctx context.Context, trade Trade) error {
		row, err := client.NormalizeTrade(ctx, trade)
		if err != nil {
			return err
		}

		if row.Wallet == "" {
			log.Printf("跳过成交：缺少钱包地址 | %s", row.BriefString())
			return nil
		}

		if row.AssetID == "" {
			log.Printf("跳过成交：缺少资产 ID | %s", row.BriefString())
			return nil
		}

		if row.Side != "BUY" && row.Side != "SELL" {
			log.Printf("跳过成交：不支持方向 %s | %s", row.Side, row.BriefString())
			return nil
		}

		if row.Size < cfg.MinSize {
			log.Printf("跳过成交：数量 %s 小于 MIN_SIZE=%v | %s", formatFloat(row.Size, 6), cfg.MinSize, row.BriefString())
			return nil
		}

		result, err := repo.ProcessTrade(ctx, row)
		if err != nil {
			return err
		}

		logTradeResult(row, result)
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	log.Println("已停止")
	return nil
}

func logTradeResult(row TradeRow, result TradeProcessResult) {
	if !result.Inserted {
		log.Printf("重复成交，已忽略 | %s", row.BriefString())
		return
	}

	log.Printf("%s | 当前价已同步 %d 条", row.BriefString(), result.PriceUpdated)

	if result.Closed > 0 {
		log.Printf("平仓更新：关闭 %d 笔买入，已实现盈亏 %s", result.Closed, formatFloat(result.RealizedPNL, 6))
	}

	if result.RemainingSell > 0 {
		log.Printf("卖出未匹配：剩余 %s 份没有找到对应买入记录", formatFloat(result.RemainingSell, 6))
	}

	if result.Settled > 0 {
		log.Printf("结算更新：%d 笔持仓按结算价 %s 完成结算", result.Settled, formatFloat(row.Price, 6))
	}
}
