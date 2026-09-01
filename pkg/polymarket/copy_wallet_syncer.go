package polymarket

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type CopyWalletCandidate struct {
	Wallet        string
	ClosedCount   int
	TotalCost     float64
	NetPNL        float64
	ROIPct        float64
	ProfitPerUSDC float64
	WinRatePct    float64
}

func StartCopyWalletSyncer(ctx context.Context, db *pgxpool.Pool, interval time.Duration, limit int) {
	if interval <= 0 {
		interval = time.Minute
	}
	if limit <= 0 {
		limit = 5
	}

	repo := NewTradeRepository(db)
	if err := repo.EnsureCopyWalletSchema(ctx); err != nil {
		log.Printf("跟单钱包刷新失败：初始化 copy_wallet 字段失败 err=%v", err)
	}
	if err := repo.EnsureCopyWalletIndex(ctx); err != nil {
		log.Printf("跟单钱包刷新提醒：创建 copy_wallet 钱包索引失败 err=%v", err)
	}

	log.Printf("跟单钱包刷新间隔：%s，候选数量：%d", interval, limit)
	syncCopyWallets(ctx, repo, limit)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			syncCopyWallets(ctx, repo, limit)
		}
	}
}

func syncCopyWallets(ctx context.Context, repo *TradeRepository, limit int) {
	if err := repo.SyncCopyWalletSequence(ctx); err != nil {
		log.Printf("跟单钱包刷新提醒：校准 copy_wallet 自增序列失败 err=%v", err)
	}
	if err := repo.BackfillCopyWalletMetrics(ctx); err != nil {
		log.Printf("跟单钱包刷新提醒：回填 copy_wallet 战绩失败 err=%v", err)
	}

	candidates, err := repo.ListTopCopyWalletCandidates(ctx, limit)
	if err != nil {
		log.Printf("跟单钱包刷新失败：查询候选钱包失败 err=%v", err)
		return
	}

	for i, candidate := range candidates {
		inserted, err := repo.UpsertCopyWalletCandidate(ctx, candidate, i+1)
		if err != nil {
			log.Printf("跟单钱包写入失败：wallet=%s err=%v", candidate.Wallet, err)
			continue
		}

		if inserted {
			logKeyEvent(
				"新增跟单钱包",
				"wallet=%s | 净赚 %s，ROI %s%%，胜率 %s%%，成交 %d 笔，成本 %s",
				candidate.Wallet,
				formatSignedFloat(candidate.NetPNL, 6),
				formatFloat(candidate.ROIPct, 2),
				formatFloat(candidate.WinRatePct, 2),
				candidate.ClosedCount,
				formatFloat(candidate.TotalCost, 6),
			)
		}
	}
}
