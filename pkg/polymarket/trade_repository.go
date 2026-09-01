package polymarket

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"time"
)

type TradeRepository struct {
	db *sql.DB
}

type TradeProcessResult struct {
	Inserted      bool
	PriceUpdated  int64
	Closed        int
	RealizedPNL   float64
	RemainingSell float64
}

type OpenMarket struct {
	AssetID     string
	ConditionID string
}

type SettlementResult struct {
	Settled int64
	PNL     float64
}

type openTrade struct {
	ID           int64
	Price        float64
	Remaining    float64
	RealizedPNL  sql.NullFloat64
	CloseValue   sql.NullFloat64
	CurrentValue sql.NullFloat64
}

func NewTradeRepository(db *sql.DB) *TradeRepository {
	return &TradeRepository{db: db}
}

func (r *TradeRepository) ProcessTrade(ctx context.Context, row TradeRow) (TradeProcessResult, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return TradeProcessResult{}, err
	}
	defer tx.Rollback()

	inserted, err := r.insertTrade(ctx, tx, row)
	if err != nil {
		return TradeProcessResult{}, err
	}

	result := TradeProcessResult{
		Inserted: inserted,
	}
	if !inserted {
		if err := tx.Commit(); err != nil {
			return TradeProcessResult{}, err
		}
		return result, nil
	}

	priceUpdated, err := r.updateOpenPrices(ctx, tx, row.AssetID, row.Price)
	if err != nil {
		return TradeProcessResult{}, err
	}

	result.PriceUpdated = priceUpdated

	if row.Side == "SELL" && row.Wallet != "" && row.AssetID != "" {
		closed, realized, remaining, err := r.closeBuyTrades(ctx, tx, row)
		if err != nil {
			return TradeProcessResult{}, err
		}
		result.Closed = closed
		result.RealizedPNL = realized
		result.RemainingSell = remaining
	}

	if err := tx.Commit(); err != nil {
		return TradeProcessResult{}, err
	}

	return result, nil
}

func (r *TradeRepository) ListOpenMarkets(ctx context.Context, staleBefore time.Time, limit int) ([]OpenMarket, error) {
	if limit <= 0 {
		limit = 100
	}

	rows, err := r.db.QueryContext(ctx, `
		SELECT asset_id, condition_id
		FROM polymarket_trades
		WHERE status IN ('OPEN', 'PARTIAL_CLOSED')
			AND remaining_size > 0
			AND asset_id IS NOT NULL
			AND asset_id <> ''
			AND condition_id IS NOT NULL
			AND condition_id <> ''
		GROUP BY asset_id, condition_id
		HAVING MIN(last_checked_at) IS NULL OR MIN(last_checked_at) < $1
		ORDER BY MIN(last_checked_at) ASC NULLS FIRST, condition_id, asset_id
		LIMIT $2
	`, staleBefore, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var markets []OpenMarket
	for rows.Next() {
		var market OpenMarket
		if err := rows.Scan(&market.AssetID, &market.ConditionID); err != nil {
			return nil, err
		}
		markets = append(markets, market)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return markets, nil
}

func (r *TradeRepository) MarkSettlementChecked(ctx context.Context, assetID string) error {
	if assetID == "" {
		return nil
	}

	_, err := r.db.ExecContext(ctx, `
		UPDATE polymarket_trades
		SET
			last_checked_at = now(),
			updated_at = now()
		WHERE asset_id = $1
			AND status IN ('OPEN', 'PARTIAL_CLOSED')
			AND remaining_size > 0
	`, assetID)
	return err
}

func (r *TradeRepository) SettleAsset(ctx context.Context, assetID string, settlementPrice float64) (SettlementResult, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return SettlementResult{}, err
	}
	defer tx.Rollback()

	settled, pnl, err := r.settleOpenTrades(ctx, tx, assetID, settlementPrice)
	if err != nil {
		return SettlementResult{}, err
	}

	if err := tx.Commit(); err != nil {
		return SettlementResult{}, err
	}

	return SettlementResult{Settled: settled, PNL: pnl}, nil
}

func (r *TradeRepository) insertTrade(ctx context.Context, tx *sql.Tx, row TradeRow) (bool, error) {
	status := "OPEN"
	closeReason := sql.NullString{}
	remainingSize := row.Size

	if row.Side == "SELL" {
		status = "CLOSED"
		closeReason = sql.NullString{String: "SOLD", Valid: true}
		remainingSize = 0
	}

	var inserted bool
	err := tx.QueryRowContext(ctx, `
		INSERT INTO polymarket_trades (
			trade_key,
			tx_hash,
			wallet,
			asset_id,
			condition_id,
			side,
			outcome,
			market_title,
			size,
			price,
			notional,
			trade_time,
			raw_trade,
			status,
			close_reason,
			remaining_size,
			current_price,
			current_value,
			unrealized_pnl,
			unrealized_pnl_pct,
			close_price,
			close_value,
			close_tx_hash,
			realized_pnl,
			realized_pnl_pct,
			last_price_at,
			last_checked_at,
			updated_at
		)
		VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb,
			$14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25,
			now(), now(), now()
		)
		ON CONFLICT (trade_key) DO NOTHING
		RETURNING true
	`,
		row.TradeKey,
		row.TxHash,
		row.Wallet,
		row.AssetID,
		nullString(row.ConditionID),
		row.Side,
		nullString(row.Outcome),
		nullString(row.MarketTitle),
		row.Size,
		row.Price,
		row.Notional,
		nullTime(row.TradeTime),
		string(row.RawTrade),
		status,
		closeReason,
		remainingSize,
		row.Price,
		remainingSize*row.Price,
		0,
		0,
		nullableFloat(row.Side == "SELL", row.Price),
		nullableFloat(row.Side == "SELL", row.Notional),
		nullableString(row.Side == "SELL", row.TxHash),
		nullableFloat(row.Side == "SELL", 0),
		nullableFloat(row.Side == "SELL", 0),
	).Scan(&inserted)

	if err == nil {
		return inserted, nil
	}

	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}

	return false, err
}

func (r *TradeRepository) updateOpenPrices(ctx context.Context, tx *sql.Tx, assetID string, price float64) (int64, error) {
	if assetID == "" {
		return 0, nil
	}

	result, err := tx.ExecContext(ctx, `
		UPDATE polymarket_trades
		SET
			current_price = $1,
			current_value = remaining_size * $1,
			unrealized_pnl = (remaining_size * $1) - (remaining_size * price),
			unrealized_pnl_pct = CASE
				WHEN (remaining_size * price) = 0 THEN 0
				ELSE (((remaining_size * $1) - (remaining_size * price)) / (remaining_size * price)) * 100
			END,
			last_price_at = now(),
			last_checked_at = now(),
			updated_at = now()
		WHERE asset_id = $2
			AND status IN ('OPEN', 'PARTIAL_CLOSED')
			AND remaining_size > 0
	`, price, assetID)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}

func (r *TradeRepository) settleOpenTrades(ctx context.Context, tx *sql.Tx, assetID string, settlementPrice float64) (int64, float64, error) {
	if assetID == "" {
		return 0, 0, nil
	}

	var settled int64
	var settlementPNL float64
	err := tx.QueryRowContext(ctx, `
		WITH candidates AS (
			SELECT
				id,
				size,
				price,
				remaining_size,
				COALESCE(realized_pnl, 0) AS existing_realized_pnl
			FROM polymarket_trades
			WHERE asset_id = $2
				AND status IN ('OPEN', 'PARTIAL_CLOSED')
				AND remaining_size > 0
			FOR UPDATE
		),
		updated AS (
			UPDATE polymarket_trades AS t
			SET
				status = 'SETTLED',
				close_reason = 'SETTLED',
				current_price = $1,
				current_value = c.remaining_size * $1,
				unrealized_pnl = 0,
				unrealized_pnl_pct = 0,
				close_price = $1,
				close_value = c.remaining_size * $1,
				realized_pnl = c.existing_realized_pnl + ((c.remaining_size * $1) - (c.remaining_size * c.price)),
				realized_pnl_pct = CASE
					WHEN (c.size * c.price) = 0 THEN 0
					ELSE (c.existing_realized_pnl + ((c.remaining_size * $1) - (c.remaining_size * c.price))) / (c.size * c.price) * 100
				END,
				settlement_price = $1,
				settled_at = now(),
				remaining_size = 0,
				last_price_at = now(),
				last_checked_at = now(),
				updated_at = now()
			FROM candidates AS c
			WHERE t.id = c.id
			RETURNING ((c.remaining_size * $1) - (c.remaining_size * c.price)) AS settlement_pnl
		)
		SELECT COUNT(*), COALESCE(SUM(settlement_pnl), 0)
		FROM updated
	`, settlementPrice, assetID).Scan(&settled, &settlementPNL)
	if err != nil {
		return 0, 0, err
	}

	return settled, settlementPNL, nil
}

func (r *TradeRepository) closeBuyTrades(ctx context.Context, tx *sql.Tx, sell TradeRow) (int, float64, float64, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT
			id,
			price,
			remaining_size,
			realized_pnl,
			close_value,
			current_value
		FROM polymarket_trades
		WHERE wallet = $1
			AND asset_id = $2
			AND side = 'BUY'
			AND status IN ('OPEN', 'PARTIAL_CLOSED')
			AND remaining_size > 0
		ORDER BY trade_time ASC NULLS LAST, id ASC
		FOR UPDATE
	`, sell.Wallet, sell.AssetID)
	if err != nil {
		return 0, 0, sell.Size, err
	}

	var buys []openTrade

	for rows.Next() {
		open := openTrade{}
		if err := rows.Scan(&open.ID, &open.Price, &open.Remaining, &open.RealizedPNL, &open.CloseValue, &open.CurrentValue); err != nil {
			rows.Close()
			return 0, 0, sell.Size, err
		}
		buys = append(buys, open)
	}

	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, 0, sell.Size, err
	}
	rows.Close()

	remainingSell := sell.Size
	closed := 0
	totalRealized := 0.0

	for _, buy := range buys {
		if remainingSell <= 0 {
			break
		}

		closeSize := math.Min(buy.Remaining, remainingSell)
		realized := closeSize * (sell.Price - buy.Price)
		nextRemaining := buy.Remaining - closeSize
		nextStatus := "PARTIAL_CLOSED"
		if nextRemaining <= 0.000000000001 {
			nextRemaining = 0
			nextStatus = "CLOSED"
		}

		nextRealized := nullFloatValue(buy.RealizedPNL) + realized
		nextCloseValue := nullFloatValue(buy.CloseValue) + closeSize*sell.Price
		currentValue := nextRemaining * sell.Price
		unrealized := currentValue - nextRemaining*buy.Price

		_, err := tx.ExecContext(ctx, `
			UPDATE polymarket_trades
			SET
				status = $1,
				close_reason = 'SOLD',
				remaining_size = $2,
				current_price = $3,
				current_value = $4,
				unrealized_pnl = $5,
				unrealized_pnl_pct = CASE
					WHEN ($2 * price) = 0 THEN 0
					ELSE ($5 / ($2 * price)) * 100
				END,
				close_price = $3,
				close_value = $6,
				close_tx_hash = $7,
				realized_pnl = $8,
				realized_pnl_pct = CASE
					WHEN (size * price) = 0 THEN 0
					ELSE ($8 / (size * price)) * 100
				END,
				last_price_at = now(),
				last_checked_at = now(),
				updated_at = now()
			WHERE id = $9
		`, nextStatus, nextRemaining, sell.Price, currentValue, unrealized, nextCloseValue, sell.TxHash, nextRealized, buy.ID)
		if err != nil {
			return closed, totalRealized, remainingSell, err
		}

		closed++
		totalRealized += realized
		remainingSell -= closeSize
	}

	return closed, totalRealized, remainingSell, nil
}

func nullString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func nullTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}

func nullableString(valid bool, value string) any {
	if !valid || value == "" {
		return nil
	}
	return value
}

func nullableFloat(valid bool, value float64) any {
	if !valid {
		return nil
	}
	return value
}

func nullFloatValue(value sql.NullFloat64) float64 {
	if !value.Valid {
		return 0
	}
	return value.Float64
}
