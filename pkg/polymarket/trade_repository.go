package polymarket

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type TradeRepository struct {
	db *pgxpool.Pool
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

func NewTradeRepository(db *pgxpool.Pool) *TradeRepository {
	return &TradeRepository{db: db}
}

func (r *TradeRepository) ProcessTrade(ctx context.Context, row TradeRow) (TradeProcessResult, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return TradeProcessResult{}, err
	}
	defer tx.Rollback(ctx)

	inserted, err := r.insertTrade(ctx, tx, row)
	if err != nil {
		return TradeProcessResult{}, err
	}

	result := TradeProcessResult{
		Inserted: inserted,
	}
	if !inserted {
		if err := tx.Commit(ctx); err != nil {
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

	if err := tx.Commit(ctx); err != nil {
		return TradeProcessResult{}, err
	}

	return result, nil
}

func (r *TradeRepository) ListOpenMarkets(ctx context.Context, staleBefore time.Time, limit int) ([]OpenMarket, error) {
	if limit <= 0 {
		limit = 100
	}

	rows, err := r.db.Query(ctx, `
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

	_, err := r.db.Exec(ctx, `
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
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return SettlementResult{}, err
	}
	defer tx.Rollback(ctx)

	settled, pnl, err := r.settleOpenTrades(ctx, tx, assetID, settlementPrice)
	if err != nil {
		return SettlementResult{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return SettlementResult{}, err
	}

	return SettlementResult{Settled: settled, PNL: pnl}, nil
}

func (r *TradeRepository) ListTopCopyWalletCandidates(ctx context.Context, limit int) ([]CopyWalletCandidate, error) {
	if limit <= 0 {
		limit = 5
	}

	rows, err := r.db.Query(ctx, `
		SELECT
			wallet,
			COUNT(*) AS closed_count,
			ROUND(SUM(size * price), 6) AS total_cost,
			ROUND(SUM(realized_pnl), 6) AS net_pnl,
			ROUND(SUM(realized_pnl) / NULLIF(SUM(size * price), 0) * 100, 2) AS roi_pct,
			ROUND(SUM(realized_pnl) / NULLIF(SUM(size * price), 0), 6) AS profit_per_usdc,
			ROUND(
				COUNT(*) FILTER (WHERE realized_pnl > 0)::numeric / NULLIF(COUNT(*), 0) * 100,
				2
			) AS win_rate_pct
		FROM polymarket_trades
		WHERE status IN ('CLOSED', 'SETTLED')
			AND wallet IS NOT NULL
			AND wallet <> ''
		GROUP BY wallet
		HAVING COUNT(*) >= 10
			AND SUM(size * price) >= 20
			AND SUM(realized_pnl) > 0
		ORDER BY profit_per_usdc DESC, net_pnl DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var candidates []CopyWalletCandidate
	for rows.Next() {
		var candidate CopyWalletCandidate
		if err := rows.Scan(
			&candidate.Wallet,
			&candidate.ClosedCount,
			&candidate.TotalCost,
			&candidate.NetPNL,
			&candidate.ROIPct,
			&candidate.ProfitPerUSDC,
			&candidate.WinRatePct,
		); err != nil {
			return nil, err
		}
		candidates = append(candidates, candidate)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return candidates, nil
}

func (r *TradeRepository) InsertCopyWalletIfMissing(ctx context.Context, wallet string) (bool, error) {
	if wallet == "" {
		return false, nil
	}

	result, err := r.db.Exec(ctx, `
		INSERT INTO copy_wallet (wallet, enable, last_copy_time)
		SELECT $1, 1, NULL
		WHERE NOT EXISTS (
			SELECT 1
			FROM copy_wallet
			WHERE wallet = $1
		)
	`, wallet)
	if err != nil {
		return false, err
	}

	return result.RowsAffected() > 0, nil
}

func (r *TradeRepository) SyncCopyWalletSequence(ctx context.Context) error {
	_, err := r.db.Exec(ctx, `
		SELECT setval(
			pg_get_serial_sequence('copy_wallet', 'id'),
			COALESCE((SELECT MAX(id) FROM copy_wallet), 0) + 1,
			false
		)
	`)
	return err
}

func (r *TradeRepository) ListEnabledCopyWallets(ctx context.Context) ([]CopyWallet, error) {
	rows, err := r.db.Query(ctx, `
		SELECT wallet, last_copy_time
		FROM copy_wallet
		WHERE enable = 1
			AND wallet IS NOT NULL
			AND wallet <> ''
		ORDER BY id ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var wallets []CopyWallet
	for rows.Next() {
		var wallet CopyWallet
		if err := rows.Scan(&wallet.Wallet, &wallet.LastCopyTime); err != nil {
			return nil, err
		}
		wallets = append(wallets, wallet)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return wallets, nil
}

func (r *TradeRepository) MarkCopyWalletChecked(ctx context.Context, wallet string) error {
	if wallet == "" {
		return nil
	}

	_, err := r.db.Exec(ctx, `
		UPDATE copy_wallet
		SET last_copy_time = now()
		WHERE wallet = $1
	`, wallet)
	return err
}

func (r *TradeRepository) GetAvailableCopyPosition(ctx context.Context, assetID string) (float64, error) {
	rows, err := r.db.Query(ctx, `
		SELECT copy_side, copy_size, matched_size, dry_run, submit_success
		FROM copy_orders
		WHERE asset_id = $1
			AND COALESCE(is_cancelled, false) = false
			AND (
				dry_run = true
				OR submit_success = true
			)
	`, assetID)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	qty := 0.0
	for rows.Next() {
		var side string
		var copySize float64
		var matchedSize sql.NullFloat64
		var dryRun bool
		var submitSuccess bool

		if err := rows.Scan(&side, &copySize, &matchedSize, &dryRun, &submitSuccess); err != nil {
			return 0, err
		}

		size := nullFloatValue(matchedSize)
		if dryRun {
			size = copySize
		}
		if !dryRun && !submitSuccess {
			continue
		}

		switch strings.ToUpper(side) {
		case "BUY":
			qty += size
		case "SELL":
			qty -= size
		}
	}

	if err := rows.Err(); err != nil {
		return 0, err
	}

	if qty < 0 {
		return 0, nil
	}
	return roundFloat(qty, 6), nil
}

func (r *TradeRepository) InsertCopyOrderIfMissing(ctx context.Context, row CopyOrderInsert) (int64, error) {
	var id int64
	err := r.db.QueryRow(ctx, `
		INSERT INTO copy_orders (
			source_wallet,
			source_tx_hash,
			source_side,
			source_size,
			source_price,
			source_notional,
			source_timestamp,
			market_title,
			condition_id,
			asset_id,
			outcome,
			market_url,
			copy_side,
			copy_size,
			copy_price,
			copy_notional,
			order_type,
			dry_run,
			submit_success,
			skip_reason,
			order_status,
			raw_response,
			detected_at,
			updated_at
		)
		SELECT
			$1, $2, $3, $4, $5, $6, $7, $8,
			$9, $10, $11, $12, $13, $14, $15, $16,
			$17, $18, $19, $20, $21, $22::jsonb,
			now(), now()
		WHERE NOT EXISTS (
			SELECT 1
			FROM copy_orders
			WHERE source_wallet = $1
				AND source_tx_hash = $2
				AND asset_id = $10
				AND source_side = $3
				AND source_size = $4
				AND source_price = $5
		)
		RETURNING id
	`,
		row.SourceWallet,
		row.SourceTxHash,
		row.SourceSide,
		row.SourceSize,
		row.SourcePrice,
		row.SourceNotional,
		nullTime(row.SourceTimestamp),
		nullString(row.MarketTitle),
		nullString(row.ConditionID),
		row.AssetID,
		nullString(row.Outcome),
		nullString(row.MarketURL),
		row.CopySide,
		row.CopySize,
		row.CopyPrice,
		row.CopyNotional,
		row.OrderType,
		row.DryRun,
		row.SubmitSuccess,
		row.SkipReason,
		row.OrderStatus,
		string(row.RawResponse),
	).Scan(&id)
	if err == nil {
		return id, nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return 0, err
}

func (r *TradeRepository) MarkPaperCopyOrderSuccess(ctx context.Context, id int64) error {
	_, err := r.db.Exec(ctx, `
		UPDATE copy_orders
		SET
			submit_success = true,
			matched_size = copy_size,
			filled_notional = copy_notional,
			is_filled = true,
			submitted_at = now(),
			updated_at = now()
		WHERE id = $1
	`, id)
	return err
}

func (r *TradeRepository) UpdateCopyOrderSuccess(ctx context.Context, id int64, rawResponse []byte) error {
	var resp map[string]any
	if err := json.Unmarshal(rawResponse, &resp); err != nil {
		resp = map[string]any{}
	}

	orderID := firstResponseString(resp, "orderID", "orderId", "id")
	orderStatus := firstResponseString(resp, "status", "order_status")
	if orderStatus == "" {
		orderStatus = "SUBMITTED"
	}
	matchedSize := firstResponseFloat(resp, "size_matched", "matchedSize")
	filledNotional := firstResponseFloat(resp, "takingAmount", "makingAmount")

	_, err := r.db.Exec(ctx, `
		UPDATE copy_orders
		SET
			submit_success = true,
			order_id = $1,
			order_status = $2,
			raw_response = $3::jsonb,
			matched_size = $4,
			filled_notional = $5,
			is_filled = CASE WHEN $4 > 0 THEN true ELSE false END,
			submitted_at = now(),
			updated_at = now()
		WHERE id = $6
	`, nullableString(true, orderID), orderStatus, string(rawResponse), matchedSize, filledNotional, id)
	return err
}

func (r *TradeRepository) UpdateCopyOrderError(ctx context.Context, id int64, orderErr error) error {
	raw, _ := json.Marshal(map[string]any{
		"error": orderErr.Error(),
		"type":  fmt.Sprintf("%T", orderErr),
	})

	_, err := r.db.Exec(ctx, `
		UPDATE copy_orders
		SET
			submit_success = false,
			order_status = 'FAILED',
			error_message = $1,
			raw_response = $2::jsonb,
			updated_at = now()
		WHERE id = $3
	`, orderErr.Error(), string(raw), id)
	return err
}

func (r *TradeRepository) ListCopyOrdersForSync(ctx context.Context, staleAfter time.Duration, limit int) ([]CopyOrderRow, error) {
	if staleAfter <= 0 {
		staleAfter = time.Minute
	}
	if limit <= 0 {
		limit = 100
	}

	rows, err := r.db.Query(ctx, `
		SELECT
			id,
			asset_id,
			copy_side,
			copy_size,
			copy_price,
			order_id,
			dry_run,
			submit_success,
			order_status,
			matched_size
		FROM copy_orders
		WHERE skip_reason IS NULL
			AND COALESCE(is_cancelled, false) = false
			AND (
				pnl_checked_at IS NULL
				OR pnl_checked_at < now() - ($1::text)::interval
			)
		ORDER BY id DESC
		LIMIT $2
	`, fmt.Sprintf("%f seconds", staleAfter.Seconds()), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var orders []CopyOrderRow
	for rows.Next() {
		var order CopyOrderRow
		if err := rows.Scan(
			&order.ID,
			&order.AssetID,
			&order.CopySide,
			&order.CopySize,
			&order.CopyPrice,
			&order.OrderID,
			&order.DryRun,
			&order.SubmitSuccess,
			&order.OrderStatus,
			&order.MatchedSize,
		); err != nil {
			return nil, err
		}
		orders = append(orders, order)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return orders, nil
}

func (r *TradeRepository) UpdateCopyOrderPNL(ctx context.Context, id int64, orderStatus string, matchedSize float64, copyPrice float64, isFilled bool, isCancelled bool, totalPNL float64) error {
	_, err := r.db.Exec(ctx, `
		UPDATE copy_orders
		SET
			order_status = $1,
			matched_size = $2,
			filled_notional = $3,
			is_filled = $4,
			is_cancelled = $5,
			unrealized_pnl = $6,
			total_pnl = $6,
			pnl_checked_at = now(),
			updated_at = now()
		WHERE id = $7
	`, orderStatus, matchedSize, roundFloat(matchedSize*copyPrice, 6), isFilled, isCancelled, roundFloat(totalPNL, 6), id)
	return err
}

func (r *TradeRepository) insertTrade(ctx context.Context, tx pgx.Tx, row TradeRow) (bool, error) {
	status := "OPEN"
	closeReason := sql.NullString{}
	remainingSize := row.Size

	if row.Side == "SELL" {
		status = "CLOSED"
		closeReason = sql.NullString{String: "SOLD", Valid: true}
		remainingSize = 0
	}

	var inserted bool
	err := tx.QueryRow(ctx, `
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

	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}

	return false, err
}

func (r *TradeRepository) updateOpenPrices(ctx context.Context, tx pgx.Tx, assetID string, price float64) (int64, error) {
	if assetID == "" {
		return 0, nil
	}

	result, err := tx.Exec(ctx, `
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

	return result.RowsAffected(), nil
}

func (r *TradeRepository) settleOpenTrades(ctx context.Context, tx pgx.Tx, assetID string, settlementPrice float64) (int64, float64, error) {
	if assetID == "" {
		return 0, 0, nil
	}

	var settled int64
	var settlementPNL float64
	err := tx.QueryRow(ctx, `
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

func (r *TradeRepository) closeBuyTrades(ctx context.Context, tx pgx.Tx, sell TradeRow) (int, float64, float64, error) {
	rows, err := tx.Query(ctx, `
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

		_, err := tx.Exec(ctx, `
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

func firstResponseString(resp map[string]any, keys ...string) string {
	for _, key := range keys {
		value := strings.TrimSpace(stringValue(resp[key]))
		if value != "" {
			return value
		}
	}
	return ""
}

func firstResponseFloat(resp map[string]any, keys ...string) float64 {
	for _, key := range keys {
		value := SafeFloat(resp[key])
		if value != 0 {
			return value
		}
	}
	return 0
}
