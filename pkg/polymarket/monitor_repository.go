package polymarket

import (
	"context"
	"database/sql"
)

type MonitorRepository struct {
	db *sql.DB
}

type MonitorUpsertResult string

const (
	MonitorInserted MonitorUpsertResult = "insert"
	MonitorUpdated  MonitorUpsertResult = "update"
)

func NewMonitorRepository(db *sql.DB) *MonitorRepository {
	return &MonitorRepository{db: db}
}

func (r *MonitorRepository) Upsert(ctx context.Context, row MonitorRow) (MonitorUpsertResult, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()

	var id int64
	var tradeCount sql.NullInt64
	err = tx.QueryRowContext(ctx, `
		SELECT id, trade_count
		FROM monitor
		WHERE wallet = $1
		ORDER BY id ASC
		LIMIT 1
		FOR UPDATE
	`, row.Wallet).Scan(&id, &tradeCount)

	if err == nil {
		nextTradeCount := int64(1)
		if tradeCount.Valid {
			nextTradeCount = tradeCount.Int64 + 1
		}

		_, err = tx.ExecContext(ctx, `
			UPDATE monitor
			SET
				side = $1,
				size = $2,
				price = $3,
				notional = $4,
				outcome = $5,
				market_title = $6,
				tx_hash = $7,
				"1d" = COALESCE(NULLIF($8, ''), "1d"),
				"1w" = COALESCE(NULLIF($9, ''), "1w"),
				"1m" = COALESCE(NULLIF($10, ''), "1m"),
				"1y" = COALESCE(NULLIF($11, ''), "1y"),
				"ltd" = COALESCE(NULLIF($12, ''), "ltd"),
				"all" = COALESCE(NULLIF($13, ''), "all"),
				update_time = $14,
				web = $15,
				trade_count = $16
			WHERE id = $17
		`,
			row.Side,
			row.Size,
			row.Price,
			row.Notional,
			row.Outcome,
			row.MarketTitle,
			row.TxHash,
			row.PNL.Day,
			row.PNL.Week,
			row.PNL.Month,
			row.PNL.Year,
			row.PNL.LTD,
			row.PNL.All,
			row.UpdateTime,
			row.Web,
			nextTradeCount,
			id,
		)
		if err != nil {
			return "", err
		}

		return MonitorUpdated, tx.Commit()
	}

	if err != sql.ErrNoRows {
		return "", err
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO monitor (
			wallet,
			side,
			size,
			price,
			notional,
			outcome,
			market_title,
			tx_hash,
			"1d",
			"1w",
			"1m",
			"1y",
			"ltd",
			"all",
			update_time,
			web,
			trade_count
		)
		VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
		)
	`,
		row.Wallet,
		row.Side,
		row.Size,
		row.Price,
		row.Notional,
		row.Outcome,
		row.MarketTitle,
		row.TxHash,
		row.PNL.Day,
		row.PNL.Week,
		row.PNL.Month,
		row.PNL.Year,
		row.PNL.LTD,
		row.PNL.All,
		row.UpdateTime,
		row.Web,
		1,
	)
	if err != nil {
		return "", err
	}

	return MonitorInserted, tx.Commit()
}
