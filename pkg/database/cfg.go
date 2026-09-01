package database

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var ErrCfgNotFound = errors.New("cfg not found")

type QueryRower interface {
	QueryRow(ctx context.Context, query string, args ...any) pgx.Row
}

type Execer interface {
	Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error)
}

func GetCfgName(ctx context.Context, db QueryRower, cfgKey string) (string, error) {
	cfgKey = strings.TrimSpace(cfgKey)
	if cfgKey == "" {
		return "", errors.New("cfg_key is required")
	}

	var cfgName string
	err := db.QueryRow(ctx, `
		SELECT cfg_name
		FROM cfg
		WHERE cfg_key = $1
		LIMIT 1
	`, cfgKey).Scan(&cfgName)
	if err == nil {
		return cfgName, nil
	}

	if errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("%w: %s", ErrCfgNotFound, cfgKey)
	}

	return "", err
}

func UpsertCfgName(ctx context.Context, db Execer, cfgKey string, cfgName string) error {
	cfgKey = strings.TrimSpace(cfgKey)
	if cfgKey == "" {
		return errors.New("cfg_key is required")
	}

	_, err := db.Exec(ctx, `
		WITH updated AS (
			UPDATE cfg
			SET cfg_name = $2
			WHERE cfg_key = $1
			RETURNING 1
		)
		INSERT INTO cfg (cfg_key, cfg_name)
		SELECT $1, $2
		WHERE NOT EXISTS (SELECT 1 FROM updated)
	`, cfgKey, cfgName)
	return err
}
