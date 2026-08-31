package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

var ErrCfgNotFound = errors.New("cfg not found")

type QueryRower interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func GetCfgName(ctx context.Context, db QueryRower, cfgKey string) (string, error) {
	cfgKey = strings.TrimSpace(cfgKey)
	if cfgKey == "" {
		return "", errors.New("cfg_key is required")
	}

	var cfgName string
	err := db.QueryRowContext(ctx, `
		SELECT cfg_name
		FROM cfg
		WHERE cfg_key = $1
		LIMIT 1
	`, cfgKey).Scan(&cfgName)
	if err == nil {
		return cfgName, nil
	}

	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("%w: %s", ErrCfgNotFound, cfgKey)
	}

	return "", err
}
