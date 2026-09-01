package polymarket

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Trade map[string]any

type TradeRow struct {
	TradeKey    string
	TxHash      string
	Wallet      string
	AssetID     string
	ConditionID string
	Side        string
	Outcome     string
	MarketTitle string
	Size        float64
	Price       float64
	Notional    float64
	TradeTime   time.Time
	RawTrade    []byte
}

func (r TradeRow) LogString() string {
	return fmt.Sprintf(
		"[%s] wallet=%s side=%s size=%s price=%s notional=%s asset=%s outcome=%s market=%s tx=%s",
		r.TradeTime.Format("2006-01-02 15:04:05"),
		r.Wallet,
		r.Side,
		formatFloat(r.Size, 12),
		formatFloat(r.Price, 12),
		formatFloat(r.Notional, 12),
		r.AssetID,
		r.Outcome,
		r.MarketTitle,
		r.TxHash,
	)
}

func TradeKey(trade Trade) string {
	keys := []string{
		"transactionHash",
		"proxyWallet",
		"asset",
		"side",
		"size",
		"price",
		"timestamp",
	}

	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, stringValue(trade[key]))
	}

	return strings.Join(parts, "|")
}

func SafeFloat(value any) float64 {
	switch v := value.(type) {
	case nil:
		return 0
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case json.Number:
		n, _ := v.Float64()
		return n
	case string:
		n, _ := strconv.ParseFloat(strings.TrimSpace(v), 64)
		return n
	default:
		n, _ := strconv.ParseFloat(strings.TrimSpace(fmt.Sprint(v)), 64)
		return n
	}
}

func stringValue(value any) string {
	if value == nil {
		return ""
	}
	return fmt.Sprint(value)
}

func firstString(trade Trade, keys ...string) string {
	for _, key := range keys {
		value := strings.TrimSpace(stringValue(trade[key]))
		if value != "" {
			return value
		}
	}
	return ""
}

func parseTradeTime(value any) time.Time {
	if value == nil {
		return time.Now()
	}

	switch v := value.(type) {
	case float64:
		return unixTime(v)
	case int64:
		return unixTime(float64(v))
	case int:
		return unixTime(float64(v))
	case json.Number:
		n, err := v.Float64()
		if err == nil {
			return unixTime(n)
		}
	case string:
		text := strings.TrimSpace(v)
		if text == "" {
			return time.Now()
		}
		if n, err := strconv.ParseFloat(text, 64); err == nil {
			return unixTime(n)
		}
		if t, err := time.Parse(time.RFC3339, text); err == nil {
			return t
		}
	}

	return time.Now()
}

func unixTime(value float64) time.Time {
	if value > 1_000_000_000_000 {
		return time.UnixMilli(int64(value))
	}
	return time.Unix(int64(value), 0)
}
