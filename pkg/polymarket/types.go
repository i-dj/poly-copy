package polymarket

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Trade map[string]any

type PNL struct {
	Day   string
	Week  string
	Month string
	Year  string
	LTD   string
	All   string
}

type MonitorRow struct {
	Wallet      string
	Side        string
	Size        string
	Price       string
	Notional    string
	Outcome     string
	MarketTitle string
	TxHash      string
	PNL         PNL
	UpdateTime  time.Time
	Web         string
}

func (r MonitorRow) LogString() string {
	return fmt.Sprintf(
		"[%s] %s %s %s @ %s $%s | 1d=%s 1w=%s 1m=%s all=%s | %s | %s | %s",
		r.UpdateTime.Format("2006-01-02 15:04:05"),
		r.Wallet,
		r.Side,
		r.Size,
		r.Price,
		r.Notional,
		r.PNL.Day,
		r.PNL.Week,
		r.PNL.Month,
		r.PNL.All,
		r.Outcome,
		r.MarketTitle,
		r.Web,
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
