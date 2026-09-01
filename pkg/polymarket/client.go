package polymarket

import (
	"context"
	"encoding/json"
	"math"
	"strconv"
	"strings"
)

type Client struct {
	cfg Config
}

func NewClient(cfg Config) *Client {
	return &Client{cfg: cfg}
}

func (c *Client) NormalizeTrade(ctx context.Context, trade Trade) (TradeRow, error) {
	_ = ctx

	wallet := firstString(trade, "proxyWallet", "wallet", "maker", "taker")
	assetID := firstString(trade, "asset", "assetId", "asset_id", "token_id", "tokenID")
	price := SafeFloat(trade["price"])
	size := SafeFloat(trade["size"])
	rawTrade, err := json.Marshal(trade)
	if err != nil {
		return TradeRow{}, err
	}

	return TradeRow{
		TradeKey:    TradeKey(trade),
		TxHash:      firstString(trade, "transactionHash", "tx_hash", "txHash"),
		Wallet:      wallet,
		AssetID:     assetID,
		ConditionID: firstString(trade, "conditionId", "condition_id", "market", "marketId"),
		Side:        strings.ToUpper(firstString(trade, "side")),
		Outcome:     firstString(trade, "outcome"),
		MarketTitle: firstString(trade, "title", "market_title", "marketTitle"),
		Size:        size,
		Price:       price,
		Notional:    math.Round(price*size*1_000_000) / 1_000_000,
		TradeTime:   parseTradeTime(trade["timestamp"]),
		RawTrade:    rawTrade,
	}, nil
}

func formatFloat(value float64, places int) string {
	base := math.Pow10(places)
	return strconv.FormatFloat(math.Round(value*base)/base, 'f', -1, 64)
}
