package polymarket

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type MarketResolution struct {
	Resolved        bool
	SettlementPrice float64
	Closed          bool
}

type gammaMarket struct {
	ConditionID   string `json:"conditionId"`
	Closed        bool   `json:"closed"`
	ClobTokenIDs  any    `json:"clobTokenIds"`
	OutcomePrices any    `json:"outcomePrices"`
}

func StartSettlementScanner(ctx context.Context, db *pgxpool.Pool, cfg Config, interval time.Duration) {
	if interval <= 0 {
		interval = time.Minute
	}

	cfg.SettlementInterval = interval
	client := NewClient(cfg)
	repo := NewTradeRepository(db)

	log.Printf("结算扫描间隔：%s", interval)
	scanSettlements(ctx, client, repo)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			scanSettlements(ctx, client, repo)
		}
	}
}

func scanSettlements(ctx context.Context, client *Client, repo *TradeRepository) {
	interval := client.cfg.SettlementInterval
	if interval <= 0 {
		interval = time.Minute
	}
	markets, err := repo.ListOpenMarkets(ctx, time.Now().Add(-interval), client.cfg.SettlementBatchSize)
	if err != nil {
		log.Printf("结算扫描失败：读取未完成记录失败 err=%v", err)
		return
	}

	if len(markets) == 0 {
		return
	}

	for _, market := range markets {
		if ctx.Err() != nil {
			return
		}

		resolution, err := client.ResolveMarketAsset(ctx, market.ConditionID, market.AssetID)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("结算扫描失败：condition_id=%s asset_id=%s err=%v", shortID(market.ConditionID), shortID(market.AssetID), err)
			continue
		}

		if !resolution.Resolved {
			if err := repo.MarkSettlementChecked(ctx, market.AssetID); err != nil {
				log.Printf("结算扫描提醒：更新检查时间失败 asset_id=%s err=%v", shortID(market.AssetID), err)
			}
			continue
		}

		result, err := repo.SettleAsset(ctx, market.AssetID, resolution.SettlementPrice)
		if err != nil {
			log.Printf("结算入库失败：asset_id=%s err=%v", shortID(market.AssetID), err)
			continue
		}

		if result.Settled == 0 {
			continue
		}

		logKeyEvent(
			pnlTitle("结算", result.PNL),
			"持仓=%d 笔，结算价=%s，盈亏=%s | asset=%s condition=%s",
			result.Settled,
			formatFloat(resolution.SettlementPrice, 6),
			formatSignedFloat(result.PNL, 6),
			shortID(market.AssetID),
			shortID(market.ConditionID),
		)
	}
}

func (c *Client) ResolveMarketAsset(ctx context.Context, conditionID, assetID string) (MarketResolution, error) {
	markets, err := c.fetchGammaMarkets(ctx, conditionID)
	if err != nil {
		return MarketResolution{}, err
	}

	for _, market := range markets {
		if !strings.EqualFold(market.ConditionID, conditionID) {
			continue
		}

		tokenIDs, err := parseStringList(market.ClobTokenIDs)
		if err != nil {
			return MarketResolution{}, fmt.Errorf("解析 clobTokenIds 失败: %w", err)
		}

		prices, err := parseFloatList(market.OutcomePrices)
		if err != nil {
			return MarketResolution{}, fmt.Errorf("解析 outcomePrices 失败: %w", err)
		}

		for i, tokenID := range tokenIDs {
			if tokenID != assetID {
				continue
			}
			if i >= len(prices) {
				return MarketResolution{}, errors.New("outcomePrices 数量少于 clobTokenIds")
			}

			price := prices[i]
			return MarketResolution{
				Resolved:        market.Closed && (price == 0 || price == 1),
				SettlementPrice: price,
				Closed:          market.Closed,
			}, nil
		}
	}

	return MarketResolution{}, nil
}

func (c *Client) fetchGammaMarkets(ctx context.Context, conditionID string) ([]gammaMarket, error) {
	endpoint, err := url.Parse(c.cfg.GammaMarketsURL)
	if err != nil {
		return nil, err
	}

	values := endpoint.Query()
	values.Set("condition_ids", conditionID)
	values.Set("closed", "true")
	endpoint.RawQuery = values.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("Gamma API 状态异常: %s", resp.Status)
	}

	var markets []gammaMarket
	if err := json.NewDecoder(resp.Body).Decode(&markets); err != nil {
		return nil, err
	}

	return markets, nil
}

func parseStringList(value any) ([]string, error) {
	switch v := value.(type) {
	case nil:
		return nil, nil
	case []string:
		return v, nil
	case []any:
		items := make([]string, 0, len(v))
		for _, item := range v {
			items = append(items, strings.TrimSpace(fmt.Sprint(item)))
		}
		return items, nil
	case string:
		text := strings.TrimSpace(v)
		if text == "" {
			return nil, nil
		}
		var items []string
		if err := json.Unmarshal([]byte(text), &items); err == nil {
			return items, nil
		}
		var anyItems []any
		if err := json.Unmarshal([]byte(text), &anyItems); err != nil {
			return nil, err
		}
		return parseStringList(anyItems)
	default:
		return nil, fmt.Errorf("不支持的列表类型 %T", value)
	}
}

func parseFloatList(value any) ([]float64, error) {
	switch v := value.(type) {
	case nil:
		return nil, nil
	case []float64:
		return v, nil
	case []any:
		items := make([]float64, 0, len(v))
		for _, item := range v {
			items = append(items, SafeFloat(item))
		}
		return items, nil
	case string:
		text := strings.TrimSpace(v)
		if text == "" {
			return nil, nil
		}
		var items []float64
		if err := json.Unmarshal([]byte(text), &items); err == nil {
			return items, nil
		}
		var stringItems []string
		if err := json.Unmarshal([]byte(text), &stringItems); err == nil {
			items := make([]float64, 0, len(stringItems))
			for _, item := range stringItems {
				n, err := strconv.ParseFloat(strings.TrimSpace(item), 64)
				if err != nil {
					return nil, err
				}
				items = append(items, n)
			}
			return items, nil
		}
		return nil, errors.New("无法解析数字列表")
	default:
		return nil, fmt.Errorf("不支持的列表类型 %T", value)
	}
}

func shortID(value string) string {
	if len(value) <= 14 {
		return value
	}
	return value[:8] + "..." + value[len(value)-6:]
}
