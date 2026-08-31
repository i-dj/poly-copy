package polymarket

import (
	"context"
	"encoding/json"
	"log"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Client struct {
	cfg    Config
	http   *http.Client
	mu     sync.Mutex
	cached map[string]cachedPNL
}

type cachedPNL struct {
	at  time.Time
	pnl PNL
}

func NewClient(cfg Config) *Client {
	return &Client{
		cfg: cfg,
		http: &http.Client{
			Timeout: cfg.PNLTimeout,
		},
		cached: make(map[string]cachedPNL),
	}
}

func (c *Client) WalletPNL(ctx context.Context, wallet string) PNL {
	wallet = strings.TrimSpace(wallet)
	if wallet == "" {
		return PNL{}
	}

	cacheKey := strings.ToLower(wallet)

	c.mu.Lock()
	if cached, ok := c.cached[cacheKey]; ok && time.Since(cached.at) < c.cfg.PNLCacheTTL {
		c.mu.Unlock()
		log.Printf("使用 PnL 缓存: wallet=%s", wallet)
		return cached.pnl
	}
	c.mu.Unlock()

	log.Printf("查询钱包 PnL: wallet=%s", wallet)
	pnl := PNL{
		Day:   c.LeaderboardPNL(ctx, wallet, "DAY"),
		Week:  c.LeaderboardPNL(ctx, wallet, "WEEK"),
		Month: c.LeaderboardPNL(ctx, wallet, "MONTH"),
		All:   c.LeaderboardPNL(ctx, wallet, "ALL"),
	}

	c.mu.Lock()
	c.cached[cacheKey] = cachedPNL{at: time.Now(), pnl: pnl}
	c.mu.Unlock()

	return pnl
}

func (c *Client) LeaderboardPNL(ctx context.Context, wallet, timePeriod string) string {
	endpoint, err := url.Parse(c.cfg.LeaderboardURL)
	if err != nil {
		log.Printf("leaderboard URL 解析失败: %v", err)
		return ""
	}

	params := endpoint.Query()
	params.Set("user", wallet)
	params.Set("timePeriod", timePeriod)
	params.Set("orderBy", "PNL")
	params.Set("category", "OVERALL")
	params.Set("limit", "1")
	params.Set("offset", "0")
	endpoint.RawQuery = params.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		log.Printf("创建 leaderboard 请求失败: wallet=%s period=%s err=%v", wallet, timePeriod, err)
		return ""
	}

	resp, err := c.http.Do(req)
	if err != nil {
		log.Printf("请求 leaderboard 失败: wallet=%s period=%s err=%v", wallet, timePeriod, err)
		return ""
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		log.Printf("leaderboard 返回异常状态: wallet=%s period=%s status=%s", wallet, timePeriod, resp.Status)
		return ""
	}

	var rows []struct {
		PNL *float64 `json:"pnl"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		log.Printf("解析 leaderboard 响应失败: wallet=%s period=%s err=%v", wallet, timePeriod, err)
		return ""
	}

	if len(rows) == 0 || rows[0].PNL == nil {
		log.Printf("leaderboard 无 PnL 数据: wallet=%s period=%s", wallet, timePeriod)
		return ""
	}

	return formatMoney(*rows[0].PNL)
}

func (c *Client) NormalizeTrade(ctx context.Context, trade Trade) MonitorRow {
	wallet := stringValue(trade["proxyWallet"])
	price := SafeFloat(trade["price"])
	size := SafeFloat(trade["size"])

	web := ""
	if wallet != "" {
		web = "https://polymarket.com/profile/" + wallet
	}

	return MonitorRow{
		Wallet:      wallet,
		Side:        stringValue(trade["side"]),
		Size:        stringValue(trade["size"]),
		Price:       stringValue(trade["price"]),
		Notional:    formatFloat(price*size, 6),
		Outcome:     stringValue(trade["outcome"]),
		MarketTitle: stringValue(trade["title"]),
		TxHash:      stringValue(trade["transactionHash"]),
		PNL:         c.WalletPNL(ctx, wallet),
		UpdateTime:  time.Now(),
		Web:         web,
	}
}

func formatMoney(value float64) string {
	rounded := math.Round(value*100) / 100
	return strconv.FormatFloat(rounded, 'f', -1, 64)
}

func formatFloat(value float64, places int) string {
	base := math.Pow10(places)
	return strconv.FormatFloat(math.Round(value*base)/base, 'f', -1, 64)
}
