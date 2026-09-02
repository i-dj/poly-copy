package polymarket

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"net/url"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type CopyTrader struct {
	cfg        Config
	httpClient *http.Client
	seen       map[string]struct{}
	lastSync   time.Time
}

type CopyWallet struct {
	Wallet            string
	LastCopyTime      sql.NullTime
	LastSeenTradeTime sql.NullTime
}

type CopyOrderRow struct {
	ID            int64
	AssetID       string
	CopySide      string
	CopySize      float64
	CopyPrice     float64
	OrderID       sql.NullString
	DryRun        bool
	SubmitSuccess bool
	OrderStatus   sql.NullString
	MatchedSize   sql.NullFloat64
}

type CopyOrderInsert struct {
	SourceWallet    string
	SourceTxHash    string
	SourceSide      string
	SourceSize      float64
	SourcePrice     float64
	SourceNotional  float64
	SourceTimestamp time.Time
	MarketTitle     string
	ConditionID     string
	AssetID         string
	Outcome         string
	MarketURL       string
	CopySide        string
	CopySize        float64
	CopyPrice       float64
	CopyNotional    float64
	OrderType       string
	DryRun          bool
	SubmitSuccess   bool
	SkipReason      sql.NullString
	OrderStatus     string
	RawResponse     []byte
}

type liveOrderRequest struct {
	Host          string  `json:"host"`
	ChainID       int     `json:"chain_id"`
	PrivateKey    string  `json:"private_key"`
	FunderAddress string  `json:"funder_address"`
	APIKey        string  `json:"api_key,omitempty"`
	APISecret     string  `json:"api_secret,omitempty"`
	APIPassphrase string  `json:"api_passphrase,omitempty"`
	TokenID       string  `json:"token_id"`
	Side          string  `json:"side"`
	Price         float64 `json:"price"`
	Size          float64 `json:"size"`
	OrderType     string  `json:"order_type"`
}

type walletBalanceRequest struct {
	Host          string `json:"host"`
	ChainID       int    `json:"chain_id"`
	PrivateKey    string `json:"private_key"`
	FunderAddress string `json:"funder_address"`
	APIKey        string `json:"api_key,omitempty"`
	APISecret     string `json:"api_secret,omitempty"`
	APIPassphrase string `json:"api_passphrase,omitempty"`
}

type orderStatusRequest struct {
	Host          string `json:"host"`
	ChainID       int    `json:"chain_id"`
	PrivateKey    string `json:"private_key"`
	FunderAddress string `json:"funder_address"`
	APIKey        string `json:"api_key,omitempty"`
	APISecret     string `json:"api_secret,omitempty"`
	APIPassphrase string `json:"api_passphrase,omitempty"`
	OrderID       string `json:"order_id"`
}

type liveOrderStatus struct {
	Status      string
	MatchedSize float64
}

type orderBookMeta struct {
	MinOrderSize float64
	TickSize     float64
}

const walletBalanceLogInterval = 10 * time.Second

func StartCopyTrader(ctx context.Context, db *pgxpool.Pool, cfg Config, pollInterval, syncInterval time.Duration) {
	if pollInterval <= 0 {
		pollInterval = 3 * time.Second
	}
	if syncInterval <= 0 {
		syncInterval = time.Minute
	}
	if cfg.CopyMode == "" {
		cfg.CopyMode = "paper"
	}
	if cfg.CopyUSDC <= 0 {
		cfg.CopyUSDC = 1
	}
	if cfg.MinCopyUSDC <= 0 {
		cfg.MinCopyUSDC = 5
	}
	if cfg.CopyTradeLimit <= 0 {
		cfg.CopyTradeLimit = 10
	}
	if cfg.CopyPriceOffset <= 0 {
		cfg.CopyPriceOffset = 0.01
	}
	if cfg.MinCopyPrice <= 0 {
		cfg.MinCopyPrice = 0.001
	}
	if cfg.MaxCopyPrice <= 0 {
		cfg.MaxCopyPrice = 0.999
	}
	if cfg.PythonBin == "" {
		cfg.PythonBin = "python3"
	}
	if cfg.LiveOrderScript == "" {
		cfg.LiveOrderScript = "pkg/polymarket/live_order.py"
	}
	if cfg.BalanceScript == "" {
		cfg.BalanceScript = "pkg/polymarket/wallet_balance.py"
	}
	if cfg.OrderStatusScript == "" {
		cfg.OrderStatusScript = "pkg/polymarket/order_status.py"
	}

	trader := &CopyTrader{
		cfg: cfg,
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
		},
		seen: make(map[string]struct{}),
	}
	repo := NewTradeRepository(db)
	if err := repo.EnsureCopyWalletSchema(ctx); err != nil {
		log.Printf("跟单失败：初始化 copy_wallet 字段失败 err=%v", err)
	}
	if err := repo.EnsureCopyWalletIndex(ctx); err != nil {
		log.Printf("跟单提醒：创建 copy_wallet 钱包索引失败 err=%v", err)
	}

	log.Printf(
		"跟单程序启动：mode=%s max_copy_usdc=%s source_min=不限制 price_range=%s-%s price_offset=%s order_type=IOC/FAK skip_up_down=%t poll=%s sync=%s",
		strings.ToLower(cfg.CopyMode),
		formatFloat(trader.maxCopyUSDC(), 6),
		formatFloat(cfg.MinCopyPrice, 6),
		formatFloat(cfg.MaxCopyPrice, 6),
		formatFloat(cfg.CopyPriceOffset, 6),
		cfg.SkipUpDownMarkets,
		pollInterval,
		syncInterval,
	)

	go trader.startWalletBalanceLogger(ctx)
	trader.runOnce(ctx, repo, syncInterval)

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			trader.runOnce(ctx, repo, syncInterval)
		}
	}
}

func (t *CopyTrader) runOnce(ctx context.Context, repo *TradeRepository, syncInterval time.Duration) {
	wallets, err := repo.ListEnabledCopyWallets(ctx)
	if err != nil {
		log.Printf("跟单失败：读取启用钱包失败 err=%v", err)
		return
	}

	if len(wallets) == 0 {
		log.Println("跟单等待：copy_wallet 没有 enable=1 的钱包")
		return
	}

	for _, wallet := range wallets {
		if ctx.Err() != nil {
			return
		}
		t.processWallet(ctx, repo, wallet)
	}

	if t.lastSync.IsZero() || time.Since(t.lastSync) >= syncInterval {
		t.syncCopyOrders(ctx, repo)
		t.lastSync = time.Now()
	}
}

func (t *CopyTrader) processWallet(ctx context.Context, repo *TradeRepository, wallet CopyWallet) {
	limit := t.cfg.CopyTradeLimit
	if !wallet.LastCopyTime.Valid {
		limit = 30
	}

	trades, err := t.fetchWalletTrades(ctx, wallet.Wallet, limit)
	if err != nil {
		if updateErr := repo.MarkCopyWalletError(ctx, wallet.Wallet, err); updateErr != nil {
			log.Printf("跟单提醒：记录钱包错误失败 wallet=%s err=%v", wallet.Wallet, updateErr)
		}
		log.Printf("跟单失败：拉取钱包成交失败 wallet=%s err=%v", wallet.Wallet, err)
		return
	}

	lastSeenTradeTime := latestTradeTime(trades)
	if !wallet.LastCopyTime.Valid {
		for _, trade := range trades {
			t.markSeen(trade)
		}
		if err := repo.MarkCopyWalletChecked(ctx, wallet.Wallet, len(trades), 0, lastSeenTradeTime, nil); err != nil {
			log.Printf("跟单提醒：初始化钱包时间失败 wallet=%s err=%v", wallet.Wallet, err)
		}
		log.Printf("跟单初始化：wallet=%s 已记录当前 %d 条成交，避免复制历史订单", wallet.Wallet, len(trades))
		return
	}

	handled := 0
	for i := len(trades) - 1; i >= 0; i-- {
		trade := trades[i]
		key := TradeKey(trade)
		if key == "" {
			continue
		}
		if _, ok := t.seen[key]; ok {
			continue
		}

		tradeTime := parseTradeTime(trade["timestamp"])
		if tradeTime.Before(wallet.LastCopyTime.Time.Add(-5 * time.Minute)) {
			t.seen[key] = struct{}{}
			continue
		}

		t.seen[key] = struct{}{}
		t.handleTrade(ctx, repo, trade)
		handled++
	}

	if err := repo.MarkCopyWalletChecked(ctx, wallet.Wallet, len(trades), handled, lastSeenTradeTime, nil); err != nil {
		log.Printf("跟单提醒：更新钱包检查时间失败 wallet=%s err=%v", wallet.Wallet, err)
	}

	if handled > 0 {
		log.Printf("跟单完成：wallet=%s 新处理 %d 条成交", wallet.Wallet, handled)
	}
}

func (t *CopyTrader) fetchWalletTrades(ctx context.Context, wallet string, limit int) ([]Trade, error) {
	endpoint, err := url.Parse(t.cfg.DataAPITradesURL)
	if err != nil {
		return nil, err
	}

	values := endpoint.Query()
	values.Set("user", wallet)
	values.Set("limit", fmt.Sprint(limit))
	values.Set("offset", "0")
	endpoint.RawQuery = values.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, err
	}

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("Data API 状态异常: %s", resp.Status)
	}

	var trades []Trade
	if err := json.NewDecoder(resp.Body).Decode(&trades); err != nil {
		return nil, err
	}

	return trades, nil
}

func (t *CopyTrader) handleTrade(ctx context.Context, repo *TradeRepository, trade Trade) {
	side := strings.ToUpper(firstString(trade, "side"))
	sourcePrice := SafeFloat(trade["price"])
	sourceSize := SafeFloat(trade["size"])
	sourceNotional := roundFloat(sourcePrice*sourceSize, 6)
	assetID := firstString(trade, "asset")
	targetNotional := t.copyNotionalForSource(sourceNotional)

	if side != "BUY" && side != "SELL" {
		return
	}

	if t.cfg.SkipUpDownMarkets && isBlockedCopyMarket(trade) {
		return
	}

	if assetID == "" {
		dbID, err := repo.InsertCopyOrderIfMissing(ctx, t.buildCopyOrder(trade, 0, sourcePrice, 0, "NO_ASSET_ID"))
		if err != nil {
			log.Printf("跟单记录失败：缺少 asset_id err=%v", err)
			return
		}
		log.Printf("跟单跳过：无 asset_id db_id=%d", dbID)
		return
	}

	copySize := 0.0
	copyNotional := 0.0
	availableCopy := 0.0
	skipReason := ""
	copyPrice := t.adjustCopyPrice(side, sourcePrice)
	if sourcePrice <= 0 {
		skipReason = "PRICE_INVALID"
	} else if sourceNotional <= 0 {
		skipReason = "SOURCE_NOTIONAL_INVALID"
	} else if sourcePrice < t.cfg.MinCopyPrice || sourcePrice > t.cfg.MaxCopyPrice {
		skipReason = "PRICE_OUT_OF_RANGE"
	} else if side == "BUY" {
		copySize, copyNotional = t.calcOrderAmount(copyPrice, targetNotional, 0)
	} else {
		available, err := repo.GetAvailableCopyPosition(ctx, assetID)
		if err != nil {
			log.Printf("跟单失败：读取可卖持仓失败 asset_id=%s err=%v", shortID(assetID), err)
			return
		}
		if available <= 0 {
			skipReason = "NO_POSITION"
		} else {
			availableCopy = available
			copySize, copyNotional = t.calcOrderAmount(copyPrice, targetNotional, available)
		}
	}

	if skipReason == "" && copySize <= 0 {
		skipReason = "COPY_AMOUNT_BELOW_PRECISION"
	}

	row := t.buildCopyOrder(trade, copySize, copyPrice, copyNotional, skipReason)

	if skipReason == "" {
		meta, err := t.fetchOrderBookMeta(ctx, assetID)
		if err != nil {
			if isContextDone(ctx, err) {
				return
			}
			skipReason = "MARKET_NOT_TRADABLE"
			row = t.buildCopyOrder(trade, 0, copyPrice, 0, skipReason)
			log.Printf("跟单跳过：市场暂不可交易 asset=%s err=%v | %s | %s", shortID(assetID), err, nonEmpty(row.Outcome, "-"), nonEmpty(row.MarketTitle, "-"))
		} else {
			copyPrice = t.alignCopyPrice(side, copyPrice, meta.TickSize)
			if side == "BUY" {
				copySize, copyNotional = t.calcOrderAmount(copyPrice, targetNotional, 0)
			} else {
				copySize, copyNotional = t.calcOrderAmount(copyPrice, targetNotional, availableCopy)
			}
			row = t.buildCopyOrder(trade, copySize, copyPrice, copyNotional, "")

			if copySize <= 0 {
				skipReason = "COPY_AMOUNT_BELOW_PRECISION"
				row = t.buildCopyOrder(trade, 0, copyPrice, 0, skipReason)
			} else if meta.MinOrderSize > 0 && copySize < meta.MinOrderSize {
				skipReason = "COPY_SIZE_BELOW_MIN_ORDER_SIZE"
				row = t.buildCopyOrder(trade, 0, copyPrice, 0, skipReason)
				log.Printf("跟单跳过：copy_size %s 小于市场最小下单数量 %s | %s | %s",
					formatFloat(copySize, 6),
					formatFloat(meta.MinOrderSize, 6),
					nonEmpty(row.Outcome, "-"),
					nonEmpty(row.MarketTitle, "-"),
				)
			}
		}
	}

	dbID, err := repo.InsertCopyOrderIfMissing(ctx, row)
	if err != nil {
		if isContextDone(ctx, err) {
			return
		}
		log.Printf("跟单记录失败：wallet=%s err=%v", row.SourceWallet, err)
		return
	}
	if dbID == 0 {
		return
	}

	if skipReason != "" {
		log.Printf("跟单跳过：db_id=%d reason=%s | 源订单 %s %s 份，单价 %s，共 %s | 过滤条件 price_range=%s-%s | %s | %s",
			dbID,
			skipReason,
			side,
			formatFloat(row.SourceSize, 6),
			formatFloat(sourcePrice, 6),
			formatFloat(sourceNotional, 6),
			formatFloat(t.cfg.MinCopyPrice, 6),
			formatFloat(t.cfg.MaxCopyPrice, 6),
			nonEmpty(row.Outcome, "-"),
			nonEmpty(row.MarketTitle, "-"),
		)
		return
	}

	logKeyEvent(
		"下单【提交】",
		"db_id=%d 方向=%s 数量=%s 单价=%s 总金额=%s 结果=提交中 | trader=%s 源金额=%s 封顶=%s 源价=%s 请求类型=IOC | %s | %s",
		dbID,
		side,
		formatFloat(copySize, 6),
		formatFloat(copyPrice, 6),
		formatFloat(copyNotional, 6),
		row.SourceWallet,
		formatFloat(sourceNotional, 6),
		formatFloat(t.maxCopyUSDC(), 6),
		formatFloat(sourcePrice, 6),
		nonEmpty(row.Outcome, "-"),
		nonEmpty(row.MarketTitle, "-"),
	)

	mode := strings.ToLower(t.cfg.CopyMode)
	if mode != "live" {
		if err := repo.MarkPaperCopyOrderSuccess(ctx, dbID); err != nil {
			log.Printf("paper 跟单标记失败：db_id=%d err=%v", dbID, err)
			return
		}
		log.Printf("paper 模式，已模拟成交 db_id=%d", dbID)
		return
	}

	resp, err := t.submitLiveOrder(ctx, row)
	if err != nil {
		if updateErr := repo.UpdateCopyOrderError(ctx, dbID, err); updateErr != nil {
			log.Printf("live 跟单失败后更新订单失败：db_id=%d err=%v", dbID, updateErr)
			return
		}
		logKeyEvent("下单【失败】", "db_id=%d 方向=%s 数量=%s 单价=%s 总金额=%s 结果=失败 | 原因=%s",
			dbID,
			row.CopySide,
			formatFloat(row.CopySize, 6),
			formatFloat(row.CopyPrice, 6),
			formatFloat(row.CopyNotional, 6),
			compactError(err.Error(), 320),
		)
		return
	}

	if err := repo.UpdateCopyOrderSuccess(ctx, dbID, resp); err != nil {
		logKeyEvent(
			"下单成功但数据库回写失败",
			"db_id=%d err=%v | order_response=%s",
			dbID,
			err,
			string(resp),
		)
		return
	}
	logKeyEvent("下单【成功】", "db_id=%d 方向=%s 数量=%s 单价=%s 总金额=%s 结果=成功 | %s",
		dbID,
		row.CopySide,
		formatFloat(row.CopySize, 6),
		formatFloat(row.CopyPrice, 6),
		formatFloat(row.CopyNotional, 6),
		orderResponseBrief(resp),
	)
}

func (t *CopyTrader) submitLiveOrder(ctx context.Context, row CopyOrderInsert) ([]byte, error) {
	if strings.TrimSpace(t.cfg.PrivateKey) == "" {
		return nil, fmt.Errorf("live 模式缺少 POLYMARKET_PRIVATE_KEY")
	}
	if strings.TrimSpace(t.cfg.FunderAddress) == "" {
		return nil, fmt.Errorf("live 模式缺少 POLYMARKET_PROXY_ADDRESS")
	}

	payload, err := json.Marshal(liveOrderRequest{
		Host:          t.cfg.CLOBURL,
		ChainID:       137,
		PrivateKey:    t.cfg.PrivateKey,
		FunderAddress: t.cfg.FunderAddress,
		APIKey:        t.cfg.CLOBAPIKey,
		APISecret:     t.cfg.CLOBAPISecret,
		APIPassphrase: t.cfg.CLOBAPIPassphrase,
		TokenID:       row.AssetID,
		Side:          row.CopySide,
		Price:         row.CopyPrice,
		Size:          row.CopySize,
		OrderType:     row.OrderType,
	})
	if err != nil {
		return nil, err
	}

	cmd := exec.CommandContext(ctx, t.cfg.PythonBin, t.cfg.LiveOrderScript)
	cmd.Stdin = bytes.NewReader(payload)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		message := strings.TrimSpace(stderr.String())
		if message == "" {
			message = strings.TrimSpace(stdout.String())
		}
		if message == "" {
			message = err.Error()
		}
		return nil, fmt.Errorf("live 下单失败: %s", message)
	}

	resp := bytes.TrimSpace(stdout.Bytes())
	if len(resp) == 0 {
		return nil, fmt.Errorf("live 下单失败: 空响应")
	}

	return append([]byte(nil), resp...), nil
}

func (t *CopyTrader) fetchWalletBalance(ctx context.Context) ([]byte, error) {
	if strings.TrimSpace(t.cfg.PrivateKey) == "" {
		return nil, fmt.Errorf("live 模式缺少 POLYMARKET_PRIVATE_KEY")
	}
	if strings.TrimSpace(t.cfg.FunderAddress) == "" {
		return nil, fmt.Errorf("live 模式缺少 POLYMARKET_PROXY_ADDRESS")
	}

	payload, err := json.Marshal(walletBalanceRequest{
		Host:          t.cfg.CLOBURL,
		ChainID:       137,
		PrivateKey:    t.cfg.PrivateKey,
		FunderAddress: t.cfg.FunderAddress,
		APIKey:        t.cfg.CLOBAPIKey,
		APISecret:     t.cfg.CLOBAPISecret,
		APIPassphrase: t.cfg.CLOBAPIPassphrase,
	})
	if err != nil {
		return nil, err
	}

	cmd := exec.CommandContext(ctx, t.cfg.PythonBin, t.cfg.BalanceScript)
	cmd.Stdin = bytes.NewReader(payload)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		message := strings.TrimSpace(stderr.String())
		if message == "" {
			message = strings.TrimSpace(stdout.String())
		}
		if message == "" {
			message = err.Error()
		}
		return nil, fmt.Errorf("读取钱包余额失败: %s", message)
	}

	resp := bytes.TrimSpace(stdout.Bytes())
	if len(resp) == 0 {
		return nil, fmt.Errorf("读取钱包余额失败: 空响应")
	}

	return append([]byte(nil), resp...), nil
}

func (t *CopyTrader) fetchLiveOrderStatus(ctx context.Context, orderID string) (liveOrderStatus, error) {
	if strings.TrimSpace(orderID) == "" {
		return liveOrderStatus{}, fmt.Errorf("缺少 order_id")
	}
	if strings.TrimSpace(t.cfg.PrivateKey) == "" {
		return liveOrderStatus{}, fmt.Errorf("live 模式缺少 POLYMARKET_PRIVATE_KEY")
	}
	if strings.TrimSpace(t.cfg.FunderAddress) == "" {
		return liveOrderStatus{}, fmt.Errorf("live 模式缺少 POLYMARKET_PROXY_ADDRESS")
	}

	payload, err := json.Marshal(orderStatusRequest{
		Host:          t.cfg.CLOBURL,
		ChainID:       137,
		PrivateKey:    t.cfg.PrivateKey,
		FunderAddress: t.cfg.FunderAddress,
		APIKey:        t.cfg.CLOBAPIKey,
		APISecret:     t.cfg.CLOBAPISecret,
		APIPassphrase: t.cfg.CLOBAPIPassphrase,
		OrderID:       orderID,
	})
	if err != nil {
		return liveOrderStatus{}, err
	}

	cmd := exec.CommandContext(ctx, t.cfg.PythonBin, t.cfg.OrderStatusScript)
	cmd.Stdin = bytes.NewReader(payload)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		message := strings.TrimSpace(stderr.String())
		if message == "" {
			message = strings.TrimSpace(stdout.String())
		}
		if message == "" {
			message = err.Error()
		}
		return liveOrderStatus{}, fmt.Errorf("查询订单状态失败: %s", message)
	}

	resp := bytes.TrimSpace(stdout.Bytes())
	if len(resp) == 0 {
		return liveOrderStatus{}, fmt.Errorf("查询订单状态失败: 空响应")
	}

	return parseLiveOrderStatus(resp)
}

func parseLiveOrderStatus(resp []byte) (liveOrderStatus, error) {
	var data map[string]any
	if err := json.Unmarshal(resp, &data); err != nil {
		return liveOrderStatus{}, err
	}

	status := firstResponseString(data, "status", "order_status", "state")
	matchedSize := firstResponseFloat(data, "size_matched", "matchedSize", "matched_size", "filled_size", "filledSize")
	if matchedSize == 0 {
		matchedSize = firstResponseFloat(data, "sizeMatched", "filled")
	}

	return liveOrderStatus{
		Status:      status,
		MatchedSize: matchedSize,
	}, nil
}

func (t *CopyTrader) startWalletBalanceLogger(ctx context.Context) {
	if strings.ToLower(t.cfg.CopyMode) != "live" {
		return
	}

	t.logWalletBalance(ctx)

	ticker := time.NewTicker(walletBalanceLogInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.logWalletBalance(ctx)
		}
	}
}

func (t *CopyTrader) logWalletBalance(ctx context.Context) {
	balanceCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()

	resp, err := t.fetchWalletBalance(balanceCtx)
	if err != nil {
		if isContextDone(ctx, err) {
			return
		}
		logKeyEvent("钱包余额读取失败", "%v", err)
		return
	}

	balance, _ := formatWalletBalance(resp)
	logKeyEvent("钱包余额", "pUSD余额=%s", balance)
}

func formatWalletBalance(resp []byte) (string, string) {
	var data map[string]any
	if err := json.Unmarshal(resp, &data); err != nil {
		return "-", "-"
	}

	return formatBalanceField(data["balance"]), formatBalanceField(firstBalanceValue(data, "allowance", "allowances"))
}

func firstBalanceValue(data map[string]any, keys ...string) any {
	for _, key := range keys {
		if value, ok := data[key]; ok {
			return value
		}
	}
	return nil
}

func formatBalanceField(value any) string {
	if value == nil {
		return "-"
	}

	switch v := value.(type) {
	case map[string]any, []any:
		raw, _ := json.Marshal(v)
		return string(raw)
	}

	text := strings.TrimSpace(fmt.Sprint(value))
	if text == "" {
		return "-"
	}

	n := SafeFloat(value)
	if n == 0 && text != "0" && text != "0.0" && text != "0.00" {
		return text
	}
	if math.Abs(n) >= 100000 {
		return formatFloat(n/1_000_000, 6) + " (raw " + text + ")"
	}

	return formatFloat(n, 6)
}

func (t *CopyTrader) buildCopyOrder(trade Trade, copySize, copyPrice, copyNotional float64, skipReason string) CopyOrderInsert {
	sourcePrice := SafeFloat(trade["price"])
	sourceSize := SafeFloat(trade["size"])
	raw, _ := json.Marshal(map[string]any{
		"mode":          strings.ToLower(t.cfg.CopyMode),
		"copy_usdc":     t.cfg.CopyUSDC,
		"min_copy_usdc": t.cfg.MinCopyUSDC,
		"max_copy_usdc": t.maxCopyUSDC(),
	})

	orderStatus := "PENDING"
	if strings.ToLower(t.cfg.CopyMode) != "live" {
		orderStatus = "PAPER"
	}
	if skipReason != "" {
		orderStatus = "SKIPPED"
	}

	return CopyOrderInsert{
		SourceWallet:    firstString(trade, "proxyWallet", "user", "wallet"),
		SourceTxHash:    firstString(trade, "transactionHash", "txHash"),
		SourceSide:      strings.ToUpper(firstString(trade, "side")),
		SourceSize:      sourceSize,
		SourcePrice:     sourcePrice,
		SourceNotional:  roundFloat(sourcePrice*sourceSize, 6),
		SourceTimestamp: parseTradeTime(trade["timestamp"]),
		MarketTitle:     firstString(trade, "title", "market", "marketTitle"),
		ConditionID:     firstString(trade, "conditionId", "conditionID"),
		AssetID:         firstString(trade, "asset", "assetId", "tokenId"),
		Outcome:         firstString(trade, "outcome"),
		MarketURL:       marketURL(trade),
		CopySide:        strings.ToUpper(firstString(trade, "side")),
		CopySize:        copySize,
		CopyPrice:       copyPrice,
		CopyNotional:    copyNotional,
		OrderType:       "IOC",
		DryRun:          strings.ToLower(t.cfg.CopyMode) != "live",
		SubmitSuccess:   false,
		SkipReason:      nullableSQLString(skipReason),
		OrderStatus:     orderStatus,
		RawResponse:     raw,
	}
}

func (t *CopyTrader) syncCopyOrders(ctx context.Context, repo *TradeRepository) {
	failed, err := repo.MarkStalePendingCopyOrdersFailed(ctx, 2*time.Minute)
	if err != nil {
		log.Printf("订单修改失败：清理卡住的 PENDING 订单失败 err=%v", err)
	} else if failed > 0 {
		logKeyEvent("订单异常", "%d 笔 PENDING 订单长时间没有 order_id，已标记 FAILED", failed)
	}

	rows, err := repo.ListCopyOrdersForSync(ctx, time.Minute, 100)
	if err != nil {
		log.Printf("跟单同步失败：读取 copy_orders 失败 err=%v", err)
		return
	}

	synced := 0
	for _, row := range rows {
		if ctx.Err() != nil {
			return
		}

		currentPrice := t.getCurrentExitPrice(ctx, row.AssetID)
		if currentPrice <= 0 {
			continue
		}

		matchedSize := nullFloatValue(row.MatchedSize)
		if row.DryRun {
			matchedSize = row.CopySize
		}

		status := row.OrderStatus.String
		if strings.ToLower(t.cfg.CopyMode) == "live" && row.OrderID.Valid {
			info, err := t.fetchLiveOrderStatus(ctx, row.OrderID.String)
			if err != nil {
				log.Printf("订单修改失败：查询 live 订单状态失败 db_id=%d order_id=%s err=%v", row.ID, row.OrderID.String, err)
			} else {
				if info.Status != "" {
					status = info.Status
				}
				if info.MatchedSize > 0 {
					matchedSize = info.MatchedSize
				}
			}
		}

		totalPNL := 0.0
		if matchedSize > 0 {
			switch strings.ToUpper(row.CopySide) {
			case "BUY":
				totalPNL = (currentPrice - row.CopyPrice) * matchedSize
			case "SELL":
				totalPNL = (row.CopyPrice - currentPrice) * matchedSize
			}
		}

		isFilled := matchedSize >= row.CopySize && row.CopySize > 0
		if status == "" {
			status = "PAPER"
		}
		isCancelled := strings.EqualFold(status, "CANCELLED") || strings.EqualFold(status, "CANCELED")

		if err := repo.UpdateCopyOrderPNL(ctx, row.ID, status, matchedSize, row.CopyPrice, isFilled, isCancelled, totalPNL); err != nil {
			log.Printf("跟单同步失败：更新 copy_order id=%d err=%v", row.ID, err)
			continue
		}
		log.Printf(
			"订单修改：db_id=%d status=%s matched=%s 当前价=%s 盈亏=%s",
			row.ID,
			status,
			formatFloat(matchedSize, 6),
			formatFloat(currentPrice, 6),
			formatSignedFloat(totalPNL, 6),
		)
		synced++
	}

	if synced > 0 {
		log.Printf("订单修改：本轮已同步 %d 条", synced)
	}
}

func (t *CopyTrader) getCurrentExitPrice(ctx context.Context, assetID string) float64 {
	endpoint, err := url.Parse(strings.TrimRight(t.cfg.CLOBURL, "/") + "/price")
	if err != nil {
		return 0
	}

	values := endpoint.Query()
	values.Set("token_id", assetID)
	values.Set("side", "SELL")
	endpoint.RawQuery = values.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return 0
	}

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return 0
	}

	var data map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return 0
	}

	return SafeFloat(data["price"])
}

func (t *CopyTrader) fetchOrderBookMeta(ctx context.Context, assetID string) (orderBookMeta, error) {
	endpoint, err := url.Parse(strings.TrimRight(t.cfg.CLOBURL, "/") + "/book")
	if err != nil {
		return orderBookMeta{}, err
	}

	values := endpoint.Query()
	values.Set("token_id", assetID)
	endpoint.RawQuery = values.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return orderBookMeta{}, err
	}

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return orderBookMeta{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return orderBookMeta{}, fmt.Errorf("orderbook 不可用: %s", resp.Status)
	}

	var data map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return orderBookMeta{}, err
	}

	return orderBookMeta{
		MinOrderSize: SafeFloat(data["min_order_size"]),
		TickSize:     SafeFloat(data["tick_size"]),
	}, nil
}

func (t *CopyTrader) maxCopyUSDC() float64 {
	return math.Max(t.cfg.CopyUSDC, t.cfg.MinCopyUSDC)
}

func isContextDone(ctx context.Context, err error) bool {
	return ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func orderResponseBrief(resp []byte) string {
	var data map[string]any
	if err := json.Unmarshal(resp, &data); err != nil {
		return "响应=" + oneLine(string(resp), 160)
	}

	parts := []string{}
	if orderID := firstResponseString(data, "orderID", "orderId", "id"); orderID != "" {
		parts = append(parts, "order_id="+shortID(orderID))
	}
	if status := firstResponseString(data, "status", "order_status"); status != "" {
		parts = append(parts, "status="+status)
	}
	if orderType := firstResponseString(data, "effectiveOrderType"); orderType != "" {
		parts = append(parts, "实际类型="+orderType)
	}
	if len(parts) == 0 {
		return "响应=已返回"
	}

	return strings.Join(parts, " ")
}

func oneLine(value string, limit int) string {
	value = strings.Join(strings.Fields(value), " ")
	if limit > 0 && len(value) > limit {
		return value[:limit] + "..."
	}
	return value
}

func compactError(value string, limit int) string {
	value = strings.Join(strings.Fields(value), " ")
	if limit <= 0 || len(value) <= limit {
		return value
	}

	half := limit / 2
	return value[:half] + " ... " + value[len(value)-half:]
}

func (t *CopyTrader) calcOrderAmount(price float64, targetNotional float64, maxSize float64) (float64, float64) {
	if price <= 0 || targetNotional <= 0 {
		return 0, 0
	}

	const sizeScale int64 = 10_000
	const centsScale int64 = 100

	priceInt, priceScale := scaledDecimal(price, 6)
	if priceInt <= 0 || priceScale <= 0 {
		return 0, 0
	}

	targetCents := int64(math.Floor(targetNotional*float64(centsScale) + 1e-9))
	if targetCents <= 0 {
		return 0, 0
	}

	maxUnits := targetCents * priceScale * sizeScale / (centsScale * priceInt)
	if maxSize > 0 {
		maxSizeUnits := int64(math.Floor(maxSize*float64(sizeScale) + 1e-9))
		if maxSizeUnits < maxUnits {
			maxUnits = maxSizeUnits
		}
	}
	if maxUnits <= 0 {
		return 0, 0
	}

	denominator := priceScale * sizeScale
	step := denominator / gcd(priceInt*centsScale, denominator)
	units := (maxUnits / step) * step
	if units <= 0 {
		return 0, 0
	}

	cents := priceInt * units * centsScale / denominator
	if cents <= 0 {
		return 0, 0
	}

	return float64(units) / float64(sizeScale), float64(cents) / float64(centsScale)
}

func scaledDecimal(value float64, places int) (int64, int64) {
	text := formatFloat(value, places)
	parts := strings.SplitN(text, ".", 2)
	if len(parts) == 1 {
		n, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return 0, 0
		}
		return n, 1
	}

	whole := parts[0]
	fraction := strings.TrimRight(parts[1], "0")
	if fraction == "" {
		n, err := strconv.ParseInt(whole, 10, 64)
		if err != nil {
			return 0, 0
		}
		return n, 1
	}

	scale := int64(math.Pow10(len(fraction)))
	n, err := strconv.ParseInt(whole+fraction, 10, 64)
	if err != nil {
		return 0, 0
	}

	return n, scale
}

func gcd(a int64, b int64) int64 {
	if a < 0 {
		a = -a
	}
	if b < 0 {
		b = -b
	}
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

func (t *CopyTrader) copyNotionalForSource(sourceNotional float64) float64 {
	if sourceNotional <= 0 {
		return 0
	}
	return roundFloat(math.Min(sourceNotional, t.maxCopyUSDC()), 6)
}

func (t *CopyTrader) adjustCopyPrice(side string, sourcePrice float64) float64 {
	switch strings.ToUpper(side) {
	case "BUY":
		return roundFloat(math.Min(sourcePrice+t.cfg.CopyPriceOffset, t.cfg.MaxCopyPrice), 6)
	case "SELL":
		return roundFloat(math.Max(sourcePrice-t.cfg.CopyPriceOffset, t.cfg.MinCopyPrice), 6)
	default:
		return sourcePrice
	}
}

func (t *CopyTrader) alignCopyPrice(side string, price float64, tickSize float64) float64 {
	if tickSize <= 0 {
		return roundFloat(price, 6)
	}

	steps := price / tickSize
	switch strings.ToUpper(side) {
	case "BUY":
		price = math.Ceil(steps) * tickSize
	case "SELL":
		price = math.Floor(steps) * tickSize
	default:
		price = math.Round(steps) * tickSize
	}

	if price > t.cfg.MaxCopyPrice {
		price = math.Floor(t.cfg.MaxCopyPrice/tickSize) * tickSize
	}
	if price < t.cfg.MinCopyPrice {
		price = math.Ceil(t.cfg.MinCopyPrice/tickSize) * tickSize
	}

	return roundFloat(price, 6)
}

func (t *CopyTrader) markSeen(trade Trade) {
	key := TradeKey(trade)
	if key != "" {
		t.seen[key] = struct{}{}
	}
}

func latestTradeTime(trades []Trade) time.Time {
	var latest time.Time
	for _, trade := range trades {
		tradeTime := parseTradeTime(trade["timestamp"])
		if latest.IsZero() || tradeTime.After(latest) {
			latest = tradeTime
		}
	}
	return latest
}

func marketURL(trade Trade) string {
	slug := firstString(trade, "eventSlug", "slug")
	if slug == "" {
		return ""
	}
	return "https://polymarket.com/event/" + slug
}

func isUpDownMarket(trade Trade) bool {
	title := strings.ToLower(firstString(trade, "title", "market", "marketTitle"))
	slug := strings.ToLower(firstString(trade, "eventSlug", "slug"))

	return strings.Contains(title, " up or down") ||
		strings.Contains(slug, "updown") ||
		strings.Contains(slug, "up-or-down")
}

func isBlockedCopyMarket(trade Trade) bool {
	return isUpDownMarket(trade) || isCryptoCopyMarket(trade)
}

func isCryptoCopyMarket(trade Trade) bool {
	text := strings.ToLower(strings.Join([]string{
		firstString(trade, "title", "market", "marketTitle"),
		firstString(trade, "eventSlug", "slug"),
		firstString(trade, "outcome"),
	}, " "))

	nameKeywords := []string{
		"bitcoin",
		"ethereum",
		"dogecoin",
		"solana",
		"litecoin",
		"cardano",
	}

	for _, keyword := range nameKeywords {
		if strings.Contains(text, keyword) {
			return true
		}
	}

	tickerKeywords := map[string]struct{}{
		"btc":  {},
		"eth":  {},
		"doge": {},
		"xrp":  {},
		"sol":  {},
		"ltc":  {},
		"ada":  {},
	}

	tokens := strings.FieldsFunc(text, func(r rune) bool {
		return (r < 'a' || r > 'z') && (r < '0' || r > '9')
	})
	for _, token := range tokens {
		if _, ok := tickerKeywords[token]; ok {
			return true
		}
	}

	return false
}

func nullableSQLString(value string) sql.NullString {
	value = strings.TrimSpace(value)
	return sql.NullString{String: value, Valid: value != ""}
}

func roundFloat(value float64, places int) float64 {
	scale := math.Pow10(places)
	return math.Round(value*scale) / scale
}
