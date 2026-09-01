package polymarket

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

type TradeHandler func(context.Context, Trade) error

const (
	websocketPingInterval      = 10 * time.Second
	websocketReadTimeout       = 10 * time.Second
	websocketNoTradeReconnect  = 10 * time.Second
	websocketForceReconnect    = 10 * time.Minute
	websocketSeenTrimThreshold = 20000
	websocketSeenTrimTarget    = 10000
	websocketMinNotionalFilter = 0
	websocketMaxMessageBytes   = 20 * 1024 * 1024
)

func (c *Client) ListenAllTrades(ctx context.Context, handler TradeHandler) error {
	seen := make(map[string]struct{})
	log.Println("成交监听规则：过滤 crypto 短周期市场，10 秒无有效成交主动重连，10 分钟强制重连")

	for {
		log.Printf("正在连接 Polymarket WebSocket: %s", c.cfg.WSURL)
		if err := c.listenOnce(ctx, seen, handler); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			log.Printf("Polymarket WebSocket 异常: %v", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.cfg.ReconnectDelay):
			log.Printf("等待 %s 后重连 Polymarket WebSocket", c.cfg.ReconnectDelay)
		}
	}
}

func (c *Client) listenOnce(ctx context.Context, seen map[string]struct{}, handler TradeHandler) error {
	dialer := websocket.DefaultDialer
	dialer.HandshakeTimeout = websocketReadTimeout
	conn, _, err := dialer.DialContext(ctx, c.cfg.WSURL, nil)
	if err != nil {
		return err
	}
	defer conn.Close()
	conn.SetReadLimit(websocketMaxMessageBytes)
	log.Println("Polymarket WebSocket 已连接")

	closeDone := make(chan struct{})
	defer close(closeDone)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-closeDone:
		}
	}()

	if err := conn.WriteJSON(map[string]any{
		"action": "subscribe",
		"subscriptions": []map[string]string{
			{
				"topic": "activity",
				"type":  "trades",
			},
			{
				"topic": "activity",
				"type":  "orders_matched",
			},
		},
	}); err != nil {
		return err
	}
	log.Println("已订阅 activity/trades + activity/orders_matched")

	pingDone := make(chan struct{})
	defer close(pingDone)
	go pingLoop(conn, pingDone)

	connectedAt := time.Now()
	lastTradeAt := time.Now()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if time.Since(connectedAt) >= websocketForceReconnect {
			return fmt.Errorf("达到 10 分钟强制重连时间")
		}

		_ = conn.SetReadDeadline(time.Now().Add(websocketReadTimeout))
		_, data, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			idle := time.Since(lastTradeAt)
			if idle >= websocketNoTradeReconnect {
				log.Printf("%s 无有效成交消息，主动重连", idle.Round(100*time.Millisecond))
				return fmt.Errorf("10 秒无有效成交，主动重连")
			}
			return err
		}

		raw := string(data)
		if raw == "PING" || raw == "PONG" || raw == "ping" || raw == "pong" {
			continue
		}

		trades, ok := parseTradeMessage(data)
		if !ok {
			continue
		}

		gotTrade := false
		for _, trade := range trades {
			if !isEffectiveMarketTrade(trade) {
				continue
			}

			key := websocketTradeKey(trade)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			trimWebsocketSeen(seen)

			if err := handler(ctx, trade); err != nil {
				return err
			}
			gotTrade = true
		}

		if gotTrade {
			lastTradeAt = time.Now()
		}
	}
}

func pingLoop(conn *websocket.Conn, done <-chan struct{}) {
	ticker := time.NewTicker(websocketPingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			deadline := time.Now().Add(5 * time.Second)
			if err := conn.WriteControl(websocket.PingMessage, nil, deadline); err != nil {
				log.Printf("心跳失败: %T: %v", err, err)
				return
			}
		}
	}
}

func parseTradeMessage(data []byte) ([]Trade, bool) {
	var message struct {
		Topic   string          `json:"topic"`
		Type    string          `json:"type"`
		Payload json.RawMessage `json:"payload"`
	}

	if err := json.Unmarshal(data, &message); err != nil {
		return nil, false
	}

	if message.Topic != "activity" {
		return nil, false
	}
	if message.Type != "trades" && message.Type != "orders_matched" {
		return nil, false
	}

	var list []Trade
	if err := json.Unmarshal(message.Payload, &list); err == nil {
		return list, true
	}

	var one Trade
	if err := json.Unmarshal(message.Payload, &one); err == nil {
		if one == nil {
			return nil, false
		}
		return []Trade{one}, true
	}

	return nil, false
}

func isEffectiveMarketTrade(trade Trade) bool {
	title := firstString(trade, "title", "market_title", "marketTitle")
	if isCryptoMarket(title) {
		return false
	}

	price := SafeFloat(trade["price"])
	size := SafeFloat(trade["size"])
	return price*size >= websocketMinNotionalFilter
}

func isCryptoMarket(title string) bool {
	title = strings.ToLower(title)
	keywords := []string{
		"bitcoin up or down",
		"ethereum up or down",
		"dogecoin up or down",
		"xrp up or down",
		"solana up or down",
		"litecoin up or down",
		"cardano up or down",
		"bitcoin above",
		"ethereum above",
		"btc updown",
		"eth updown",
		"doge updown",
		"xrp updown",
		"sol updown",
		"btc",
		"eth",
		"doge",
		"xrp",
		"solana",
		"litecoin",
		"cardano",
	}

	for _, keyword := range keywords {
		if strings.Contains(title, keyword) {
			return true
		}
	}

	return false
}

func websocketTradeKey(trade Trade) string {
	txHash := firstString(trade, "transactionHash", "tx_hash", "txHash")
	wallet := strings.ToLower(firstString(trade, "proxyWallet", "wallet", "maker", "taker"))
	asset := firstString(trade, "asset", "assetId", "asset_id", "token_id", "tokenID")
	side := strings.ToUpper(firstString(trade, "side"))
	size := stringValue(trade["size"])
	price := stringValue(trade["price"])
	timestamp := stringValue(trade["timestamp"])

	if txHash != "" {
		return strings.Join([]string{txHash, wallet, asset, side, size, price}, ":")
	}

	return strings.Join([]string{wallet, asset, side, size, price, timestamp}, ":")
}

func trimWebsocketSeen(seen map[string]struct{}) {
	if len(seen) <= websocketSeenTrimThreshold {
		return
	}

	removeCount := len(seen) - websocketSeenTrimTarget
	for key := range seen {
		delete(seen, key)
		removeCount--
		if removeCount <= 0 {
			return
		}
	}
}
