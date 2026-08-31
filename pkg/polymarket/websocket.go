package polymarket

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/gorilla/websocket"
)

type TradeHandler func(context.Context, Trade) error

func (c *Client) ListenAllTrades(ctx context.Context, handler TradeHandler) error {
	seen := make(map[string]struct{})

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
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, c.cfg.WSURL, nil)
	if err != nil {
		return err
	}
	defer conn.Close()
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
		},
	}); err != nil {
		return err
	}
	log.Println("已订阅全市场实时成交 activity/trades")

	pingDone := make(chan struct{})
	defer close(pingDone)
	go pingLoop(conn, pingDone)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		_, data, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}

		raw := string(data)
		if raw == "ping" || raw == "pong" {
			continue
		}

		trades, ok := parseTradeMessage(data)
		if !ok {
			continue
		}
		log.Printf("收到成交消息: payload_count=%d", len(trades))

		for _, trade := range trades {
			key := TradeKey(trade)
			if _, exists := seen[key]; exists {
				log.Printf("跳过重复成交: key=%s", key)
				continue
			}
			seen[key] = struct{}{}

			if err := handler(ctx, trade); err != nil {
				return err
			}
		}
	}
}

func pingLoop(conn *websocket.Conn, done <-chan struct{}) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			_ = conn.WriteMessage(websocket.TextMessage, []byte("ping"))
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
		return []Trade{one}, true
	}

	return nil, false
}
