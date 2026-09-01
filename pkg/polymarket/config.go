package polymarket

import "time"

type Config struct {
	WSURL               string
	GammaMarketsURL     string
	MinSize             float64
	ReconnectDelay      time.Duration
	SettlementInterval  time.Duration
	SettlementBatchSize int
}
