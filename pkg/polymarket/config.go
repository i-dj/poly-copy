package polymarket

import "time"

type Config struct {
	WSURL          string
	LeaderboardURL string
	MinSize        float64
	PNLCacheTTL    time.Duration
	PNLTimeout     time.Duration
	ReconnectDelay time.Duration
}
