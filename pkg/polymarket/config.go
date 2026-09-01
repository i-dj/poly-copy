package polymarket

import "time"

type Config struct {
	WSURL          string
	MinSize        float64
	ReconnectDelay time.Duration
}
