package polymarket

import "time"

type Config struct {
	WSURL               string
	GammaMarketsURL     string
	CLOBURL             string
	DataAPITradesURL    string
	PrivateKey          string
	FunderAddress       string
	PythonBin           string
	LiveOrderScript     string
	BalanceScript       string
	OrderStatusScript   string
	MinSize             float64
	ReconnectDelay      time.Duration
	SettlementInterval  time.Duration
	SettlementBatchSize int
	CopyMode            string
	CopyUSDC            float64
	MinCopyUSDC         float64
	CopyTradeLimit      int
	MinCopyPrice        float64
	MaxCopyPrice        float64
	SkipUpDownMarkets   bool
}
