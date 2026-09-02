package polymarket

import "testing"

func TestCalcOrderAmountUsesCLOBPrecision(t *testing.T) {
	trader := &CopyTrader{}

	tests := []struct {
		name             string
		price            float64
		targetNotional   float64
		maxSize          float64
		expectedSize     float64
		expectedNotional float64
	}{
		{
			name:             "two cent price",
			price:            0.97,
			targetNotional:   5,
			expectedSize:     5,
			expectedNotional: 4.85,
		},
		{
			name:             "sub cent price",
			price:            0.003,
			targetNotional:   5,
			expectedSize:     1660,
			expectedNotional: 4.98,
		},
		{
			name:             "sell respects available size",
			price:            0.5,
			targetNotional:   5,
			maxSize:          3,
			expectedSize:     3,
			expectedNotional: 1.5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size, notional := trader.calcOrderAmount(tt.price, tt.targetNotional, tt.maxSize)
			if size != tt.expectedSize {
				t.Fatalf("size = %v, want %v", size, tt.expectedSize)
			}
			if notional != tt.expectedNotional {
				t.Fatalf("notional = %v, want %v", notional, tt.expectedNotional)
			}
		})
	}
}
