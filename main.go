package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"poly-copy/pkg/database"
	"poly-copy/pkg/polymarket"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := database.Connect()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	log.Println("数据库连接成功")

	repo := polymarket.NewTradeRepository(db)
	if err := repo.EnsureSettledSchema(ctx); err != nil {
		log.Fatal(err)
	}

	settlementInterval := 1 * time.Minute
	copyWalletInterval := 1 * time.Minute
	copyWalletLimit := 5
	copyTraderPollInterval := 1 * time.Second
	copyTraderSyncInterval := 1 * time.Minute
	copyMode := "live"
	privateKey := mustGetCfg(ctx, db, "POLYMARKET_PRIVATE_KEY")
	funderAddress := mustGetCfg(ctx, db, "POLYMARKET_PROXY_ADDRESS")
	clobAPIKey := getCfg(ctx, db, "CLOB_API_KEY")
	clobAPISecret := getCfg(ctx, db, "CLOB_API_SECRET")
	clobAPIPassphrase := getCfg(ctx, db, "CLOB_API_PASSPHRASE")
	if clobAPIKey != "" && clobAPISecret != "" && clobAPIPassphrase != "" {
		log.Println("CLOB API 凭证：已从 cfg 表读取")
	} else {
		log.Println("CLOB API 凭证：cfg 表未配置完整，正在动态生成并写入 cfg")
		creds, err := createCLOBAPICreds(ctx, "python3", "pkg/polymarket/create_api_creds.py", "https://clob.polymarket.com", privateKey, funderAddress)
		if err != nil {
			log.Fatalf("CLOB API 凭证生成失败：%v", err)
		}
		if err := saveCLOBAPICreds(ctx, db, creds); err != nil {
			log.Fatalf("CLOB API 凭证写入 cfg 失败：%v", err)
		}
		clobAPIKey = creds.APIKey
		clobAPISecret = creds.APISecret
		clobAPIPassphrase = creds.APIPassphrase
		log.Println("CLOB API 凭证：已动态生成并写入 cfg")
	}

	cfg := polymarket.Config{
		WSURL:               "wss://ws-live-data.polymarket.com",
		GammaMarketsURL:     "https://gamma-api.polymarket.com/markets",
		CLOBURL:             "https://clob.polymarket.com",
		DataAPITradesURL:    "https://data-api.polymarket.com/trades",
		PrivateKey:          privateKey,
		FunderAddress:       funderAddress,
		CLOBAPIKey:          clobAPIKey,
		CLOBAPISecret:       clobAPISecret,
		CLOBAPIPassphrase:   clobAPIPassphrase,
		PythonBin:           "python3",
		LiveOrderScript:     "pkg/polymarket/live_order.py",
		BalanceScript:       "pkg/polymarket/wallet_balance.py",
		OrderStatusScript:   "pkg/polymarket/order_status.py",
		MinSize:             0,
		ReconnectDelay:      2 * time.Second,
		SettlementBatchSize: 100,
		CopyMode:            copyMode,
		CopyUSDC:            1,
		MinCopyUSDC:         5,
		MinSourceNotional:   20,
		MaxWalletLoss:       10,
		CopyTradeLimit:      10,
		CopyPriceOffset:     0,
		MinCopyPrice:        0.001,
		MaxCopyPrice:        0.999,
		SkipUpDownMarkets:   true,
	}

	go polymarket.StartSettlementScanner(ctx, db, cfg, settlementInterval)
	go polymarket.StartCopyWalletSyncer(ctx, db, copyWalletInterval, copyWalletLimit)
	go polymarket.StartCopyTrader(ctx, db, cfg, copyTraderPollInterval, copyTraderSyncInterval)

	if err := polymarket.RunTradeTracker(ctx, db, cfg); err != nil {
		log.Fatal(err)
	}
}

func getCfg(ctx context.Context, db database.QueryRower, key string) string {
	value, err := database.GetCfgName(ctx, db, key)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(value)
}

func mustGetCfg(ctx context.Context, db database.QueryRower, key string) string {
	value, err := database.GetCfgName(ctx, db, key)
	if err != nil {
		log.Fatalf("读取配置失败：%s err=%v", key, err)
	}

	value = strings.TrimSpace(value)
	if value == "" {
		log.Fatalf("读取配置失败：%s 为空", key)
	}

	return value
}

type clobAPICreds struct {
	APIKey        string `json:"api_key"`
	APISecret     string `json:"api_secret"`
	APIPassphrase string `json:"api_passphrase"`
}

func createCLOBAPICreds(ctx context.Context, pythonBin string, script string, clobURL string, privateKey string, funderAddress string) (clobAPICreds, error) {
	payload, err := json.Marshal(map[string]any{
		"host":           clobURL,
		"chain_id":       137,
		"private_key":    privateKey,
		"funder_address": funderAddress,
	})
	if err != nil {
		return clobAPICreds{}, err
	}

	cmd := exec.CommandContext(ctx, pythonBin, script)
	cmd.Stdin = bytes.NewReader(payload)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		detail := strings.TrimSpace(stderr.String())
		if detail == "" {
			detail = strings.TrimSpace(stdout.String())
		}
		if detail != "" {
			return clobAPICreds{}, fmt.Errorf("%w: %s", err, detail)
		}
		return clobAPICreds{}, err
	}

	var creds clobAPICreds
	if err := json.Unmarshal(stdout.Bytes(), &creds); err != nil {
		return clobAPICreds{}, fmt.Errorf("解析 CLOB API 凭证失败：%w: %s", err, strings.TrimSpace(stdout.String()))
	}

	creds.APIKey = strings.TrimSpace(creds.APIKey)
	creds.APISecret = strings.TrimSpace(creds.APISecret)
	creds.APIPassphrase = strings.TrimSpace(creds.APIPassphrase)
	if creds.APIKey == "" || creds.APISecret == "" || creds.APIPassphrase == "" {
		return clobAPICreds{}, fmt.Errorf("生成结果不完整")
	}

	return creds, nil
}

func saveCLOBAPICreds(ctx context.Context, db database.Execer, creds clobAPICreds) error {
	values := map[string]string{
		"CLOB_API_KEY":        creds.APIKey,
		"CLOB_API_SECRET":     creds.APISecret,
		"CLOB_API_PASSPHRASE": creds.APIPassphrase,
	}

	for key, value := range values {
		if err := database.UpsertCfgName(ctx, db, key, value); err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
	}

	return nil
}
