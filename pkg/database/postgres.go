package database

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

type Config struct {
	URL      string
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
	SSLMode  string
}

func LoadConfig(envFiles ...string) (Config, error) {
	_ = godotenv.Load(envFiles...)

	cfg := Config{
		URL:      strings.TrimSpace(os.Getenv("DATABASE_URL")),
		Host:     getEnv("DB_HOST", "localhost"),
		Port:     getEnvInt("DB_PORT", 5432),
		User:     strings.TrimSpace(os.Getenv("DB_USER")),
		Password: os.Getenv("DB_PASSWORD"),
		DBName:   strings.TrimSpace(os.Getenv("DB_NAME")),
		SSLMode:  getEnv("DB_SSLMODE", "disable"),
	}

	if cfg.URL != "" {
		return cfg, nil
	}

	if cfg.User == "" || cfg.DBName == "" {
		return Config{}, errors.New("database config missing: set DATABASE_URL or DB_USER and DB_NAME")
	}

	return cfg, nil
}

func NewDB(cfg Config) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DSN())
	if err != nil {
		return nil, err
	}

	poolCfg.MaxConns = int32(getEnvInt("DB_MAX_OPEN_CONNS", 25))
	poolCfg.MinConns = int32(getEnvInt("DB_MIN_IDLE_CONNS", 2))
	poolCfg.MaxConnLifetime = time.Duration(getEnvInt("DB_CONN_MAX_LIFETIME_MINUTES", 5)) * time.Minute

	db, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(context.Background()); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

func Connect(envFiles ...string) (*pgxpool.Pool, error) {
	cfg, err := LoadConfig(envFiles...)
	if err != nil {
		return nil, err
	}

	return NewDB(cfg)
}

func (cfg Config) DSN() string {
	if cfg.URL != "" {
		return cleanPGXURL(cfg.URL)
	}

	values := url.Values{}
	values.Set("sslmode", cfg.SSLMode)

	return (&url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(cfg.User, cfg.Password),
		Host:     fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Path:     cfg.DBName,
		RawQuery: values.Encode(),
	}).String()
}

func cleanPGXURL(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}

	values := parsed.Query()
	values.Del("binary_parameters")
	parsed.RawQuery = values.Encode()

	return parsed.String()
}

func getEnv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func getEnvInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	n, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return n
}
