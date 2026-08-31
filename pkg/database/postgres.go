package database

import (
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
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

func NewDB(cfg Config) (*sql.DB, error) {
	db, err := sql.Open("postgres", cfg.DSN())
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(getEnvInt("DB_MAX_OPEN_CONNS", 25))
	db.SetMaxIdleConns(getEnvInt("DB_MAX_IDLE_CONNS", 25))
	db.SetConnMaxLifetime(time.Duration(getEnvInt("DB_CONN_MAX_LIFETIME_MINUTES", 5)) * time.Minute)

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}

func Connect(envFiles ...string) (*sql.DB, error) {
	cfg, err := LoadConfig(envFiles...)
	if err != nil {
		return nil, err
	}

	return NewDB(cfg)
}

func (cfg Config) DSN() string {
	if cfg.URL != "" {
		return cfg.URL
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
