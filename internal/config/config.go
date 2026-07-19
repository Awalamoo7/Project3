package config

import (
	"fmt"
	"os"
)

// Config holds all runtime configuration, sourced entirely from
// environment variables. No secrets are ever hardcoded.
type Config struct {
	MansaAPIKey       string
	OpenExchangeAppID string
	RedisURL          string
	Port              string
}

const defaultPort = "8080"

// Load reads configuration from the environment and validates that
// required values are present.
func Load() (*Config, error) {
	cfg := &Config{
		MansaAPIKey:       os.Getenv("MANSA_API_KEY"),
		OpenExchangeAppID: os.Getenv("OPENEXCHANGE_APP_ID"),
		RedisURL:          os.Getenv("REDIS_URL"),
		Port:              os.Getenv("PORT"),
	}

	if cfg.Port == "" {
		cfg.Port = defaultPort
	}

	var missing []string
	if cfg.MansaAPIKey == "" {
		missing = append(missing, "MANSA_API_KEY")
	}
	if cfg.OpenExchangeAppID == "" {
		missing = append(missing, "OPENEXCHANGE_APP_ID")
	}
	if cfg.RedisURL == "" {
		missing = append(missing, "REDIS_URL")
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("config: missing required environment variables: %v", missing)
	}

	return cfg, nil
}
