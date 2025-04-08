package config

import (
	"fmt"
	"github.com/caarlos0/env/v9"
	"github.com/joho/godotenv"
)

type Config struct {
	NodesAddresses []string `env:"NODES_ADDRESSES,required"`
	NodesIds       []string `env:"NODES_IDS,required"`
}

func NewConfiguration(filePath string) (*Config, error) {
	if err := godotenv.Load(filePath); err != nil {
		return nil, fmt.Errorf("failed to load environment variables: %w", err)
	}

	cfg := &Config{}

	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("failed to parse environment variables: %w", err)
	}

	return cfg, nil
}
