package config

import (
	"os"

	toml "github.com/pelletier/go-toml/v2"
)

type Config struct {
	Boring []BoringConfig `toml:"boring"`
}

type BoringConfig struct {
	Name      string           `toml:"name"`
	Mode      string           `toml:"mode"`
	Producer  map[string]any   `toml:"producer"`
	Consumers []map[string]any `toml:"consumer"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := toml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
