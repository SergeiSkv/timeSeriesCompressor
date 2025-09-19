package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Timestamp string        `yaml:"timestamp"`
	Values    []string      `yaml:"values"`
	GroupBy   []string      `yaml:"groupby"`
	Unique    []string      `yaml:"unique"`
	Method    string        `yaml:"method"`
	Window    time.Duration `yaml:"window"`
	Workers   int           `yaml:"workers"`
	NATS      NATSConfig    `yaml:"nats"`
}

type NATSConfig struct {
	URL           string `yaml:"url"`
	Subject       string `yaml:"subject"`
	Queue         string `yaml:"queue"`
	OutputSubject string `yaml:"output_subject"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		return nil, err
	}

	// Defaults
	if cfg.Timestamp == "" {
		cfg.Timestamp = "timestamp"
	}
	if len(cfg.Values) == 0 {
		cfg.Values = []string{"value"}
	}
	if cfg.Method == "" {
		cfg.Method = "sum"
	}
	if cfg.Window == 0 {
		cfg.Window = time.Minute
	}
	if cfg.Workers == 0 {
		cfg.Workers = 4
	}
	if cfg.NATS.URL == "" {
		cfg.NATS.URL = "nats://localhost:4222"
	}
	if cfg.NATS.Subject == "" {
		cfg.NATS.Subject = "timeseries.raw"
	}
	if cfg.NATS.Queue == "" {
		cfg.NATS.Queue = "compressor"
	}
	if cfg.NATS.OutputSubject == "" {
		cfg.NATS.OutputSubject = "timeseries.compressed"
	}

	return &cfg, nil
}