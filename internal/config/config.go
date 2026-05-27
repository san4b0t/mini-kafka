package config

import (
	"os"
	"strings"
)

type NodeConfig struct {
	ID   string
	Addr string
}

type Config struct {
	BrokerID string
	Port     string
	DataDir  string
	Nodes    map[string]NodeConfig // map node ID to node config
}

func Load() *Config {
	cfg := &Config{
		BrokerID: getEnv("BROKER_ID", "1"),
		Port:     getEnv("PORT", "8081"),
		DataDir:  getEnv("DATA_DIR", "./data"),
		Nodes:    make(map[string]NodeConfig),
	}

	// Format: NODES=1:localhost:8081,2:localhost:8082,3:localhost:8083
	nodesEnv := getEnv("NODES", "1:localhost:8081")
	parts := strings.Split(nodesEnv, ",")
	for _, p := range parts {
		kv := strings.SplitN(p, ":", 2)
		if len(kv) == 2 {
			cfg.Nodes[kv[0]] = NodeConfig{ID: kv[0], Addr: kv[1]}
		}
	}

	return cfg
}

func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return fallback
}
