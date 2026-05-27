package main

import (
	"log/slog"
	"os"

	"github.com/san4b0t/mini-kafka/internal/broker"
	"github.com/san4b0t/mini-kafka/internal/config"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg := config.Load()

	srv, err := broker.NewServer(cfg)
	if err != nil {
		slog.Error("Failed to initialize server", "error", err)
		os.Exit(1)
	}

	if err := srv.Serve(); err != nil {
		slog.Error("Server crashed", "error", err)
		os.Exit(1)
	}
}
