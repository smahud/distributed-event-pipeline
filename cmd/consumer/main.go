package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/smahud/distributed-event-pipeline/pkg/config"
	"github.com/smahud/distributed-event-pipeline/pkg/consumer"
	"github.com/smahud/distributed-event-pipeline/pkg/metrics"
	"github.com/smahud/distributed-event-pipeline/pkg/processor"
	"go.uber.org/zap"
)

func main() {
	// Initialize logger
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	// Load configuration
	cfg, err := config.Load("configs/config.yaml")
	if err != nil {
		logger.Fatal("Failed to load config", zap.Error(err))
	}

	// Initialize metrics
	if cfg.Metrics.Enabled {
		metrics.Init(cfg.Metrics.Port, cfg.Metrics.Path)
		logger.Info("Metrics server started", zap.Int("port", cfg.Metrics.Port))
	}

	// Initialize tracing
	if cfg.Tracing.Enabled {
		shutdown, err := initTracing(cfg)
		if err != nil {
			logger.Warn("Failed to initialize tracing", zap.Error(err))
		} else {
			defer shutdown(context.Background())
			logger.Info("Tracing initialized", zap.String("endpoint", cfg.Tracing.OTLPEndpoint))
		}
	}

	// Create processor
	proc, err := processor.New(cfg, logger)
	if err != nil {
		logger.Fatal("Failed to create processor", zap.Error(err))
	}
	defer proc.Close()

	// Create consumer
	cons, err := consumer.New(cfg, proc, logger)
	if err != nil {
		logger.Fatal("Failed to create consumer", zap.Error(err))
	}

	// Start consumer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := cons.Start(ctx); err != nil && err != context.Canceled {
			logger.Error("Consumer error", zap.Error(err))
		}
	}()

	// HTTP server for health checks
	go startHTTPServer(cfg, logger)

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("Shutting down...")
	cancel()

	// Graceful shutdown timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := cons.Stop(shutdownCtx); err != nil {
		logger.Error("Error during shutdown", zap.Error(err))
	}

	logger.Info("Shutdown complete")
}

func initTracing(cfg *config.Config) (func(context.Context) error, error) {
	// OpenTelemetry tracing initialization
	// Implementation would go here
	return func(context.Context) error { return nil }, nil
}

func startHTTPServer(cfg *config.Config, logger *zap.Logger) {
	// HTTP server for health checks
	// Implementation would go here
}