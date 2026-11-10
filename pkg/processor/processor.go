package processor

import (
	"context"
	"encoding/json"
	"time"

	"github.com/smahud/distributed-event-pipeline/pkg/config"
	"github.com/smahud/distributed-event-pipeline/pkg/metrics"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type Event struct {
	ID        string                 `json:"id"`
	Timestamp time.Time              `json:"timestamp"`
	Type      string                 `json:"type"`
	Payload   map[string]interface{} `json:"payload"`
	Metadata  map[string]string      `json:"metadata,omitempty"`
}

type ProcessResult struct {
	Success bool
	Error   string
}

type Processor struct {
	cfg       *config.Config
	logger    *zap.Logger
	chClient  driver.Conn
	redisClient *redis.Client
	batchBuffer []Event
}

func New(cfg *config.Config, logger *zap.Logger) (*Processor, error) {
	// Initialize ClickHouse connection
	chClient, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.ClickHouse.DSN},
		Auth: clickhouse.Auth{
			Database: cfg.ClickHouse.Database,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout:     10 * time.Second,
		MaxOpenConns:    cfg.ClickHouse.MaxOpenConns,
		MaxIdleConns:    cfg.ClickHouse.MaxIdleConns,
		ConnMaxLifetime: cfg.ClickHouse.ConnMaxLifetime,
		Compression:     &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
	})
	if err != nil {
		return nil, err
	}

	// Test connection
	if err := chClient.Ping(context.Background()); err != nil {
		return nil, err
	}

	// Initialize Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr:         cfg.Redis.Address,
		Password:     cfg.Redis.Password,
		DB:           cfg.Redis.DB,
		PoolSize:     cfg.Redis.PoolSize,
		DialTimeout:  cfg.Redis.DialTimeout,
		ReadTimeout:  cfg.Redis.ReadTimeout,
		WriteTimeout: cfg.Redis.WriteTimeout,
	})

	// Test Redis connection
	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		logger.Warn("Redis connection failed", zap.Error(err))
		// Don't fail, Redis is optional for caching
	}

	// Create tables if not exist
	if err := createTables(context.Background(), chClient); err != nil {
		return nil, err
	}

	return &Processor{
		cfg:         cfg,
		logger:      logger,
		chClient:    chClient,
		redisClient: redisClient,
		batchBuffer: make([]Event, 0, cfg.Worker.Concurrency),
	}, nil
}

func createTables(ctx context.Context, chClient driver.Conn) error {
	queries := []string{
		`CREATE DATABASE IF NOT EXISTS events`,
		`CREATE TABLE IF NOT EXISTS events.raw (
			id String,
			timestamp DateTime64(3),
			type String,
			payload String,
			metadata String,
			received_at DateTime64(3) DEFAULT now()
		) ENGINE = MergeTree()
		PARTITION BY toDate(timestamp)
		ORDER BY (type, timestamp)
		TTL timestamp + INTERVAL 30 DAY
		SETTINGS index_granularity = 8192`,
		`CREATE TABLE IF NOT EXISTS events.enriched (
			id String,
			timestamp DateTime64(3),
			type String,
			payload String,
			metadata String,
			enriched_at DateTime64(3) DEFAULT now()
		) ENGINE = MergeTree()
		PARTITION BY toDate(timestamp)
		ORDER BY (type, timestamp)
		TTL timestamp + INTERVAL 90 DAY
		SETTINGS index_granularity = 8192`,
		`CREATE TABLE IF NOT EXISTS events.dlq (
			id String,
			timestamp DateTime64(3),
			type String,
			payload String,
			metadata String,
			error String,
			original_topic String,
			failed_at DateTime64(3) DEFAULT now()
		) ENGINE = MergeTree()
		PARTITION BY toDate(failed_at)
		ORDER BY failed_at
		TTL failed_at + INTERVAL 7 DAY
		SETTINGS index_granularity = 8192`,
	}

	for _, q := range queries {
		if err := chClient.Exec(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

func (p *Processor) ProcessBatch(ctx context.Context, messages []kafka.Message) []ProcessResult {
	results := make([]ProcessResult, len(messages))

	for i, msg := range messages {
		// Parse event
		var event Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			results[i] = ProcessResult{Success: false, Error: "invalid JSON: " + err.Error()}
			metrics.ProcessorParseErrorsTotal.Inc()
			continue
		}

		// Validate event
		if event.ID == "" {
			event.ID = generateID()
		}
		if event.Timestamp.IsZero() {
			event.Timestamp = time.Now()
		}

		// Enrich event
		enrichedEvent, err := p.enrichEvent(ctx, event)
		if err != nil {
			results[i] = ProcessResult{Success: false, Error: "enrichment failed: " + err.Error()}
			metrics.ProcessorEnrichmentErrorsTotal.Inc()
			continue
		}

		// Store raw event
		if err := p.storeRawEvent(ctx, event); err != nil {
			results[i] = ProcessResult{Success: false, Error: "raw storage failed: " + err.Error()}
			metrics.ProcessorStorageErrorsTotal.Inc()
			continue
		}

		// Store enriched event
		if err := p.storeEnrichedEvent(ctx, enrichedEvent); err != nil {
			results[i] = ProcessResult{Success: false, Error: "enriched storage failed: " + err.Error()}
			metrics.ProcessorStorageErrorsTotal.Inc()
			continue
		}

		results[i] = ProcessResult{Success: true}
		metrics.ProcessorEventsProcessedTotal.Inc()
	}

	return results
}

func (p *Processor) enrichEvent(ctx context.Context, event Event) (Event, error) {
	enriched := event

	// Add processing metadata
	if enriched.Metadata == nil {
		enriched.Metadata = make(map[string]string)
	}
	enriched.Metadata["processed_at"] = time.Now().Format(time.RFC3339Nano)
	enriched.Metadata["processor_version"] = "1.0.0"

	// Lookup enrichment data from Redis (if available)
	if p.redisClient != nil {
		if data, err := p.redisClient.Get(ctx, "enrich:"+event.Type).Result(); err == nil {
			var enrichData map[string]interface{}
			if json.Unmarshal([]byte(data), &enrichData) == nil {
				for k, v := range enrichData {
					enriched.Payload["enriched_"+k] = v
				}
			}
		}
	}

	return enriched, nil
}

func (p *Processor) storeRawEvent(ctx context.Context, event Event) error {
	payload, _ := json.Marshal(event.Payload)
	metadata, _ := json.Marshal(event.Metadata)

	return p.chClient.Exec(ctx,
		`INSERT INTO events.raw (id, timestamp, type, payload, metadata) VALUES (?, ?, ?, ?, ?)`,
		event.ID, event.Timestamp, event.Type, string(payload), string(metadata),
	)
}

func (p *Processor) storeEnrichedEvent(ctx context.Context, event Event) error {
	payload, _ := json.Marshal(event.Payload)
	metadata, _ := json.Marshal(event.Metadata)

	return p.chClient.Exec(ctx,
		`INSERT INTO events.enriched (id, timestamp, type, payload, metadata) VALUES (?, ?, ?, ?, ?)`,
		event.ID, event.Timestamp, event.Type, string(payload), string(metadata),
	)
}

func (p *Processor) Close() error {
	if p.chClient != nil {
		return p.chClient.Close()
	}
	if p.redisClient != nil {
		return p.redisClient.Close()
	}
	return nil
}

func generateID() string {
	return time.Now().Format("20060102150405.000000") + "-" + randomString(8)
}

func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[time.Now().UnixNano()%int64(len(letters))]
		time.Sleep(time.Nanosecond)
	}
	return string(b)
}