package config

import (
	"os"
	"time"

	"github.com/goccy/go-yaml"
)

type Config struct {
	Kafka   KafkaConfig   `yaml:"kafka"`
	ClickHouse ClickHouseConfig `yaml:"clickhouse"`
	Redis   RedisConfig   `yaml:"redis"`
	Worker  WorkerConfig  `yaml:"worker"`
	Metrics MetricsConfig `yaml:"metrics"`
	Tracing TracingConfig `yaml:"tracing"`
	Logging LoggingConfig `yaml:"logging"`
	HTTP    HTTPConfig    `yaml:"http"`
}

type KafkaConfig struct {
	Brokers        []string         `yaml:"brokers"`
	Topics         KafkaTopics      `yaml:"topics"`
	ConsumerGroup  string           `yaml:"consumer_group"`
	Producer       ProducerConfig   `yaml:"producer"`
	Consumer       ConsumerConfig   `yaml:"consumer"`
}

type KafkaTopics struct {
	Raw       string `yaml:"raw"`
	Enriched  string `yaml:"enriched"`
	DLQ       string `yaml:"dlq"`
}

type ProducerConfig struct {
	Acks            string `yaml:"acks"`
	Retries         int    `yaml:"retries"`
	BatchSize       int    `yaml:"batch_size"`
	LingerMs        int    `yaml:"linger_ms"`
	CompressionType string `yaml:"compression_type"`
}

type ConsumerConfig struct {
	FetchMinBytes       int    `yaml:"fetch_min_bytes"`
	FetchMaxWaitMs      int    `yaml:"fetch_max_wait_ms"`
	MaxPollRecords      int    `yaml:"max_poll_records"`
	SessionTimeoutMs    int    `yaml:"session_timeout_ms"`
	HeartbeatIntervalMs int    `yaml:"heartbeat_interval_ms"`
	AutoOffsetReset     string `yaml:"auto_offset_reset"`
	EnableAutoCommit    bool   `yaml:"enable_auto_commit"`
}

type ClickHouseConfig struct {
	DSN             string        `yaml:"dsn"`
	Database        string        `yaml:"database"`
	BatchSize       int           `yaml:"batch_size"`
	FlushInterval   time.Duration `yaml:"flush_interval"`
	MaxOpenConns    int           `yaml:"max_open_conns"`
	MaxIdleConns    int           `yaml:"max_idle_conns"`
	ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime"`
}

type RedisConfig struct {
	Address        string        `yaml:"address"`
	Password       string        `yaml:"password"`
	DB             int           `yaml:"db"`
	PoolSize       int           `yaml:"pool_size"`
	DialTimeout    time.Duration `yaml:"dial_timeout"`
	ReadTimeout    time.Duration `yaml:"read_timeout"`
	WriteTimeout   time.Duration `yaml:"write_timeout"`
}

type WorkerConfig struct {
	Concurrency       int           `yaml:"concurrency"`
	BatchTimeout      time.Duration `yaml:"batch_timeout"`
	MaxRetries        int           `yaml:"max_retries"`
	RetryBackoff      time.Duration `yaml:"retry_backoff"`
	DeadLetterTopic   string        `yaml:"dead_letter_topic"`
}

type MetricsConfig struct {
	Enabled bool   `yaml:"enabled"`
	Port    int    `yaml:"port"`
	Path    string `yaml:"path"`
}

type TracingConfig struct {
	Enabled      bool    `yaml:"enabled"`
	OTLPEndpoint string  `yaml:"otlp_endpoint"`
	ServiceName  string  `yaml:"service_name"`
	SampleRate   float64 `yaml:"sample_rate"`
}

type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
	Output string `yaml:"output"`
}

type HTTPConfig struct {
	Address      string        `yaml:"address"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
	IdleTimeout  time.Duration `yaml:"idle_timeout"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	// Set defaults
	setDefaults(&cfg)

	return &cfg, nil
}

func setDefaults(cfg *Config) {
	if len(cfg.Kafka.Brokers) == 0 {
		cfg.Kafka.Brokers = []string{"kafka:9092"}
	}
	if cfg.Kafka.Topics.Raw == "" {
		cfg.Kafka.Topics.Raw = "events.raw"
	}
	if cfg.Kafka.Topics.Enriched == "" {
		cfg.Kafka.Topics.Enriched = "events.enriched"
	}
	if cfg.Kafka.Topics.DLQ == "" {
		cfg.Kafka.Topics.DLQ = "events.dlq"
	}
	if cfg.Kafka.ConsumerGroup == "" {
		cfg.Kafka.ConsumerGroup = "pipeline-workers"
	}
	if cfg.ClickHouse.DSN == "" {
		cfg.ClickHouse.DSN = "clickhouse://default:@clickhouse:9000/events"
	}
	if cfg.ClickHouse.BatchSize == 0 {
		cfg.ClickHouse.BatchSize = 10000
	}
	if cfg.ClickHouse.FlushInterval == 0 {
		cfg.ClickHouse.FlushInterval = 5 * time.Second
	}
	if cfg.Redis.Address == "" {
		cfg.Redis.Address = "redis:6379"
	}
	if cfg.Worker.Concurrency == 0 {
		cfg.Worker.Concurrency = 32
	}
	if cfg.Worker.BatchTimeout == 0 {
		cfg.Worker.BatchTimeout = 100 * time.Millisecond
	}
	if cfg.Worker.MaxRetries == 0 {
		cfg.Worker.MaxRetries = 3
	}
	if cfg.Worker.RetryBackoff == 0 {
		cfg.Worker.RetryBackoff = time.Second
	}
	if cfg.Metrics.Path == "" {
		cfg.Metrics.Path = "/metrics"
	}
	if cfg.Tracing.ServiceName == "" {
		cfg.Tracing.ServiceName = "distributed-event-pipeline"
	}
	if cfg.HTTP.Address == "" {
		cfg.HTTP.Address = ":8080"
	}
}