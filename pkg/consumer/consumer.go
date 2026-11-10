package consumer

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/smahud/distributed-event-pipeline/pkg/config"
	"github.com/smahud/distributed-event-pipeline/pkg/processor"
	"github.com/smahud/distributed-event-pipeline/pkg/metrics"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type Consumer struct {
	cfg       *config.Config
	processor *processor.Processor
	logger    *zap.Logger
	reader    *kafka.Reader
	dlqWriter *kafka.Writer
	wg        sync.WaitGroup
	running   bool
	mu        sync.Mutex
}

func New(cfg *config.Config, proc *processor.Processor, logger *zap.Logger) (*Consumer, error) {
	// Create Kafka reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Kafka.Brokers,
		Topic:          cfg.Kafka.Topics.Raw,
		GroupID:        cfg.Kafka.ConsumerGroup,
		MinBytes:       cfg.Kafka.Consumer.FetchMinBytes,
		MaxBytes:       10e6, // 10MB
		MaxWait:        time.Duration(cfg.Kafka.Consumer.FetchMaxWaitMs) * time.Millisecond,
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset,
		RetryBackoff:   time.Duration(cfg.Kafka.Consumer.RetryBackoff) * time.Millisecond,
	})

	// Create DLQ writer
	dlqWriter := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Kafka.Brokers...),
		Topic:        cfg.Kafka.Topics.DLQ,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}

	return &Consumer{
		cfg:       cfg,
		processor: proc,
		logger:    logger,
		reader:    reader,
		dlqWriter: dlqWriter,
	}, nil
}

func (c *Consumer) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return errors.New("consumer already running")
	}
	c.running = true
	c.mu.Unlock()

	c.logger.Info("Starting consumer",
		zap.String("topic", c.cfg.Kafka.Topics.Raw),
		zap.String("group", c.cfg.Kafka.ConsumerGroup),
		zap.Int("concurrency", c.cfg.Worker.Concurrency),
	)

	// Start worker pool
	for i := 0; i < c.cfg.Worker.Concurrency; i++ {
		c.wg.Add(1)
		go c.worker(ctx, i)
	}

	// Start metrics reporter
	c.wg.Add(1)
	go c.reportMetrics(ctx)

	// Wait for context cancellation
	<-ctx.Done()
	c.logger.Info("Consumer context cancelled, stopping...")

	return nil
}

func (c *Consumer) worker(ctx context.Context, id int) {
	defer c.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			c.processBatch(ctx, id)
		}
	}
}

func (c *Consumer) processBatch(ctx context.Context, workerID int) {
	// Read batch of messages
	batch := make([]kafka.Message, 0, c.cfg.Worker.Concurrency)

	// Read first message (blocking)
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			c.logger.Error("Failed to fetch message", zap.Error(err), zap.Int("worker", workerID))
			metrics.ConsumerErrorsTotal.Inc()
		}
		return
	}
	batch = append(batch, msg)

	// Try to read more messages (non-blocking)
	for len(batch) < c.cfg.Worker.Concurrency {
		select {
		case <-ctx.Done():
			break
		default:
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				break
			}
			batch = append(batch, msg)
		}
	}

	if len(batch) == 0 {
		return
	}

	// Process batch
	start := time.Now()
	results := c.processor.ProcessBatch(ctx, batch)
	duration := time.Since(start)

	// Record metrics
	metrics.ConsumerBatchSize.Observe(float64(len(batch)))
	metrics.ConsumerBatchDuration.Observe(duration.Seconds())

	// Commit successful messages, send failed to DLQ
	for i, result := range results {
		msg := batch[i]
		if result.Success {
			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				c.logger.Error("Failed to commit message", zap.Error(err))
				metrics.ConsumerCommitErrorsTotal.Inc()
			}
			metrics.ConsumerMessagesProcessedTotal.Inc()
		} else {
			// Send to DLQ
			dlqMsg := kafka.Message{
				Key:   msg.Key,
				Value: msg.Value,
				Headers: append(msg.Headers,
					kafka.Header{Key: "dlq-reason", Value: []byte(result.Error)},
					kafka.Header{Key: "dlq-original-topic", Value: []byte(c.cfg.Kafka.Topics.Raw)},
					kafka.Header{Key: "dlq-worker-id", Value: []byte(string(rune(workerID)))},
				),
			}
			if err := c.dlqWriter.WriteMessages(ctx, dlqMsg); err != nil {
				c.logger.Error("Failed to write to DLQ", zap.Error(err))
				metrics.ConsumerDLQErrorsTotal.Inc()
			} else {
				metrics.ConsumerDLQMessagesTotal.Inc()
			}

			// Still commit to avoid reprocessing
			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				c.logger.Error("Failed to commit DLQ message", zap.Error(err))
			}
		}
	}
}

func (c *Consumer) reportMetrics(ctx context.Context) {
	defer c.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			lag, err := c.reader.Lag()
			if err == nil {
				metrics.ConsumerLag.Set(float64(lag))
			}
			c.logger.Debug("Consumer metrics",
				zap.Int64("lag", lag),
				zap.Int("concurrency", c.cfg.Worker.Concurrency),
			)
		}
	}
}

func (c *Consumer) Stop(ctx context.Context) error {
	c.mu.Lock()
	c.running = false
	c.mu.Unlock()

	c.logger.Info("Stopping consumer...")

	// Close reader
	if err := c.reader.Close(); err != nil {
		c.logger.Error("Error closing reader", zap.Error(err))
		return err
	}

	// Close DLQ writer
	if err := c.dlqWriter.Close(); err != nil {
		c.logger.Error("Error closing DLQ writer", zap.Error(err))
		return err
	}

	// Wait for workers with timeout
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		c.logger.Info("Consumer stopped gracefully")
		return nil
	case <-ctx.Done():
		c.logger.Warn("Consumer shutdown timeout")
		return ctx.Err()
	}
}