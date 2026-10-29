package kafka

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/jjk/kafka-keeper/internal/model"
	"github.com/twmb/franz-go/pkg/kgo"
)

type ConsumerSession struct {
	service     *Service
	client      *kgo.Client
	cleanup     func()
	connID      string
	topic       string
	credentials ClientCredentials

	mu          sync.Mutex
	paused      bool
	closed      bool
	stateChange chan struct{}
	uncommitted map[partitionKey]*kgo.Record
	closeOnce   sync.Once
}

type partitionKey struct {
	topic     string
	partition int32
}

func (s *Service) StartConsumer(ctx context.Context, conn model.Connection, options ConsumerOptions) (*ConsumerSession, error) {
	if strings.TrimSpace(options.GroupID) == "" {
		return nil, fmt.Errorf("Consumer Group 不能为空")
	}
	if strings.TrimSpace(options.Topic) == "" {
		return nil, fmt.Errorf("Topic 不能为空")
	}
	if err := ctx.Err(); err != nil {
		return nil, safeError("启动消费", err)
	}
	credentials, err := s.clientCredentials(conn)
	if err != nil {
		return nil, err
	}
	clientOptions, cleanup, err := clientOptions(conn, credentials)
	if err != nil {
		return nil, safeError("创建消费客户端", err, credentials.secrets()...)
	}
	defer func() {
		if cleanup != nil {
			cleanup()
		}
	}()
	resetOffset := kgo.NewOffset().AtEnd()
	if options.FromStart {
		resetOffset = kgo.NewOffset().AtStart()
	}
	clientOptions = append(clientOptions,
		kgo.ConsumerGroup(options.GroupID),
		kgo.ConsumeTopics(options.Topic),
		kgo.ConsumeResetOffset(resetOffset),
	)
	if !options.AutoCommit {
		clientOptions = append(clientOptions, kgo.DisableAutoCommit())
	}
	client, err := kgo.NewClient(clientOptions...)
	if err != nil {
		return nil, safeError("创建消费客户端", err, credentials.secrets()...)
	}
	session := &ConsumerSession{
		service:     s,
		client:      client,
		cleanup:     cleanup,
		connID:      conn.ID,
		topic:       options.Topic,
		credentials: credentials,
		stateChange: make(chan struct{}),
		uncommitted: make(map[partitionKey]*kgo.Record),
	}
	if err := s.registerConsumer(session); err != nil {
		client.Close()
		return nil, err
	}
	cleanup = nil
	return session, nil
}

func (c *ConsumerSession) Poll(ctx context.Context, maxRecords int) ([]ConsumerRecord, error) {
	if maxRecords <= 0 {
		maxRecords = 100
	}
	for {
		if err := c.waitWhilePaused(ctx); err != nil {
			return nil, err
		}
		fetches := c.client.PollRecords(ctx, maxRecords)
		if err := fetches.Err(); err != nil {
			return nil, safeError("消费消息", err, c.credentials.secrets()...)
		}
		records := make([]ConsumerRecord, 0)
		fetches.EachRecord(func(record *kgo.Record) {
			records = append(records, ConsumerRecord{Message: messageFromRecord(record)})
			c.mu.Lock()
			key := partitionKey{topic: record.Topic, partition: record.Partition}
			current := c.uncommitted[key]
			if current == nil || record.Offset > current.Offset {
				c.uncommitted[key] = record
			}
			c.mu.Unlock()
		})
		if len(records) > 0 {
			return records, nil
		}
		c.mu.Lock()
		paused := c.paused
		c.mu.Unlock()
		if !paused {
			return records, nil
		}
	}
}

func (c *ConsumerSession) waitWhilePaused(ctx context.Context) error {
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return safeError("消费消息", kgo.ErrClientClosed, c.credentials.secrets()...)
		}
		if !c.paused {
			c.mu.Unlock()
			return nil
		}
		changed := c.stateChange
		c.mu.Unlock()

		select {
		case <-ctx.Done():
			return safeError("消费消息", ctx.Err(), c.credentials.secrets()...)
		case <-changed:
		}
	}
}

func (c *ConsumerSession) signalStateChangeLocked() {
	close(c.stateChange)
	c.stateChange = make(chan struct{})
}

func (c *ConsumerSession) Commit(ctx context.Context) error {
	c.mu.Lock()
	records := make([]*kgo.Record, 0, len(c.uncommitted))
	for _, record := range c.uncommitted {
		records = append(records, record)
	}
	c.mu.Unlock()
	if len(records) == 0 {
		return nil
	}
	if err := c.client.CommitRecords(ctx, records...); err != nil {
		return safeError("提交消费进度", err, c.credentials.secrets()...)
	}

	c.mu.Lock()
	for _, record := range records {
		key := partitionKey{topic: record.Topic, partition: record.Partition}
		if c.uncommitted[key] == record {
			delete(c.uncommitted, key)
		}
	}
	c.mu.Unlock()
	return nil
}

func (c *ConsumerSession) Pause() {
	c.client.PauseFetchTopics(c.topic)
	c.mu.Lock()
	if !c.paused {
		c.paused = true
		c.signalStateChangeLocked()
	}
	c.mu.Unlock()
}

func (c *ConsumerSession) Resume() {
	c.client.ResumeFetchTopics(c.topic)
	c.mu.Lock()
	if c.paused {
		c.paused = false
		c.signalStateChangeLocked()
	}
	c.mu.Unlock()
}

func (c *ConsumerSession) Paused() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.paused
}

func (c *ConsumerSession) Close() {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.signalStateChangeLocked()
		c.mu.Unlock()
		if session := c.service.removeConsumer(c.connID, c); session != nil {
			session.closeClient()
		}
	})
}

func (c *ConsumerSession) closeClient() {
	c.client.Close()
	if c.cleanup != nil {
		c.cleanup()
		c.cleanup = nil
	}
}
