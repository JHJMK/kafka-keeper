package kafka

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jjk/kafka-keeper/internal/model"
	"github.com/twmb/franz-go/pkg/kgo"
)

func (s *Service) Produce(ctx context.Context, conn model.Connection, request ProduceRequest) (ProduceResult, error) {
	if strings.TrimSpace(request.Topic) == "" {
		return ProduceResult{}, fmt.Errorf("Topic 不能为空")
	}
	if !request.AutoPartition && request.Partition < 0 {
		return ProduceResult{}, fmt.Errorf("Partition 必须为非负整数")
	}
	compression, err := producerCompression(request.Compression)
	if err != nil {
		return ProduceResult{}, err
	}

	credentials, err := s.clientCredentials(conn)
	if err != nil {
		return ProduceResult{}, err
	}
	options, cleanup, err := clientOptions(conn, credentials)
	if err != nil {
		return ProduceResult{}, safeError("创建发送消息客户端", err, credentials.secrets()...)
	}
	defer cleanup()
	options = append(options, kgo.ProducerBatchCompression(compression))
	if request.AutoPartition {
		options = append(options, kgo.RecordPartitioner(kgo.StickyPartitioner()))
	} else {
		options = append(options, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	}
	producer, err := kgo.NewClient(options...)
	if err != nil {
		return ProduceResult{}, safeError("创建发送消息客户端", err, credentials.secrets()...)
	}
	defer producer.Close()

	record := &kgo.Record{
		Topic:     request.Topic,
		Partition: request.Partition,
		Key:       append([]byte(nil), request.Key...),
		Value:     append([]byte(nil), request.Value...),
		Headers:   make([]kgo.RecordHeader, 0, len(request.Headers)),
		Timestamp: request.Timestamp,
	}
	if record.Timestamp.IsZero() {
		record.Timestamp = time.Now()
	}
	for _, header := range request.Headers {
		record.Headers = append(record.Headers, kgo.RecordHeader{
			Key:   header.Key,
			Value: append([]byte(nil), header.Value...),
		})
	}

	started := time.Now()
	written, err := producer.ProduceSync(ctx, record).First()
	if err != nil {
		return ProduceResult{}, safeError("发送消息", err, credentials.secrets()...)
	}
	return ProduceResult{
		Topic:     written.Topic,
		Partition: written.Partition,
		Offset:    written.Offset,
		Elapsed:   time.Since(started),
	}, nil
}

func producerCompression(value string) (kgo.CompressionCodec, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "none":
		return kgo.NoCompression(), nil
	case "gzip":
		return kgo.GzipCompression(), nil
	case "snappy":
		return kgo.SnappyCompression(), nil
	case "lz4":
		return kgo.Lz4Compression(), nil
	case "zstd":
		return kgo.ZstdCompression(), nil
	default:
		return kgo.CompressionCodec{}, fmt.Errorf("不支持的压缩方式 %q", value)
	}
}
