package kafka

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/jjk/kafka-keeper/internal/model"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const maxBrowseMessages = 500

func (s *Service) BrowseMessages(ctx context.Context, conn model.Connection, request BrowseRequest) ([]Message, error) {
	if request.Topic == "" {
		return nil, fmt.Errorf("Topic 不能为空")
	}
	if request.Mode == BrowseByRange {
		if request.Start.IsZero() || request.End.IsZero() {
			return nil, fmt.Errorf("时间范围不能为空")
		}
		if !request.End.After(request.Start) {
			return nil, fmt.Errorf("结束时间必须晚于开始时间")
		}
	}
	limit := request.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > maxBrowseMessages {
		limit = maxBrowseMessages
	}
	partitions, err := s.browsePartitions(ctx, conn, request)
	if err != nil {
		return nil, err
	}
	limits := fairPartitionLimits(partitions, limit)
	all := make([]Message, 0, limit)
	for _, partition := range partitions {
		partitionLimit := limits[partition]
		if partitionLimit <= 0 {
			continue
		}
		messages, err := s.browsePartition(ctx, conn, request, partition, partitionLimit)
		if err != nil {
			return nil, err
		}
		all = append(all, messages...)
	}
	slices.SortFunc(all, func(a, b Message) int {
		if a.Partition != b.Partition {
			if a.Partition < b.Partition {
				return -1
			}
			return 1
		}
		if a.Offset < b.Offset {
			return -1
		}
		if a.Offset > b.Offset {
			return 1
		}
		return 0
	})
	return all, nil
}

func fairPartitionLimits(partitions []int32, total int) map[int32]int {
	limits := make(map[int32]int, len(partitions))
	if total <= 0 || len(partitions) == 0 {
		return limits
	}
	base := total / len(partitions)
	remainder := total % len(partitions)
	for index, partition := range partitions {
		limits[partition] = base
		if index < remainder {
			limits[partition]++
		}
	}
	return limits
}

func (s *Service) browsePartitions(ctx context.Context, conn model.Connection, request BrowseRequest) ([]int32, error) {
	if !request.AllPartitions {
		if request.Partition < 0 {
			return nil, fmt.Errorf("Partition 不能为负数")
		}
		return []int32{request.Partition}, nil
	}
	client, err := s.client(ctx, conn)
	if err != nil {
		return nil, err
	}
	metadata, err := kadm.NewClient(client.raw).Metadata(ctx, request.Topic)
	if err != nil {
		return nil, safeError("读取 Topic 分区", err)
	}
	topic, ok := metadata.Topics[request.Topic]
	if !ok || topic.Err != nil {
		if !ok {
			return nil, fmt.Errorf("Topic %q 不存在", request.Topic)
		}
		return nil, safeError("读取 Topic 分区", topic.Err)
	}
	partitions := topic.Partitions.Numbers()
	slices.Sort(partitions)
	return partitions, nil
}

func (s *Service) browsePartition(ctx context.Context, conn model.Connection, request BrowseRequest, partition int32, limit int) ([]Message, error) {
	client, err := s.client(ctx, conn)
	if err != nil {
		return nil, err
	}
	admin := kadm.NewClient(client.raw)
	start, err := browseStartOffset(ctx, admin, request, partition)
	if err != nil {
		return nil, err
	}
	end, err := browseEndOffset(ctx, admin, request, partition)
	if err != nil {
		return nil, err
	}
	if end <= start {
		return []Message{}, nil
	}
	expected := limit
	if end-start < int64(limit) {
		expected = int(end - start)
	}

	credentials, err := s.clientCredentials(conn)
	if err != nil {
		return nil, err
	}
	options, cleanup, err := clientOptions(conn, credentials)
	if err != nil {
		return nil, safeError("创建浏览消息客户端", err, credentials.secrets()...)
	}
	defer cleanup()
	options = append(options, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
		request.Topic: {partition: kgo.NewOffset().At(start)},
	}))
	consumer, err := kgo.NewClient(options...)
	if err != nil {
		return nil, safeError("创建浏览消息客户端", err, credentials.secrets()...)
	}
	defer consumer.Close()

	messages := make([]Message, 0, expected)
	remaining := expected
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return nil, safeError("浏览消息", err)
		}
		fetchMax := remaining
		if fetchMax > 100 {
			fetchMax = 100
		}
		fetches := consumer.PollRecords(ctx, fetchMax)
		if err := fetches.Err(); err != nil {
			return nil, safeError("浏览消息", err)
		}
		rawSeen := 0
		reachedBoundary := false
		fetches.EachRecord(func(record *kgo.Record) {
			if reachedBoundary {
				return
			}
			if record.Offset >= end || (request.Mode == BrowseByRange && !record.Timestamp.Before(request.End)) {
				reachedBoundary = true
				return
			}
			rawSeen++
			if len(messages) >= limit {
				reachedBoundary = true
				return
			}
			messages = append(messages, messageFromRecord(record))
		})
		if reachedBoundary {
			return messages, nil
		}
		remaining -= rawSeen
		if remaining > 0 && rawSeen == 0 {
			select {
			case <-ctx.Done():
				return nil, safeError("浏览消息", ctx.Err())
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
	return messages, nil
}

func browseEndOffset(ctx context.Context, admin *kadm.Client, request BrowseRequest, partition int32) (int64, error) {
	if request.Mode == BrowseByRange {
		offsets, err := admin.ListOffsetsAfterMilli(ctx, request.End.UnixMilli(), request.Topic)
		if err != nil {
			return 0, safeError("按结束时间定位 Offset", err)
		}
		offset, ok := offsets.Lookup(request.Topic, partition)
		if !ok || offset.Err != nil {
			if !ok {
				return 0, fmt.Errorf("无法定位 Topic %q Partition %d", request.Topic, partition)
			}
			return 0, safeError("按结束时间定位 Offset", offset.Err)
		}
		return offset.Offset, nil
	}
	offsets, err := admin.ListEndOffsets(ctx, request.Topic)
	if err != nil {
		return 0, safeError("读取结束 Offset", err)
	}
	offset, ok := offsets.Lookup(request.Topic, partition)
	if !ok {
		return 0, fmt.Errorf("Topic %q Partition %d 不存在", request.Topic, partition)
	}
	if offset.Err != nil {
		return 0, safeError("读取结束 Offset", offset.Err)
	}
	return offset.Offset, nil
}

func browseStartOffset(ctx context.Context, admin *kadm.Client, request BrowseRequest, partition int32) (int64, error) {
	switch request.Mode {
	case BrowseByOffset:
		if request.Offset < 0 {
			return 0, fmt.Errorf("Offset 不能为负数")
		}
		return request.Offset, nil
	case BrowseByTime, BrowseByRange:
		if request.Start.IsZero() {
			return 0, fmt.Errorf("时间范围不能为空")
		}
		offsets, err := admin.ListOffsetsAfterMilli(ctx, request.Start.UnixMilli(), request.Topic)
		if err != nil {
			return 0, safeError("按时间定位 Offset", err)
		}
		offset, ok := offsets.Lookup(request.Topic, partition)
		if !ok || offset.Err != nil {
			if !ok {
				return 0, fmt.Errorf("无法定位 Topic %q Partition %d", request.Topic, partition)
			}
			return 0, safeError("按时间定位 Offset", offset.Err)
		}
		return offset.Offset, nil
	default:
		return 0, fmt.Errorf("不支持的浏览模式 %q", request.Mode)
	}
}

func messageFromRecord(record *kgo.Record) Message {
	headers := make([]Header, 0, len(record.Headers))
	for _, header := range record.Headers {
		headers = append(headers, Header{
			Key:   header.Key,
			Value: append([]byte(nil), header.Value...),
		})
	}
	return Message{
		Topic:     record.Topic,
		Partition: record.Partition,
		Offset:    record.Offset,
		Timestamp: record.Timestamp,
		Key:       append([]byte(nil), record.Key...),
		Value:     append([]byte(nil), record.Value...),
		Headers:   headers,
	}
}
