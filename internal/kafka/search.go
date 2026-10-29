package kafka

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jjk/kafka-keeper/internal/model"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	defaultSearchScan    = 10_000
	defaultSearchResults = 500
)

func (s *Service) SearchMessages(ctx context.Context, conn model.Connection, request SearchRequest) (SearchResult, error) {
	if request.Topic == "" {
		return SearchResult{}, fmt.Errorf("Topic 不能为空")
	}
	if request.Partition < -1 {
		return SearchResult{}, fmt.Errorf("Partition 必须为 -1 或非负整数")
	}
	if !request.Start.IsZero() && !request.End.IsZero() && !request.End.After(request.Start) {
		return SearchResult{}, fmt.Errorf("结束时间必须晚于开始时间")
	}
	if request.OffsetStart != nil && *request.OffsetStart < 0 {
		return SearchResult{}, fmt.Errorf("起始 Offset 不能为负数")
	}
	if request.OffsetEnd != nil && *request.OffsetEnd < 0 {
		return SearchResult{}, fmt.Errorf("结束 Offset 不能为负数")
	}
	if err := ctx.Err(); err != nil {
		return SearchResult{}, safeError("查找消息", err)
	}
	maxScan := request.MaxScan
	if maxScan <= 0 {
		maxScan = defaultSearchScan
	}
	if maxScan > defaultSearchScan {
		maxScan = defaultSearchScan
	}
	maxResults := request.MaxResults
	if maxResults <= 0 {
		maxResults = defaultSearchResults
	}
	if maxResults > defaultSearchResults {
		maxResults = defaultSearchResults
	}
	partitions, err := s.searchPartitions(ctx, conn, request)
	if err != nil {
		return SearchResult{}, err
	}
	result := SearchResult{Messages: make([]Message, 0)}
	for _, partition := range partitions {
		if result.Scanned >= maxScan || len(result.Messages) >= maxResults {
			break
		}
		if err := s.searchPartition(ctx, conn, request, partition, maxScan-result.Scanned, maxResults-len(result.Messages), &result); err != nil {
			return SearchResult{}, err
		}
	}
	if result.Scanned >= maxScan || len(result.Messages) >= maxResults {
		result.Truncated = true
	}
	return result, nil
}

func (s *Service) searchPartitions(ctx context.Context, conn model.Connection, request SearchRequest) ([]int32, error) {
	if request.Partition < -1 {
		return nil, fmt.Errorf("Partition 必须为 -1 或非负整数")
	}
	if request.Partition >= 0 {
		return []int32{request.Partition}, nil
	}
	return s.browsePartitions(ctx, conn, BrowseRequest{
		Topic: request.Topic, AllPartitions: true,
	})
}

func (s *Service) searchPartition(
	ctx context.Context,
	conn model.Connection,
	request SearchRequest,
	partition int32,
	maxScan int,
	maxResults int,
	result *SearchResult,
) error {
	client, err := s.client(ctx, conn)
	if err != nil {
		return err
	}
	admin := kadm.NewClient(client.raw)
	start, err := searchStartOffset(ctx, admin, request, partition)
	if err != nil {
		return err
	}
	end, err := searchEndOffset(ctx, admin, request, partition)
	if err != nil {
		return err
	}
	if start < 0 {
		start = 0
	}
	if end <= start {
		return nil
	}
	available := end - start
	if available > int64(maxScan) {
		available = int64(maxScan)
	}
	credentials, err := s.clientCredentials(conn)
	if err != nil {
		return err
	}
	options, cleanup, err := clientOptions(conn, credentials)
	if err != nil {
		return safeError("创建查找消息客户端", err, credentials.secrets()...)
	}
	defer cleanup()
	options = append(options, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
		request.Topic: {partition: kgo.NewOffset().At(start)},
	}))
	consumer, err := kgo.NewClient(options...)
	if err != nil {
		return safeError("创建查找消息客户端", err, credentials.secrets()...)
	}
	defer consumer.Close()

	remaining := int(available)
	matched := 0
	for remaining > 0 && matched < maxResults {
		fetchMax := remaining
		if fetchMax > 100 {
			fetchMax = 100
		}
		fetches := consumer.PollRecords(ctx, fetchMax)
		if err := fetches.Err(); err != nil {
			return safeError("查找消息", err)
		}
		fetched := 0
		fetches.EachRecord(func(record *kgo.Record) {
			if fetched >= remaining || matched >= maxResults {
				return
			}
			if record.Offset >= end {
				remaining = 0
				return
			}
			fetched++
			result.Scanned++
			if searchMatches(request, record) {
				result.Messages = append(result.Messages, messageFromRecord(record))
				matched++
			}
		})
		remaining -= fetched
		if fetched == 0 && remaining > 0 {
			select {
			case <-ctx.Done():
				return safeError("查找消息", ctx.Err())
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
	return nil
}

func searchStartOffset(ctx context.Context, admin *kadm.Client, request SearchRequest, partition int32) (int64, error) {
	if request.OffsetStart != nil {
		return *request.OffsetStart, nil
	}
	if request.Start.IsZero() {
		return 0, nil
	}
	offsets, err := admin.ListOffsetsAfterMilli(ctx, request.Start.UnixMilli(), request.Topic)
	if err != nil {
		return 0, safeError("按时间定位搜索起点", err)
	}
	offset, ok := offsets.Lookup(request.Topic, partition)
	if !ok || offset.Err != nil {
		if !ok {
			return 0, fmt.Errorf("无法定位 Topic %q Partition %d", request.Topic, partition)
		}
		return 0, safeError("按时间定位搜索起点", offset.Err)
	}
	return offset.Offset, nil
}

func searchEndOffset(ctx context.Context, admin *kadm.Client, request SearchRequest, partition int32) (int64, error) {
	if request.OffsetEnd != nil {
		return *request.OffsetEnd, nil
	}
	if !request.End.IsZero() {
		offsets, err := admin.ListOffsetsAfterMilli(ctx, request.End.UnixMilli(), request.Topic)
		if err != nil {
			return 0, safeError("按时间定位搜索终点", err)
		}
		offset, ok := offsets.Lookup(request.Topic, partition)
		if !ok || offset.Err != nil {
			if !ok {
				return 0, fmt.Errorf("无法定位 Topic %q Partition %d", request.Topic, partition)
			}
			return 0, safeError("按时间定位搜索终点", offset.Err)
		}
		return offset.Offset, nil
	}
	offsets, err := admin.ListEndOffsets(ctx, request.Topic)
	if err != nil {
		return 0, safeError("读取搜索结束 Offset", err)
	}
	offset, ok := offsets.Lookup(request.Topic, partition)
	if !ok || offset.Err != nil {
		if !ok {
			return 0, fmt.Errorf("Topic %q Partition %d 不存在", request.Topic, partition)
		}
		return 0, safeError("读取搜索结束 Offset", offset.Err)
	}
	return offset.Offset, nil
}

func searchMatches(request SearchRequest, record *kgo.Record) bool {
	if request.Key != "" {
		key := string(record.Key)
		if request.KeyContains {
			if !strings.Contains(key, request.Key) {
				return false
			}
		} else if key != request.Key {
			return false
		}
	}
	if request.HeaderKey != "" || request.HeaderValue != "" {
		matched := false
		for _, header := range record.Headers {
			if request.HeaderKey != "" && !strings.Contains(header.Key, request.HeaderKey) {
				continue
			}
			if request.HeaderValue != "" && !strings.Contains(string(header.Value), request.HeaderValue) {
				continue
			}
			matched = true
			break
		}
		if !matched {
			return false
		}
	}
	return true
}
