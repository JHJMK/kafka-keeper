package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func produceSearchRecords(t *testing.T, broker, topic string) {
	t.Helper()
	client := newRawTestClient(t, broker)
	records := []*kgo.Record{
		{
			Topic: topic, Partition: 0, Key: []byte("match-one"), Value: []byte("plain"),
			Timestamp: time.UnixMilli(1_800_000_000_000),
			Headers:   []kgo.RecordHeader{{Key: "env", Value: []byte("prod")}},
		},
		{
			Topic: topic, Partition: 0, Key: []byte("other"), Value: []byte("needle-in-value"),
			Timestamp: time.UnixMilli(1_800_000_000_001),
			Headers:   []kgo.RecordHeader{{Key: "env", Value: []byte("dev")}},
		},
		{
			Topic: topic, Partition: 0, Key: []byte("match-two"), Value: []byte("another"),
			Timestamp: time.UnixMilli(1_800_000_000_002),
			Headers:   []kgo.RecordHeader{{Key: "env", Value: []byte("prod")}},
		},
	}
	if err := client.ProduceSync(context.Background(), records...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

func TestSearchMatchesOnlyKeyAndHeaders(t *testing.T) {
	cluster := newSeededCluster(t, 1, "search")
	broker := cluster.ListenAddrs()[0]
	produceSearchRecords(t, broker, "search")
	service := newIntegrationService(t, broker)
	conn := testConnection("search", broker)

	byKey, err := service.SearchMessages(context.Background(), conn, SearchRequest{
		Topic: "search", Partition: 0, Key: "match", KeyContains: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(byKey.Messages) != 2 || byKey.Scanned != 3 {
		t.Fatalf("key search = %#v", byKey)
	}

	byValue, err := service.SearchMessages(context.Background(), conn, SearchRequest{
		Topic: "search", Partition: 0, Key: "needle-in-value",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(byValue.Messages) != 0 {
		t.Fatalf("Value was searched: %#v", byValue.Messages)
	}

	byHeader, err := service.SearchMessages(context.Background(), conn, SearchRequest{
		Topic: "search", Partition: 0, HeaderKey: "env", HeaderValue: "prod",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(byHeader.Messages) != 2 {
		t.Fatalf("header search = %#v", byHeader)
	}
}

func TestSearchHonorsScanLimitAndCancellation(t *testing.T) {
	cluster := newSeededCluster(t, 1, "search")
	broker := cluster.ListenAddrs()[0]
	produceSearchRecords(t, broker, "search")
	service := newIntegrationService(t, broker)
	conn := testConnection("search", broker)

	result, err := service.SearchMessages(context.Background(), conn, SearchRequest{
		Topic: "search", Partition: 0, MaxScan: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Scanned != 2 || !result.Truncated {
		t.Fatalf("scan limit result = %#v", result)
	}

	result, err = service.SearchMessages(context.Background(), conn, SearchRequest{
		Topic: "search", Partition: 0, MaxResults: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Messages) != 2 || !result.Truncated {
		t.Fatalf("result limit = %#v", result)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = service.SearchMessages(ctx, conn, SearchRequest{Topic: "search", Partition: 0})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled search error = %v", err)
	}
}

func TestSearchAllPartitionsWhenPartitionIsMinusOne(t *testing.T) {
	cluster := newSeededCluster(t, 2, "search")
	broker := cluster.ListenAddrs()[0]
	produceTestRecords(t, broker, "search", 0, "one", "two")
	produceTestRecords(t, broker, "search", 1, "three")
	service := newIntegrationService(t, broker)

	result, err := service.SearchMessages(context.Background(), testConnection("search", broker), SearchRequest{
		Topic: "search", Partition: -1, KeyContains: true, Key: "key-",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Messages) != 3 {
		t.Fatalf("all-partition search = %#v", result)
	}
	partitions := map[int32]bool{}
	for _, message := range result.Messages {
		partitions[message.Partition] = true
	}
	if !partitions[0] || !partitions[1] {
		t.Fatalf("all-partition search missed a partition: %#v", result.Messages)
	}
}

func TestSearchTimeRangeEndIsExclusive(t *testing.T) {
	cluster := newSeededCluster(t, 1, "search")
	broker := cluster.ListenAddrs()[0]
	produceTestRecords(t, broker, "search", 0, "one", "two", "three", "four")
	service := newIntegrationService(t, broker)
	base := time.UnixMilli(1_800_000_000_000)

	result, err := service.SearchMessages(context.Background(), testConnection("search", broker), SearchRequest{
		Topic: "search", Partition: 0, Start: base, End: base.Add(2 * time.Millisecond),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Messages) != 2 || result.Messages[0].Offset != 0 || result.Messages[1].Offset != 1 {
		t.Fatalf("exclusive search end = %#v", result)
	}
}
