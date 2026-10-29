package kafka

import (
	"context"
	"testing"
	"time"
)

func TestBrowseByOffsetAndTimeRange(t *testing.T) {
	cluster := newSeededCluster(t, 2, "orders")
	broker := cluster.ListenAddrs()[0]
	produceTestRecords(t, broker, "orders", 0, "one", "two", "three")
	produceTestRecords(t, broker, "orders", 1, "four", "five")
	service := newIntegrationService(t, broker)
	conn := testConnection("browse", broker)
	base := time.UnixMilli(1_800_000_000_000)

	byOffset, err := service.BrowseMessages(context.Background(), conn, BrowseRequest{
		Topic: "orders", Mode: BrowseByOffset, Partition: 0, Offset: 1, Limit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(byOffset) != 1 || byOffset[0].ValueText() != "two" {
		t.Fatalf("offset browse = %#v", byOffset)
	}

	byRange, err := service.BrowseMessages(context.Background(), conn, BrowseRequest{
		Topic: "orders", Mode: BrowseByRange, AllPartitions: true,
		Start: base.Add(-time.Second), End: base.Add(10 * time.Second), Limit: 500,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(byRange) != 5 {
		t.Fatalf("range browse count = %d, messages = %#v", len(byRange), byRange)
	}
	if byRange[0].Partition != 0 || byRange[0].Offset != 0 {
		t.Fatalf("range browse not sorted: %#v", byRange)
	}
}

func TestBrowseEnforcesMaximumMessageLimit(t *testing.T) {
	cluster := newSeededCluster(t, 1, "many")
	broker := cluster.ListenAddrs()[0]
	values := make([]string, 501)
	for i := range values {
		values[i] = "message"
	}
	produceTestRecords(t, broker, "many", 0, values...)
	service := newIntegrationService(t, broker)

	messages, err := service.BrowseMessages(context.Background(), testConnection("many", broker), BrowseRequest{
		Topic: "many", Mode: BrowseByOffset, Partition: 0, Limit: 900,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 500 {
		t.Fatalf("message count = %d, want 500", len(messages))
	}
}

func TestBrowseAllPartitionsSharesLimitFairly(t *testing.T) {
	cluster := newSeededCluster(t, 2, "orders")
	broker := cluster.ListenAddrs()[0]
	produceTestRecords(t, broker, "orders", 0, "one", "two", "three", "four", "five")
	produceTestRecords(t, broker, "orders", 1, "six", "seven")
	service := newIntegrationService(t, broker)
	base := time.UnixMilli(1_800_000_000_000)

	messages, err := service.BrowseMessages(context.Background(), testConnection("browse", broker), BrowseRequest{
		Topic: "orders", Mode: BrowseByRange, AllPartitions: true,
		Start: base.Add(-time.Second), End: base.Add(10 * time.Second), Limit: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 3 {
		t.Fatalf("message count = %d, want 3: %#v", len(messages), messages)
	}
	counts := map[int32]int{}
	for _, message := range messages {
		counts[message.Partition]++
	}
	if counts[0] == 0 || counts[1] == 0 {
		t.Fatalf("all partitions did not receive a fair share: %#v", messages)
	}
}

func TestBrowseRangeExcludesEndBoundaryAndTerminates(t *testing.T) {
	cluster := newSeededCluster(t, 1, "orders")
	broker := cluster.ListenAddrs()[0]
	produceTestRecords(t, broker, "orders", 0, "one", "two", "three")
	service := newIntegrationService(t, broker)
	base := time.UnixMilli(1_800_000_000_000)

	messages, err := service.BrowseMessages(context.Background(), testConnection("browse", broker), BrowseRequest{
		Topic: "orders", Mode: BrowseByRange, Partition: 0,
		Start: base, End: base.Add(2 * time.Millisecond), Limit: 500,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 || messages[0].Offset != 0 || messages[1].Offset != 1 {
		t.Fatalf("exclusive time range = %#v", messages)
	}
}
