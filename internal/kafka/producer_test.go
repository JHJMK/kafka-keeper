package kafka

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestProduceWritesMessageFieldsToSelectedPartition(t *testing.T) {
	cluster := newSeededCluster(t, 2, "orders")
	broker := cluster.ListenAddrs()[0]
	service := newIntegrationService(t, broker)
	conn := testConnection("producer", broker)
	timestamp := time.UnixMilli(1_800_000_000_123)

	result, err := service.Produce(context.Background(), conn, ProduceRequest{
		Topic:       "orders",
		Partition:   1,
		Key:         []byte("order-42"),
		Value:       []byte(`{"status":"paid"}`),
		Headers:     []Header{{Key: "source", Value: []byte("test")}},
		Compression: "zstd",
		Timestamp:   timestamp,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Topic != "orders" || result.Partition != 1 || result.Offset != 0 {
		t.Fatalf("Produce() = %#v", result)
	}
	if result.Elapsed <= 0 {
		t.Fatalf("elapsed = %v", result.Elapsed)
	}

	messages, err := service.BrowseMessages(context.Background(), conn, BrowseRequest{
		Topic: "orders", Mode: BrowseByOffset, Partition: 1, Offset: 0, Limit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 {
		t.Fatalf("messages = %#v", messages)
	}
	message := messages[0]
	if !bytes.Equal(message.Key, []byte("order-42")) || !bytes.Equal(message.Value, []byte(`{"status":"paid"}`)) {
		t.Fatalf("produced key/value = %q/%q", message.Key, message.Value)
	}
	if len(message.Headers) != 1 || message.Headers[0].Key != "source" || string(message.Headers[0].Value) != "test" {
		t.Fatalf("produced headers = %#v", message.Headers)
	}
	if !message.Timestamp.Equal(timestamp) {
		t.Fatalf("timestamp = %v, want %v", message.Timestamp, timestamp)
	}
}

func TestProduceAutoPartitionsMessage(t *testing.T) {
	cluster := newSeededCluster(t, 2, "orders")
	broker := cluster.ListenAddrs()[0]
	service := newIntegrationService(t, broker)

	result, err := service.Produce(context.Background(), testConnection("auto-producer", broker), ProduceRequest{
		Topic:         "orders",
		AutoPartition: true,
		Value:         []byte("automatic"),
		Compression:   "none",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Partition < 0 || result.Partition > 1 {
		t.Fatalf("partition = %d", result.Partition)
	}
	raw := newRawTestClient(t, broker)
	record, err := raw.ProduceSync(context.Background(), &kgo.Record{
		Topic: "orders", Partition: 0, Value: []byte("control"),
	}).First()
	if err != nil || record == nil {
		t.Fatalf("control produce: %v", err)
	}
}

func TestProduceRejectsInvalidRequests(t *testing.T) {
	service := newIntegrationService(t, "127.0.0.1:1")
	conn := testConnection("invalid-producer", "127.0.0.1:1")
	tests := []struct {
		name    string
		request ProduceRequest
		want    string
	}{
		{name: "empty topic", request: ProduceRequest{Value: []byte("x")}, want: "Topic"},
		{name: "negative partition", request: ProduceRequest{Topic: "orders", Partition: -1, Value: []byte("x")}, want: "Partition"},
		{name: "unknown compression", request: ProduceRequest{Topic: "orders", AutoPartition: true, Compression: "brotli"}, want: "压缩"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := service.Produce(context.Background(), conn, test.request)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want containing %q", err, test.want)
			}
		})
	}
}
