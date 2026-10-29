package kafka

import (
	"context"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestMetadataListsTopicsOverviewAndPartitionOffsets(t *testing.T) {
	cluster := newSeededCluster(t, 2, "orders")
	broker := cluster.ListenAddrs()[0]
	produceTestRecords(t, broker, "orders", 0, "one", "two", "three")
	service := newIntegrationService(t, broker)
	conn := testConnection("metadata", broker)

	topics, err := service.ListTopics(context.Background(), conn)
	if err != nil {
		t.Fatal(err)
	}
	if len(topics) != 1 || topics[0].Name != "orders" || topics[0].PartitionCount != 2 {
		t.Fatalf("ListTopics() = %#v", topics)
	}

	overview, err := service.LoadOverview(context.Background(), conn)
	if err != nil {
		t.Fatal(err)
	}
	if overview.TopicCount != 1 || len(overview.Brokers) != 1 {
		t.Fatalf("LoadOverview() = %#v", overview)
	}
	if overview.Brokers[0].Controller != true {
		t.Fatalf("controller broker missing: %#v", overview.Brokers)
	}

	detail, err := service.LoadTopicDetail(context.Background(), conn, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if detail.Summary.Name != "orders" || len(detail.Partitions) != 2 {
		t.Fatalf("LoadTopicDetail() = %#v", detail)
	}
	if detail.Partitions[0].StartOffset != 0 || detail.Partitions[0].EndOffset != 3 {
		t.Fatalf("partition offsets = %#v", detail.Partitions[0])
	}
	if detail.Partitions[0].Leader < 0 || len(detail.Partitions[0].Replicas) == 0 {
		t.Fatalf("partition topology missing: %#v", detail.Partitions[0])
	}
	if len(detail.Configs) == 0 {
		t.Fatal("expected topic configs")
	}
}

func TestLoadTopicDetailKeepsMetadataWhenConfigsAreForbidden(t *testing.T) {
	cluster := newSeededCluster(t, 2, "orders")
	broker := cluster.ListenAddrs()[0]
	produceTestRecords(t, broker, "orders", 0, "one", "two", "three")
	cluster.Fault(kfake.Fault{
		Keys:  []kmsg.Key{kmsg.DescribeConfigs},
		Topic: "orders",
		Err:   kerr.TopicAuthorizationFailed,
		Count: -1,
	})
	service := newIntegrationService(t, broker)

	detail, err := service.LoadTopicDetail(context.Background(), testConnection("metadata", broker), "orders")
	if err != nil {
		t.Fatal(err)
	}
	if detail.ConfigError == "" {
		t.Fatal("expected a non-fatal topic config error")
	}
	if !strings.Contains(detail.ConfigError, `Topic "orders"`) ||
		!strings.Contains(detail.ConfigError, "DESCRIBE_CONFIGS") ||
		!strings.Contains(detail.ConfigError, "TOPIC_AUTHORIZATION_FAILED") {
		t.Fatalf("config error is not actionable: %q", detail.ConfigError)
	}
	if detail.Summary.PartitionCount != 2 || len(detail.Partitions) != 2 {
		t.Fatalf("partition metadata was lost: %#v", detail)
	}
	if detail.Partitions[0].EndOffset != 3 {
		t.Fatalf("partition offsets were lost: %#v", detail.Partitions[0])
	}
}
