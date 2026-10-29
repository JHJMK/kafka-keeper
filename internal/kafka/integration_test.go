package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

type staticCredentialReader string

func (r staticCredentialReader) Get(string) (string, bool, error) {
	return string(r), r != "", nil
}

func (r staticCredentialReader) GetProxyPassword(string) (string, bool, error) {
	return "", false, nil
}

func newIntegrationService(t *testing.T, broker string) *Service {
	t.Helper()
	service := newService(staticCredentialReader(""), NewFranzClient)
	t.Cleanup(service.Close)
	return service
}

func newRawTestClient(t *testing.T, broker string) *kgo.Client {
	t.Helper()
	client, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

func produceTestRecords(t *testing.T, broker, topic string, partition int32, values ...string) {
	t.Helper()
	client := newRawTestClient(t, broker)
	records := make([]*kgo.Record, 0, len(values))
	for i, value := range values {
		records = append(records, &kgo.Record{
			Topic:     topic,
			Partition: partition,
			Key:       []byte("key-" + value),
			Value:     []byte(value),
			Timestamp: time.UnixMilli(int64(1_800_000_000_000 + i)),
		})
	}
	if err := client.ProduceSync(context.Background(), records...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

func newSeededCluster(t *testing.T, partitions int32, topic string) *kfake.Cluster {
	t.Helper()
	cluster := kfake.MustCluster(
		kfake.NumBrokers(1),
		kfake.SeedTopics(partitions, topic),
	)
	t.Cleanup(cluster.Close)
	return cluster
}
