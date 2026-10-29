package kafka

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

func pollConsumerRecords(t *testing.T, session *ConsumerSession, want int) []ConsumerRecord {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	var records []ConsumerRecord
	for len(records) < want {
		batch, err := session.Poll(ctx, want-len(records))
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, batch...)
	}
	return records
}

func TestConsumerSessionConsumesPausesResumesAndCommits(t *testing.T) {
	cluster := newSeededCluster(t, 1, "orders")
	broker := cluster.ListenAddrs()[0]
	produceTestRecords(t, broker, "orders", 0, "one", "two", "three")
	service := newIntegrationService(t, broker)
	conn := testConnection("consumer", broker)

	session, err := service.StartConsumer(context.Background(), conn, ConsumerOptions{
		GroupID: "studio-group", Topic: "orders", FromStart: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	records := pollConsumerRecords(t, session, 3)
	if len(records) != 3 || records[0].ValueText() != "one" || records[2].ValueText() != "three" {
		t.Fatalf("records = %#v", records)
	}

	session.Pause()
	if !session.Paused() {
		t.Fatal("session is not paused")
	}
	if _, err := service.Produce(context.Background(), conn, ProduceRequest{
		Topic: "orders", Partition: 0, Value: []byte("four"), Compression: "none",
	}); err != nil {
		t.Fatal(err)
	}
	pauseCtx, pauseCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer pauseCancel()
	if batch, err := session.Poll(pauseCtx, 1); err == nil || len(batch) != 0 || !IsCanceled(err) {
		t.Fatalf("paused poll = %#v, %v", batch, err)
	}

	session.Resume()
	records = append(records, pollConsumerRecords(t, session, 1)...)
	if records[3].ValueText() != "four" || records[3].Offset != 3 {
		t.Fatalf("resumed records = %#v", records)
	}
	if err := session.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	session.Close()

	raw := newRawTestClient(t, broker)
	admin := kadm.NewClient(raw)
	offsets, err := admin.FetchOffsets(context.Background(), "studio-group")
	if err != nil {
		t.Fatal(err)
	}
	offset, ok := offsets.Lookup("orders", 0)
	if !ok || offset.Err != nil || offset.At != 4 {
		t.Fatalf("committed offset = %#v, exists=%v", offset, ok)
	}
}

func TestConsumerSessionAllowsOnlyOneActiveSessionPerConnection(t *testing.T) {
	cluster := newSeededCluster(t, 1, "orders")
	broker := cluster.ListenAddrs()[0]
	service := newIntegrationService(t, broker)
	conn := testConnection("single-consumer", broker)
	options := ConsumerOptions{GroupID: "first", Topic: "orders", FromStart: true}

	first, err := service.StartConsumer(context.Background(), conn, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.StartConsumer(context.Background(), conn, ConsumerOptions{
		GroupID: "second", Topic: "orders", FromStart: true,
	}); err == nil || !strings.Contains(err.Error(), "已有") {
		t.Fatalf("second start error = %v", err)
	}
	first.Close()

	second, err := service.StartConsumer(context.Background(), conn, options)
	if err != nil {
		t.Fatal(err)
	}
	second.Close()
}

func TestConsumerPollReturnsContextCancellation(t *testing.T) {
	cluster := newSeededCluster(t, 1, "orders")
	broker := cluster.ListenAddrs()[0]
	service := newIntegrationService(t, broker)
	session, err := service.StartConsumer(context.Background(), testConnection("cancel-consumer", broker), ConsumerOptions{
		GroupID: "cancel-group", Topic: "orders", FromStart: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := session.Poll(ctx, 1); err == nil || !IsCanceled(err) {
		t.Fatalf("poll error = %v", err)
	}
}
