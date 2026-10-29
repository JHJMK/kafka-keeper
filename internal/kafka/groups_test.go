package kafka

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
)

func commitTestOffset(t *testing.T, broker, group, topic string, partition int32, offset int64) {
	t.Helper()
	client := newRawTestClient(t, broker)
	admin := kadm.NewClient(client)
	offsets := kadm.Offsets{}
	offsets.Add(kadm.Offset{Topic: topic, Partition: partition, At: offset})
	if err := admin.CommitAllOffsets(context.Background(), group, offsets); err != nil {
		t.Fatal(err)
	}
}

func TestGroupsIncludeCoordinatorOffsetsAndLag(t *testing.T) {
	cluster := newSeededCluster(t, 1, "orders")
	broker := cluster.ListenAddrs()[0]
	produceTestRecords(t, broker, "orders", 0, "one", "two", "three")
	commitTestOffset(t, broker, "group-a", "orders", 0, 2)
	service := newIntegrationService(t, broker)
	conn := testConnection("groups", broker)

	groups, err := service.ListGroups(context.Background(), conn)
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 1 {
		t.Fatalf("ListGroups() = %#v", groups)
	}
	group := groups[0]
	if group.GroupID != "group-a" || group.TotalLag != 1 {
		t.Fatalf("group summary = %#v", group)
	}
	if group.Coordinator == "" {
		t.Fatalf("coordinator missing: %#v", group)
	}

	detail, err := service.LoadGroupDetail(context.Background(), conn, "group-a")
	if err != nil {
		t.Fatal(err)
	}
	if detail.Summary.GroupID != "group-a" || detail.Summary.TotalLag != 1 {
		t.Fatalf("group detail summary = %#v", detail.Summary)
	}
	if len(detail.Offsets) != 1 {
		t.Fatalf("group offsets = %#v", detail.Offsets)
	}
	offset := detail.Offsets[0]
	if offset.Topic != "orders" || offset.Partition != 0 || offset.Current != 2 || offset.End != 3 || offset.Lag != 1 {
		t.Fatalf("group offset = %#v", offset)
	}
}
