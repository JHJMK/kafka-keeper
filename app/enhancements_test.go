package app

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/egoist/mygo/ui"

	"github.com/jjk/kafka-keeper/internal/kafka"
	"github.com/jjk/kafka-keeper/internal/model"
)

func TestNewBrowseFormDefaultsPartitionToZero(t *testing.T) {
	if got := New().Snapshot().Browse.Partition; got != "0" {
		t.Fatalf("Browse.Partition = %q, want 0", got)
	}
}

func TestSelectTopicResetsBrowsePartitionToZero(t *testing.T) {
	app := New()
	app.update(func(state *State) {
		state.ActiveConnectionID = "test"
		state.Connections = []model.Connection{{ID: "test", Name: "测试 Kafka"}}
		state.Browse.Partition = "-1"
	})

	app.SelectTopic("orders")

	if got := app.Snapshot().Browse.Partition; got != "0" {
		t.Fatalf("Browse.Partition after SelectTopic = %q, want 0", got)
	}
}

func TestKeySearchPlaceholderHidesOnlyForSubstringMatching(t *testing.T) {
	if got := keySearchPlaceholder(false); got != "完整 Key" {
		t.Fatalf("exact-match placeholder = %q, want 完整 Key", got)
	}
	if got := keySearchPlaceholder(true); got != "" {
		t.Fatalf("substring-match placeholder = %q, want empty", got)
	}
}

func TestFilterGroupsCanShowOnlyActiveGroups(t *testing.T) {
	items := []kafka.GroupSummary{
		{GroupID: "active-a", MemberCount: 2},
		{GroupID: "empty", MemberCount: 0},
		{GroupID: "active-b", MemberCount: 1},
	}

	got := filterGroups(items, "", true)
	if len(got) != 2 || got[0].GroupID != "active-a" || got[1].GroupID != "active-b" {
		t.Fatalf("active groups = %#v", got)
	}
}

func TestFilterGroupsCombinesActiveFilterAndTextSearch(t *testing.T) {
	items := []kafka.GroupSummary{
		{GroupID: "orders-active", State: "Stable", MemberCount: 2},
		{GroupID: "orders-empty", State: "Empty", MemberCount: 0},
		{GroupID: "payments-active", State: "Stable", MemberCount: 1},
	}

	got := filterGroups(items, "orders", true)
	if len(got) != 1 || got[0].GroupID != "orders-active" {
		t.Fatalf("filtered groups = %#v", got)
	}
}

func TestFilterTopicOptionsUsesCaseInsensitivePrefix(t *testing.T) {
	options := []string{"orders.created", "orders.retry", "payments", "Orders.audit"}

	got := filterTopicOptions(options, "ORD")
	want := []string{"orders.created", "orders.retry", "Orders.audit"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("filterTopicOptions = %#v, want %#v", got, want)
	}
	if got := filterTopicOptions(options, "ders"); len(got) != 0 {
		t.Fatalf("substring unexpectedly matched topics: %#v", got)
	}
	if got := filterTopicOptions(options, "  "); !reflect.DeepEqual(got, options) {
		t.Fatalf("blank query returned %#v, want all options", got)
	}
}

func TestParseAutoRefreshInterval(t *testing.T) {
	got, err := parseAutoRefreshInterval("15")
	if err != nil {
		t.Fatal(err)
	}
	if got != 15*time.Second {
		t.Fatalf("interval = %s, want 15s", got)
	}
	for _, raw := range []string{"", "0", "-1", "1.5", "abc"} {
		if _, err := parseAutoRefreshInterval(raw); err == nil {
			t.Fatalf("parseAutoRefreshInterval(%q) unexpectedly succeeded", raw)
		}
	}
}

func TestRunAutoRefreshRefreshesImmediatelyAndOnInterval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := make(chan struct{}, 4)
	go runAutoRefresh(ctx, 5*time.Millisecond, func() { calls <- struct{}{} })

	for i := 0; i < 2; i++ {
		select {
		case <-calls:
		case <-time.After(time.Second):
			t.Fatal("auto refresh did not run")
		}
	}
	cancel()
}

func TestSearchControlsShowSubstringMatchingLabel(t *testing.T) {
	app := New()
	app.update(func(state *State) {
		state.Page = PageTopics
		state.ActiveConnectionID = "test"
		state.Connections = []model.Connection{{ID: "test", Name: "测试 Kafka"}}
		state.SelectedTopic = "orders"
		state.TopicTab = 3
	})

	tester := ui.NewTester(app.View(), 1400, 900)
	if !tester.HasText("Key 子串匹配") {
		t.Fatal("substring matching label is missing")
	}
	if tester.HasText("Key 包含匹配") {
		t.Fatal("old substring matching label remains")
	}
}

func TestGroupsPageShowsActiveOnlyControl(t *testing.T) {
	app := New()
	app.update(func(state *State) {
		state.Page = PageGroups
		state.ActiveConnectionID = "test"
		state.Connections = []model.Connection{{ID: "test", Name: "测试 Kafka"}}
		state.Groups = []kafka.GroupSummary{{GroupID: "active", MemberCount: 1}}
	})

	tester := ui.NewTester(app.View(), 1400, 900)
	if !tester.HasText("仅查看活跃 Group") {
		t.Fatal("active Consumer Group filter is missing")
	}
	if _, ok := tester.Find("活跃 Group 过滤开关"); !ok {
		t.Fatal("active Consumer Group switch is missing")
	}
}

func TestSendPageUsesEditableTopicComboboxAndLabelsAutoPartition(t *testing.T) {
	app := New()
	app.update(func(state *State) {
		state.Page = PageSend
		state.ActiveConnectionID = "test"
		state.Connections = []model.Connection{{ID: "test", Name: "测试 Kafka"}}
		state.Topics = []kafka.TopicSummary{{Name: "orders.created"}, {Name: "payments"}}
	})

	tester := ui.NewTester(app.View(), 1400, 900)
	if _, ok := tester.Find("Topic 下拉输入框"); !ok {
		t.Fatal("editable Topic combobox is missing")
	}
	if !tester.HasText("自动分区") {
		t.Fatal("visible auto-partition label is missing")
	}
	label, ok := tester.Find("自动分区")
	if !ok {
		t.Fatal("auto-partition label is missing")
	}
	toggle, ok := tester.Find("自动分区开关")
	if !ok {
		t.Fatal("auto-partition switch is missing")
	}
	if label.X >= toggle.X {
		t.Fatalf("auto-partition label X = %v, switch X = %v; label must be on the left", label.X, toggle.X)
	}
}

func TestSendTopicComboboxFiltersByPrefixAndAcceptsNewTopic(t *testing.T) {
	app := New()
	app.update(func(state *State) {
		state.Page = PageSend
		state.ActiveConnectionID = "test"
		state.Connections = []model.Connection{{ID: "test", Name: "测试 Kafka"}}
		state.Topics = []kafka.TopicSummary{
			{Name: "orders.created"},
			{Name: "payments"},
			{Name: "Orders.audit"},
		}
	})

	tester := ui.NewTester(app.View(), 1400, 900)
	if err := tester.Click("Topic 下拉输入框"); err != nil {
		t.Fatal(err)
	}
	tester.Type("ORD")
	if !tester.HasText("orders.created") || !tester.HasText("Orders.audit") {
		t.Fatalf("prefix matches missing: %q", tester.Texts())
	}
	if tester.HasText("payments") {
		t.Fatalf("non-prefix topic appeared while filtering: %q", tester.Texts())
	}
	tester.Type("ERS.CUSTOM")
	if got := app.Snapshot().Produce.Topic; got != "ORDERS.CUSTOM" {
		t.Fatalf("typed new topic = %q, want ORDERS.CUSTOM", got)
	}
}

func TestAutoRefreshControlsAppearBesideRefreshOnSupportedPages(t *testing.T) {
	for _, page := range []Page{PageOverview, PageTopics, PageGroups} {
		page := page
		t.Run(string(page), func(t *testing.T) {
			app := New()
			app.update(func(state *State) {
				state.Page = page
				state.ActiveConnectionID = "test"
				state.Connections = []model.Connection{{ID: "test", Name: "测试 Kafka"}}
			})

			tester := ui.NewTester(app.View(), 1400, 900)
			refresh, ok := tester.Find("刷新")
			if !ok {
				t.Fatal("refresh button is missing")
			}
			interval, ok := tester.Find("自动刷新间隔秒数")
			if !ok {
				t.Fatal("auto-refresh interval input is missing")
			}
			auto, ok := tester.Find("自动刷新")
			if !ok {
				t.Fatal("auto-refresh button is missing")
			}
			if gap := auto.X - (interval.X + interval.W); gap < 4 {
				t.Fatalf("auto-refresh button is too close to interval input: gap=%v", gap)
			}
			if gap := refresh.X - (auto.X + auto.W); gap < 4 {
				t.Fatalf("refresh button is not to the right of auto-refresh: gap=%v", gap)
			}
		})
	}
}

func TestBrowseAllPartitionsSwitchHasVisibleLabel(t *testing.T) {
	app := New()
	app.update(func(state *State) {
		state.Page = PageTopics
		state.ActiveConnectionID = "test"
		state.Connections = []model.Connection{{ID: "test", Name: "测试 Kafka"}}
		state.SelectedTopic = "orders"
		state.TopicTab = 2
	})

	tester := ui.NewTester(app.View(), 1400, 900)
	label, ok := tester.Find("全部分区")
	if !ok {
		t.Fatal("all-partitions label is missing")
	}
	toggle, ok := tester.Find("全部分区开关")
	if !ok {
		t.Fatal("all-partitions switch is missing")
	}
	if label.X >= toggle.X {
		t.Fatalf("all-partitions label X = %v, switch X = %v; label must be on the left", label.X, toggle.X)
	}
}

func TestTopicPartitionColumnsFitNarrowDesktopWindow(t *testing.T) {
	app := New()
	app.update(func(state *State) {
		state.Page = PageTopics
		state.ActiveConnectionID = "test"
		state.Connections = []model.Connection{{ID: "test", Name: "测试 Kafka"}}
		state.SelectedTopic = "orders"
		state.TopicTab = 0
		state.TopicDetail.Partitions = []kafka.PartitionInfo{{ID: 0, Leader: 1, Replicas: []int32{1, 2}}}
	})

	const width = 1200
	tester := ui.NewTester(app.View(), width, 900)
	for _, title := range []string{"Partition", "Leader", "Replicas", "Start", "End", "Messages"} {
		column, ok := tester.Find(title)
		if !ok {
			t.Fatalf("partition column %q is missing", title)
		}
		if column.X+column.W > width {
			t.Fatalf("partition column %q is clipped: right=%v window=%v", title, column.X+column.W, width)
		}
	}
}

func TestAutoRefreshShowsVisibleIntervalUnitAndDescription(t *testing.T) {
	app := New()
	app.update(func(state *State) {
		state.Page = PageOverview
		state.ActiveConnectionID = "test"
		state.Connections = []model.Connection{{ID: "test", Name: "测试 Kafka"}}
	})

	tester := ui.NewTester(app.View(), 1400, 900)
	if !tester.HasText("刷新间隔") {
		t.Fatal("visible auto-refresh interval description is missing")
	}
	if _, ok := tester.Find("秒"); !ok {
		t.Fatal("visible auto-refresh interval unit is missing")
	}
}

func TestChangingPageStopsAutoRefresh(t *testing.T) {
	app := New()
	app.update(func(state *State) {
		state.Page = PageOverview
		state.AutoRefreshEnabled = true
		state.AutoRefreshPage = PageOverview
	})

	app.Navigate(PageTopics)
	state := app.Snapshot()
	if state.AutoRefreshEnabled {
		t.Fatal("auto refresh remained enabled after changing page")
	}
	if state.AutoRefreshPage != "" {
		t.Fatalf("auto refresh page = %q, want empty", state.AutoRefreshPage)
	}
}

func TestAutoRefreshLabelMentionsCurrentPage(t *testing.T) {
	state := State{Page: PageTopics}
	if got := autoRefreshButtonLabel(state); strings.TrimSpace(got) == "" {
		t.Fatal("auto-refresh button label is empty")
	}
}

func TestConsumerSessionSwitchControlsHaveVisibleLabels(t *testing.T) {
	app := New()
	app.update(func(state *State) {
		state.Page = PageGroups
		state.ActiveConnectionID = "test"
		state.Connections = []model.Connection{{ID: "test", Name: "测试 Kafka"}}
	})

	tester := ui.NewTester(app.View(), 1400, 900)
	for _, label := range []string{"从最早 Offset 开始", "自动提交 Offset"} {
		if !tester.HasText(label) {
			t.Fatalf("%s label is missing", label)
		}
	}
}
