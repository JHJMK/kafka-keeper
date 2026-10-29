package app

import (
	"testing"
	"time"

	"github.com/egoist/mygo"
	"github.com/egoist/mygo/ui"

	"github.com/jjk/kafka-keeper/internal/kafka"
	"github.com/jjk/kafka-keeper/internal/model"
	"github.com/jjk/kafka-keeper/internal/store"
)

func TestViewShowsEmptyStateAndOpensConnectionDialog(t *testing.T) {
	app := New()
	tester := ui.NewTester(app.View(), 1200, 760)

	if !tester.HasText("Kafka Keeper") {
		t.Fatal("brand is missing")
	}
	if !tester.HasText("还没有 Kafka 连接") {
		t.Fatal("empty connection state is missing")
	}
	if err := tester.Click("添加连接"); err != nil {
		t.Fatal(err)
	}
	tester.Frame()
	if !tester.HasText("新建 Kafka 连接") {
		t.Fatal("connection dialog did not open")
	}
	if !tester.HasText("不使用") {
		t.Fatal("unauthenticated SASL option is blank")
	}
}

func TestCAFileFieldOpensNativePickerAndStoresSelection(t *testing.T) {
	app := New()
	selected := "/tmp/root-ca.pem"
	app.chooseFile = func(options mygo.OpenDialogOptions) ([]string, error) {
		if options.Title != "选择 CA 证书" {
			t.Fatalf("dialog title = %q", options.Title)
		}
		if len(options.Filters) != 1 || options.Filters[0].Name != "证书文件" {
			t.Fatalf("dialog filters = %#v", options.Filters)
		}
		return []string{selected}, nil
	}
	app.update(func(state *State) {
		state.ConnectionDialog = true
		state.ConnectionForm = ConnectionForm{TLSEnabled: true}
	})

	tester := ui.NewTester(app.View(), 1200, 760)
	if !tester.HasText("选择 CA 证书") {
		t.Fatal("CA certificate picker is missing")
	}
	if err := tester.Click("选择 CA 证书"); err != nil {
		t.Fatal(err)
	}
	if got := app.Snapshot().ConnectionForm.CAFile; got != selected {
		t.Fatalf("CAFile = %q, want %q", got, selected)
	}
}

func TestConnectionDialogShowsSSHProxyOptionsAndTrustsNewHostKey(t *testing.T) {
	app := New()
	app.update(func(state *State) {
		state.ConnectionDialog = true
		state.ConnectionForm = ConnectionForm{
			Name:          "VPC Kafka",
			Servers:       "10.0.0.8:9092",
			ProxyEnabled:  true,
			ProxyAddress:  "203.0.113.10",
			ProxyPort:     "22",
			ProxyUsername: "jump",
			ProxyPassword: "ssh-secret",
		}
		state.ConnectionHostKeyCandidate = "new-host-key"
		state.ConnectionHostKeyFingerprint = "SHA256:new-host"
	})

	tester := ui.NewTester(app.View(), 1200, 760)
	for _, label := range []string{"通过 SSH 跳板机访问", "SSH 代理节点", "SSH 用户名", "SHA256:new-host", "信任并继续"} {
		if !tester.HasText(label) {
			t.Fatalf("SSH proxy field %q is missing: %#v", label, tester.Texts())
		}
	}
	if err := tester.Click("信任并继续"); err != nil {
		t.Fatal(err)
	}
	tester.Frame()
	state := app.Snapshot()
	if state.ConnectionForm.ProxyHostKey != "new-host-key" {
		t.Fatalf("ProxyHostKey = %q, want new-host-key", state.ConnectionForm.ProxyHostKey)
	}
	if state.ConnectionHostKeyCandidate != "" {
		t.Fatal("trusted host key candidate was not cleared")
	}
}

func TestTopicDetailShowsPartitionDataWhenConfigsFail(t *testing.T) {
	app := New()
	app.update(func(state *State) {
		state.Page = PageTopics
		state.ActiveConnectionID = "test"
		state.Connections = []model.Connection{{ID: "test", Name: "测试 Kafka"}}
		state.SelectedTopic = "test"
		state.TopicTab = 1
		state.TopicDetail = kafka.TopicDetail{
			Summary: kafka.TopicSummary{Name: "test", PartitionCount: 3},
			Partitions: []kafka.PartitionInfo{
				{ID: 0, Leader: 1, Replicas: []int32{1}},
				{ID: 1, Leader: 2, Replicas: []int32{2}},
				{ID: 2, Leader: 3, Replicas: []int32{3}},
			},
			ConfigError: "读取 Topic 配置: TOPIC_AUTHORIZATION_FAILED",
		}
	})

	tester := ui.NewTester(app.View(), 1400, 900)
	if !tester.HasText("3 partitions · 0 groups") {
		t.Fatal("topic summary did not use loaded partition metadata")
	}
	if !tester.HasText("读取 Topic 配置: TOPIC_AUTHORIZATION_FAILED") {
		t.Fatal("topic config error is not shown in the config tab")
	}
}

func TestGroupsPageAllowsStartingANewConsumerGroup(t *testing.T) {
	app := New()
	app.update(func(state *State) {
		state.Page = PageGroups
		state.ActiveConnectionID = "test"
		state.Connections = []model.Connection{{ID: "test", Name: "测试 Kafka"}}
	})

	tester := ui.NewTester(app.View(), 1400, 900)
	if !tester.HasText("新建消费会话") {
		t.Fatal("empty groups page has no way to start a new consumer group")
	}
	if !tester.HasText("开始消费") {
		t.Fatal("new consumer group form is missing")
	}
}

func TestToastIsKeptUntilItsLifetimeEnds(t *testing.T) {
	app := New()
	app.ShowToast("消息已发送")

	tester := ui.NewTester(app.View(), 1200, 760)
	assertToastVisible(t, tester, "消息已发送")

	if got := app.Snapshot().Toast; got != "消息已发送" {
		t.Fatalf("Toast after first render = %q, want message", got)
	}
	tester.Frame()
	if got := app.Snapshot().Toast; got != "消息已发送" {
		t.Fatalf("Toast after second render = %q, want message", got)
	}

	tester.Advance(4 * time.Second)
	if tester.HasText("消息已发送") {
		t.Fatal("toast still shows after its lifetime")
	}
	if got := app.Snapshot().Toast; got != "" {
		t.Fatalf("Toast after its lifetime = %q, want empty", got)
	}
}

func TestToastExpiresWhileWindowIsUnfocused(t *testing.T) {
	app := New()
	app.ShowToast("消息已发送")

	tester := ui.NewTester(app.View(), 1200, 760)
	assertToastVisible(t, tester, "消息已发送")

	tester.SetFocused(false)
	tester.Advance(4 * time.Second)
	if tester.HasText("消息已发送") {
		t.Fatal("toast still shows after its lifetime while the window is unfocused")
	}

	tester.SetFocused(true)
	tester.Frame()
	if tester.HasText("消息已发送") {
		t.Fatal("toast reappeared when the window regained focus")
	}
}

func assertToastVisible(t *testing.T, tester *ui.Tester, message string) {
	t.Helper()
	rect, ok := tester.Find(message)
	if !ok {
		t.Fatal("toast is missing")
	}
	pixel := tester.Image().RGBAAt(int(rect.X)-8, int(rect.Y+rect.H/2))
	if pixel.R >= 100 || pixel.G >= 130 || pixel.B >= 160 {
		t.Fatalf("toast is transparent: background pixel = %#v", pixel)
	}
}

func TestConnectionMenuEditsItsOwnRow(t *testing.T) {
	connections := store.NewFileStore(t.TempDir() + "/connections.json")
	for _, connection := range []model.Connection{
		{ID: "first", Name: "First", BootstrapServers: []string{"first:9092"}},
		{ID: "second", Name: "Second", BootstrapServers: []string{"second:9092"}},
	} {
		if err := connections.Save(connection); err != nil {
			t.Fatal(err)
		}
	}
	app := NewWithDependencies(Dependencies{
		Connections: connections,
		Credentials: &memoryCredentials{values: map[string]string{}},
	})
	app.update(func(state *State) { state.ActiveConnectionID = "first" })

	tester := ui.NewTester(app.View(), 1200, 760)
	if err := tester.Click("连接操作：Second"); err != nil {
		t.Fatal(err)
	}
	if !tester.HasText("编辑连接") || !tester.HasText("删除连接") {
		t.Fatalf("connection menu is missing: %#v", tester.Texts())
	}
	if err := tester.Click("编辑连接"); err != nil {
		t.Fatal(err)
	}
	tester.Frame()

	if got := app.Snapshot().ConnectionForm.ID; got != "second" {
		t.Fatalf("edited connection ID = %q, want second", got)
	}
	if got := app.Snapshot().ActiveConnectionID; got != "first" {
		t.Fatalf("ActiveConnectionID = %q, want first", got)
	}
}

func TestConnectionFormForEditLoadsSavedPassword(t *testing.T) {
	connections := store.NewFileStore(t.TempDir() + "/connections.json")
	connection := model.Connection{
		ID:               "second",
		Name:             "Second",
		BootstrapServers: []string{"second:9092"},
		SASL:             model.SASLConfig{Mechanism: model.SASLPlain, Username: "jjk"},
	}
	if err := connections.Save(connection); err != nil {
		t.Fatal(err)
	}
	app := NewWithDependencies(Dependencies{
		Connections: connections,
		Credentials: &memoryCredentials{values: map[string]string{"second": "saved-secret"}},
	})

	tester := ui.NewTester(app.View(), 1200, 760)
	if err := tester.Click("连接操作：Second"); err != nil {
		t.Fatal(err)
	}
	if err := tester.Click("编辑连接"); err != nil {
		t.Fatal(err)
	}
	tester.Frame()

	if got := app.Snapshot().ConnectionForm.Password; got != "saved-secret" {
		t.Fatalf("edit form password = %q, want saved password", got)
	}
	if err := tester.Click("SASL 密码输入框"); err != nil {
		t.Fatal(err)
	}
	tester.Key(ui.Cmd, ui.KeyA)
	tester.Key(ui.Cmd, ui.KeyC)
	if got := tester.Clipboard(); got != "saved-secret" {
		t.Fatalf("password input copied %q, want plain-text saved password", got)
	}
}

func TestBrowseLayoutSeparatesModeParametersAndActions(t *testing.T) {
	app := New()
	app.update(func(state *State) {
		state.Page = PageTopics
		state.ActiveConnectionID = "test"
		state.Connections = []model.Connection{{ID: "test", Name: "测试 Kafka"}}
		state.SelectedTopic = "orders"
		state.TopicTab = 2
		state.Browse.Mode = string(kafka.BrowseByOffset)
	})

	tester := ui.NewTester(app.View(), 1400, 900)
	mode, ok := tester.Find("按 Offset")
	if !ok {
		t.Fatal("browse mode tab is missing")
	}
	offset, ok := tester.Find("Offset")
	if !ok {
		t.Fatal("offset parameter is missing")
	}
	limit, ok := tester.Find("条数上限")
	if !ok {
		t.Fatal("limit field is missing")
	}
	all, ok := tester.Find("全部分区")
	if !ok {
		t.Fatal("all partitions switch is missing")
	}

	if offset.Y <= mode.Y {
		t.Fatalf("offset parameter Y = %v, mode Y = %v; expected parameter below mode", offset.Y, mode.Y)
	}
	if limit.Y <= offset.Y {
		t.Fatalf("limit Y = %v, offset Y = %v; expected limit below parameter row", limit.Y, offset.Y)
	}
	limitCenter := limit.Y + limit.H/2
	allCenter := all.Y + all.H/2
	if diff := limitCenter - allCenter; diff < -8 || diff > 8 {
		t.Fatalf("limit and all-partitions centers differ: limit=%v all=%v", limitCenter, allCenter)
	}
}

func TestSearchControlsUseAlignedRows(t *testing.T) {
	app := New()
	app.update(func(state *State) {
		state.Page = PageTopics
		state.ActiveConnectionID = "test"
		state.Connections = []model.Connection{{ID: "test", Name: "测试 Kafka"}}
		state.SelectedTopic = "orders"
		state.TopicTab = 3
	})

	tester := ui.NewTester(app.View(), 1400, 900)
	key, ok := tester.Find("Key")
	if !ok {
		t.Fatal("Key field is missing")
	}
	keyContains, ok := tester.Find("Key 子串匹配")
	if !ok {
		t.Fatal("Key substring switch is missing")
	}
	headerKey, ok := tester.Find("Header Key")
	if !ok {
		t.Fatal("Header Key field is missing")
	}
	headerValue, ok := tester.Find("Header Value")
	if !ok {
		t.Fatal("Header Value field is missing")
	}
	start, ok := tester.Find("开始查找")
	if !ok {
		t.Fatal("start search button is missing")
	}

	keyCenter := key.Y + key.H/2
	switchCenter := keyContains.Y + keyContains.H/2
	if diff := keyCenter - switchCenter; diff < -10 || diff > 10 {
		t.Fatalf("Key and switch centers differ: key=%v switch=%v", keyCenter, switchCenter)
	}
	if headerKey.Y <= key.Y+key.H {
		t.Fatalf("Header Key Y = %v, expected a separate row below Key", headerKey.Y)
	}
	headerCenter := headerKey.Y + headerKey.H/2
	valueCenter := headerValue.Y + headerValue.H/2
	if diff := headerCenter - valueCenter; diff < -10 || diff > 10 {
		t.Fatalf("Header Key and Header Value centers differ: key=%v value=%v", headerCenter, valueCenter)
	}
	if start.Y <= key.Y+key.H {
		t.Fatalf("start button Y = %v, expected a separate row below the fields", start.Y)
	}
}

func TestSearchControlsFitMinimumWindow(t *testing.T) {
	app := New()
	app.update(func(state *State) {
		state.Page = PageTopics
		state.ActiveConnectionID = "test"
		state.Connections = []model.Connection{{ID: "test", Name: "测试 Kafka"}}
		state.SelectedTopic = "orders"
		state.TopicTab = 3
	})

	const width = 1100
	tester := ui.NewTester(app.View(), width, 720)
	for _, label := range []string{
		"Key",
		"Header Key",
		"Header Value",
		"Partition (-1 全部)",
		"开始时间",
		"结束时间",
		"起始 Offset",
		"结束 Offset",
		"最大扫描数",
		"最大结果数",
	} {
		rect, ok := tester.Find(label)
		if !ok {
			t.Fatalf("%s field is missing", label)
		}
		if right := rect.X + rect.W; right > width-16 {
			t.Fatalf("%s extends past the window: right=%v width=%v", label, right, width)
		}
	}
}
