package app

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/egoist/mygo/i18n"
	"github.com/egoist/mygo/ui"

	"github.com/jjk/kafka-keeper/internal/kafka"
	"github.com/jjk/kafka-keeper/internal/model"
	"github.com/jjk/kafka-keeper/internal/store"
)

func TestBrowseTimeUsesEditableNativeDateAndTimeInputs(t *testing.T) {
	app := New()
	start := time.Date(2026, 10, 15, 8, 37, 0, 0, time.Local)
	app.update(func(state *State) {
		state.Page = PageTopics
		state.ActiveConnectionID = "test"
		state.Connections = []model.Connection{{ID: "test", Name: "测试 Kafka"}}
		state.SelectedTopic = "orders"
		state.TopicTab = 2
		state.Browse.Mode = string(kafka.BrowseByTime)
		state.Browse.Start = start
	})

	tester := ui.NewTester(app.View(), 1400, 900)
	tester.SetLocale("en-GB")
	tester.SetRegion(i18n.Region{Locale: "en-GB", FirstWeekday: time.Monday, HourCycle: 24})
	tester.Frame()

	if _, ok := tester.Find("起始日期"); !ok {
		t.Fatalf("native date input is missing: %q", tester.Texts())
	}
	if _, ok := tester.Find("起始时刻"); !ok {
		t.Fatalf("native time input is missing: %q", tester.Texts())
	}
	if err := tester.Click("起始日期"); err != nil {
		t.Fatal(err)
	}
	if !tester.HasText("October 2026") {
		t.Fatalf("date input did not open its calendar: %q", tester.Texts())
	}
	if err := tester.Click("20 October 2026"); err != nil {
		t.Fatal(err)
	}
	if err := tester.Click("08"); err != nil {
		t.Fatal(err)
	}
	tester.Key(0, ui.KeyUp)

	got := app.Snapshot().Browse.Start
	want := time.Date(2026, 10, 20, 9, 37, 0, 0, time.Local)
	if !got.Equal(want) {
		t.Fatalf("Browse.Start = %v, want %v", got, want)
	}
}

func TestSearchAndProduceRenderNativeDateTimeInputs(t *testing.T) {
	start := time.Date(2026, 10, 15, 8, 37, 0, 0, time.Local)
	end := time.Date(2026, 10, 16, 18, 42, 0, 0, time.Local)

	t.Run("search", func(t *testing.T) {
		app := New()
		app.update(func(state *State) {
			state.Page = PageTopics
			state.ActiveConnectionID = "test"
			state.Connections = []model.Connection{{ID: "test", Name: "测试 Kafka"}}
			state.SelectedTopic = "orders"
			state.TopicTab = 3
			state.Search.Start = start
			state.Search.StartEnabled = true
			state.Search.End = end
			state.Search.EndEnabled = true
		})
		tester := ui.NewTester(app.View(), 1400, 900)
		for _, label := range []string{"开始日期", "开始时刻", "结束日期", "结束时刻"} {
			if _, ok := tester.Find(label); !ok {
				t.Fatalf("%s input is missing: %q", label, tester.Texts())
			}
		}
	})

	t.Run("produce", func(t *testing.T) {
		app := New()
		app.update(func(state *State) {
			state.Page = PageSend
			state.ActiveConnectionID = "test"
			state.Connections = []model.Connection{{ID: "test", Name: "测试 Kafka"}}
			state.Produce.Timestamp = start
			state.Produce.TimestampEnabled = true
		})
		tester := ui.NewTester(app.View(), 1400, 900)
		for _, label := range []string{"消息日期", "消息时刻"} {
			if _, ok := tester.Find(label); !ok {
				t.Fatalf("%s input is missing: %q", label, tester.Texts())
			}
		}
	})
}

func TestSearchTimeSwitchesControlOptionalRequestTimes(t *testing.T) {
	start := time.Date(2026, 10, 15, 8, 37, 0, 0, time.Local)
	end := time.Date(2026, 10, 16, 18, 42, 0, 0, time.Local)
	for _, test := range []struct {
		name      string
		startOn   bool
		endOn     bool
		wantStart time.Time
		wantEnd   time.Time
	}{
		{name: "disabled", wantStart: time.Time{}, wantEnd: time.Time{}},
		{name: "enabled", startOn: true, endOn: true, wantStart: start, wantEnd: end},
	} {
		t.Run(test.name, func(t *testing.T) {
			fake := &captureKafka{search: make(chan kafka.SearchRequest, 1)}
			app := newTimeActionApp(t, fake)
			app.update(func(state *State) {
				state.Search.Start = start
				state.Search.StartEnabled = test.startOn
				state.Search.End = end
				state.Search.EndEnabled = test.endOn
			})
			app.SearchMessages()

			request := awaitValue(t, fake.search)
			if !request.Start.Equal(test.wantStart) {
				t.Fatalf("Search.Start = %v, want %v", request.Start, test.wantStart)
			}
			if !request.End.Equal(test.wantEnd) {
				t.Fatalf("Search.End = %v, want %v", request.End, test.wantEnd)
			}
		})
	}
}

func TestProduceTimestampSwitchControlsOptionalRequestTime(t *testing.T) {
	stamp := time.Date(2026, 10, 15, 8, 37, 0, 0, time.Local)
	for _, test := range []struct {
		name    string
		enabled bool
		want    time.Time
	}{
		{name: "disabled", want: time.Time{}},
		{name: "enabled", enabled: true, want: stamp},
	} {
		t.Run(test.name, func(t *testing.T) {
			fake := &captureKafka{produce: make(chan kafka.ProduceRequest, 1)}
			app := newTimeActionApp(t, fake)
			app.update(func(state *State) {
				state.Produce.Topic = "orders"
				state.Produce.Timestamp = stamp
				state.Produce.TimestampEnabled = test.enabled
			})
			app.ProduceMessage()

			request := awaitValue(t, fake.produce)
			if !request.Timestamp.Equal(test.want) {
				t.Fatalf("Produce.Timestamp = %v, want %v", request.Timestamp, test.want)
			}
		})
	}
}

func TestBrowseRangeForwardsNativeDateTimes(t *testing.T) {
	start := time.Date(2026, 10, 15, 8, 37, 0, 0, time.Local)
	end := time.Date(2026, 10, 16, 18, 42, 0, 0, time.Local)
	fake := &captureKafka{browse: make(chan kafka.BrowseRequest, 1)}
	app := newTimeActionApp(t, fake)
	app.update(func(state *State) {
		state.Browse.Mode = string(kafka.BrowseByRange)
		state.Browse.Start = start
		state.Browse.End = end
	})
	app.BrowseMessages()

	request := awaitValue(t, fake.browse)
	if !request.Start.Equal(start) || !request.End.Equal(end) {
		t.Fatalf("browse range = %v to %v, want %v to %v", request.Start, request.End, start, end)
	}
}

type captureKafka struct {
	KafkaAPI
	browse  chan kafka.BrowseRequest
	search  chan kafka.SearchRequest
	produce chan kafka.ProduceRequest
}

func (k *captureKafka) BrowseMessages(_ context.Context, _ model.Connection, request kafka.BrowseRequest) ([]kafka.Message, error) {
	k.browse <- request
	return nil, nil
}

func (k *captureKafka) SearchMessages(_ context.Context, _ model.Connection, request kafka.SearchRequest) (kafka.SearchResult, error) {
	k.search <- request
	return kafka.SearchResult{}, nil
}

func (k *captureKafka) Produce(_ context.Context, _ model.Connection, request kafka.ProduceRequest) (kafka.ProduceResult, error) {
	k.produce <- request
	return kafka.ProduceResult{}, nil
}

func newTimeActionApp(t *testing.T, fake *captureKafka) *App {
	t.Helper()
	connections := store.NewFileStore(filepath.Join(t.TempDir(), "connections.json"))
	if err := connections.Save(model.Connection{
		ID:               "test",
		Name:             "测试 Kafka",
		BootstrapServers: []string{"localhost:9092"},
	}); err != nil {
		t.Fatal(err)
	}
	app := NewWithDependencies(Dependencies{
		Connections: connections,
		Credentials: &memoryCredentials{values: map[string]string{}},
		Kafka:       fake,
	})
	app.update(func(state *State) {
		state.Page = PageTopics
		state.SelectedTopic = "orders"
	})
	return app
}

func awaitValue[T any](t *testing.T, values <-chan T) T {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(time.Second):
		t.Fatal("Kafka request was not sent")
		var zero T
		return zero
	}
}
