package kafka

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jjk/kafka-keeper/internal/model"
)

func testConnection(id, server string) model.Connection {
	return model.Connection{
		ID: id, Name: id,
		BootstrapServers: []string{server},
	}
}

func testClient(closed *atomic.Int32, pingErr error) *Client {
	return &Client{
		ping: func(context.Context) error { return pingErr },
		close: func() {
			closed.Add(1)
		},
	}
}

func TestManagerConcurrentFirstAcquireReturnsOneActiveClient(t *testing.T) {
	var created, closed atomic.Int32
	gate := make(chan struct{})
	ready := make(chan struct{}, 32)
	manager := NewManager(func(context.Context, model.Connection, ClientCredentials) (*Client, error) {
		created.Add(1)
		ready <- struct{}{}
		<-gate
		return testClient(&closed, nil), nil
	})
	t.Cleanup(manager.Close)

	conn := testConnection("one", "localhost:9092")
	const callers = 20
	results := make(chan *Client, callers)
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client, err := manager.Get(context.Background(), conn, ClientCredentials{KafkaPassword: "secret"})
			if err != nil {
				errs <- err
				return
			}
			results <- client
		}()
	}
	<-ready
	<-ready
	close(gate)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		t.Fatal(err)
	}
	var active *Client
	for client := range results {
		if active == nil {
			active = client
		}
		if client != active {
			t.Fatal("concurrent callers received different active clients")
		}
	}
	if active == nil {
		t.Fatal("no active client")
	}
	if got, want := closed.Load(), created.Load()-1; got != want {
		t.Fatalf("closed clients = %d, want %d from %d candidates", got, want, created.Load())
	}
}

func TestManagerConfigChangeClosesEverySupersededClient(t *testing.T) {
	var oldClosed, newClosed, newCreated atomic.Int32
	var phase atomic.Int32
	gate := make(chan struct{})
	ready := make(chan struct{}, 32)
	manager := NewManager(func(context.Context, model.Connection, ClientCredentials) (*Client, error) {
		switch phase.Load() {
		case 0:
			return testClient(&oldClosed, nil), nil
		default:
			newCreated.Add(1)
			ready <- struct{}{}
			<-gate
			return testClient(&newClosed, nil), nil
		}
	})

	oldConn := testConnection("one", "old:9092")
	if _, err := manager.Get(context.Background(), oldConn, ClientCredentials{KafkaPassword: "secret"}); err != nil {
		t.Fatal(err)
	}
	phase.Store(1)
	newConn := testConnection("one", "new:9092")
	const callers = 12
	results := make(chan *Client, callers)
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client, err := manager.Get(context.Background(), newConn, ClientCredentials{KafkaPassword: "secret"})
			if err != nil {
				errs <- err
				return
			}
			results <- client
		}()
	}
	<-ready
	<-ready
	close(gate)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		t.Fatal(err)
	}
	var active *Client
	for client := range results {
		if active == nil {
			active = client
		}
		if client != active {
			t.Fatal("concurrent callers received different replacement clients")
		}
	}
	if active == nil {
		t.Fatal("no replacement client")
	}
	if oldClosed.Load() != 1 {
		t.Fatalf("old client closed %d times, want 1", oldClosed.Load())
	}
	if got, want := newClosed.Load(), newCreated.Load()-1; got != want {
		t.Fatalf("superseded new clients closed = %d, want %d from %d candidates", got, want, newCreated.Load())
	}

	current, err := manager.Get(context.Background(), newConn, ClientCredentials{KafkaPassword: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	if current != active {
		t.Fatal("final Get returned a different active replacement client")
	}
	manager.Close()
	if got := newClosed.Load(); got != newCreated.Load() {
		t.Fatalf("closed new clients = %d, want all %d", got, newCreated.Load())
	}
}

func TestManagerKeepsOldClientWhenReplacementFails(t *testing.T) {
	var oldClosed, failedClosed atomic.Int32
	var fail atomic.Bool
	manager := NewManager(func(context.Context, model.Connection, ClientCredentials) (*Client, error) {
		if fail.Load() {
			return testClient(&failedClosed, errors.New("ping failed")), nil
		}
		return testClient(&oldClosed, nil), nil
	})
	t.Cleanup(manager.Close)

	oldConn := testConnection("one", "old:9092")
	oldClient, err := manager.Get(context.Background(), oldConn, ClientCredentials{KafkaPassword: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	fail.Store(true)
	if _, err := manager.Get(context.Background(), testConnection("one", "new:9092"), ClientCredentials{KafkaPassword: "secret"}); err == nil {
		t.Fatal("expected replacement failure")
	}
	if oldClosed.Load() != 0 {
		t.Fatal("old client was closed after a failed replacement")
	}
	got, err := manager.Get(context.Background(), oldConn, ClientCredentials{KafkaPassword: "secret"})
	if err != nil || got != oldClient {
		t.Fatalf("old client unavailable: client=%p err=%v", got, err)
	}
	if failedClosed.Load() != 1 {
		t.Fatalf("failed candidate closed %d times, want 1", failedClosed.Load())
	}
}

func TestManagerRemoveClosesClient(t *testing.T) {
	var closed atomic.Int32
	manager := NewManager(func(context.Context, model.Connection, ClientCredentials) (*Client, error) {
		return testClient(&closed, nil), nil
	})
	conn := testConnection("one", "localhost:9092")
	if _, err := manager.Get(context.Background(), conn, ClientCredentials{KafkaPassword: "secret"}); err != nil {
		t.Fatal(err)
	}
	manager.Remove("one")
	manager.Close()
	if closed.Load() != 1 {
		t.Fatalf("closed = %d, want 1", closed.Load())
	}
}

func TestManagerProxyPasswordChangeReplacesClient(t *testing.T) {
	var created, closed atomic.Int32
	manager := NewManager(func(context.Context, model.Connection, ClientCredentials) (*Client, error) {
		created.Add(1)
		return testClient(&closed, nil), nil
	})
	t.Cleanup(manager.Close)
	conn := testConnection("one", "10.0.0.8:9092")
	conn.Proxy = model.SSHProxyConfig{Enabled: true, Address: "203.0.113.10", Username: "jump", HostKey: "host-key"}

	first, err := manager.Get(context.Background(), conn, ClientCredentials{ProxyPassword: "first"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := manager.Get(context.Background(), conn, ClientCredentials{ProxyPassword: "second"})
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("proxy password change reused the cached Kafka client")
	}
	if created.Load() != 2 || closed.Load() != 1 {
		t.Fatalf("created = %d, closed = %d; want 2, 1", created.Load(), closed.Load())
	}
}

func TestManagerPingTimeoutLeavesNoClient(t *testing.T) {
	var closed atomic.Int32
	manager := NewManager(func(ctx context.Context, _ model.Connection, _ ClientCredentials) (*Client, error) {
		client := testClient(&closed, nil)
		client.ping = func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}
		return client, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := manager.Get(ctx, testConnection("one", "localhost:9092"), ClientCredentials{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Get() error = %v, want deadline exceeded", err)
	}
	if closed.Load() != 1 {
		t.Fatalf("failed client closed %d times, want 1", closed.Load())
	}
}
