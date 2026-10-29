package app

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/egoist/mygo"

	"github.com/jjk/kafka-keeper/internal/kafka"
	"github.com/jjk/kafka-keeper/internal/model"
	"github.com/jjk/kafka-keeper/internal/store"
)

type memoryCredentials struct {
	values      map[string]string
	proxyValues map[string]string
}

func TestNewBrowseFormDefaultsOffsetToZero(t *testing.T) {
	if got := New().Snapshot().Browse.Offset; got != "0" {
		t.Fatalf("Browse.Offset = %q, want 0", got)
	}
}

func (m *memoryCredentials) Set(id, password string) error {
	m.values[id] = password
	return nil
}

func (m *memoryCredentials) Get(id string) (string, bool, error) {
	password, ok := m.values[id]
	return password, ok, nil
}

func (m *memoryCredentials) Delete(id string) error {
	delete(m.values, id)
	return nil
}

func (m *memoryCredentials) SessionOnly(string) bool { return false }

func (m *memoryCredentials) SetProxyPassword(id, password string) error {
	if m.proxyValues == nil {
		m.proxyValues = make(map[string]string)
	}
	m.proxyValues[id] = password
	return nil
}

func (m *memoryCredentials) GetProxyPassword(id string) (string, bool, error) {
	password, ok := m.proxyValues[id]
	return password, ok, nil
}

func (m *memoryCredentials) DeleteProxyPassword(id string) error {
	delete(m.proxyValues, id)
	return nil
}

func (m *memoryCredentials) ProxySessionOnly(string) bool { return false }

func TestNewWithDependenciesLoadsAndSelectsFirstConnection(t *testing.T) {
	connections := store.NewFileStore(t.TempDir() + "/connections.json")
	want := model.Connection{
		ID:               "local",
		Name:             "本地 Kafka",
		BootstrapServers: []string{"localhost:9092"},
	}
	if err := connections.Save(want); err != nil {
		t.Fatal(err)
	}

	got := NewWithDependencies(Dependencies{
		Connections: connections,
		Credentials: &memoryCredentials{values: map[string]string{}},
	})

	state := got.Snapshot()
	if state.ActiveConnectionID != want.ID {
		t.Fatalf("ActiveConnectionID = %q, want %q", state.ActiveConnectionID, want.ID)
	}
	if len(state.Connections) != 1 || state.Connections[0].Name != want.Name {
		t.Fatalf("Connections = %#v", state.Connections)
	}
}

func TestSaveConnectionPersistsPasswordAndSelectsSavedConnection(t *testing.T) {
	connections := store.NewFileStore(t.TempDir() + "/connections.json")
	credentials := &memoryCredentials{values: map[string]string{}}
	app := NewWithDependencies(Dependencies{
		Connections: connections,
		Credentials: credentials,
	})

	err := app.SaveConnection(ConnectionForm{
		Name:          "开发集群",
		Servers:       "kafka-a:9092, kafka-b:9092",
		ClientID:      "studio",
		SASLMechanism: model.SASLPlain,
		Username:      "alice",
		Password:      "secret",
	})
	if err != nil {
		t.Fatal(err)
	}

	items, err := connections.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("saved %d connections, want 1", len(items))
	}
	if got := items[0].BootstrapServers; len(got) != 2 || got[0] != "kafka-a:9092" || got[1] != "kafka-b:9092" {
		t.Fatalf("BootstrapServers = %#v", got)
	}
	if got := credentials.values[items[0].ID]; got != "secret" {
		t.Fatalf("stored password = %q, want secret", got)
	}
	if got := app.Snapshot().ActiveConnectionID; got != items[0].ID {
		t.Fatalf("ActiveConnectionID = %q, want %q", got, items[0].ID)
	}
}

func TestSaveConnectionPersistsProxyConfigurationAndPasswordSeparately(t *testing.T) {
	connections := store.NewFileStore(t.TempDir() + "/connections.json")
	credentials := &memoryCredentials{values: map[string]string{}}
	app := NewWithDependencies(Dependencies{Connections: connections, Credentials: credentials})
	form := testProxyConnectionForm()
	app.markConnectionTested(t, form)

	if err := app.SaveConnection(form); err != nil {
		t.Fatal(err)
	}

	items, err := connections.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("saved %d connections, want 1", len(items))
	}
	connection := items[0]
	if !connection.Proxy.Enabled || connection.Proxy.Address != "203.0.113.10" ||
		connection.Proxy.Port != 2222 || connection.Proxy.Username != "jump" ||
		connection.Proxy.HostKey != "host-key" {
		t.Fatalf("Proxy = %#v", connection.Proxy)
	}
	if got := credentials.proxyValues[connection.ID]; got != "ssh-secret" {
		t.Fatalf("stored SSH password = %q, want ssh-secret", got)
	}
	if _, found := credentials.values[connection.ID]; found {
		t.Fatal("SSH password was stored in the Kafka credential namespace")
	}
}

func TestSaveConnectionRequiresTestedProxyConfiguration(t *testing.T) {
	connections := store.NewFileStore(t.TempDir() + "/connections.json")
	app := NewWithDependencies(Dependencies{
		Connections: connections,
		Credentials: &memoryCredentials{values: map[string]string{}},
	})
	form := testProxyConnectionForm()

	if err := app.SaveConnection(form); err == nil {
		t.Fatal("SaveConnection() succeeded without testing the proxy configuration")
	}
	items, err := connections.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 0 {
		t.Fatalf("saved %d connections without a test", len(items))
	}
}

func TestSaveConnectionRejectsProxyChangeAfterSuccessfulTest(t *testing.T) {
	connections := store.NewFileStore(t.TempDir() + "/connections.json")
	app := NewWithDependencies(Dependencies{
		Connections: connections,
		Credentials: &memoryCredentials{values: map[string]string{}},
	})
	form := testProxyConnectionForm()
	app.markConnectionTested(t, form)
	form.ProxyAddress = "203.0.113.11"

	if err := app.SaveConnection(form); err == nil {
		t.Fatal("SaveConnection() reused a host key after the SSH address changed")
	}
	items, err := connections.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 0 {
		t.Fatalf("saved %d connections after the tested proxy changed", len(items))
	}
}

func TestSaveConnectionAllowsUnchangedStoredProxyWithoutRetest(t *testing.T) {
	connections := store.NewFileStore(t.TempDir() + "/connections.json")
	credentials := &memoryCredentials{values: map[string]string{}}
	app := NewWithDependencies(Dependencies{Connections: connections, Credentials: credentials})
	form := testProxyConnectionForm()
	app.markConnectionTested(t, form)
	if err := app.SaveConnection(form); err != nil {
		t.Fatal(err)
	}
	connection := app.Snapshot().Connections[0]
	form.ID = connection.ID
	form.Name = "修改后的连接名称"

	if err := app.SaveConnection(form); err != nil {
		t.Fatalf("SaveConnection() rejected an unchanged stored proxy: %v", err)
	}
	items, err := connections.List()
	if err != nil {
		t.Fatal(err)
	}
	if got := items[0].Name; got != "修改后的连接名称" {
		t.Fatalf("Name = %q, want updated name", got)
	}
}

func TestConnectionFormForEditLoadsProxyPassword(t *testing.T) {
	connections := store.NewFileStore(t.TempDir() + "/connections.json")
	connection := model.Connection{
		ID:               "proxy",
		Name:             "Proxy Kafka",
		BootstrapServers: []string{"10.0.0.8:9092"},
		Proxy: model.SSHProxyConfig{
			Enabled: true, Address: "203.0.113.10", Port: 22, Username: "jump", HostKey: "host-key",
		},
	}
	if err := connections.Save(connection); err != nil {
		t.Fatal(err)
	}
	credentials := &memoryCredentials{
		values:      map[string]string{},
		proxyValues: map[string]string{"proxy": "ssh-secret"},
	}
	app := NewWithDependencies(Dependencies{Connections: connections, Credentials: credentials})

	form, err := app.connectionFormForEdit(connection.ID)
	if err != nil {
		t.Fatal(err)
	}
	if form.ProxyPassword != "ssh-secret" {
		t.Fatalf("ProxyPassword = %q, want ssh-secret", form.ProxyPassword)
	}
}

func TestDeleteConnectionRemovesProxyPassword(t *testing.T) {
	connections := store.NewFileStore(t.TempDir() + "/connections.json")
	connection := model.Connection{
		ID:               "proxy",
		Name:             "Proxy Kafka",
		BootstrapServers: []string{"10.0.0.8:9092"},
		Proxy: model.SSHProxyConfig{
			Enabled: true, Address: "203.0.113.10", Port: 22, Username: "jump", HostKey: "host-key",
		},
	}
	if err := connections.Save(connection); err != nil {
		t.Fatal(err)
	}
	credentials := &memoryCredentials{
		values:      map[string]string{"proxy": "kafka-secret"},
		proxyValues: map[string]string{"proxy": "ssh-secret"},
	}
	app := NewWithDependencies(Dependencies{Connections: connections, Credentials: credentials})

	if err := app.DeleteConnection(connection.ID); err != nil {
		t.Fatal(err)
	}
	if _, found := credentials.proxyValues[connection.ID]; found {
		t.Fatal("SSH proxy credential survived connection deletion")
	}
}

func TestTestConnectionShowsHostKeyCandidateWithoutPersistingConnection(t *testing.T) {
	fake := &connectionTestKafka{err: &kafka.SSHHostKeyError{
		Fingerprint: "SHA256:new-host",
		HostKey:     "new-host-key",
	}}
	connections := store.NewFileStore(t.TempDir() + "/connections.json")
	app := NewWithDependencies(Dependencies{
		Connections: connections,
		Credentials: &memoryCredentials{values: map[string]string{}},
		Kafka:       fake,
	})

	app.TestConnection(testProxyConnectionForm())
	waitForConnectionTest(t, app)
	state := app.Snapshot()
	if state.ConnectionHostKeyCandidate != "new-host-key" || state.ConnectionHostKeyFingerprint != "SHA256:new-host" {
		t.Fatalf("host key candidate = %q, %q", state.ConnectionHostKeyCandidate, state.ConnectionHostKeyFingerprint)
	}
	if state.ConnectionTestedFingerprint != "" {
		t.Fatal("unconfirmed host key was marked as tested")
	}
	items, err := connections.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 0 {
		t.Fatalf("testing persisted %d connections", len(items))
	}
}

func TestSuccessfulTestConnectionRecordsTestedConfiguration(t *testing.T) {
	app := NewWithDependencies(Dependencies{
		Connections: store.NewFileStore(t.TempDir() + "/connections.json"),
		Credentials: &memoryCredentials{values: map[string]string{}},
		Kafka:       &connectionTestKafka{},
	})
	form := testProxyConnectionForm()

	app.TestConnection(form)
	waitForConnectionTest(t, app)
	if got := app.Snapshot().ConnectionTestedFingerprint; got == "" {
		t.Fatal("successful connection test did not record the tested configuration")
	}
}

func testProxyConnectionForm() ConnectionForm {
	return ConnectionForm{
		Name:          "VPC Kafka",
		Servers:       "10.0.0.8:9092",
		ProxyEnabled:  true,
		ProxyAddress:  "203.0.113.10",
		ProxyPort:     "2222",
		ProxyUsername: "jump",
		ProxyPassword: "ssh-secret",
		ProxyHostKey:  "host-key",
	}
}

func (a *App) markConnectionTested(t *testing.T, form ConnectionForm) {
	t.Helper()
	connection, err := a.connectionFromForm(form)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint := connectionTransportFingerprint(connection, kafka.ClientCredentials{
		KafkaPassword: form.Password,
		ProxyPassword: form.ProxyPassword,
	})
	a.update(func(state *State) { state.ConnectionTestedFingerprint = fingerprint })
}

type connectionTestKafka struct {
	KafkaAPI
	err error
}

func (k *connectionTestKafka) TestConnection(context.Context, model.Connection, kafka.ClientCredentials) error {
	return k.err
}

func waitForConnectionTest(t *testing.T, app *App) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if !app.Snapshot().ConnectionTesting {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("connection test did not finish")
}

func TestSaveConnectionCopiesCAFileIntoUserData(t *testing.T) {
	dataDir := t.TempDir()
	mygo.App.SetPath(mygo.PathUserData, dataDir)
	t.Cleanup(func() { mygo.App.SetPath(mygo.PathUserData, "") })

	source := filepath.Join(t.TempDir(), "ca.pem")
	certificate := []byte("-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----\n")
	if err := os.WriteFile(source, certificate, 0o600); err != nil {
		t.Fatal(err)
	}

	connections := store.NewFileStore(filepath.Join(dataDir, "connections.json"))
	app := NewWithDependencies(Dependencies{
		Connections: connections,
		Credentials: &memoryCredentials{values: map[string]string{}},
	})
	err := app.SaveConnection(ConnectionForm{
		Name:       "TLS 集群",
		Servers:    "kafka:9093",
		TLSEnabled: true,
		CAFile:     source,
	})
	if err != nil {
		t.Fatal(err)
	}

	items, err := connections.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("saved %d connections, want 1", len(items))
	}
	want := filepath.Join(dataDir, "ca-certificates", items[0].ID+".pem")
	if got := items[0].TLS.CAFile; got != want {
		t.Fatalf("TLS.CAFile = %q, want %q", got, want)
	}
	got, err := os.ReadFile(want)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, certificate) {
		t.Fatalf("copied CA = %q, want %q", got, certificate)
	}
}

func TestDeleteConnectionRemovesManagedCAFile(t *testing.T) {
	dataDir := t.TempDir()
	mygo.App.SetPath(mygo.PathUserData, dataDir)
	t.Cleanup(func() { mygo.App.SetPath(mygo.PathUserData, "") })

	source := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(source, []byte("certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	connections := store.NewFileStore(filepath.Join(dataDir, "connections.json"))
	app := NewWithDependencies(Dependencies{
		Connections: connections,
		Credentials: &memoryCredentials{values: map[string]string{}},
	})
	if err := app.SaveConnection(ConnectionForm{
		Name:       "TLS 集群",
		Servers:    "kafka:9093",
		TLSEnabled: true,
		CAFile:     source,
	}); err != nil {
		t.Fatal(err)
	}
	connection := app.Snapshot().Connections[0]

	if err := app.DeleteConnection(connection.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(connection.TLS.CAFile); !os.IsNotExist(err) {
		t.Fatalf("managed CA still exists after deleting connection: %v", err)
	}
}

func TestDeleteConnectionKeepsActiveConnectionWhenDeletingAnother(t *testing.T) {
	connections := store.NewFileStore(t.TempDir() + "/connections.json")
	for _, connection := range []model.Connection{
		{ID: "first", Name: "First", BootstrapServers: []string{"first:9092"}},
		{ID: "second", Name: "Second", BootstrapServers: []string{"second:9092"}},
	} {
		if err := connections.Save(connection); err != nil {
			t.Fatal(err)
		}
	}
	credentials := &memoryCredentials{values: map[string]string{"first": "one", "second": "two"}}
	app := NewWithDependencies(Dependencies{Connections: connections, Credentials: credentials})
	app.update(func(state *State) { state.ActiveConnectionID = "first" })

	if err := app.DeleteConnection("second"); err != nil {
		t.Fatal(err)
	}
	if got := app.Snapshot().ActiveConnectionID; got != "first" {
		t.Fatalf("ActiveConnectionID = %q, want first", got)
	}
	if _, found := credentials.values["second"]; found {
		t.Fatal("deleted connection credentials remain")
	}
}

func TestDeleteActiveConnectionSelectsTheNextConnection(t *testing.T) {
	connections := store.NewFileStore(t.TempDir() + "/connections.json")
	for _, connection := range []model.Connection{
		{ID: "first", Name: "First", BootstrapServers: []string{"first:9092"}},
		{ID: "second", Name: "Second", BootstrapServers: []string{"second:9092"}},
	} {
		if err := connections.Save(connection); err != nil {
			t.Fatal(err)
		}
	}
	credentials := &memoryCredentials{values: map[string]string{"first": "one", "second": "two"}}
	app := NewWithDependencies(Dependencies{Connections: connections, Credentials: credentials})
	app.update(func(state *State) { state.ActiveConnectionID = "first" })

	if err := app.DeleteConnection("first"); err != nil {
		t.Fatal(err)
	}
	if got := app.Snapshot().ActiveConnectionID; got != "second" {
		t.Fatalf("ActiveConnectionID = %q, want second", got)
	}
}

func TestDeleteLastActiveConnectionClearsSelection(t *testing.T) {
	connections := store.NewFileStore(t.TempDir() + "/connections.json")
	for _, connection := range []model.Connection{
		{ID: "first", Name: "First", BootstrapServers: []string{"first:9092"}},
		{ID: "second", Name: "Second", BootstrapServers: []string{"second:9092"}},
	} {
		if err := connections.Save(connection); err != nil {
			t.Fatal(err)
		}
	}
	credentials := &memoryCredentials{values: map[string]string{"first": "one", "second": "two"}}
	app := NewWithDependencies(Dependencies{Connections: connections, Credentials: credentials})
	app.update(func(state *State) { state.ActiveConnectionID = "second" })

	if err := app.DeleteConnection("second"); err != nil {
		t.Fatal(err)
	}
	if got := app.Snapshot().ActiveConnectionID; got != "" {
		t.Fatalf("ActiveConnectionID = %q, want empty", got)
	}
}
