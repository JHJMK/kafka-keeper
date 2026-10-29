package store

import (
	"errors"
	"testing"
)

var errUnexpectedBackend = errors.New("backend unavailable")

type fakeSecrets struct {
	values      map[string][]byte
	unsupported bool
	getErr      error
	getCalls    int
}

func (f *fakeSecrets) Set(key string, value []byte) error {
	if f.unsupported {
		return errors.ErrUnsupported
	}
	f.values[key] = append([]byte(nil), value...)
	return nil
}

func (f *fakeSecrets) Get(key string) ([]byte, error) {
	f.getCalls++
	if f.getErr != nil {
		return nil, f.getErr
	}
	value, ok := f.values[key]
	if !ok {
		return nil, ErrSecretNotFound
	}
	return append([]byte(nil), value...), nil
}

func (f *fakeSecrets) Delete(key string) error {
	if f.unsupported {
		return errors.ErrUnsupported
	}
	delete(f.values, key)
	return nil
}

func TestCredentialsUseSessionFallbackWhenBackendUnsupported(t *testing.T) {
	backend := &fakeSecrets{values: map[string][]byte{}, unsupported: true}
	creds := NewCredentials(backend)

	if err := creds.Set("one", "secret"); err != nil {
		t.Fatal(err)
	}
	password, found, err := creds.Get("one")
	if err != nil || !found || password != "secret" {
		t.Fatalf("Get() = %q, %v, %v", password, found, err)
	}
	if !creds.SessionOnly("one") {
		t.Fatal("expected session-only credential")
	}
	if err := creds.Delete("one"); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := creds.Get("one"); found {
		t.Fatal("credential survived delete")
	}
}

func TestCredentialsUseBackendWhenAvailable(t *testing.T) {
	backend := &fakeSecrets{values: map[string][]byte{}}
	creds := NewCredentials(backend)
	if err := creds.Set("one", "secret"); err != nil {
		t.Fatal(err)
	}
	if len(backend.values) != 1 {
		t.Fatalf("backend values = %#v", backend.values)
	}
	if creds.SessionOnly("one") {
		t.Fatal("credential should be persistent")
	}
}

func TestCredentialsDistinguishNotFoundFromBackendFailure(t *testing.T) {
	backend := &fakeSecrets{values: map[string][]byte{}}
	creds := NewCredentials(backend)

	password, found, err := creds.Get("missing")
	if err != nil || found || password != "" {
		t.Fatalf("missing Get() = %q, %v, %v", password, found, err)
	}

	backend.getErr = errUnexpectedBackend
	if _, _, err := creds.Get("one"); !errors.Is(err, errUnexpectedBackend) {
		t.Fatalf("backend failure = %v, want %v", err, errUnexpectedBackend)
	}
}

func TestCredentialsCacheLoadedPassword(t *testing.T) {
	backend := &fakeSecrets{values: map[string][]byte{"kafka-connection/one": []byte("secret")}}
	creds := NewCredentials(backend)

	for range 2 {
		password, found, err := creds.Get("one")
		if err != nil || !found || password != "secret" {
			t.Fatalf("Get() = %q, %v, %v", password, found, err)
		}
	}
	if backend.getCalls != 1 {
		t.Fatalf("backend Get() calls = %d, want 1", backend.getCalls)
	}
}

func TestProxyCredentialsUseSeparateNamespace(t *testing.T) {
	backend := &fakeSecrets{values: map[string][]byte{}}
	creds := NewCredentials(backend)

	if err := creds.Set("one", "kafka-secret"); err != nil {
		t.Fatal(err)
	}
	if err := creds.SetProxyPassword("one", "ssh-secret"); err != nil {
		t.Fatal(err)
	}
	if got := string(backend.values["kafka-connection/one"]); got != "kafka-secret" {
		t.Fatalf("Kafka credential = %q", got)
	}
	if got := string(backend.values["kafka-proxy/one"]); got != "ssh-secret" {
		t.Fatalf("proxy credential = %q", got)
	}

	kafkaPassword, found, err := creds.Get("one")
	if err != nil || !found || kafkaPassword != "kafka-secret" {
		t.Fatalf("Get() = %q, %v, %v", kafkaPassword, found, err)
	}
	proxyPassword, found, err := creds.GetProxyPassword("one")
	if err != nil || !found || proxyPassword != "ssh-secret" {
		t.Fatalf("GetProxyPassword() = %q, %v, %v", proxyPassword, found, err)
	}

	if err := creds.DeleteProxyPassword("one"); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := creds.GetProxyPassword("one"); found {
		t.Fatal("proxy credential survived delete")
	}
	if _, found, _ := creds.Get("one"); !found {
		t.Fatal("deleting proxy credential removed Kafka credential")
	}
}

func TestProxyCredentialsUseSessionFallbackWhenBackendUnsupported(t *testing.T) {
	backend := &fakeSecrets{values: map[string][]byte{}, unsupported: true}
	creds := NewCredentials(backend)

	if err := creds.SetProxyPassword("one", "ssh-secret"); err != nil {
		t.Fatal(err)
	}
	password, found, err := creds.GetProxyPassword("one")
	if err != nil || !found || password != "ssh-secret" {
		t.Fatalf("GetProxyPassword() = %q, %v, %v", password, found, err)
	}
	if !creds.ProxySessionOnly("one") {
		t.Fatal("expected session-only proxy credential")
	}
}
