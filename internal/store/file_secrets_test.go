package store

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestFileSecretsPersistsWithPrivatePermissions(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "kafka-keeper")
	path := filepath.Join(dir, "credentials.json")
	secrets := NewFileSecrets(path)

	if err := secrets.Set("kafka-connection/one", []byte("secret")); err != nil {
		t.Fatal(err)
	}
	reloaded := NewFileSecrets(path)
	value, err := reloaded.Get("kafka-connection/one")
	if err != nil || string(value) != "secret" {
		t.Fatalf("Get() = %q, %v", value, err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("credentials file mode = %o, want 600", got)
	}
	dirInfo, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := dirInfo.Mode().Perm(); got != 0o700 {
		t.Fatalf("credentials directory mode = %o, want 700", got)
	}
}

func TestFileSecretsDelete(t *testing.T) {
	secrets := NewFileSecrets(filepath.Join(t.TempDir(), "credentials.json"))
	if err := secrets.Set("key", []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := secrets.Delete("key"); err != nil {
		t.Fatal(err)
	}
	if _, err := secrets.Get("key"); !errors.Is(err, ErrSecretNotFound) {
		t.Fatalf("Get() error = %v, want ErrSecretNotFound", err)
	}
	if err := secrets.Delete("missing"); err != nil {
		t.Fatalf("deleting a missing secret: %v", err)
	}
}
