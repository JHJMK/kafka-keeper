package store

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jjk/kafka-keeper/internal/model"
)

func testConnection(id, name string) model.Connection {
	now := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	return model.Connection{
		ID: id, Name: name,
		BootstrapServers: []string{"127.0.0.1:9092"},
		CreatedAt:        now, UpdatedAt: now,
	}
}

func TestFileStoreSaveListUpdateDelete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "connections.json")
	s := NewFileStore(path)

	if err := s.Save(testConnection("one", "本地")); err != nil {
		t.Fatal(err)
	}
	items, err := s.List()
	if err != nil || len(items) != 1 || items[0].Name != "本地" {
		t.Fatalf("List() = %#v, %v", items, err)
	}

	updated := testConnection("one", "本地更新")
	if err := s.Save(updated); err != nil {
		t.Fatal(err)
	}
	items, _ = s.List()
	if len(items) != 1 || items[0].Name != "本地更新" {
		t.Fatalf("update failed: %#v", items)
	}
	if err := s.Save(testConnection("two", "本地更新")); err == nil {
		t.Fatal("expected duplicate name error")
	}
	if err := s.Delete("one"); err != nil {
		t.Fatal(err)
	}
	items, _ = s.List()
	if len(items) != 0 {
		t.Fatalf("delete failed: %#v", items)
	}
}

func TestFileStoreWritesPrivateJSONAtomically(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "connections.json")
	s := NewFileStore(path)
	if err := s.Save(testConnection("one", "本地")); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %o, want 600", info.Mode().Perm())
	}
	matches, _ := filepath.Glob(filepath.Join(dir, ".connections-*.tmp"))
	if len(matches) != 0 {
		t.Fatalf("temporary files left behind: %#v", matches)
	}
}
