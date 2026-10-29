package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/jjk/kafka-keeper/internal/model"
)

type FileStore struct {
	mu   sync.Mutex
	path string
}

func NewFileStore(path string) *FileStore {
	return &FileStore{path: path}
}

func (s *FileStore) List() ([]model.Connection, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.read()
}

func (s *FileStore) Save(conn model.Connection) error {
	if err := conn.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	items, err := s.read()
	if err != nil {
		return err
	}
	name := strings.ToLower(strings.TrimSpace(conn.Name))
	for i := range items {
		if items[i].ID != conn.ID && strings.ToLower(strings.TrimSpace(items[i].Name)) == name {
			return fmt.Errorf("连接名称 %q 已存在", conn.Name)
		}
		if items[i].ID == conn.ID {
			items[i] = conn
			return s.write(items)
		}
	}
	items = append(items, conn)
	return s.write(items)
}

func (s *FileStore) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	items, err := s.read()
	if err != nil {
		return err
	}
	for i := range items {
		if items[i].ID == id {
			items = append(items[:i], items[i+1:]...)
			return s.write(items)
		}
	}
	return errors.New("连接不存在")
}

func (s *FileStore) read() ([]model.Connection, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return []model.Connection{}, nil
	}
	if err != nil {
		return nil, err
	}
	var items []model.Connection
	if err := json.Unmarshal(data, &items); err != nil {
		return nil, fmt.Errorf("读取连接配置失败: %w", err)
	}
	return items, nil
}

func (s *FileStore) write(items []model.Connection) error {
	data, err := json.MarshalIndent(items, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".connections-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}
	if err := tmp.Chmod(0o600); err != nil {
		cleanup()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return nil
}
