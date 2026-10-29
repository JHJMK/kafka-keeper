package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// FileSecrets stores credentials in a private JSON file in the application's
// user-data directory. It deliberately has no operating-system keychain
// dependency.
type FileSecrets struct {
	mu   sync.Mutex
	path string
}

func NewFileSecrets(path string) SecretBackend {
	return &FileSecrets{path: path}
}

func (s *FileSecrets) Set(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	values, err := s.read()
	if err != nil {
		return err
	}
	values[key] = append([]byte(nil), value...)
	return s.write(values)
}

func (s *FileSecrets) Get(key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	values, err := s.read()
	if err != nil {
		return nil, err
	}
	value, found := values[key]
	if !found {
		return nil, ErrSecretNotFound
	}
	return append([]byte(nil), value...), nil
}

func (s *FileSecrets) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	values, err := s.read()
	if err != nil {
		return err
	}
	if _, found := values[key]; !found {
		return nil
	}
	delete(values, key)
	return s.write(values)
}

func (s *FileSecrets) read() (map[string][]byte, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return map[string][]byte{}, nil
	}
	if err != nil {
		return nil, err
	}
	var values map[string][]byte
	if err := json.Unmarshal(data, &values); err != nil {
		return nil, fmt.Errorf("读取凭据文件失败: %w", err)
	}
	if values == nil {
		values = map[string][]byte{}
	}
	return values, nil
}

func (s *FileSecrets) write(values map[string][]byte) error {
	data, err := json.MarshalIndent(values, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".credentials-*.tmp")
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
