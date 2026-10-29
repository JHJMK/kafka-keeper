package store

import "github.com/jjk/kafka-keeper/internal/model"

type ConnectionStore interface {
	List() ([]model.Connection, error)
	Save(model.Connection) error
	Delete(id string) error
}

type SecretBackend interface {
	Set(key string, value []byte) error
	Get(key string) ([]byte, error)
	Delete(key string) error
}
