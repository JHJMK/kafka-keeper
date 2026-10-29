package kafka

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"

	"github.com/jjk/kafka-keeper/internal/model"
)

var ErrNilClient = errors.New("客户端工厂返回 nil")

type clientEntry struct {
	fingerprint string
	client      *Client
}

type Manager struct {
	mu      sync.Mutex
	clients map[string]clientEntry
	factory ClientFactory
}

func NewManager(factory ClientFactory) *Manager {
	return &Manager{clients: make(map[string]clientEntry), factory: factory}
}

func (m *Manager) Get(ctx context.Context, conn model.Connection, credentials ClientCredentials) (*Client, error) {
	fingerprint := connectionFingerprint(conn, credentials)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		m.mu.Lock()
		existing, hadExisting := m.clients[conn.ID]
		if hadExisting && existing.fingerprint == fingerprint {
			m.mu.Unlock()
			return existing.client, nil
		}
		m.mu.Unlock()

		candidate, err := m.factory(ctx, conn, credentials)
		if err != nil {
			return nil, safeError("创建 Kafka Client", err, credentials.secrets()...)
		}
		if candidate == nil {
			return nil, safeError("创建 Kafka Client", ErrNilClient, credentials.secrets()...)
		}
		if err := candidate.Ping(ctx); err != nil {
			candidate.Close()
			return nil, safeError("连接 Kafka", err, credentials.secrets()...)
		}

		m.mu.Lock()
		current, exists := m.clients[conn.ID]
		if exists && current.fingerprint == fingerprint {
			m.mu.Unlock()
			candidate.Close()
			return current.client, nil
		}
		changedWhileCreating := exists && (!hadExisting || current.fingerprint != existing.fingerprint)
		if changedWhileCreating {
			m.mu.Unlock()
			candidate.Close()
			continue
		}
		var replaced *Client
		if exists {
			replaced = current.client
		}
		m.clients[conn.ID] = clientEntry{fingerprint: fingerprint, client: candidate}
		m.mu.Unlock()

		if replaced != nil {
			replaced.Close()
		}
		return candidate, nil
	}
}

func (m *Manager) Remove(id string) {
	m.mu.Lock()
	entry, ok := m.clients[id]
	if ok {
		delete(m.clients, id)
	}
	m.mu.Unlock()
	if ok {
		entry.client.Close()
	}
}

func (m *Manager) Close() {
	m.mu.Lock()
	entries := make([]clientEntry, 0, len(m.clients))
	for id, entry := range m.clients {
		entries = append(entries, entry)
		delete(m.clients, id)
	}
	m.mu.Unlock()
	for _, entry := range entries {
		entry.client.Close()
	}
}

func connectionFingerprint(conn model.Connection, credentials ClientCredentials) string {
	data, _ := json.Marshal(conn)
	kafkaPasswordHash := sha256.Sum256([]byte(credentials.KafkaPassword))
	proxyPasswordHash := sha256.Sum256([]byte(credentials.ProxyPassword))
	return hex.EncodeToString(data) + ":" + hex.EncodeToString(kafkaPasswordHash[:]) + ":" + hex.EncodeToString(proxyPasswordHash[:])
}
