package kafka

import (
	"context"
	"fmt"
	"sync"

	"github.com/jjk/kafka-keeper/internal/model"
)

type CredentialReader interface {
	Get(id string) (string, bool, error)
	GetProxyPassword(id string) (string, bool, error)
}

type Service struct {
	credentials CredentialReader
	manager     *Manager
	factory     ClientFactory

	consumerMu sync.Mutex
	consumers  map[string]*ConsumerSession
}

func NewService(credentials CredentialReader) *Service {
	factory := NewFranzClient
	return &Service{
		credentials: credentials,
		manager:     NewManager(factory),
		factory:     factory,
		consumers:   make(map[string]*ConsumerSession),
	}
}

func newService(credentials CredentialReader, factory ClientFactory) *Service {
	return &Service{
		credentials: credentials,
		manager:     NewManager(factory),
		factory:     factory,
		consumers:   make(map[string]*ConsumerSession),
	}
}

func (s *Service) TestConnection(ctx context.Context, conn model.Connection, credentials ClientCredentials) error {
	client, err := s.factory(ctx, conn, credentials)
	if err != nil {
		return safeError("创建 Kafka Client", err, credentials.secrets()...)
	}
	defer client.Close()
	if err := client.Ping(ctx); err != nil {
		return safeError("连接 Kafka", err, credentials.secrets()...)
	}
	return nil
}

func (s *Service) ForgetConnection(id string) {
	if session := s.removeConsumer(id, nil); session != nil {
		session.Close()
	}
	s.manager.Remove(id)
}

func (s *Service) Close() {
	s.consumerMu.Lock()
	sessions := make([]*ConsumerSession, 0, len(s.consumers))
	for id, session := range s.consumers {
		sessions = append(sessions, session)
		delete(s.consumers, id)
	}
	s.consumerMu.Unlock()
	for _, session := range sessions {
		session.closeClient()
	}
	s.manager.Close()
}

func (s *Service) registerConsumer(session *ConsumerSession) error {
	s.consumerMu.Lock()
	defer s.consumerMu.Unlock()
	if _, exists := s.consumers[session.connID]; exists {
		return fmt.Errorf("该连接已有活动的 Consumer Group 消费会话")
	}
	s.consumers[session.connID] = session
	return nil
}

func (s *Service) removeConsumer(connID string, expected *ConsumerSession) *ConsumerSession {
	s.consumerMu.Lock()
	defer s.consumerMu.Unlock()
	session, exists := s.consumers[connID]
	if !exists || (expected != nil && session != expected) {
		return nil
	}
	delete(s.consumers, connID)
	return session
}

func (s *Service) client(ctx context.Context, conn model.Connection) (*Client, error) {
	credentials, err := s.clientCredentials(conn)
	if err != nil {
		return nil, err
	}
	client, err := s.manager.Get(ctx, conn, credentials)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func (s *Service) clientCredentials(conn model.Connection) (ClientCredentials, error) {
	password, _, err := s.credentials.Get(conn.ID)
	if err != nil {
		return ClientCredentials{}, safeError("读取 Kafka 凭据", err)
	}
	credentials := ClientCredentials{KafkaPassword: password}
	if !conn.Proxy.Enabled {
		return credentials, nil
	}
	proxyPassword, _, err := s.credentials.GetProxyPassword(conn.ID)
	if err != nil {
		return ClientCredentials{}, safeError("读取 SSH 代理凭据", err)
	}
	credentials.ProxyPassword = proxyPassword
	return credentials, nil
}
