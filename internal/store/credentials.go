package store

import (
	"errors"
	"sync"
)

var ErrSecretNotFound = errors.New("凭据不存在")

const (
	credentialPrefix      = "kafka-connection/"
	proxyCredentialPrefix = "kafka-proxy/"
)

type Credentials struct {
	mu             sync.Mutex
	backend        SecretBackend
	passwords      map[string]string
	fallback       map[string]string
	session        map[string]bool
	proxyPasswords map[string]string
	proxyFallback  map[string]string
	proxySession   map[string]bool
}

func NewCredentials(backend SecretBackend) *Credentials {
	return &Credentials{
		backend:        backend,
		passwords:      make(map[string]string),
		fallback:       make(map[string]string),
		session:        make(map[string]bool),
		proxyPasswords: make(map[string]string),
		proxyFallback:  make(map[string]string),
		proxySession:   make(map[string]bool),
	}
}

func (c *Credentials) Set(id, password string) error {
	key := credentialPrefix + id
	c.mu.Lock()
	defer c.mu.Unlock()
	err := c.backend.Set(key, []byte(password))
	if errors.Is(err, errors.ErrUnsupported) {
		c.fallback[id] = password
		c.session[id] = true
		return nil
	}
	if err != nil {
		return err
	}
	c.passwords[id] = password
	delete(c.fallback, id)
	delete(c.session, id)
	return nil
}

func (c *Credentials) Get(id string) (string, bool, error) {
	key := credentialPrefix + id
	c.mu.Lock()
	defer c.mu.Unlock()
	if password, ok := c.passwords[id]; ok {
		return password, true, nil
	}
	if password, ok := c.fallback[id]; ok {
		return password, true, nil
	}

	data, err := c.backend.Get(key)
	if errors.Is(err, ErrSecretNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	password := string(data)
	c.passwords[id] = password
	return password, true, nil
}

func (c *Credentials) Delete(id string) error {
	key := credentialPrefix + id
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.passwords, id)
	delete(c.fallback, id)
	delete(c.session, id)
	err := c.backend.Delete(key)
	if errors.Is(err, errors.ErrUnsupported) {
		return nil
	}
	return err
}

func (c *Credentials) SessionOnly(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.session[id]
}

func (c *Credentials) SetProxyPassword(id, password string) error {
	key := proxyCredentialPrefix + id
	c.mu.Lock()
	defer c.mu.Unlock()
	err := c.backend.Set(key, []byte(password))
	if errors.Is(err, errors.ErrUnsupported) {
		c.proxyFallback[id] = password
		c.proxySession[id] = true
		return nil
	}
	if err != nil {
		return err
	}
	c.proxyPasswords[id] = password
	delete(c.proxyFallback, id)
	delete(c.proxySession, id)
	return nil
}

func (c *Credentials) GetProxyPassword(id string) (string, bool, error) {
	key := proxyCredentialPrefix + id
	c.mu.Lock()
	defer c.mu.Unlock()
	if password, ok := c.proxyPasswords[id]; ok {
		return password, true, nil
	}
	if password, ok := c.proxyFallback[id]; ok {
		return password, true, nil
	}

	data, err := c.backend.Get(key)
	if errors.Is(err, ErrSecretNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	password := string(data)
	c.proxyPasswords[id] = password
	return password, true, nil
}

func (c *Credentials) DeleteProxyPassword(id string) error {
	key := proxyCredentialPrefix + id
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.proxyPasswords, id)
	delete(c.proxyFallback, id)
	delete(c.proxySession, id)
	err := c.backend.Delete(key)
	if errors.Is(err, errors.ErrUnsupported) {
		return nil
	}
	return err
}

func (c *Credentials) ProxySessionOnly(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.proxySession[id]
}
