package model

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	SASLNone        = ""
	SASLPlain       = "plain"
	SASLScramSHA256 = "scram-sha-256"
	SASLScramSHA512 = "scram-sha-512"
)

type TLSConfig struct {
	Enabled bool   `json:"enabled"`
	CAFile  string `json:"caFile,omitempty"`
}

type SASLConfig struct {
	Mechanism string `json:"mechanism,omitempty"`
	Username  string `json:"username,omitempty"`
}

type SSHProxyConfig struct {
	Enabled  bool   `json:"enabled"`
	Address  string `json:"address,omitempty"`
	Port     int    `json:"port,omitempty"`
	Username string `json:"username,omitempty"`
	HostKey  string `json:"hostKey,omitempty"`
}

type Connection struct {
	ID               string         `json:"id"`
	Name             string         `json:"name"`
	BootstrapServers []string       `json:"bootstrapServers"`
	ClientID         string         `json:"clientId,omitempty"`
	TLS              TLSConfig      `json:"tls"`
	SASL             SASLConfig     `json:"sasl"`
	Proxy            SSHProxyConfig `json:"proxy"`
	CreatedAt        time.Time      `json:"createdAt"`
	UpdatedAt        time.Time      `json:"updatedAt"`
}

func (c Connection) Validate() error {
	if strings.TrimSpace(c.ID) == "" {
		return errors.New("连接 ID 不能为空")
	}
	if strings.TrimSpace(c.Name) == "" {
		return errors.New("连接名称不能为空")
	}
	if len(c.BootstrapServers) == 0 {
		return errors.New("Bootstrap Servers 至少需要一个地址")
	}
	for _, server := range c.BootstrapServers {
		if strings.TrimSpace(server) == "" {
			return errors.New("Bootstrap Servers 不能包含空地址")
		}
	}
	switch c.SASL.Mechanism {
	case SASLNone, SASLPlain, SASLScramSHA256, SASLScramSHA512:
	default:
		return fmt.Errorf("不支持的 SASL 机制 %q", c.SASL.Mechanism)
	}
	if c.SASL.Mechanism != SASLNone && strings.TrimSpace(c.SASL.Username) == "" {
		return errors.New("SASL 用户名不能为空")
	}
	if c.Proxy.Enabled {
		if strings.TrimSpace(c.Proxy.Address) == "" {
			return errors.New("SSH 代理节点不能为空")
		}
		if port := c.Proxy.Port; port < 0 || port > 65535 {
			return errors.New("SSH 端口必须在 1 到 65535 之间")
		}
		if strings.TrimSpace(c.Proxy.Username) == "" {
			return errors.New("SSH 用户名不能为空")
		}
	}
	return nil
}

func (p SSHProxyConfig) PortOrDefault() int {
	if p.Port == 0 {
		return 22
	}
	return p.Port
}

func (p SSHProxyConfig) Endpoint() string {
	return fmt.Sprintf("%s:%d", strings.TrimSpace(p.Address), p.PortOrDefault())
}

func ParseServers(raw string) []string {
	parts := strings.Split(raw, ",")
	servers := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			servers = append(servers, value)
		}
	}
	return servers
}

func (c Connection) ServersText() string {
	return strings.Join(c.BootstrapServers, ", ")
}
