package model

import (
	"strings"
	"testing"
	"time"
)

func validConnection() Connection {
	now := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	return Connection{
		ID: "one", Name: "本地集群",
		BootstrapServers: []string{"127.0.0.1:9092"},
		SASL:             SASLConfig{Mechanism: SASLNone},
		CreatedAt:        now, UpdatedAt: now,
	}
}

func TestConnectionValidation(t *testing.T) {
	valid := validConnection()
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid connection rejected: %v", err)
	}

	invalid := valid
	invalid.Name = " "
	if err := invalid.Validate(); err == nil || !strings.Contains(err.Error(), "名称") {
		t.Fatalf("expected name error, got %v", err)
	}

	invalid = valid
	invalid.BootstrapServers = nil
	if err := invalid.Validate(); err == nil || !strings.Contains(err.Error(), "Bootstrap") {
		t.Fatalf("expected bootstrap error, got %v", err)
	}

	invalid = valid
	invalid.SASL = SASLConfig{Mechanism: SASLPlain}
	if err := invalid.Validate(); err == nil || !strings.Contains(err.Error(), "用户名") {
		t.Fatalf("expected username error, got %v", err)
	}
}

func TestConnectionValidationForSSHProxy(t *testing.T) {
	valid := validConnection()
	valid.Proxy = SSHProxyConfig{
		Enabled:  true,
		Address:  "jump.example.com",
		Port:     22,
		Username: "devops",
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid SSH proxy rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*SSHProxyConfig)
		want   string
	}{
		{name: "missing address", mutate: func(proxy *SSHProxyConfig) { proxy.Address = " " }, want: "代理节点"},
		{name: "invalid port", mutate: func(proxy *SSHProxyConfig) { proxy.Port = 70000 }, want: "SSH 端口"},
		{name: "missing username", mutate: func(proxy *SSHProxyConfig) { proxy.Username = "" }, want: "SSH 用户名"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			connection := valid
			test.mutate(&connection.Proxy)
			if err := connection.Validate(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate() error = %v, want message containing %q", err, test.want)
			}
		})
	}
}

func TestSSHProxyPortDefaultsTo22(t *testing.T) {
	proxy := SSHProxyConfig{Enabled: true, Address: "jump.example.com", Username: "devops"}
	if got := proxy.PortOrDefault(); got != 22 {
		t.Fatalf("PortOrDefault() = %d, want 22", got)
	}
	if got := proxy.Endpoint(); got != "jump.example.com:22" {
		t.Fatalf("Endpoint() = %q, want jump.example.com:22", got)
	}
}

func TestParseServersAndFormatting(t *testing.T) {
	got := ParseServers(" localhost:9092, kafka:9093 ,, ")
	if len(got) != 2 || got[0] != "localhost:9092" || got[1] != "kafka:9093" {
		t.Fatalf("ParseServers() = %#v", got)
	}
	conn := validConnection()
	conn.BootstrapServers = got
	if conn.ServersText() != "localhost:9092, kafka:9093" {
		t.Fatalf("ServersText() = %q", conn.ServersText())
	}
}
