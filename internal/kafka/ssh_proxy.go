package kafka

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/jjk/kafka-keeper/internal/model"
)

const sshConnectTimeout = 8 * time.Second

var ErrSSHProxyClosed = errors.New("SSH 代理已关闭")

type SSHHostKeyError struct {
	Fingerprint string
	HostKey     string
}

func (e *SSHHostKeyError) Error() string {
	return fmt.Sprintf("SSH 主机指纹尚未确认：%s", e.Fingerprint)
}

type sshProxyDialer struct {
	proxy    model.SSHProxyConfig
	password string

	mu     sync.Mutex
	client *ssh.Client
	closed bool
}

func newSSHProxyDialer(proxy model.SSHProxyConfig, password string) (*sshProxyDialer, error) {
	if !proxy.Enabled {
		return nil, errors.New("SSH 代理未启用")
	}
	if _, err := sshHostKeyCallback(proxy.HostKey); err != nil {
		return nil, err
	}
	return &sshProxyDialer{proxy: proxy, password: password}, nil
}

func (d *sshProxyDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	client, err := d.getClient(ctx)
	if err != nil {
		return nil, err
	}
	conn, err := client.DialContext(ctx, network, address)
	if err == nil {
		return conn, nil
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	d.dropClient(client)
	client, reconnectErr := d.getClient(ctx)
	if reconnectErr != nil {
		return nil, reconnectErr
	}
	conn, retryErr := client.DialContext(ctx, network, address)
	if retryErr != nil {
		return nil, fmt.Errorf("SSH 转发到 %s 失败: %w", address, retryErr)
	}
	return conn, nil
}

func (d *sshProxyDialer) getClient(ctx context.Context) (*ssh.Client, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil, ErrSSHProxyClosed
	}
	if d.client != nil {
		return d.client, nil
	}

	callback, err := sshHostKeyCallback(d.proxy.HostKey)
	if err != nil {
		return nil, err
	}
	config := &ssh.ClientConfig{
		User:            strings.TrimSpace(d.proxy.Username),
		Auth:            []ssh.AuthMethod{ssh.Password(d.password)},
		HostKeyCallback: callback,
		Timeout:         sshConnectTimeout,
	}

	dialer := net.Dialer{Timeout: sshConnectTimeout}
	rawConn, err := dialer.DialContext(ctx, "tcp", d.proxy.Endpoint())
	if err != nil {
		return nil, fmt.Errorf("连接 SSH 代理 %s 失败: %w", d.proxy.Endpoint(), err)
	}
	deadline := time.Now().Add(sshConnectTimeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	_ = rawConn.SetDeadline(deadline)
	sshConn, channels, requests, err := ssh.NewClientConn(rawConn, d.proxy.Endpoint(), config)
	if err != nil {
		_ = rawConn.Close()
		return nil, fmt.Errorf("建立 SSH 连接失败: %w", err)
	}
	_ = rawConn.SetDeadline(time.Time{})
	d.client = ssh.NewClient(sshConn, channels, requests)
	return d.client, nil
}

func (d *sshProxyDialer) dropClient(client *ssh.Client) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.client == client {
		d.client = nil
	}
	_ = client.Close()
}

func (d *sshProxyDialer) Close() {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	d.closed = true
	client := d.client
	d.client = nil
	d.mu.Unlock()
	if client != nil {
		_ = client.Close()
	}
}

func sshHostKeyCallback(encoded string) (ssh.HostKeyCallback, error) {
	encoded = strings.TrimSpace(encoded)
	if encoded == "" {
		return func(_ string, _ net.Addr, key ssh.PublicKey) error {
			return &SSHHostKeyError{
				Fingerprint: ssh.FingerprintSHA256(key),
				HostKey:     base64.StdEncoding.EncodeToString(key.Marshal()),
			}
		}, nil
	}
	expected, err := parseSSHHostKey(encoded)
	if err != nil {
		return nil, fmt.Errorf("读取 SSH 主机指纹失败: %w", err)
	}
	return func(_ string, _ net.Addr, actual ssh.PublicKey) error {
		if !bytes.Equal(expected.Marshal(), actual.Marshal()) {
			return fmt.Errorf("SSH 主机指纹已变化，拒绝连接；当前指纹 %s", ssh.FingerprintSHA256(actual))
		}
		return nil
	}, nil
}

func parseSSHHostKey(encoded string) (ssh.PublicKey, error) {
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err == nil {
		if key, parseErr := ssh.ParsePublicKey(decoded); parseErr == nil {
			return key, nil
		}
	}
	key, _, _, _, err := ssh.ParseAuthorizedKey([]byte(encoded))
	if err != nil {
		return nil, errors.New("主机指纹格式无效")
	}
	return key, nil
}

func SSHHostKeyFingerprint(encoded string) (string, error) {
	key, err := parseSSHHostKey(encoded)
	if err != nil {
		return "", err
	}
	return ssh.FingerprintSHA256(key), nil
}

func newProxyKafkaDialer(sshDialer *sshProxyDialer, tlsConfig *tls.Config) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := sshDialer.DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		if tlsConfig == nil {
			return conn, nil
		}

		config := tlsConfig.Clone()
		if config.ServerName == "" {
			host, _, splitErr := net.SplitHostPort(address)
			if splitErr != nil {
				_ = conn.Close()
				return nil, fmt.Errorf("读取 Kafka Broker 地址失败: %w", splitErr)
			}
			config.ServerName = host
		}
		tlsConn := tls.Client(conn, config)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("Kafka TLS 握手失败: %w", err)
		}
		return tlsConn, nil
	}
}
