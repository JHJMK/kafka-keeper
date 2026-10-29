package kafka

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/jjk/kafka-keeper/internal/model"
)

type directTCPIPPayload struct {
	Host           string
	Port           uint32
	OriginatorHost string
	OriginatorPort uint32
}

type testSSHServer struct {
	t        *testing.T
	listener net.Listener
	signer   ssh.Signer
	password string

	mu          sync.Mutex
	connections []net.Conn
	closed      bool
	done        chan struct{}
}

type proxyCredentialReader struct {
	kafkaPassword string
	proxyPassword string
}

func (r proxyCredentialReader) Get(string) (string, bool, error) {
	return r.kafkaPassword, r.kafkaPassword != "", nil
}

func (r proxyCredentialReader) GetProxyPassword(string) (string, bool, error) {
	return r.proxyPassword, r.proxyPassword != "", nil
}

func newTestSSHServer(t *testing.T, password string) *testSSHServer {
	t.Helper()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &testSSHServer{
		t: t, listener: listener, signer: signer, password: password,
		done: make(chan struct{}),
	}
	go server.serve()
	t.Cleanup(server.Close)
	return server
}

func (s *testSSHServer) hostKey() string {
	return base64.StdEncoding.EncodeToString(s.signer.PublicKey().Marshal())
}

func (s *testSSHServer) proxyConfig() model.SSHProxyConfig {
	host, portText, _ := net.SplitHostPort(s.listener.Addr().String())
	port, _ := strconv.Atoi(portText)
	return model.SSHProxyConfig{
		Enabled: true, Address: host, Port: port, Username: "tester", HostKey: s.hostKey(),
	}
}

func (s *testSSHServer) serve() {
	defer close(s.done)
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			_ = conn.Close()
			return
		}
		s.connections = append(s.connections, conn)
		s.mu.Unlock()
		go s.handle(conn)
	}
}

func (s *testSSHServer) handle(conn net.Conn) {
	config := &ssh.ServerConfig{
		PasswordCallback: func(_ ssh.ConnMetadata, password []byte) (*ssh.Permissions, error) {
			if string(password) != s.password {
				return nil, errors.New("bad password")
			}
			return nil, nil
		},
	}
	config.AddHostKey(s.signer)
	serverConn, channels, requests, err := ssh.NewServerConn(conn, config)
	if err != nil {
		_ = conn.Close()
		return
	}
	defer serverConn.Close()
	go ssh.DiscardRequests(requests)
	for newChannel := range channels {
		if newChannel.ChannelType() != "direct-tcpip" {
			_ = newChannel.Reject(ssh.UnknownChannelType, "unsupported channel")
			continue
		}
		var payload directTCPIPPayload
		if err := ssh.Unmarshal(newChannel.ExtraData(), &payload); err != nil {
			_ = newChannel.Reject(ssh.ConnectionFailed, "invalid direct-tcpip request")
			continue
		}
		target, err := net.Dial("tcp", net.JoinHostPort(payload.Host, strconv.Itoa(int(payload.Port))))
		if err != nil {
			_ = newChannel.Reject(ssh.ConnectionFailed, err.Error())
			continue
		}
		channel, channelRequests, err := newChannel.Accept()
		if err != nil {
			_ = target.Close()
			continue
		}
		go ssh.DiscardRequests(channelRequests)
		go func() {
			defer target.Close()
			defer channel.Close()
			_, _ = io.Copy(target, channel)
		}()
		go func() {
			defer target.Close()
			defer channel.Close()
			_, _ = io.Copy(channel, target)
		}()
	}
}

func (s *testSSHServer) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	_ = s.listener.Close()
	connections := append([]net.Conn(nil), s.connections...)
	s.mu.Unlock()
	for _, conn := range connections {
		_ = conn.Close()
	}
	select {
	case <-s.done:
	case <-time.After(time.Second):
	}
}

func startEchoServer(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()
	return listener
}

func TestSSHProxyReturnsFingerprintForUnknownHost(t *testing.T) {
	server := newTestSSHServer(t, "ssh-secret")
	proxy := server.proxyConfig()
	proxy.HostKey = ""
	dialer, err := newSSHProxyDialer(proxy, "ssh-secret")
	if err != nil {
		t.Fatal(err)
	}
	defer dialer.Close()

	_, err = dialer.DialContext(context.Background(), "tcp", "127.0.0.1:9092")
	var hostKeyErr *SSHHostKeyError
	if !errors.As(err, &hostKeyErr) {
		t.Fatalf("DialContext() error = %v, want SSHHostKeyError", err)
	}
	want, err := SSHHostKeyFingerprint(server.hostKey())
	if err != nil {
		t.Fatal(err)
	}
	if hostKeyErr.Fingerprint != want || hostKeyErr.HostKey != server.hostKey() {
		t.Fatalf("host key error = %#v, want fingerprint %q", hostKeyErr, want)
	}
}

func TestSSHProxyRejectsChangedHostKey(t *testing.T) {
	server := newTestSSHServer(t, "ssh-secret")
	otherServer := newTestSSHServer(t, "ssh-secret")
	proxy := server.proxyConfig()
	proxy.HostKey = otherServer.hostKey()
	dialer, err := newSSHProxyDialer(proxy, "ssh-secret")
	if err != nil {
		t.Fatal(err)
	}
	defer dialer.Close()

	_, err = dialer.DialContext(context.Background(), "tcp", "127.0.0.1:9092")
	if err == nil || !containsAll(err.Error(), "指纹已变化", "拒绝连接") {
		t.Fatalf("DialContext() error = %v, want host key mismatch", err)
	}
	var hostKeyErr *SSHHostKeyError
	if errors.As(err, &hostKeyErr) {
		t.Fatal("changed host key must not be offered as a new trust candidate")
	}
}

func TestSSHProxyForwardsTCPAndReconnects(t *testing.T) {
	server := newTestSSHServer(t, "ssh-secret")
	echo := startEchoServer(t)
	dialer, err := newSSHProxyDialer(server.proxyConfig(), "ssh-secret")
	if err != nil {
		t.Fatal(err)
	}
	defer dialer.Close()

	assertEcho := func() {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		conn, err := dialer.DialContext(ctx, "tcp", echo.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if _, err := conn.Write([]byte("hello via ssh")); err != nil {
			t.Fatal(err)
		}
		response := make([]byte, len("hello via ssh"))
		if _, err := io.ReadFull(conn, response); err != nil {
			t.Fatal(err)
		}
		if string(response) != "hello via ssh" {
			t.Fatalf("echo response = %q", response)
		}
	}

	assertEcho()
	dialer.mu.Lock()
	_ = dialer.client.Close()
	dialer.mu.Unlock()
	assertEcho()
}

func TestKafkaMetadataUsesSSHProxy(t *testing.T) {
	cluster := newSeededCluster(t, 2, "orders")
	broker := cluster.ListenAddrs()[0]
	server := newTestSSHServer(t, "ssh-secret")

	conn := testConnection("ssh-proxy", broker)
	conn.Proxy = server.proxyConfig()
	service := newService(proxyCredentialReader{proxyPassword: "ssh-secret"}, NewFranzClient)
	t.Cleanup(service.Close)

	topics, err := service.ListTopics(context.Background(), conn)
	if err != nil {
		t.Fatal(err)
	}
	if len(topics) != 1 || topics[0].Name != "orders" || topics[0].PartitionCount != 2 {
		t.Fatalf("ListTopics() through SSH proxy = %#v", topics)
	}
}

func containsAll(value string, parts ...string) bool {
	for _, part := range parts {
		if !strings.Contains(value, part) {
			return false
		}
	}
	return true
}
