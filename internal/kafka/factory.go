package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/jjk/kafka-keeper/internal/model"
)

type Client struct {
	raw   *kgo.Client
	ping  func(context.Context) error
	close func()
}

func newClient(raw *kgo.Client) *Client {
	return &Client{raw: raw, close: raw.Close}
}

func (c *Client) Ping(ctx context.Context) error {
	if c.ping != nil {
		return c.ping(ctx)
	}
	return c.raw.Ping(ctx)
}

func (c *Client) Close() {
	if c == nil || c.close == nil {
		return
	}
	c.close()
}

type ClientCredentials struct {
	KafkaPassword string
	ProxyPassword string
}

func (c ClientCredentials) secrets() []string {
	return []string{c.KafkaPassword, c.ProxyPassword}
}

type ClientFactory func(context.Context, model.Connection, ClientCredentials) (*Client, error)

func NewFranzClient(_ context.Context, conn model.Connection, credentials ClientCredentials) (*Client, error) {
	opts, cleanup, err := clientOptions(conn, credentials)
	if err != nil {
		return nil, err
	}
	raw, err := kgo.NewClient(opts...)
	if err != nil {
		cleanup()
		return nil, err
	}
	client := newClient(raw)
	client.close = func() {
		raw.Close()
		cleanup()
	}
	return client, nil
}

func clientOptions(conn model.Connection, credentials ClientCredentials) ([]kgo.Opt, func(), error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(conn.BootstrapServers...),
		kgo.DialTimeout(8 * time.Second),
		kgo.RequestTimeoutOverhead(10 * time.Second),
	}
	clientID := conn.ClientID
	if clientID == "" {
		clientID = "kafka-keeper"
	}
	opts = append(opts, kgo.ClientID(clientID))

	var tlsConfig *tls.Config
	if conn.TLS.Enabled {
		var err error
		tlsConfig, err = kafkaTLSConfig(conn)
		if err != nil {
			return nil, nil, err
		}
	}

	cleanup := func() {}
	if conn.Proxy.Enabled {
		proxyDialer, err := newSSHProxyDialer(conn.Proxy, credentials.ProxyPassword)
		if err != nil {
			return nil, nil, err
		}
		opts = append(opts, kgo.Dialer(newProxyKafkaDialer(proxyDialer, tlsConfig)))
		cleanup = proxyDialer.Close
	} else if tlsConfig != nil {
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}
	switch conn.SASL.Mechanism {
	case model.SASLNone:
	case model.SASLPlain:
		opts = append(opts, kgo.SASL(plain.Auth{User: conn.SASL.Username, Pass: credentials.KafkaPassword}.AsMechanism()))
	case model.SASLScramSHA256:
		opts = append(opts, kgo.SASL(scram.Auth{User: conn.SASL.Username, Pass: credentials.KafkaPassword}.AsSha256Mechanism()))
	case model.SASLScramSHA512:
		opts = append(opts, kgo.SASL(scram.Auth{User: conn.SASL.Username, Pass: credentials.KafkaPassword}.AsSha512Mechanism()))
	default:
		cleanup()
		return nil, nil, fmt.Errorf("不支持的 SASL 机制 %q", conn.SASL.Mechanism)
	}
	return opts, cleanup, nil
}

func kafkaTLSConfig(conn model.Connection) (*tls.Config, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if conn.TLS.CAFile == "" {
		return tlsConfig, nil
	}
	pem, err := os.ReadFile(conn.TLS.CAFile)
	if err != nil {
		return nil, fmt.Errorf("读取 TLS CA 证书失败: %w", err)
	}
	pool, err := x509.SystemCertPool()
	if err != nil {
		pool = x509.NewCertPool()
	}
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("TLS CA 证书不包含有效证书")
	}
	tlsConfig.RootCAs = pool
	return tlsConfig, nil
}
