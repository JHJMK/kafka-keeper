package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/jjk/kafka-keeper/internal/model"
)

func TestServiceTestConnectionPreservesSSHHostKeyError(t *testing.T) {
	want := &SSHHostKeyError{Fingerprint: "SHA256:candidate", HostKey: "candidate-key"}
	service := newService(staticCredentialReader(""), func(context.Context, model.Connection, ClientCredentials) (*Client, error) {
		return nil, want
	})

	err := service.TestConnection(context.Background(), testConnection("one", "kafka:9092"), ClientCredentials{})
	var got *SSHHostKeyError
	if !errors.As(err, &got) {
		t.Fatalf("TestConnection() error = %v, want SSHHostKeyError", err)
	}
	if got.Fingerprint != want.Fingerprint || got.HostKey != want.HostKey {
		t.Fatalf("SSHHostKeyError = %#v, want %#v", got, want)
	}
}
