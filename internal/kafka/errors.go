package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

type OperationError struct {
	Operation string
	Message   string
	Cause     error
}

func (e *OperationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Operation, e.Message)
}

func (e *OperationError) Unwrap() error {
	return e.Cause
}

func safeError(operation string, err error, secrets ...string) error {
	if err == nil {
		return nil
	}
	message := err.Error()
	for _, secret := range secrets {
		if secret != "" {
			message = strings.ReplaceAll(message, secret, "[REDACTED]")
		}
	}
	return &OperationError{Operation: operation, Message: message, Cause: err}
}

func IsCanceled(err error) bool {
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded)
}
