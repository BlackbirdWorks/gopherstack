package container

import (
	"context"
	"strings"
)

// Starter is the slice of Runtime that CreateAndStartRetry needs.
type Starter interface {
	CreateAndStart(ctx context.Context, spec Spec) (string, error)
}

// PortRetries bounds CreateAndStartRetry attempts.
const PortRetries = 5

// IsPortConflict reports whether err is a runtime host-port bind failure.
func IsPortConflict(err error) bool {
	if err == nil {
		return false
	}

	msg := err.Error()

	return strings.Contains(msg, "port is already allocated") ||
		strings.Contains(msg, "address already in use") ||
		strings.Contains(msg, "ports are not available")
}

// CreateAndStartRetry starts the spec returned by prepare. On a host-port bind conflict it calls
// release, then prepare again for fresh ports, up to PortRetries attempts. release is also not
// called on success; the caller owns the ports of the container it got back.
func CreateAndStartRetry(
	ctx context.Context,
	rt Starter,
	prepare func() (Spec, error),
	release func(),
) (string, error) {
	var lastErr error

	for range PortRetries {
		spec, err := prepare()
		if err != nil {
			return "", err
		}

		id, err := rt.CreateAndStart(ctx, spec)
		if err == nil {
			return id, nil
		}

		lastErr = err

		if release != nil {
			release()
		}

		if !IsPortConflict(err) || ctx.Err() != nil {
			return "", err
		}
	}

	return "", lastErr
}
