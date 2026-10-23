package container

import (
	"context"
	"errors"
	"fmt"
	"slices"
)

// ErrOCIRuntimeMissing is returned when a requested OCI runtime is not registered with the daemon.
var ErrOCIRuntimeMissing = errors.New("OCI runtime is not registered with the container daemon")

// ErrOCIRuntimeUnverifiable is returned when the daemon's registered runtimes cannot be listed.
var ErrOCIRuntimeUnverifiable = errors.New("cannot list the container daemon's OCI runtimes")

// OCIRuntimeAPIClient is implemented by clients that can list the daemon's registered OCI runtimes.
type OCIRuntimeAPIClient interface {
	Runtimes(ctx context.Context) ([]string, error)
}

// OCIRuntimeLister is implemented by runtimes that can list the daemon's registered OCI runtimes.
type OCIRuntimeLister interface {
	OCIRuntimes(ctx context.Context) ([]string, error)
}

// OCIRuntimes returns the OCI runtime names registered with the daemon.
func (r *DockerRuntime) OCIRuntimes(ctx context.Context) ([]string, error) {
	c, ok := r.docker.(OCIRuntimeAPIClient)
	if !ok {
		return nil, ErrOCIRuntimeUnverifiable
	}

	return c.Runtimes(ctx)
}

// ResolveOCIRuntime returns the first candidate registered with rt's daemon, or an error naming them all.
func ResolveOCIRuntime(ctx context.Context, rt Runtime, candidates ...string) (string, error) {
	lister, ok := rt.(OCIRuntimeLister)
	if !ok {
		return "", ErrOCIRuntimeUnverifiable
	}

	have, err := lister.OCIRuntimes(ctx)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrOCIRuntimeUnverifiable, err)
	}

	for _, c := range candidates {
		if slices.Contains(have, c) {
			return c, nil
		}
	}

	return "", fmt.Errorf("%w: want one of %v, daemon has %v", ErrOCIRuntimeMissing, candidates, have)
}
