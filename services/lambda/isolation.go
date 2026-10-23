package lambda

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

// Supported values of Settings.Isolation.
const (
	IsolationContainer   = "container"
	IsolationGVisor      = "gvisor"
	IsolationKata        = "kata"
	IsolationFirecracker = "firecracker"
)

// ErrInvalidIsolation is returned for an unknown --lambda-isolation value.
var ErrInvalidIsolation = errors.New("invalid lambda isolation")

// ErrIsolationUnsupported is returned for an isolation mode that is not implemented.
var ErrIsolationUnsupported = errors.New("lambda isolation mode is not supported")

// ErrIsolationNeedsRuntime is returned when an isolated mode is requested but no container runtime is available.
var ErrIsolationNeedsRuntime = errors.New("lambda isolation requires a reachable container runtime")

func isolationFromEnv() string {
	if v := os.Getenv("LAMBDA_ISOLATION"); v != "" {
		return v
	}

	return IsolationContainer
}

// isolationCandidates returns the daemon runtime names acceptable for an isolation mode.
func isolationCandidates(mode string) ([]string, error) {
	switch mode {
	case "", IsolationContainer:
		return nil, nil
	case IsolationGVisor:
		return []string{"runsc"}, nil
	case IsolationKata:
		return []string{"kata", "kata-runtime"}, nil
	case IsolationFirecracker:
		return nil, fmt.Errorf(
			"%w: firecracker needs /dev/kvm, a firecracker binary and a microVM runner (see services/lambda/PARITY.md)",
			ErrIsolationUnsupported)
	default:
		return nil, fmt.Errorf("%w %q: want container, gvisor, kata or firecracker", ErrInvalidIsolation, mode)
	}
}

// resolveIsolation validates mode against the daemon and returns the OCI runtime name ("" for the default).
func resolveIsolation(ctx context.Context, mode string, rt container.Runtime) (string, error) {
	candidates, err := isolationCandidates(mode)
	if err != nil || len(candidates) == 0 {
		return "", err
	}

	if rt == nil {
		return "", fmt.Errorf("%w (--lambda-isolation=%s)", ErrIsolationNeedsRuntime, mode)
	}

	name, err := container.ResolveOCIRuntime(ctx, rt, candidates...)
	if err != nil {
		return "", fmt.Errorf("--lambda-isolation=%s: %w", mode, err)
	}

	return name, nil
}
