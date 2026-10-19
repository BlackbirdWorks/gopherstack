package container

import (
	"context"
	"os"
)

// Probe reports which container runtime is reachable, honouring cfg.Runtime and
// CONTAINER_RUNTIME; when neither is set it tries Docker, then Podman.
func Probe(ctx context.Context, cfg Config) (RuntimeName, error) {
	name := cfg.Runtime
	if name == "" {
		name = RuntimeName(os.Getenv("CONTAINER_RUNTIME"))
	}

	candidates := []RuntimeName{RuntimeDocker, RuntimePodman}

	switch name {
	case "", RuntimeAuto:
	default:
		candidates = []RuntimeName{name}
	}

	var lastErr error

	for _, cand := range candidates {
		c := cfg
		c.Runtime = cand

		rt, err := NewRuntime(c)
		if err != nil {
			lastErr = err

			continue
		}

		pingErr := rt.Ping(ctx)
		_ = rt.Close()

		if pingErr == nil {
			return cand, nil
		}

		lastErr = pingErr
	}

	return "", lastErr
}
