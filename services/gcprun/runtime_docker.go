package gcprun

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const runtimePingTimeout = 5 * time.Second

// lazyRuntime connects to the container runtime on first use and retries while it is unreachable.
type lazyRuntime struct {
	log *slog.Logger
	rt  container.Runtime
	mu  *lockmetrics.RWMutex
}

func newLazyRuntime(ctx *service.AppContext) *lazyRuntime {
	return &lazyRuntime{log: ctx.Logger, mu: lockmetrics.New("gcprun.runtime")}
}

func (l *lazyRuntime) get() (Runner, error) {
	l.mu.Lock("get")
	defer l.mu.Unlock()

	if l.rt != nil {
		return l.rt, nil
	}

	rt, err := container.NewRuntime(container.Config{Logger: l.log})
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), runtimePingTimeout)
	defer cancel()

	if err = rt.Ping(ctx); err != nil {
		_ = rt.Close()

		return nil, fmt.Errorf("ping: %w", err)
	}

	l.rt = rt

	return rt, nil
}

// Close releases the runtime client.
func (l *lazyRuntime) Close() error {
	l.mu.Lock("Close")
	defer l.mu.Unlock()

	if c, ok := l.rt.(io.Closer); ok {
		l.rt = nil

		return c.Close()
	}

	return nil
}
