package gcpspanner

import (
	"log/slog"

	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

type fixedLauncher struct {
	stop func()
	addr string
}

func (f fixedLauncher) launch(context.Context) (*running, error) {
	return &running{addr: f.addr, stop: func(context.Context) {
		if f.stop != nil {
			f.stop()
		}
	}}, nil
}

// UseEmulatorAt makes the data plane connect to an already running emulator at addr.
func (b *InMemoryBackend) UseEmulatorAt(addr string, stop func()) {
	b.SetLauncher(fixedLauncher{addr: addr, stop: stop})
}

type failingLauncher struct{ err error }

func (f failingLauncher) launch(context.Context) (*running, error) { return nil, f.err }

// UseNoRuntime makes launching fail as if no container runtime existed.
func (b *InMemoryBackend) UseNoRuntime() {
	b.SetLauncher(failingLauncher{err: ErrRuntimeUnavailable})
}

// SetClock replaces the backend clock.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.now = now
}

// EmulatorRunning reports whether a data plane connection is open.
func (b *InMemoryBackend) EmulatorRunning() bool { return b.emu.current() != nil }

// NewDockerLauncher exposes the Docker launcher to tests.
func NewDockerLauncher(log *slog.Logger, pool *portalloc.Allocator, image string, startup time.Duration) Launcher {
	return newDockerLauncher(log, pool, image, startup)
}

// Launcher is the data-plane launcher accepted by SetLauncher.
type Launcher = launcher
