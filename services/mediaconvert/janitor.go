package mediaconvert

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	// janitorInterval is how often the janitor tick fires.
	janitorInterval = 500 * time.Millisecond
)

// StartJanitor launches the background goroutine that advances job phases and
// sweeps expired tokens. It runs until ctx is cancelled.
func StartJanitor(ctx context.Context, b *InMemoryBackend) {
	go runJanitor(ctx, b)
}

// runJanitor is the goroutine body for the janitor loop.
func runJanitor(ctx context.Context, b *InMemoryBackend) {
	ticker := time.NewTicker(janitorInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			janitorTick(ctx, b)
		}
	}
}

// janitorTick performs one cycle: advance job phases and sweep tokens.
func janitorTick(ctx context.Context, b *InMemoryBackend) {
	advanced := b.AdvanceJobPhase()
	b.SweepExpiredTokens()

	if advanced {
		logger.Load(ctx).Debug("mediaconvert: janitor advanced job phase(s)")
	}
}
