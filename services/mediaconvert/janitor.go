package mediaconvert

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

const (
	// janitorInterval is how often the janitor tick fires.
	janitorInterval = 500 * time.Millisecond
)

// StartJanitor launches the background goroutine that advances job phases and
// sweeps expired tokens. It runs until ctx is cancelled.
func StartJanitor(ctx context.Context, b *InMemoryBackend) {
	// Group.Ticker spawns the sweep goroutine itself; it exits when ctx is
	// cancelled, matching the previous fire-and-forget behaviour.
	g := worker.NewGroup(ctx, "mediaconvert")
	g.Ticker("JobPhaseAdvancer", janitorInterval, 0,
		func(ctx context.Context) { janitorTick(ctx, b) })
}

// janitorTick performs one cycle: advance job phases and sweep tokens.
func janitorTick(ctx context.Context, b *InMemoryBackend) {
	advanced := b.AdvanceJobPhase()
	b.SweepExpiredTokens()

	if advanced {
		logger.Load(ctx).DebugContext(ctx, "mediaconvert: janitor advanced job phase(s)")
	}
}
