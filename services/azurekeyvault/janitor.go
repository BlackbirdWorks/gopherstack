package azurekeyvault

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

const (
	defaultJanitorInterval = 30 * time.Second
	janitorService         = "azurekeyvault"
	janitorComponent       = "LifetimeActionSweeper"
)

// Janitor runs key rotation policies and certificate AutoRenew lifetime
// actions. Notify and EmailContacts actions are never delivered: there is no
// Event Grid or mail transport in the emulator.
type Janitor struct {
	Backend  *InMemoryBackend
	Interval time.Duration
}

// NewJanitor creates a Janitor; a zero interval selects the default.
func NewJanitor(backend *InMemoryBackend, interval time.Duration) *Janitor {
	if interval == 0 {
		interval = defaultJanitorInterval
	}

	return &Janitor{Backend: backend, Interval: interval}
}

// Run blocks until ctx is cancelled, sweeping on every tick.
func (j *Janitor) Run(ctx context.Context) {
	g := worker.NewGroup(ctx, janitorService)
	g.Ticker(janitorComponent, j.Interval, 0, j.sweep)

	<-ctx.Done()
	g.Stop()
}

func (j *Janitor) sweep(ctx context.Context) {
	stats := j.Backend.sweepOnce()

	if n := stats.Rotated + stats.Renewed; n > 0 {
		telemetry.RecordWorkerItems(janitorService, janitorComponent, n)
		logger.Load(ctx).InfoContext(ctx, "azurekeyvault janitor: applied lifetime actions",
			"keysRotated", stats.Rotated, "certsRenewed", stats.Renewed)
	}

	telemetry.RecordWorkerTask(janitorService, janitorComponent, "success")
}

// sweepStats tallies one sweep pass.
type sweepStats struct {
	Rotated int
	Renewed int
}

// sweepOnce applies every due lifetime action in every vault under one lock.
func (b *InMemoryBackend) sweepOnce() sweepStats {
	b.mu.Lock("sweepOnce")
	defer b.mu.Unlock()

	now := b.now()

	var total sweepStats

	for _, v := range b.vaults {
		v.sweep(now.Unix())
		total.Rotated += v.rotateDueKeys(now)
		total.Renewed += v.renewDueCerts(now)
	}

	return total
}
