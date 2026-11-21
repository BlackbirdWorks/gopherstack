package azurearm

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const apimBGStopWait = 10 * time.Second

// apimBG tracks the gateway's long-lived work, WebSocket sessions and one-way requests, so that Reset and Close
// can cancel it and wait for it.
type apimBG struct {
	mu     *lockmetrics.RWMutex
	live   map[uint64]apimBGUnit
	next   uint64
	closed bool
}

type apimBGUnit struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func newAPIMBG() *apimBG {
	return &apimBG{mu: lockmetrics.New("azurearm.apim.bg"), live: map[uint64]apimBGUnit{}}
}

// begin registers a unit of work; it reports false once the provider is closed.
func (b *apimBG) begin(cancel context.CancelFunc) (uint64, bool) {
	b.mu.Lock("begin")
	defer b.mu.Unlock()

	if b.closed {
		return 0, false
	}

	b.next++
	b.live[b.next] = apimBGUnit{cancel: cancel, done: make(chan struct{})}

	return b.next, true
}

func (b *apimBG) end(id uint64) {
	b.mu.Lock("end")

	if u, ok := b.live[id]; ok {
		delete(b.live, id)
		close(u.done)
	}

	b.mu.Unlock()
}

// stop cancels all registered work and waits for it, bounded; final also refuses new work.
func (b *apimBG) stop(final bool) {
	b.mu.Lock("stop")

	if final {
		b.closed = true
	}

	units := make([]apimBGUnit, 0, len(b.live))
	for _, u := range b.live {
		units = append(units, u)
	}

	b.mu.Unlock()

	deadline := time.NewTimer(apimBGStopWait)
	defer deadline.Stop()

	for _, u := range units {
		u.cancel()
	}

	for _, u := range units {
		select {
		case <-u.done:
		case <-deadline.C:
			return
		}
	}
}

// Close cancels every open WebSocket session and in-flight one-way request and refuses new ones.
func (p *APIMProvider) Close() { p.bg.stop(true) }
