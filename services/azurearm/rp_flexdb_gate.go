package azurearm

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// flexGate serialises work per key (one source server's topology changes, one server's parameter pushes) without
// holding the provider lock. An entry exists only while its key is held.
type flexGate struct {
	mu   *lockmetrics.RWMutex
	held map[string]chan struct{}
	// onWait is a test hook called when a caller starts waiting for a held key.
	onWait func(key string)
}

func newFlexGate() *flexGate {
	return &flexGate{mu: lockmetrics.New("azurearm.flexdb.gate"), held: map[string]chan struct{}{}}
}

// acquire waits until key is free or ctx ends; the returned function releases it.
func (g *flexGate) acquire(ctx context.Context, key string) (func(), error) {
	for {
		g.mu.Lock("acquire")

		wait, busy := g.held[key]
		if !busy {
			done := make(chan struct{})
			g.held[key] = done
			g.mu.Unlock()

			return func() {
				g.mu.Lock("release")
				delete(g.held, key)
				g.mu.Unlock()
				close(done)
			}, nil
		}

		g.mu.Unlock()

		if g.onWait != nil {
			g.onWait(key)
		}

		select {
		case <-wait:
		case <-ctx.Done():
			return nil, fmt.Errorf("waiting for %s: %w", key, ctx.Err())
		}
	}
}
