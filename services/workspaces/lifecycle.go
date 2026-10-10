package workspaces

import (
	"encoding/base64"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

func (b *InMemoryBackend) now() time.Time {
	if b.clock != nil {
		return b.clock()
	}

	return time.Now()
}

// SetLifecycleDelay sets how long new WorkSpaces report PENDING and new directory
// registrations report REGISTERING. Zero (the default) settles instantly.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()

	b.lifecycleDelay = d
}

// SetClock overrides the backend clock for deterministic lifecycle tests; nil restores time.Now.
func (b *InMemoryBackend) SetClock(clock func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.clock = clock
}

func (b *InMemoryBackend) settleDeadline() time.Time {
	if b.lifecycleDelay <= 0 {
		return time.Time{}
	}

	return b.now().Add(b.lifecycleDelay)
}

// stateOf is the observable WorkSpace state: PENDING until the settle deadline passes.
func (b *InMemoryBackend) stateOf(w *storedWorkspace) string {
	if !w.pendingUntil.IsZero() && b.now().Before(w.pendingUntil) {
		return statePending
	}

	return w.State
}

func (b *InMemoryBackend) directoryRegistering(directoryID string) bool {
	until, ok := b.dirRegisteringUntil[directoryID]

	return ok && b.now().Before(until)
}

func checkPageToken(token string) error {
	if token == "" {
		return nil
	}

	if raw, err := base64.StdEncoding.DecodeString(token); err != nil || len(raw) == 0 {
		return awserr.New("The NextToken is invalid.", awserr.ErrInvalidParameter)
	}

	return nil
}
