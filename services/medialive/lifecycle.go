package medialive

import (
	"fmt"
	"time"
)

// phase is a transient state label (CREATING, STARTING, STOPPING, DELETING) shown until a deadline.
type phase struct {
	until time.Time
	label string
}

func (p phase) active(now time.Time) bool {
	return p.label != "" && now.Before(p.until)
}

// SetLifecycleDelay sets how long channels and multiplexes report CREATING, STARTING, STOPPING
// and DELETING after the corresponding call. Zero (the default) settles immediately.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()

	b.lifecycleDelay = d
}

func (b *InMemoryBackend) newPhase(label string) phase {
	if b.lifecycleDelay <= 0 {
		return phase{}
	}

	return phase{label: label, until: b.now().Add(b.lifecycleDelay)}
}

func (b *InMemoryBackend) channelState(ch *storedChannel) string {
	if ch.phase.active(b.now()) {
		return ch.phase.label
	}

	return ch.State
}

func (b *InMemoryBackend) multiplexState(m *storedMultiplex) string {
	if m.phase.active(b.now()) {
		return m.phase.label
	}

	return m.State
}

func (b *InMemoryBackend) pruneDeletedChannelsLocked(now time.Time) {
	for _, ch := range b.channels.All() {
		if ch.State == stateDeleted && !ch.DeletedAt.IsZero() && now.Sub(ch.DeletedAt) >= medialiveDeletedTTL {
			b.channels.Delete(ch.ID)
		}
	}
}

// liveChannels returns channels that are not DELETED. Caller holds b.mu.
func (b *InMemoryBackend) liveChannels() []*storedChannel {
	all := b.channels.All()
	out := all[:0:0]

	for _, ch := range all {
		if ch.State != stateDeleted {
			out = append(out, ch)
		}
	}

	return out
}

// liveChannel returns a channel that exists and is not DELETED. Caller holds b.mu.
func (b *InMemoryBackend) liveChannel(id string) (*storedChannel, error) {
	ch, ok := b.channels.Get(id)
	if !ok || ch.State == stateDeleted {
		return nil, fmt.Errorf("%w: channel %s not found", ErrNotFound, id)
	}

	return ch, nil
}

func (b *InMemoryBackend) multiplexView(m *storedMultiplex) *Multiplex {
	out := m.toMultiplex()
	out.State = b.multiplexState(m)

	return out
}
