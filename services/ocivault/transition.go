package ocivault

import "time"

// TransitionDelay is how long a resource stays in a transitional state (CREATING, ENABLING, ...) by default.
const TransitionDelay = 2 * time.Second

type transition struct {
	SettleAt *time.Time `json:"settleAt,omitempty"`
	SettleTo string     `json:"settleTo,omitempty"`
}

// SetTransitionDelay overrides how long transitional states last; zero makes every transition instant.
func (b *InMemoryBackend) SetTransitionDelay(d time.Duration) {
	b.mu.Lock("SetTransitionDelay")
	b.delay = d
	b.mu.Unlock()
}

// begin puts state into via and schedules the move to final on the backend clock.
func (b *InMemoryBackend) begin(state *string, tr *transition, via, final string) {
	if b.delay <= 0 {
		*state, tr.SettleAt, tr.SettleTo = final, nil, ""

		return
	}

	at := b.nowUTC().Add(b.delay)
	*state, tr.SettleAt, tr.SettleTo = via, &at, final
}

func (tr *transition) settle(state *string, now time.Time) {
	if tr.SettleTo != "" && due(tr.SettleAt, now) {
		*state, tr.SettleAt, tr.SettleTo = tr.SettleTo, nil, ""
	}
}
