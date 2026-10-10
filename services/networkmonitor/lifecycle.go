package networkmonitor

import "time"

const statePending = "PENDING"

// SetActivationDelay keeps new monitors and probes in PENDING for d before they
// report ACTIVE (default 0: ACTIVE immediately).
func (b *InMemoryBackend) SetActivationDelay(d time.Duration) {
	b.mu.Lock("SetActivationDelay")
	defer b.mu.Unlock()

	b.activationDelay = d
}

func (b *InMemoryBackend) initialState() string {
	if b.activationDelay > 0 {
		return statePending
	}

	return monitorStateActive
}

func (b *InMemoryBackend) settledState(state string, created *time.Time, now time.Time) string {
	if b.activationDelay > 0 && state == statePending && created != nil && now.Sub(*created) >= b.activationDelay {
		return monitorStateActive
	}

	return state
}

func (b *InMemoryBackend) viewMonitor(m *Monitor) *Monitor {
	cp := monitorCopy(m)
	if cp == nil {
		return nil
	}

	now := time.Now().UTC()
	cp.State = b.settledState(cp.State, cp.CreatedAt, now)

	for _, p := range cp.Probes {
		p.State = b.settledState(p.State, p.CreatedAt, now)
	}

	return cp
}

func (b *InMemoryBackend) viewProbe(p *Probe) *Probe {
	cp := probeCopy(p)
	cp.State = b.settledState(cp.State, cp.CreatedAt, time.Now().UTC())

	return cp
}
