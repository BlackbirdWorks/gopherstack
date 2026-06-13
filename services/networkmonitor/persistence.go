package networkmonitor

import (
	"encoding/json"
	"log/slog"
)

type backendSnapshot struct {
	Monitors     map[string]map[string]*Monitor `json:"monitors"`
	NextProbeSeq int64                          `json:"next_probe_seq"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()

	monsCopy := make(map[string]map[string]*Monitor, len(b.monitors))

	for region, regionMons := range b.monitors {
		monsCopy[region] = make(map[string]*Monitor, len(regionMons))

		for k, v := range regionMons {
			monsCopy[region][k] = monitorCopy(v)
		}
	}

	snap := backendSnapshot{
		Monitors:     monsCopy,
		NextProbeSeq: b.nextProbeSeq,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("networkmonitor: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if snap.Monitors != nil {
		b.monitors = snap.Monitors
	} else {
		b.monitors = make(map[string]map[string]*Monitor)
	}

	b.arnIndex = make(map[string]map[string]string)

	for region, regionMons := range b.monitors {
		b.arnIndex[region] = make(map[string]string, len(regionMons))

		for name, m := range regionMons {
			b.arnIndex[region][m.MonitorArn] = name
		}
	}

	b.nextProbeSeq = snap.NextProbeSeq

	return nil
}
