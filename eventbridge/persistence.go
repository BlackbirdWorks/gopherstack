package eventbridge

import (
	"encoding/json"
)

type backendSnapshot struct {
	Buses     map[string]*EventBus          `json:"buses"`
	Rules     map[string]map[string]*Rule   `json:"rules"`
	Targets   map[string]map[string]*Target `json:"targets"`
	AccountID string                        `json:"accountID"`
	Region    string                        `json:"region"`
	EventLog  []EventLogEntry               `json:"eventLog"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Buses:     b.buses,
		Rules:     b.rules,
		Targets:   b.targets,
		EventLog:  b.eventLog,
		AccountID: b.accountID,
		Region:    b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		b.logger.Warn("persistence: snapshot marshal failed", "service", "eventbridge", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
// The logger and delivery targets are not restored — they are re-wired by the CLI.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Buses == nil {
		snap.Buses = make(map[string]*EventBus)
	}

	if snap.Rules == nil {
		snap.Rules = make(map[string]map[string]*Rule)
	}

	if snap.Targets == nil {
		snap.Targets = make(map[string]map[string]*Target)
	}

	b.buses = snap.Buses
	b.rules = snap.Rules
	b.targets = snap.Targets
	b.eventLog = snap.EventLog
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}
