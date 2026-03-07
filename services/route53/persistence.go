package route53

import (
	"encoding/json"
)

type zoneDataSnapshot struct {
	Records map[string]*ResourceRecordSet `json:"records"`
	Zone    HostedZone                    `json:"zone"`
}

type backendSnapshot struct {
	Zones        map[string]*zoneDataSnapshot `json:"zones"`
	HealthChecks map[string]*HealthCheck      `json:"healthChecks,omitempty"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Zones:        make(map[string]*zoneDataSnapshot, len(b.zones)),
		HealthChecks: make(map[string]*HealthCheck, len(b.healthChecks)),
	}

	for id, zd := range b.zones {
		snap.Zones[id] = &zoneDataSnapshot{
			Zone:    zd.zone,
			Records: zd.records,
		}
	}

	for id, hc := range b.healthChecks {
		cp := *hc
		snap.HealthChecks[id] = &cp
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
// The DNS registrar is not restored — it must be re-wired by the caller after restore.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Zones == nil {
		snap.Zones = make(map[string]*zoneDataSnapshot)
	}

	b.zones = make(map[string]*zoneData, len(snap.Zones))

	for id, zds := range snap.Zones {
		if zds.Records == nil {
			zds.Records = make(map[string]*ResourceRecordSet)
		}

		b.zones[id] = &zoneData{
			zone:    zds.Zone,
			records: zds.Records,
		}
	}

	if snap.HealthChecks == nil {
		snap.HealthChecks = make(map[string]*HealthCheck)
	}

	b.healthChecks = make(map[string]*HealthCheck, len(snap.HealthChecks))

	for id, hc := range snap.HealthChecks {
		cp := *hc
		b.healthChecks[id] = &cp
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	return h.Backend.Snapshot()
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	return h.Backend.Restore(data)
}
