package route53

import (
	"encoding/json"
)

type zoneDataSnapshot struct {
	Records map[string]*ResourceRecordSet `json:"records"`
	Zone    HostedZone                    `json:"zone"`
}

type backendSnapshot struct {
	Zones map[string]*zoneDataSnapshot `json:"zones"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Zones: make(map[string]*zoneDataSnapshot, len(b.zones)),
	}

	for id, zd := range b.zones {
		snap.Zones[id] = &zoneDataSnapshot{
			Zone:    zd.zone,
			Records: zd.records,
		}
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

	return nil
}
