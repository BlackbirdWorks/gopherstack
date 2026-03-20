package servicediscovery

import "encoding/json"

type backendSnapshot struct {
	Namespaces  map[string]*Namespace  `json:"namespaces"`
	Services    map[string]*Service    `json:"services"`
	Instances   map[string]*Instance   `json:"instances"`
	Operations  map[string]*Operation  `json:"operations"`
	AccountID   string                 `json:"accountID"`
	Region      string                 `json:"region"`
	NsCounter   int                    `json:"nsCounter"`
	SvcCounter  int                    `json:"svcCounter"`
	InstCounter int                    `json:"instCounter"`
	OpCounter   int                    `json:"opCounter"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Namespaces:  b.namespaces,
		Services:    b.services,
		Instances:   b.instances,
		Operations:  b.operations,
		AccountID:   b.accountID,
		Region:      b.region,
		NsCounter:   b.nsCounter,
		SvcCounter:  b.svcCounter,
		InstCounter: b.instCounter,
		OpCounter:   b.opCounter,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Namespaces == nil {
		snap.Namespaces = make(map[string]*Namespace)
	}

	if snap.Services == nil {
		snap.Services = make(map[string]*Service)
	}

	if snap.Instances == nil {
		snap.Instances = make(map[string]*Instance)
	}

	if snap.Operations == nil {
		snap.Operations = make(map[string]*Operation)
	}

	b.namespaces = snap.Namespaces
	b.services = snap.Services
	b.instances = snap.Instances
	b.operations = snap.Operations
	b.accountID = snap.AccountID
	b.region = snap.Region
	b.nsCounter = snap.NsCounter
	b.svcCounter = snap.SvcCounter
	b.instCounter = snap.InstCounter
	b.opCounter = snap.OpCounter

	b.nsARNIndex = make(map[string]string, len(b.namespaces))
	b.nsNameIndex = make(map[string]string, len(b.namespaces))
	b.svcARNIndex = make(map[string]string, len(b.services))

	for id, ns := range b.namespaces {
		b.nsARNIndex[ns.ARN] = id
		b.nsNameIndex[ns.Name] = id
	}

	for id, svc := range b.services {
		b.svcARNIndex[svc.ARN] = id
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
