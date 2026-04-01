package elbv2

import (
	"encoding/json"
	"errors"
)

// errBackendNotInMemory is returned when the Handler's backend cannot be cast to *InMemoryBackend.
var errBackendNotInMemory = errors.New("elbv2: backend is not *InMemoryBackend")

type backendSnapshot struct {
	LoadBalancers map[string]*LoadBalancer `json:"loadBalancers"`
	TargetGroups  map[string]*TargetGroup  `json:"targetGroups"`
	Listeners     map[string]*Listener     `json:"listeners"`
	Rules         map[string]*Rule         `json:"rules"`
	TrustStores   map[string]*TrustStore   `json:"trustStores"`
	AccountID     string                   `json:"accountID"`
	Region        string                   `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		LoadBalancers: b.loadBalancers,
		TargetGroups:  b.targetGroups,
		Listeners:     b.listeners,
		Rules:         b.rules,
		TrustStores:   b.trustStores,
		AccountID:     b.accountID,
		Region:        b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
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

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.LoadBalancers == nil {
		snap.LoadBalancers = make(map[string]*LoadBalancer)
	}

	if snap.TargetGroups == nil {
		snap.TargetGroups = make(map[string]*TargetGroup)
	}

	if snap.Listeners == nil {
		snap.Listeners = make(map[string]*Listener)
	}

	if snap.Rules == nil {
		snap.Rules = make(map[string]*Rule)
	}

	if snap.TrustStores == nil {
		snap.TrustStores = make(map[string]*TrustStore)
	}

	b.loadBalancers = snap.LoadBalancers
	b.targetGroups = snap.TargetGroups
	b.listeners = snap.Listeners
	b.rules = snap.Rules
	b.trustStores = snap.TrustStores
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	b, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return nil
	}

	return b.Snapshot()
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	b, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return errBackendNotInMemory
	}

	return b.Restore(data)
}
