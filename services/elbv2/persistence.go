package elbv2

import (
	"context"
	"errors"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// errBackendNotInMemory is returned when the Handler's backend cannot be cast to *InMemoryBackend.
var errBackendNotInMemory = errors.New("elbv2: backend is not *InMemoryBackend")

type backendSnapshot struct {
	LoadBalancers map[string]*LoadBalancer        `json:"loadBalancers"`
	TargetGroups  map[string]*TargetGroup         `json:"targetGroups"`
	Listeners     map[string]*Listener            `json:"listeners"`
	Rules         map[string]*Rule                `json:"rules"`
	TrustStores   map[string]*TrustStore          `json:"trustStores"`
	TargetReadyAt map[string]map[string]time.Time `json:"targetReadyAt"`
	AccountID     string                          `json:"accountID"`
	Region        string                          `json:"region"`
	RuleCounter   int                             `json:"ruleCounter"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		LoadBalancers: b.loadBalancers,
		TargetGroups:  b.targetGroups,
		Listeners:     b.listeners,
		Rules:         b.rules,
		TrustStores:   b.trustStores,
		TargetReadyAt: b.targetReadyAt,
		RuleCounter:   b.ruleCounter,
		AccountID:     b.accountID,
		Region:        b.region,
	}

	return persistence.MarshalSnapshot(ctx, "elbv2", snap)
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "elbv2", data, &snap); err != nil {
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
	b.targetReadyAt = snap.TargetReadyAt
	b.ruleCounter = snap.RuleCounter
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	b, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return nil
	}

	return b.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	b, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return errBackendNotInMemory
	}

	return b.Restore(ctx, data)
}
