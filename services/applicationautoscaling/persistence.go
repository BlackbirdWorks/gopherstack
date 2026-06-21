package applicationautoscaling

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

type backendSnapshot struct {
	ScalableTargets  map[string]*ScalableTarget  `json:"scalableTargets"`
	ScalingPolicies  map[string]*ScalingPolicy   `json:"scalingPolicies"`
	ScheduledActions map[string]*ScheduledAction `json:"scheduledActions"`
	AccountID        string                      `json:"accountID"`
	Region           string                      `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		ScalableTargets:  b.scalableTargets,
		ScalingPolicies:  b.scalingPolicies,
		ScheduledActions: b.scheduledActions,
		AccountID:        b.accountID,
		Region:           b.region,
	}

	return persistence.MarshalSnapshot(ctx, "applicationautoscaling", snap)
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "applicationautoscaling", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.ScalableTargets == nil {
		snap.ScalableTargets = make(map[string]*ScalableTarget)
	}

	if snap.ScalingPolicies == nil {
		snap.ScalingPolicies = make(map[string]*ScalingPolicy)
	}

	if snap.ScheduledActions == nil {
		snap.ScheduledActions = make(map[string]*ScheduledAction)
	}

	b.scalableTargets = snap.ScalableTargets
	b.scalingPolicies = snap.ScalingPolicies
	b.scheduledActions = snap.ScheduledActions
	b.accountID = snap.AccountID
	b.region = snap.Region

	b.targetARNIndex = make(map[string]string, len(b.scalableTargets))

	for key, t := range b.scalableTargets {
		b.targetARNIndex[t.ARN] = key
	}

	b.policyNameIndex = make(map[string]string, len(b.scalingPolicies))

	for _, p := range b.scalingPolicies {
		b.policyNameIndex[policyNameKey(p.ServiceNamespace, p.ResourceID, p.ScalableDimension, p.PolicyName)] = p.ARN
	}

	b.actionNameIndex = make(map[string]string, len(b.scheduledActions))

	for _, a := range b.scheduledActions {
		b.actionNameIndex[actionNameKey(a.ServiceNamespace, a.ResourceID, a.ScalableDimension, a.ScheduledActionName)] = a.ARN
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
