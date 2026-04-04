package autoscaling

import (
	"encoding/json"
)

type backendSnapshot struct {
	Groups               map[string]*AutoScalingGroup           `json:"groups"`
	LaunchConfigurations map[string]*LaunchConfiguration        `json:"launchConfigurations"`
	Activities           map[string][]ScalingActivity           `json:"activities"`
	ScheduledActions     map[string]map[string]*ScheduledAction `json:"scheduledActions"`
	InstanceRefreshes    map[string][]*InstanceRefresh          `json:"instanceRefreshes"`
	LifecycleHooks       map[string]map[string]*LifecycleHook   `json:"lifecycleHooks"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Groups:               b.groups,
		LaunchConfigurations: b.launchConfigurations,
		Activities:           b.activities,
		ScheduledActions:     b.scheduledActions,
		InstanceRefreshes:    b.instanceRefreshes,
		LifecycleHooks:       b.lifecycleHooks,
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

	if snap.Groups != nil {
		b.groups = snap.Groups
	} else {
		b.groups = make(map[string]*AutoScalingGroup)
	}

	if snap.LaunchConfigurations != nil {
		b.launchConfigurations = snap.LaunchConfigurations
	} else {
		b.launchConfigurations = make(map[string]*LaunchConfiguration)
	}

	if snap.Activities != nil {
		b.activities = snap.Activities
	} else {
		b.activities = make(map[string][]ScalingActivity)
	}

	if snap.ScheduledActions != nil {
		b.scheduledActions = snap.ScheduledActions
	} else {
		b.scheduledActions = make(map[string]map[string]*ScheduledAction)
	}

	if snap.InstanceRefreshes != nil {
		b.instanceRefreshes = snap.InstanceRefreshes
	} else {
		b.instanceRefreshes = make(map[string][]*InstanceRefresh)
	}

	if snap.LifecycleHooks != nil {
		b.lifecycleHooks = snap.LifecycleHooks
	} else {
		b.lifecycleHooks = make(map[string]map[string]*LifecycleHook)
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	type snapshotter interface{ Snapshot() []byte }
	if s, ok := h.Backend.(snapshotter); ok {
		return s.Snapshot()
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	type restorer interface{ Restore([]byte) error }
	if r, ok := h.Backend.(restorer); ok {
		return r.Restore(data)
	}

	return nil
}
