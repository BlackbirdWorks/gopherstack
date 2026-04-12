package scheduler

import (
	"encoding/json"
	"log/slog"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type backendSnapshot struct {
	Schedules      map[string]*Schedule      `json:"schedules"`
	ScheduleGroups map[string]*ScheduleGroup `json:"scheduleGroups"`
	AccountID      string                    `json:"accountID"`
	Region         string                    `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Schedules:      b.schedules,
		ScheduleGroups: b.scheduleGroups,
		AccountID:      b.accountID,
		Region:         b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("scheduler: failed to snapshot state", "error", err)

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

	if snap.Schedules == nil {
		snap.Schedules = make(map[string]*Schedule)
	}

	if snap.ScheduleGroups == nil {
		snap.ScheduleGroups = make(map[string]*ScheduleGroup)
	}

	for _, s := range b.schedules {
		if s.Tags != nil {
			s.Tags.Close()
		}
	}

	for _, g := range b.scheduleGroups {
		if g.Tags != nil {
			g.Tags.Close()
		}
	}

	b.schedules = snap.Schedules
	b.scheduleGroups = snap.ScheduleGroups
	b.accountID = snap.AccountID
	b.region = snap.Region

	// Rebuild ARN indexes from restored state.
	b.scheduleARNIndex = make(map[string]string, len(b.schedules))
	for name, s := range b.schedules {
		b.scheduleARNIndex[s.ARN] = name
	}

	b.scheduleGroupARNIndex = make(map[string]string, len(b.scheduleGroups))
	for name, g := range b.scheduleGroups {
		b.scheduleGroupARNIndex[g.ARN] = name
	}

	// Rebuild Tags with proper lockmetrics names after JSON deserialization.
	for name, s := range b.schedules {
		if s.Tags != nil {
			restored := tags.FromMap("scheduler.group."+name+".tags", s.Tags.Clone())
			s.Tags.Close()
			s.Tags = restored
		} else {
			s.Tags = tags.New("scheduler.group." + name + ".tags")
		}
	}

	for name, g := range b.scheduleGroups {
		if g.Tags != nil {
			restored := tags.FromMap("scheduler.schedulegroup."+name+".tags", g.Tags.Clone())
			g.Tags.Close()
			g.Tags = restored
		} else {
			g.Tags = tags.New("scheduler.schedulegroup." + name + ".tags")
		}
	}

	// Ensure the default group exists after restore.
	if _, ok := b.scheduleGroups[defaultGroupName]; !ok {
		b.seedDefaultGroup()
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

// Reset implements service.Resettable by delegating to the backend.
func (h *Handler) Reset() {
	h.Backend.Reset()
}
