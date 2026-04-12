package scheduler

import (
	"encoding/json"
	"log/slog"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// snapshotTimeLayout is the time format used when persisting timestamps.
const snapshotTimeLayout = time.RFC3339Nano

// persistedSchedule is the serialisation-friendly representation of Schedule.
// It replaces the live *tags.Tags pointer with a plain map so JSON round-trips
// work without carrying Prometheus metric state.
type persistedSchedule struct {
	CreationDate               string             `json:"creationDate"`
	LastModificationDate       string             `json:"lastModificationDate"`
	StartDate                  string             `json:"startDate,omitempty"`
	EndDate                    string             `json:"endDate,omitempty"`
	Tags                       map[string]string  `json:"tags,omitempty"`
	Target                     Target             `json:"target"`
	Name                       string             `json:"name"`
	ARN                        string             `json:"arn"`
	ScheduleExpression         string             `json:"scheduleExpression"`
	ScheduleExpressionTimezone string             `json:"scheduleExpressionTimezone,omitempty"`
	Description                string             `json:"description,omitempty"`
	GroupName                  string             `json:"groupName"`
	State                      string             `json:"state"`
	ActionAfterCompletion      string             `json:"actionAfterCompletion,omitempty"`
	KmsKeyArn                  string             `json:"kmsKeyArn,omitempty"`
	AccountID                  string             `json:"accountID"`
	Region                     string             `json:"region"`
	FlexibleTimeWindow         FlexibleTimeWindow `json:"flexibleTimeWindow"`
}

// persistedScheduleGroup is the serialisation-friendly representation of ScheduleGroup.
type persistedScheduleGroup struct {
	CreationDate         string            `json:"creationDate"`
	LastModificationDate string            `json:"lastModificationDate"`
	Tags                 map[string]string `json:"tags,omitempty"`
	Description          string            `json:"description,omitempty"`
	Name                 string            `json:"name"`
	ARN                  string            `json:"arn"`
	State                string            `json:"state"`
}

type backendSnapshot struct {
	Schedules      map[string]*persistedSchedule      `json:"schedules"`
	ScheduleGroups map[string]*persistedScheduleGroup `json:"scheduleGroups"`
	AccountID      string                             `json:"accountID"`
	Region         string                             `json:"region"`
}

// ensureNonNilMaps guarantees the snapshot maps are always non-nil after decoding.
func ensureNonNilMaps(snap *backendSnapshot) {
	if snap.Schedules == nil {
		snap.Schedules = make(map[string]*persistedSchedule)
	}

	if snap.ScheduleGroups == nil {
		snap.ScheduleGroups = make(map[string]*persistedScheduleGroup)
	}
}

// parseSnapshotTime parses a timestamp stored by Snapshot.
// Returns [time.Time]{} on failure so the caller proceeds with a sensible default.
func parseSnapshotTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}

	t, err := time.Parse(snapshotTimeLayout, s)
	if err != nil {
		return time.Time{}
	}

	return t
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	// Build persisted copies that do not carry live Prometheus state.
	schedules := make(map[string]*persistedSchedule, len(b.schedules))
	for key, s := range b.schedules {
		var tagMap map[string]string
		if s.Tags != nil {
			tagMap = s.Tags.Clone()
		}

		schedules[key] = &persistedSchedule{
			Name:                       s.Name,
			ARN:                        s.ARN,
			GroupName:                  s.GroupName,
			ScheduleExpression:         s.ScheduleExpression,
			ScheduleExpressionTimezone: s.ScheduleExpressionTimezone,
			Description:                s.Description,
			Target:                     s.Target,
			State:                      s.State,
			FlexibleTimeWindow:         s.FlexibleTimeWindow,
			ActionAfterCompletion:      s.ActionAfterCompletion,
			KmsKeyArn:                  s.KmsKeyArn,
			AccountID:                  s.AccountID,
			Region:                     s.Region,
			CreationDate:               s.CreationDate.Format(snapshotTimeLayout),
			LastModificationDate:       s.LastModificationDate.Format(snapshotTimeLayout),
			Tags:                       tagMap,
		}

		if s.StartDate != nil {
			schedules[key].StartDate = s.StartDate.Format(snapshotTimeLayout)
		}

		if s.EndDate != nil {
			schedules[key].EndDate = s.EndDate.Format(snapshotTimeLayout)
		}
	}

	groups := make(map[string]*persistedScheduleGroup, len(b.scheduleGroups))
	for name, g := range b.scheduleGroups {
		var tagMap map[string]string
		if g.Tags != nil {
			tagMap = g.Tags.Clone()
		}

		groups[name] = &persistedScheduleGroup{
			Name:                 g.Name,
			ARN:                  g.ARN,
			Description:          g.Description,
			State:                g.State,
			CreationDate:         g.CreationDate.Format(snapshotTimeLayout),
			LastModificationDate: g.LastModificationDate.Format(snapshotTimeLayout),
			Tags:                 tagMap,
		}
	}

	snap := backendSnapshot{
		Schedules:      schedules,
		ScheduleGroups: groups,
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

	ensureNonNilMaps(&snap)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	// Release Prometheus metrics held by the current state.
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

	// Rebuild live Schedule objects from their persisted counterparts.
	b.schedules = make(map[string]*Schedule, len(snap.Schedules))
	b.scheduleARNIndex = make(map[string]string, len(snap.Schedules))

	for key, ps := range snap.Schedules {
		groupName := ps.GroupName
		if groupName == "" {
			groupName = defaultGroupName
		}

		s := &Schedule{
			Name:                       ps.Name,
			ARN:                        ps.ARN,
			GroupName:                  groupName,
			ScheduleExpression:         ps.ScheduleExpression,
			ScheduleExpressionTimezone: ps.ScheduleExpressionTimezone,
			Description:                ps.Description,
			Target:                     ps.Target,
			State:                      ps.State,
			FlexibleTimeWindow:         ps.FlexibleTimeWindow,
			ActionAfterCompletion:      ps.ActionAfterCompletion,
			KmsKeyArn:                  ps.KmsKeyArn,
			AccountID:                  ps.AccountID,
			Region:                     ps.Region,
			CreationDate:               parseSnapshotTime(ps.CreationDate),
			LastModificationDate:       parseSnapshotTime(ps.LastModificationDate),
			Tags: tags.FromMap(
				"scheduler.schedule."+groupName+"."+ps.Name+".tags",
				ps.Tags,
			),
		}

		if ps.StartDate != "" {
			t := parseSnapshotTime(ps.StartDate)
			s.StartDate = &t
		}

		if ps.EndDate != "" {
			t := parseSnapshotTime(ps.EndDate)
			s.EndDate = &t
		}
		b.schedules[key] = s
		b.scheduleARNIndex[s.ARN] = key
	}

	// Rebuild live ScheduleGroup objects from their persisted counterparts.
	b.scheduleGroups = make(map[string]*ScheduleGroup, len(snap.ScheduleGroups))
	b.scheduleGroupARNIndex = make(map[string]string, len(snap.ScheduleGroups))

	for name, pg := range snap.ScheduleGroups {
		g := &ScheduleGroup{
			Name:                 pg.Name,
			ARN:                  pg.ARN,
			Description:          pg.Description,
			State:                pg.State,
			CreationDate:         parseSnapshotTime(pg.CreationDate),
			LastModificationDate: parseSnapshotTime(pg.LastModificationDate),
			Tags:                 tags.FromMap("scheduler.schedulegroup."+name+".tags", pg.Tags),
		}
		b.scheduleGroups[name] = g
		b.scheduleGroupARNIndex[g.ARN] = name
	}

	b.accountID = snap.AccountID
	b.region = snap.Region

	// Ensure the default group always exists after restore.
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
