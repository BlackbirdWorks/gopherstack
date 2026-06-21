package scheduler

import (
	"context"
	"encoding/json"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
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
	// Schedules and ScheduleGroups are nested by region (outer key = region).
	Schedules      map[string]map[string]*persistedSchedule      `json:"schedules"`
	ScheduleGroups map[string]map[string]*persistedScheduleGroup `json:"scheduleGroups"`
	AccountID      string                                        `json:"accountID"`
	Region         string                                        `json:"region"`
}

// ensureNonNilMaps guarantees the snapshot maps are always non-nil after decoding.
func ensureNonNilMaps(snap *backendSnapshot) {
	if snap.Schedules == nil {
		snap.Schedules = make(map[string]map[string]*persistedSchedule)
	}

	if snap.ScheduleGroups == nil {
		snap.ScheduleGroups = make(map[string]map[string]*persistedScheduleGroup)
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
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	// Build persisted copies that do not carry live Prometheus state.
	schedules := make(map[string]map[string]*persistedSchedule, len(b.schedules))
	for region, regionSchedules := range b.schedules {
		regionMap := make(map[string]*persistedSchedule, len(regionSchedules))
		for key, s := range regionSchedules {
			var tagMap map[string]string
			if s.Tags != nil {
				tagMap = s.Tags.Clone()
			}

			ps := &persistedSchedule{
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
				ps.StartDate = s.StartDate.Format(snapshotTimeLayout)
			}

			if s.EndDate != nil {
				ps.EndDate = s.EndDate.Format(snapshotTimeLayout)
			}
			regionMap[key] = ps
		}
		schedules[region] = regionMap
	}

	groups := make(map[string]map[string]*persistedScheduleGroup, len(b.scheduleGroups))
	for region, regionGroups := range b.scheduleGroups {
		regionMap := make(map[string]*persistedScheduleGroup, len(regionGroups))
		for name, g := range regionGroups {
			var tagMap map[string]string
			if g.Tags != nil {
				tagMap = g.Tags.Clone()
			}

			regionMap[name] = &persistedScheduleGroup{
				Name:                 g.Name,
				ARN:                  g.ARN,
				Description:          g.Description,
				State:                g.State,
				CreationDate:         g.CreationDate.Format(snapshotTimeLayout),
				LastModificationDate: g.LastModificationDate.Format(snapshotTimeLayout),
				Tags:                 tagMap,
			}
		}
		groups[region] = regionMap
	}

	snap := backendSnapshot{
		Schedules:      schedules,
		ScheduleGroups: groups,
		AccountID:      b.accountID,
		Region:         b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "scheduler: failed to snapshot state", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	ensureNonNilMaps(&snap)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.closeAllTagMetrics()

	// Rebuild live Schedule objects from their persisted counterparts.
	b.schedules = make(map[string]map[string]*Schedule, len(snap.Schedules))
	b.scheduleARNIndex = make(map[string]map[string]string, len(snap.Schedules))

	for region, regionSchedules := range snap.Schedules {
		for key, ps := range regionSchedules {
			s := scheduleFromPersisted(ps)
			b.schedulesStore(region)[key] = s
			b.scheduleARNStore(region)[s.ARN] = key
		}
	}

	// Rebuild live ScheduleGroup objects from their persisted counterparts.
	b.scheduleGroups = make(map[string]map[string]*ScheduleGroup, len(snap.ScheduleGroups))
	b.scheduleGroupARNIndex = make(map[string]map[string]string, len(snap.ScheduleGroups))

	for region, regionGroups := range snap.ScheduleGroups {
		// Initialise the region maps directly (without seeding a default group) so the
		// restored snapshot's own "default" group is used as-is.
		if b.scheduleGroups[region] == nil {
			b.scheduleGroups[region] = make(map[string]*ScheduleGroup)
		}

		for name, pg := range regionGroups {
			g := scheduleGroupFromPersisted(name, pg)
			b.scheduleGroups[region][name] = g
			b.scheduleGroupARNStore(region)[g.ARN] = name
		}
	}

	b.accountID = snap.AccountID
	b.region = snap.Region

	// Ensure the default group always exists after restore in the default region.
	// scheduleGroupsStore seeds the built-in "default" group when the region map is
	// first created; if the region already existed we add it explicitly.
	if _, ok := b.scheduleGroupsStore(b.region)[defaultGroupName]; !ok {
		b.seedDefaultGroup(b.region)
	}

	return nil
}

// closeAllTagMetrics releases the Prometheus metrics held by all live schedules and
// schedule groups across every region. Callers must hold b.mu.
func (b *InMemoryBackend) closeAllTagMetrics() {
	for _, regionSchedules := range b.schedules {
		for _, s := range regionSchedules {
			if s.Tags != nil {
				s.Tags.Close()
			}
		}
	}

	for _, regionGroups := range b.scheduleGroups {
		for _, g := range regionGroups {
			if g.Tags != nil {
				g.Tags.Close()
			}
		}
	}
}

// scheduleFromPersisted rebuilds a live Schedule from its persisted representation.
func scheduleFromPersisted(ps *persistedSchedule) *Schedule {
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

	return s
}

// scheduleGroupFromPersisted rebuilds a live ScheduleGroup from its persisted representation.
func scheduleGroupFromPersisted(name string, pg *persistedScheduleGroup) *ScheduleGroup {
	return &ScheduleGroup{
		Name:                 pg.Name,
		ARN:                  pg.ARN,
		Description:          pg.Description,
		State:                pg.State,
		CreationDate:         parseSnapshotTime(pg.CreationDate),
		LastModificationDate: parseSnapshotTime(pg.LastModificationDate),
		Tags:                 tags.FromMap("scheduler.schedulegroup."+name+".tags", pg.Tags),
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}

// Reset implements service.Resettable by delegating to the backend.
func (h *Handler) Reset() {
	h.Backend.Reset()
}
