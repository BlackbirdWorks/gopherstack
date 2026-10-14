package arczonalshift

import (
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// AutoshiftSummary is one AWS-initiated autoshift.
type AutoshiftSummary struct {
	EndTime   *float64 `json:"endTime,omitempty"`
	AwayFrom  string   `json:"awayFrom"`
	Status    string   `json:"status"`
	StartTime float64  `json:"startTime"`
}

// ShiftInResource is a shift as shown inside a managed resource.
type ShiftInResource struct {
	AppliedStatus      string  `json:"appliedStatus"`
	AwayFrom           string  `json:"awayFrom"`
	Comment            string  `json:"comment"`
	ResourceIdentifier string  `json:"resourceIdentifier"`
	ZonalShiftID       string  `json:"zonalShiftId"`
	PracticeRunOutcome string  `json:"practiceRunOutcome,omitempty"`
	ShiftType          string  `json:"shiftType,omitempty"`
	ExpiryTime         float64 `json:"expiryTime"`
	StartTime          float64 `json:"startTime"`
}

// AutoshiftInResource is an autoshift as shown inside a managed resource.
type AutoshiftInResource struct {
	AppliedStatus string  `json:"appliedStatus"`
	AwayFrom      string  `json:"awayFrom"`
	StartTime     float64 `json:"startTime"`
}

// ManagedSummary is the wire form of a managed resource in ListManagedResources.
type ManagedSummary struct {
	AppliedWeights       map[string]float32    `json:"appliedWeights"`
	Arn                  string                `json:"arn"`
	Name                 string                `json:"name"`
	PracticeRunStatus    string                `json:"practiceRunStatus"`
	ZonalAutoshiftStatus string                `json:"zonalAutoshiftStatus"`
	AvailabilityZones    []string              `json:"availabilityZones"`
	Autoshifts           []AutoshiftInResource `json:"autoshifts"`
	ZonalShifts          []ShiftInResource     `json:"zonalShifts"`
}

// ManagedDetail is the wire form of GetManagedResource.
type ManagedDetail struct {
	AppliedWeights           map[string]float32    `json:"appliedWeights"`
	PracticeRunConfiguration *PracticeView         `json:"practiceRunConfiguration,omitempty"`
	Arn                      string                `json:"arn"`
	Name                     string                `json:"name"`
	ZonalAutoshiftStatus     string                `json:"zonalAutoshiftStatus"`
	Autoshifts               []AutoshiftInResource `json:"autoshifts"`
	ZonalShifts              []ShiftInResource     `json:"zonalShifts"`
}

// shiftsInResource lists the active shifts of a resource with their appliedStatus.
// Customer zonal shifts take precedence over practice runs (arczonalshift types.ZonalShiftInResource docs).
func (b *InMemoryBackend) shiftsInResource(resource string, now time.Time) []ShiftInResource {
	active := b.activeShifts(resource, "", now)
	customer := false

	for _, s := range active {
		customer = customer || s.Type == shiftZonal
	}

	out := make([]ShiftInResource, 0, len(active))

	for _, s := range active {
		applied := appliedNo
		if s.Type == shiftZonal || !customer {
			applied = appliedYes
		}

		v := ShiftInResource{
			AppliedStatus:      applied,
			AwayFrom:           s.AwayFrom,
			Comment:            s.Comment,
			ResourceIdentifier: s.Resource,
			ZonalShiftID:       s.ID,
			ShiftType:          s.Type,
			ExpiryTime:         awstime.Epoch(s.ExpiryTime),
			StartTime:          awstime.Epoch(s.StartTime),
		}
		if s.Type == shiftPractice {
			v.PracticeRunOutcome = s.Outcome
		}

		out = append(out, v)
	}

	return out
}

func weights(zoneIDs []string, shifts []ShiftInResource) map[string]float32 {
	w := make(map[string]float32, len(zoneIDs))
	for _, z := range zoneIDs {
		w[z] = 1
	}

	for _, s := range shifts {
		if s.AppliedStatus == appliedYes {
			w[s.AwayFrom] = 0
		}
	}

	return w
}

func (b *InMemoryBackend) practiceStatus(resource string) string {
	if b.configs.Has(resource) {
		return autoshiftEnabled
	}

	return autoshiftDisabled
}

// GetManagedResource describes one managed resource.
func (b *InMemoryBackend) GetManagedResource(resource string) (ManagedDetail, error) {
	m, err := b.managedFor(resource)
	if err != nil {
		return ManagedDetail{}, err
	}

	b.mu.Lock("GetManagedResource")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	b.settleLocked(now)

	shifts := b.shiftsInResource(resource, now)
	d := ManagedDetail{
		AppliedWeights: weights(
			m.ZoneIDs,
			shifts,
		),
		Arn:                  m.ARN,
		Name:                 m.Name,
		ZonalAutoshiftStatus: b.autoshiftStatus(resource),
		Autoshifts:           []AutoshiftInResource{},
		ZonalShifts:          shifts,
	}

	if cfg, ok := b.configs.Get(resource); ok {
		d.PracticeRunConfiguration = b.practiceOut(m, cfg).Config
	}

	return d, nil
}

// ListManagedResources lists the load balancers of region as zonal-shift managed resources.
func (b *InMemoryBackend) ListManagedResources(region, token string, maxResults int) ([]ManagedSummary, string, error) {
	all := b.listManaged(region)
	sort.SliceStable(all, func(i, j int) bool { return all[i].ARN < all[j].ARN })

	pg, err := paginate(all, token, maxResults)
	if err != nil {
		return nil, "", err
	}

	b.mu.Lock("ListManagedResources")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	b.settleLocked(now)

	out := make([]ManagedSummary, 0, len(pg.Data))

	for _, m := range pg.Data {
		shifts := b.shiftsInResource(m.ARN, now)
		out = append(out, ManagedSummary{
			AppliedWeights:       weights(m.ZoneIDs, shifts),
			Arn:                  m.ARN,
			Name:                 m.Name,
			PracticeRunStatus:    b.practiceStatus(m.ARN),
			ZonalAutoshiftStatus: b.autoshiftStatus(m.ARN),
			AvailabilityZones: append(
				[]string{},
				m.ZoneIDs...),
			Autoshifts:  []AutoshiftInResource{},
			ZonalShifts: shifts,
		})
	}

	return out, pg.Next, nil
}

// ListAutoshifts lists AWS-initiated autoshifts; this emulator never initiates one, so only the filter is validated.
func (b *InMemoryBackend) ListAutoshifts(status, token string, maxResults int) ([]AutoshiftSummary, string, error) {
	if status != "" && status != statusActive && status != "COMPLETED" {
		return nil, "", invalidReason("InvalidStatus", "status must be ACTIVE or COMPLETED")
	}

	pg, err := paginate([]AutoshiftSummary{}, token, maxResults)

	return pg.Data, pg.Next, err
}

// ObserverNotificationStatus returns the account's autoshift observer notification status.
func (b *InMemoryBackend) ObserverNotificationStatus() string {
	b.mu.RLock("GetAutoshiftObserverNotificationStatus")
	defer b.mu.RUnlock()

	if s, ok := b.settings.Get(settingsID); ok {
		return s.Observer
	}

	return autoshiftDisabled
}

// SetObserverNotificationStatus enables or disables autoshift observer notifications.
func (b *InMemoryBackend) SetObserverNotificationStatus(status string) (string, error) {
	if status != autoshiftEnabled && status != autoshiftDisabled {
		return "", invalidReason("InvalidStatus", "status must be ENABLED or DISABLED")
	}

	b.mu.Lock("UpdateAutoshiftObserverNotificationStatus")
	defer b.mu.Unlock()

	b.settings.Put(&Settings{ID: settingsID, Observer: status})

	return status, nil
}
