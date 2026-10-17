package arczonalshift

import (
	"slices"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	listDefault = 100
	listMax     = 100

	defaultPracticeRun = 30 * time.Minute
	maxShiftDuration   = 72 * time.Hour
	settingsID         = "account"
)

// InMemoryBackend holds zonal shift state; managed resources are read live from ELBv2.
type InMemoryBackend struct {
	appConfig        any
	registry         *store.Registry
	shifts           *store.Table[Shift]
	configs          *store.Table[PracticeConfig]
	resources        *store.Table[ResourceSettings]
	settings         *store.Table[Settings]
	mu               *lockmetrics.RWMutex
	accountID        string
	practiceDuration time.Duration
}

// NewInMemoryBackend creates an empty backend for accountID.
func NewInMemoryBackend(accountID string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:         store.NewRegistry(),
		mu:               lockmetrics.New("arczonalshift"),
		accountID:        accountID,
		practiceDuration: defaultPracticeRun,
	}

	b.shifts = store.Register(b.registry, "shifts", store.New(func(v *Shift) string { return v.ID }))
	b.configs = store.Register(b.registry, "configs", store.New(func(v *PracticeConfig) string { return v.Resource }))
	b.resources = store.Register(
		b.registry,
		"resources",
		store.New(func(v *ResourceSettings) string { return v.Resource }),
	)
	b.settings = store.Register(b.registry, "settings", store.New(func(v *Settings) string { return v.ID }))

	return b
}

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
}

// SetPracticeRunDuration sets how long a practice run stays ACTIVE (default 30 minutes); d <= 0 restores the default.
func (b *InMemoryBackend) SetPracticeRunDuration(d time.Duration) {
	b.mu.Lock("SetPracticeRunDuration")
	defer b.mu.Unlock()

	if d <= 0 {
		d = defaultPracticeRun
	}

	b.practiceDuration = d
}

// SetAppConfig records service.AppContext.Config so siblings are resolved at call time.
func (b *InMemoryBackend) SetAppConfig(cfg any) { b.appConfig = cfg }

func (s *Shift) status(now time.Time) string {
	switch {
	case s.CanceledAt != nil:
		return statusCanceled
	case !now.Before(s.ExpiryTime):
		return statusExpired
	default:
		return statusActive
	}
}

func sortedShifts(items []*Shift) []*Shift {
	out := slices.Clone(items)
	sort.SliceStable(out, func(i, j int) bool {
		if !out[i].StartTime.Equal(out[j].StartTime) {
			return out[i].StartTime.Before(out[j].StartTime)
		}

		return out[i].ID < out[j].ID
	})

	return out
}
