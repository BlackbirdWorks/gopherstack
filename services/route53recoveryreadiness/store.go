package route53recoveryreadiness

import (
	"maps"
	"regexp"
	"slices"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	listDefault = 100
	listMax     = 1000
	maxTags     = 50
)

var nameRE = regexp.MustCompile(`^[a-zA-Z0-9_]{1,64}$`)

// InMemoryBackend holds Route 53 Recovery Readiness state.
type InMemoryBackend struct {
	appConfig any
	registry  *store.Registry
	cells     *store.Table[Cell]
	groups    *store.Table[RecoveryGroup]
	sets      *store.Table[ResourceSet]
	checks    *store.Table[ReadinessCheck]
	auths     *store.Table[Authorization]
	mu        *lockmetrics.RWMutex
	accountID string
}

// NewInMemoryBackend creates an empty backend for accountID.
func NewInMemoryBackend(accountID string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:  store.NewRegistry(),
		mu:        lockmetrics.New("route53recoveryreadiness"),
		accountID: accountID,
	}

	b.cells = store.Register(b.registry, "cells", store.New(func(v *Cell) string { return v.Name }))
	b.groups = store.Register(b.registry, "groups", store.New(func(v *RecoveryGroup) string { return v.Name }))
	b.sets = store.Register(b.registry, "sets", store.New(func(v *ResourceSet) string { return v.Name }))
	b.checks = store.Register(b.registry, "checks", store.New(func(v *ReadinessCheck) string { return v.Name }))
	b.auths = store.Register(b.registry, "auths", store.New(func(v *Authorization) string { return v.Value }))

	return b
}

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
}

// SetAppConfig records service.AppContext.Config so siblings are resolved at call time.
func (b *InMemoryBackend) SetAppConfig(cfg any) { b.appConfig = cfg }

func (b *InMemoryBackend) arnFor(kind, name string) string {
	return arn.BuildGlobal(arnService, "", b.accountID, kind+"/"+name)
}

func validName(kind, name string) error {
	if !nameRE.MatchString(name) {
		return restroute.Validation("%s name must be 1-64 characters of letters, digits and underscores", kind)
	}

	return nil
}

func sorted[T any](items []*T, key func(*T) string) []*T {
	out := slices.Clone(items)
	sort.SliceStable(out, func(i, j int) bool { return key(out[i]) < key(out[j]) })

	return out
}

func cloneTags(t map[string]string) map[string]string {
	if len(t) == 0 {
		return nil
	}

	return maps.Clone(t)
}

func (b *InMemoryBackend) getCell(name string) (*Cell, error) {
	c, ok := b.cells.Get(name)
	if !ok {
		return nil, restroute.NotFound("Cell %s was not found", name)
	}

	return c, nil
}

func (b *InMemoryBackend) getGroup(name string) (*RecoveryGroup, error) {
	g, ok := b.groups.Get(name)
	if !ok {
		return nil, restroute.NotFound("Recovery group %s was not found", name)
	}

	return g, nil
}

func (b *InMemoryBackend) getSet(name string) (*ResourceSet, error) {
	s, ok := b.sets.Get(name)
	if !ok {
		return nil, restroute.NotFound("Resource set %s was not found", name)
	}

	return s, nil
}

func (b *InMemoryBackend) getCheck(name string) (*ReadinessCheck, error) {
	c, ok := b.checks.Get(name)
	if !ok {
		return nil, restroute.NotFound("Readiness check %s was not found", name)
	}

	return c, nil
}

// cellByARN resolves a cell ARN.
func (b *InMemoryBackend) cellByARN(a string) (*Cell, bool) {
	for _, c := range b.cells.All() {
		if c.ARN == a {
			return c, true
		}
	}

	return nil, false
}
