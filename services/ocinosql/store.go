package ocinosql

import (
	"cmp"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	// Region is the fixed region baked into emulated OCIDs.
	Region           = "us-ashburn-1"
	defaultListLimit = 100
	ocidTailLen      = 52

	defaultTransition = 2 * time.Second
)

// InMemoryBackend holds every table and work request under one lock.
type InMemoryBackend struct {
	mu          *lockmetrics.RWMutex
	now         func() time.Time
	tables      map[string]*Table
	workReqs    map[string]*WorkRequest
	Auth        *ociauth.Registry
	compartment func(string) bool
	transition  time.Duration
	seq         int64
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{mu: lockmetrics.New("ocinosql"), now: time.Now, transition: defaultTransition}
	b.resetMaps()
	b.Auth = ociauth.NewRegistry()

	return b
}

func (b *InMemoryBackend) resetMaps() {
	b.tables, b.workReqs = map[string]*Table{}, map[string]*WorkRequest{}
}

// SetClock injects the time source used for lifecycle transitions and TTL expiry.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	b.now = now
	b.mu.Unlock()
}

// SetTransition sets how long CREATING, UPDATING and DELETING states last.
func (b *InMemoryBackend) SetTransition(d time.Duration) {
	b.mu.Lock("SetTransition")
	b.transition = d
	b.mu.Unlock()
}

// SetCompartmentChecker validates compartmentId arguments against another service (OCI Identity).
func (b *InMemoryBackend) SetCompartmentChecker(fn func(string) bool) {
	b.mu.Lock("SetCompartmentChecker")
	b.compartment = fn
	b.mu.Unlock()
}

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	b.resetMaps()
	b.mu.Unlock()
	b.Auth.Reset()
}

func (b *InMemoryBackend) nowUTC() time.Time { return b.now().UTC() }

func (b *InMemoryBackend) nextSeq() int64 {
	b.seq++

	return b.seq
}

func newEtag() string { return uuid.NewString() }

func newOCID(kind string) string {
	return "ocid1." + kind + ".oc1." + Region + "." + awsid.NewN("", ocidTailLen)
}

func checkEtag(current, ifMatch string) *APIError {
	if ifMatch != "" && ifMatch != current {
		return errEtag()
	}

	return nil
}

func (b *InMemoryBackend) transitionAt() *time.Time {
	t := b.nowUTC().Add(b.transition)

	return &t
}

func (b *InMemoryBackend) checkCompartment(id string) *APIError {
	if id == "" {
		return errMissing("compartmentId is required")
	}

	if b.compartment != nil && !b.compartment(id) {
		return errNotFound(id)
	}

	return nil
}

// ListOpts carries the shared list query parameters.
type ListOpts struct {
	SortBy string
	Order  string
	Page   string
	Limit  int
}

func paginate[T any](items []T, o ListOpts, name func(T) string, created func(T) (time.Time, int64)) page.Page[T] {
	byName := strings.EqualFold(o.SortBy, "name")
	desc := !byName
	switch strings.ToUpper(o.Order) {
	case "ASC":
		desc = false
	case "DESC":
		desc = true
	}

	slices.SortStableFunc(items, func(a, c T) int {
		var r int
		if byName {
			r = cmp.Compare(strs.Fold(name(a)), strs.Fold(name(c)))
		} else {
			ta, sa := created(a)
			tc, sc := created(c)
			r = cmp.Or(ta.Compare(tc), cmp.Compare(sa, sc))
		}

		if desc {
			return -r
		}

		return r
	})

	return page.New(items, o.Page, o.Limit, defaultListLimit)
}

// Sweep applies lifecycle transitions against the backend clock.
func (b *InMemoryBackend) Sweep() {
	b.mu.Lock("Sweep")
	defer b.mu.Unlock()

	now := b.nowUTC()
	for _, t := range b.tables {
		settle(t, now)
	}
}

// SweepRows removes expired rows from every table.
func (b *InMemoryBackend) SweepRows() {
	b.mu.Lock("SweepRows")
	defer b.mu.Unlock()

	now := b.nowUTC()
	for _, t := range b.tables {
		purgeExpired(t, now)
	}
}

func due(at *time.Time, now time.Time) bool { return at != nil && !at.After(now) }

// settle moves a table and its indexes through their pending transitions.
func settle(t *Table, now time.Time) {
	if due(t.TransitionAt, now) {
		t.TransitionAt = nil

		switch t.State {
		case stateCreating, stateUpdating:
			t.State = stateActive
		case stateDeleting:
			t.State = stateDeleted
			t.Rows, t.Indexes, t.Usage = nil, nil, nil
		}
	}

	for name, ix := range t.Indexes {
		if !due(ix.TransitionAt, now) {
			continue
		}

		ix.TransitionAt = nil

		if ix.State == stateDeleting {
			delete(t.Indexes, name)

			continue
		}

		ix.State = stateActive
	}
}

func purgeExpired(t *Table, now time.Time) {
	for k, r := range t.Rows {
		if r.Expire != nil && !r.Expire.After(now) {
			delete(t.Rows, k)
		}
	}
}
