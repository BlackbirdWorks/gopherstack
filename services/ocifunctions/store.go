package ocifunctions

import (
	"cmp"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	// Region is the fixed region baked into emulated OCIDs.
	Region           = "us-ashburn-1"
	defaultListLimit = 100
	ocidTailLen      = 52

	defaultTransition = 2 * time.Second
	maxName           = 255
)

// InMemoryBackend holds every application, function and work request under one lock.
type InMemoryBackend struct {
	mu         *lockmetrics.RWMutex
	clock      atomic.Pointer[func() time.Time]
	observer   atomic.Pointer[InvokeObserver]
	apps       map[string]*Application
	funcs      map[string]*Function
	workReqs   map[string]*WorkRequest
	Auth       *ociauth.Registry
	exec       *executor
	sink       func(ResourceEvent)
	pending    []ResourceEvent
	transition time.Duration
	seq        int64
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{mu: lockmetrics.New("ocifunctions"), transition: defaultTransition}
	b.SetClock(time.Now)
	b.resetMaps()
	b.Auth = ociauth.NewRegistry()
	b.exec = newExecutor(b.nowUTC)

	return b
}

func (b *InMemoryBackend) resetMaps() {
	b.apps, b.funcs, b.workReqs = map[string]*Application{}, map[string]*Function{}, map[string]*WorkRequest{}
}

// SetClock injects the time source used for lifecycle transitions and warm-container expiry.
func (b *InMemoryBackend) SetClock(now func() time.Time) { b.clock.Store(&now) }

// SetTransition sets how long CREATING and DELETING states last.
func (b *InMemoryBackend) SetTransition(d time.Duration) {
	b.mu.Lock("SetTransition")
	b.transition = d
	b.mu.Unlock()
}

// Reset clears all state and removes every function container.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	b.resetMaps()
	b.mu.Unlock()
	b.Auth.Reset()
	b.exec.drain()
}

func (b *InMemoryBackend) nowUTC() time.Time { return (*b.clock.Load())().UTC() }

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

func validateName(name string) *APIError {
	if strings.TrimSpace(name) == "" {
		return errMissing("displayName is required")
	}

	if len(name) > maxName {
		return errParam("displayName must be at most 255 characters")
	}

	return nil
}

func (b *InMemoryBackend) transitionAt() *time.Time {
	t := b.nowUTC().Add(b.transition)

	return &t
}

// ListOpts carries the shared list query parameters.
type ListOpts struct {
	SortBy string
	Order  string
	Page   string
	Limit  int
}

func paginate[T any](items []T, o ListOpts, name, id func(T) string, created func(T) (time.Time, int64)) page.Page[T] {
	desc := true

	switch strings.ToLower(o.SortBy) {
	case "displayname", "id":
		desc = false
	}

	switch strings.ToUpper(o.Order) {
	case "ASC":
		desc = false
	case "DESC":
		desc = true
	}

	slices.SortStableFunc(items, func(a, c T) int {
		var r int

		switch strings.ToLower(o.SortBy) {
		case "displayname":
			r = cmp.Or(cmp.Compare(name(a), name(c)), cmp.Compare(id(a), id(c)))
		case "id":
			r = cmp.Compare(id(a), id(c))
		default:
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

func settle(state string, at *time.Time, now time.Time) (string, *time.Time) {
	if at == nil || at.After(now) {
		return state, at
	}

	switch state {
	case stateCreating:
		return stateActive, nil
	case stateDeleting:
		return stateDeleted, nil
	}

	return state, nil
}

// Sweep applies lifecycle transitions and drops containers of deleted functions.
func (b *InMemoryBackend) Sweep() {
	b.mu.Lock("Sweep")
	now := b.nowUTC()

	var gone []string

	for _, a := range b.apps {
		a.State, a.TransitionAt = settle(a.State, a.TransitionAt, now)
	}

	for id, f := range b.funcs {
		f.State, f.TransitionAt = settle(f.State, f.TransitionAt, now)

		if f.State == stateDeleted {
			gone = append(gone, id)
		}
	}
	b.mu.Unlock()

	for _, id := range gone {
		b.exec.dropFunction(id)
	}

	b.exec.reap()
}
