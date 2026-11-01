package ocilogging

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
)

const (
	// Region is the fixed region baked into emulated OCIDs.
	Region           = "us-ashburn-1"
	defaultListLimit = 100
	ocidTailLen      = 52

	defaultTransition = 2 * time.Second
	maxName           = 255
	maxDescription    = 400
	retentionStep     = 30
	maxRetentionDays  = 180
	hoursPerDay       = 24
)

// InMemoryBackend holds every log group, log, agent configuration, work request and ingested entry under one lock.
type InMemoryBackend struct {
	mu         *lockmetrics.RWMutex
	now        func() time.Time
	groups     map[string]*LogGroup
	logs       map[string]*Log
	agents     map[string]*AgentConfig
	workReqs   map[string]*WorkRequest
	entries    map[string][]*Entry
	Auth       *ociauth.Registry
	transition time.Duration
	seq        int64
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{mu: lockmetrics.New("ocilogging"), now: time.Now, transition: defaultTransition}
	b.resetMaps()
	b.Auth = ociauth.NewRegistry()

	return b
}

func (b *InMemoryBackend) resetMaps() {
	b.groups, b.logs, b.agents = map[string]*LogGroup{}, map[string]*Log{}, map[string]*AgentConfig{}
	b.workReqs, b.entries = map[string]*WorkRequest{}, map[string][]*Entry{}
}

// SetClock injects the time source used for lifecycle, timestamps and retention.
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

// ListOpts carries the shared list query parameters.
type ListOpts struct {
	SortBy string
	Order  string
	Page   string
	Limit  int
}

func paginate[T any](items []T, o ListOpts, name func(T) string, created func(T) (time.Time, int64)) page.Page[T] {
	byName := strings.EqualFold(o.SortBy, "displayName")
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
			r = cmp.Compare(name(a), name(c))
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

// Sweep applies lifecycle transitions and retention against the backend clock.
func (b *InMemoryBackend) Sweep() {
	b.mu.Lock("Sweep")
	defer b.mu.Unlock()

	b.settleAll()
}

func due(at *time.Time, now time.Time) bool { return at != nil && !at.After(now) }

// settleAll completes finished transitions and drops entries past their log's retention; the caller holds the lock.
func (b *InMemoryBackend) settleAll() {
	now := b.nowUTC()

	for id, g := range b.groups {
		if !due(g.TransitionAt, now) {
			continue
		}

		g.TransitionAt = nil

		if g.State == stateDeleting {
			delete(b.groups, id)

			continue
		}

		g.State = stateActive
	}

	for id, l := range b.logs {
		if !due(l.TransitionAt, now) {
			continue
		}

		l.TransitionAt = nil

		switch {
		case l.State == stateDeleting:
			delete(b.logs, id)
			delete(b.entries, id)
		case l.Enabled:
			l.State = stateActive
		default:
			l.State = stateInactive
		}
	}

	for id, a := range b.agents {
		if !due(a.TransitionAt, now) {
			continue
		}

		a.TransitionAt = nil

		if a.State == stateDeleting {
			delete(b.agents, id)

			continue
		}

		a.State = stateActive
	}

	for id, es := range b.entries {
		l, ok := b.logs[id]
		if !ok {
			delete(b.entries, id)

			continue
		}

		cutoff := now.Add(-time.Duration(l.Retention) * hoursPerDay * time.Hour)
		b.entries[id] = slices.DeleteFunc(es, func(e *Entry) bool { return !e.Ingested.After(cutoff) })
	}
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

func firstErr(errs ...*APIError) *APIError {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}

	return nil
}

//go:fix inline
