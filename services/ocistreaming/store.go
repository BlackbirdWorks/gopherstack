package ocistreaming

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
)

// KeyChecker reports the lifecycle state of a KMS key OCID and whether it exists.
type KeyChecker func(keyID string) (state string, found bool)

// InMemoryBackend holds every pool, stream, harness, group and work request under one lock.
type InMemoryBackend struct {
	mu         *lockmetrics.RWMutex
	now        func() time.Time
	pools      map[string]*Pool
	streams    map[string]*Stream
	harnesses  map[string]*Harness
	groups     map[string]*Group
	workReqs   map[string]*WorkRequest
	Auth       *ociauth.Registry
	keyChecker KeyChecker
	transition time.Duration
	seq        int64
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{mu: lockmetrics.New("ocistreaming"), now: time.Now, transition: defaultTransition}
	b.resetMaps()
	b.Auth = ociauth.NewRegistry()

	return b
}

func (b *InMemoryBackend) resetMaps() {
	b.pools, b.streams, b.harnesses = map[string]*Pool{}, map[string]*Stream{}, map[string]*Harness{}
	b.groups, b.workReqs = map[string]*Group{}, map[string]*WorkRequest{}
}

// SetClock injects the time source used for lifecycle transitions, retention and reservations.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	b.now = now
	b.mu.Unlock()
}

// SetTransition sets how long CREATING and DELETING states last.
func (b *InMemoryBackend) SetTransition(d time.Duration) {
	b.mu.Lock("SetTransition")
	b.transition = d
	b.mu.Unlock()
}

// SetKeyChecker wires validation of customEncryptionKey references against a Vault backend.
func (b *InMemoryBackend) SetKeyChecker(fn KeyChecker) {
	b.mu.Lock("SetKeyChecker")
	b.keyChecker = fn
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

func validateName(name string) *APIError {
	if strings.TrimSpace(name) == "" {
		return errMissing("name is required")
	}

	if len(name) > maxName {
		return errParam("name must be at most 255 characters")
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
	byName := strings.EqualFold(o.SortBy, "NAME")
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

// checkKey validates a pool's customEncryptionKey reference.
func (b *InMemoryBackend) checkKey(keyID string) *APIError {
	if !strings.HasPrefix(keyID, "ocid1.key.") {
		return errParam("kmsKeyId must be the OCID of a Vault key")
	}

	if b.keyChecker == nil {
		return nil
	}

	state, found := b.keyChecker(keyID)
	switch {
	case !found:
		return errParam("kmsKeyId " + keyID + " does not reference an existing key")
	case state != "ENABLED":
		return errParam("kmsKeyId " + keyID + " is in state " + state + "; the key must be ENABLED")
	}

	return nil
}

// Sweep applies lifecycle transitions and retention against the backend clock.
func (b *InMemoryBackend) Sweep() {
	b.mu.Lock("Sweep")
	defer b.mu.Unlock()

	now := b.nowUTC()

	for _, p := range b.pools {
		p.State, p.TransitionAt = settle(p.State, p.TransitionAt, now)
	}

	for _, h := range b.harnesses {
		h.State, h.TransitionAt = settle(h.State, h.TransitionAt, now)
	}

	for id, s := range b.streams {
		s.State, s.TransitionAt = settle(s.State, s.TransitionAt, now)

		if s.State == stateDeleted {
			s.Parts = nil
			b.dropGroups(id)

			continue
		}

		retention := time.Duration(s.Retention) * time.Hour
		for _, part := range s.Parts {
			part.expire(now, retention)
		}
	}
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

func (p *Partition) expire(now time.Time, retention time.Duration) {
	n := 0
	for n < len(p.Messages) && now.Sub(p.Messages[n].Time) > retention {
		n++
	}

	if n > 0 {
		p.Messages = slices.Clone(p.Messages[n:])
		p.Base += int64(n)
	}
}

func (b *InMemoryBackend) dropGroups(streamID string) {
	for k, g := range b.groups {
		if g.StreamID == streamID {
			delete(b.groups, k)
		}
	}
}
