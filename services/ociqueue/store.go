package ociqueue

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

// InMemoryBackend holds every queue and work request under one lock.
type InMemoryBackend struct {
	mu         *lockmetrics.RWMutex
	now        func() time.Time
	after      func(time.Duration) <-chan time.Time
	queues     map[string]*Queue
	workReqs   map[string]*WorkRequest
	Auth       *ociauth.Registry
	keyChecker KeyChecker
	transition time.Duration
	seq        int64
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		mu: lockmetrics.New("ociqueue"), now: time.Now, after: time.After, transition: defaultTransition,
	}
	b.resetMaps()
	b.Auth = ociauth.NewRegistry()

	return b
}

func (b *InMemoryBackend) resetMaps() {
	b.queues, b.workReqs = map[string]*Queue{}, map[string]*WorkRequest{}
}

// SetClock injects the time source used for lifecycle, visibility and retention.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	b.now = now
	b.mu.Unlock()
}

// SetTimer injects the wait used by long polls; it must return a channel that fires after d.
func (b *InMemoryBackend) SetTimer(after func(time.Duration) <-chan time.Time) {
	b.mu.Lock("SetTimer")
	b.after = after
	b.mu.Unlock()
}

// SetTransition sets how long CREATING, UPDATING and DELETING states last.
func (b *InMemoryBackend) SetTransition(d time.Duration) {
	b.mu.Lock("SetTransition")
	b.transition = d
	b.mu.Unlock()
}

// SetKeyChecker wires validation of customEncryptionKeyId references against a Vault backend.
func (b *InMemoryBackend) SetKeyChecker(fn KeyChecker) {
	b.mu.Lock("SetKeyChecker")
	b.keyChecker = fn
	b.mu.Unlock()
}

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")

	for _, q := range b.queues {
		q.notify()
	}

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

func (b *InMemoryBackend) checkKey(keyID string) *APIError {
	if !strings.HasPrefix(keyID, "ocid1.key.") {
		return errParam("customEncryptionKeyId must be the OCID of a Vault key")
	}

	if b.keyChecker == nil {
		return nil
	}

	state, found := b.keyChecker(keyID)
	switch {
	case !found:
		return errParam("customEncryptionKeyId " + keyID + " does not reference an existing key")
	case state != "ENABLED":
		return errParam("customEncryptionKeyId " + keyID + " is in state " + state + "; the key must be ENABLED")
	}

	return nil
}

// Sweep applies lifecycle transitions, retention and dead lettering against the backend clock.
func (b *InMemoryBackend) Sweep() {
	b.mu.Lock("Sweep")
	defer b.mu.Unlock()

	now := b.nowUTC()
	for _, q := range b.queues {
		b.settle(q, now)
	}
}

// settle moves one queue through its transition and applies retention and the dead letter threshold.
func (b *InMemoryBackend) settle(q *Queue, now time.Time) {
	if q.TransitionAt != nil && !q.TransitionAt.After(now) {
		q.TransitionAt = nil

		switch q.State {
		case stateCreating, stateUpdating:
			q.State = stateActive
		case stateDeleting:
			q.State = stateDeleted
			q.Messages, q.DLQ = nil, nil
			q.notify()

			return
		}
	}

	q.Messages = slices.DeleteFunc(q.Messages, func(m *Message) bool { return !m.Expire.After(now) })
	q.DLQ = slices.DeleteFunc(q.DLQ, func(m *Message) bool { return !m.Expire.After(now) })

	if q.DLQCount <= 0 {
		return
	}

	kept := q.Messages[:0]

	for _, m := range q.Messages {
		if !m.VisibleAfter.After(now) && m.Deliveries >= q.DLQCount {
			q.DLQ = append(q.DLQ, m)

			continue
		}

		kept = append(kept, m)
	}

	clear(q.Messages[len(kept):])
	q.Messages = kept
}

func (q *Queue) notify() {
	if q.wake != nil {
		close(q.wake)
	}

	q.wake = make(chan struct{})
}

func (q *Queue) waiter() <-chan struct{} {
	if q.wake == nil {
		q.wake = make(chan struct{})
	}

	return q.wake
}

func (b *InMemoryBackend) settleID(id string) {
	if q, ok := b.queues[id]; ok {
		b.settle(q, b.nowUTC())
	}
}
