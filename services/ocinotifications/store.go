package ocinotifications

import (
	"cmp"
	"context"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	// Region is the fixed region baked into emulated OCIDs.
	Region           = "us-ashburn-1"
	defaultListLimit = 100
	ocidTailLen      = 52
	shortIDLen       = 12

	defaultTransition = 2 * time.Second
	maxName           = 256
	attemptTimeout    = 15 * time.Second
	maxKeptDeliveries = 2000
)

// FunctionInvoker invokes an OCI Function by OCID with the message as payload.
type FunctionInvoker func(ctx context.Context, functionID string, header http.Header, body []byte) error

// InMemoryBackend holds every topic, subscription and delivery under one lock.
type InMemoryBackend struct {
	mu         *lockmetrics.RWMutex
	now        func() time.Time
	after      func(time.Duration) <-chan time.Time
	kick       chan struct{}
	client     *http.Client
	invoker    FunctionInvoker
	topics     map[string]*Topic
	subs       map[string]*Subscription
	deliveries map[string]*Delivery
	Auth       *ociauth.Registry
	transition time.Duration
	seq        int64
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		mu: lockmetrics.New("ocinotifications"), now: time.Now, after: time.After, transition: defaultTransition,
		kick: make(chan struct{}, 1), client: httpclient.New(attemptTimeout),
	}
	b.resetMaps()
	b.Auth = ociauth.NewRegistry()

	return b
}

func (b *InMemoryBackend) resetMaps() {
	b.topics, b.subs, b.deliveries = map[string]*Topic{}, map[string]*Subscription{}, map[string]*Delivery{}
}

// SetClock injects the time source used for lifecycle transitions and delivery retries.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	b.now = now
	b.mu.Unlock()
}

// SetTimer injects the wait the dispatcher uses until the next retry; it must return a channel firing after d.
func (b *InMemoryBackend) SetTimer(after func(time.Duration) <-chan time.Time) {
	b.mu.Lock("SetTimer")
	b.after = after
	b.mu.Unlock()
}

// SetTransition sets how long CREATING and DELETING topic states last.
func (b *InMemoryBackend) SetTransition(d time.Duration) {
	b.mu.Lock("SetTransition")
	b.transition = d
	b.mu.Unlock()
}

// SetFunctionInvoker wires ORACLE_FUNCTIONS subscriptions to the OCI Functions backend.
func (b *InMemoryBackend) SetFunctionInvoker(fn FunctionInvoker) {
	b.mu.Lock("SetFunctionInvoker")
	b.invoker = fn
	b.mu.Unlock()
}

// Kick wakes the dispatcher so it re-evaluates which deliveries are due.
func (b *InMemoryBackend) Kick() {
	select {
	case b.kick <- struct{}{}:
	default:
	}
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

// ListOpts carries the shared list query parameters.
type ListOpts struct {
	SortBy string
	Order  string
	Page   string
	Limit  int
}

func paginate[T any](items []T, o ListOpts, state func(T) string, created func(T) (time.Time, int64)) page.Page[T] {
	byState := strings.EqualFold(o.SortBy, "LIFECYCLESTATE")
	desc := !byState
	switch strings.ToUpper(o.Order) {
	case "ASC":
		desc = false
	case "DESC":
		desc = true
	}

	slices.SortStableFunc(items, func(a, c T) int {
		ta, sa := created(a)
		tc, sc := created(c)
		r := cmp.Or(ta.Compare(tc), cmp.Compare(sa, sc))

		if byState {
			r = cmp.Or(cmp.Compare(state(a), state(c)), r)
		}

		if desc {
			return -r
		}

		return r
	})

	return page.New(items, o.Page, o.Limit, defaultListLimit)
}

// Sweep applies topic lifecycle transitions against the backend clock; deleted topics take their subscriptions along.
func (b *InMemoryBackend) Sweep() {
	b.mu.Lock("Sweep")
	defer b.mu.Unlock()

	b.sweepLocked()
}

func (b *InMemoryBackend) sweepLocked() {
	now := b.nowUTC()

	for id, t := range b.topics {
		if t.TransitionAt == nil || t.TransitionAt.After(now) {
			continue
		}

		t.TransitionAt = nil

		switch t.State {
		case stateCreating:
			t.State = stateActive
		case stateDeleting:
			delete(b.topics, id)
			b.dropTopicChildren(id)
		}
	}
}

func (b *InMemoryBackend) dropTopicChildren(topicID string) {
	for id, s := range b.subs {
		if s.TopicID == topicID {
			delete(b.subs, id)
		}
	}

	for id, d := range b.deliveries {
		if d.TopicID == topicID {
			delete(b.deliveries, id)
		}
	}
}
