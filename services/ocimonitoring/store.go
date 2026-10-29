package ocimonitoring

import (
	"cmp"
	"context"
	"slices"
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
	maxName          = 255
	metricRetention  = 90 * 24 * time.Hour
	maxHistory       = 1000
)

// Publisher delivers an alarm notification to a Notifications topic.
type Publisher func(ctx context.Context, topicID, title, body string, asJSON bool) error

// TopicChecker reports whether a Notifications topic exists.
type TopicChecker func(topicID string) bool

// InMemoryBackend holds every metric stream and alarm under one lock.
type InMemoryBackend struct {
	mu           *lockmetrics.RWMutex
	now          func() time.Time
	streams      map[string]*Stream
	alarms       map[string]*Alarm
	Auth         *ociauth.Registry
	publisher    Publisher
	topicChecker TopicChecker
	seq          int64
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{mu: lockmetrics.New("ocimonitoring"), now: time.Now}
	b.resetMaps()
	b.Auth = ociauth.NewRegistry()

	return b
}

func (b *InMemoryBackend) resetMaps() {
	b.streams, b.alarms = map[string]*Stream{}, map[string]*Alarm{}
}

// SetClock injects the time source used for datapoint validation, evaluation and retention.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	b.now = now
	b.mu.Unlock()
}

// SetPublisher wires alarm notification delivery to a Notifications backend.
func (b *InMemoryBackend) SetPublisher(p Publisher) {
	b.mu.Lock("SetPublisher")
	b.publisher = p
	b.mu.Unlock()
}

// SetTopicChecker wires validation of alarm destinations against a Notifications backend.
func (b *InMemoryBackend) SetTopicChecker(c TopicChecker) {
	b.mu.Lock("SetTopicChecker")
	b.topicChecker = c
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

// ListOpts carries the shared list query parameters.
type ListOpts struct {
	SortBy string
	Order  string
	Page   string
	Limit  int
}

func paginate[T any](items []T, o ListOpts, less func(a, c T, by string) int, defaultBy string) page.Page[T] {
	by := o.SortBy
	if by == "" {
		by = defaultBy
	}

	desc := strs.Equal(o.Order, "DESC")

	slices.SortStableFunc(items, func(a, c T) int {
		r := less(a, c, by)
		if desc {
			return -r
		}

		return r
	})

	return page.New(items, o.Page, o.Limit, defaultListLimit)
}

func firstErr(errs ...*APIError) *APIError {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}

	return nil
}

func compareStr(a, c string) int { return cmp.Compare(a, c) }

// Sweep drops datapoints past retention, then evaluates every alarm and delivers the notifications it raised.
func (b *InMemoryBackend) Sweep(ctx context.Context) {
	b.mu.Lock("Sweep")
	now := b.nowUTC()
	b.prune(now)
	out := b.evaluateAll(now)
	pub := b.publisher
	b.mu.Unlock()

	deliver(ctx, pub, out)
}

func (b *InMemoryBackend) prune(now time.Time) {
	cutoff := now.Add(-metricRetention)

	for k, s := range b.streams {
		s.Points = slices.DeleteFunc(s.Points, func(p Point) bool { return p.T.Before(cutoff) })
		if len(s.Points) == 0 {
			delete(b.streams, k)
		}
	}
}
