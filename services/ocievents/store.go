package ocievents

import (
	"cmp"
	"context"
	"slices"
	"strings"
	"sync"
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
	maxActions       = 10
	maxRules         = 50
	maxDisplayName   = 255
	maxDeliveries    = 1000
)

// Notifier publishes an event message to an ONS topic; wired to OCI Notifications in internal/app.
type Notifier interface {
	TopicActive(id string) bool
	PublishEvent(topicID, body string) error
}

// Streamer appends an event message to a stream; wired to OCI Streaming in internal/app.
type Streamer interface {
	StreamActive(id string) bool
	AppendMessage(streamID string, key, value []byte) error
}

// FunctionInvoker invokes a function with an event as its payload; wired to OCI Functions in internal/app.
type FunctionInvoker interface {
	FunctionActive(id string) bool
	InvokeFunction(ctx context.Context, functionID string, body []byte) error
}

// InMemoryBackend holds every rule under one lock.
type InMemoryBackend struct {
	notifier    Notifier
	invoker     FunctionInvoker
	streamer    Streamer
	within      func(id, ancestor string) bool
	Auth        *ociauth.Registry
	compartment func(string) bool
	mu          *lockmetrics.RWMutex
	rules       map[string]*Rule
	now         func() time.Time
	spawn       func(func())
	deliveries  []*Delivery
	wg          sync.WaitGroup
	seq         int64
}

// NewInMemoryBackend creates an empty backend that delivers events on goroutines tracked by Wait.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{mu: lockmetrics.New("ocievents"), now: time.Now, Auth: ociauth.NewRegistry()}
	b.rules = map[string]*Rule{}
	b.spawn = func(fn func()) { b.wg.Go(fn) }

	return b
}

// Wait blocks until every in-flight delivery has finished.
func (b *InMemoryBackend) Wait() { b.wg.Wait() }

// SetClock injects the time source for timestamps and delivery records.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	b.now = now
	b.mu.Unlock()
}

// SetDispatcher replaces how deliveries are run; tests pass a function that runs them inline.
func (b *InMemoryBackend) SetDispatcher(fn func(func())) {
	b.mu.Lock("SetDispatcher")
	b.spawn = fn
	b.mu.Unlock()
}

// SetCompartmentChecker validates compartmentId arguments against another service (OCI Identity).
func (b *InMemoryBackend) SetCompartmentChecker(fn func(string) bool) {
	b.mu.Lock("SetCompartmentChecker")
	b.compartment = fn
	b.mu.Unlock()
}

// SetCompartmentTree tells the backend whether a compartment is nested below another (OCI Identity).
func (b *InMemoryBackend) SetCompartmentTree(fn func(id, ancestor string) bool) {
	b.mu.Lock("SetCompartmentTree")
	b.within = fn
	b.mu.Unlock()
}

// SetNotifier wires the ONS action target.
func (b *InMemoryBackend) SetNotifier(n Notifier) {
	b.mu.Lock("SetNotifier")
	b.notifier = n
	b.mu.Unlock()
}

// SetStreamer wires the OSS action target.
func (b *InMemoryBackend) SetStreamer(s Streamer) {
	b.mu.Lock("SetStreamer")
	b.streamer = s
	b.mu.Unlock()
}

// SetFunctionInvoker wires the FAAS action target.
func (b *InMemoryBackend) SetFunctionInvoker(f FunctionInvoker) {
	b.mu.Lock("SetFunctionInvoker")
	b.invoker = f
	b.mu.Unlock()
}

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	b.rules, b.deliveries = map[string]*Rule{}, nil
	b.mu.Unlock()
	b.Auth.Reset()
}

func (b *InMemoryBackend) nowUTC() time.Time { return b.now().UTC() }

func newOCID(kind string) string {
	return "ocid1." + kind + ".oc1." + Region + "." + awsid.NewN("", ocidTailLen)
}

func newEtag() string { return uuid.NewString() }

func checkEtag(current, ifMatch string) *APIError {
	if ifMatch != "" && ifMatch != current {
		return errEtag()
	}

	return nil
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

func (b *InMemoryBackend) inScope(eventCompartment, ruleCompartment string) bool {
	if eventCompartment == ruleCompartment {
		return true
	}

	return b.within != nil && b.within(eventCompartment, ruleCompartment)
}

// ListOpts carries the shared list query parameters.
type ListOpts struct {
	SortBy string
	Order  string
	Page   string
	Limit  int
}

func paginate(items []*Rule, o ListOpts) page.Page[*Rule] {
	desc := !strings.EqualFold(o.SortBy, "DISPLAY_NAME") && !strings.EqualFold(o.SortBy, "ID")
	if strings.EqualFold(o.Order, "ASC") {
		desc = false
	} else if strings.EqualFold(o.Order, "DESC") {
		desc = true
	}

	slices.SortStableFunc(items, func(a, c *Rule) int {
		var r int

		switch strings.ToUpper(o.SortBy) {
		case "DISPLAY_NAME":
			r = cmp.Compare(strs.Fold(a.DisplayName), strs.Fold(c.DisplayName))
		case "ID":
			r = cmp.Compare(a.ID, c.ID)
		default:
			r = cmp.Or(a.TimeCreated.Compare(c.TimeCreated), cmp.Compare(a.Seq, c.Seq))
		}

		if desc {
			return -r
		}

		return r
	})

	return page.New(items, o.Page, o.Limit, defaultListLimit)
}
