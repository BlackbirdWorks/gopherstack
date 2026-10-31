package ocicontainerinstances

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
	Region           = "iad"
	defaultListLimit = 100
	ocidTailLen      = 52
	defaultTransit   = 2 * time.Second
	wrInProgress     = "IN_PROGRESS"
	wrSucceeded      = "SUCCEEDED"
	wrFailed         = "FAILED"
)

// SecretReader returns the decoded content of a Vault secret.
type SecretReader func(secretID string) (string, error)

// InMemoryBackend holds every container instance and work request under one lock.
type InMemoryBackend struct {
	mu          *lockmetrics.RWMutex
	now         func() time.Time
	instances   map[string]*Instance
	containers  map[string]string
	workReqs    map[string]*WorkRequest
	Auth        *ociauth.Registry
	compartment func(string) bool
	subnet      func(string) bool
	secrets     SecretReader
	engine      *Engine
	transition  time.Duration
	seq         int64
}

// NewInMemoryBackend creates an empty backend running in state-only mode.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		mu: lockmetrics.New("ocicontainerinstances"), now: time.Now, transition: defaultTransit,
		Auth: ociauth.NewRegistry(),
	}
	b.resetMaps()

	return b
}

func (b *InMemoryBackend) resetMaps() {
	b.instances, b.containers, b.workReqs = map[string]*Instance{}, map[string]string{}, map[string]*WorkRequest{}
}

// SetClock injects the time source used for lifecycle transitions.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	b.now = now
	b.mu.Unlock()
}

// SetTransition sets how long asynchronous state-only operations take to complete.
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

// SetSubnetChecker validates VNIC subnetIds against another service (OCI Core).
func (b *InMemoryBackend) SetSubnetChecker(fn func(string) bool) {
	b.mu.Lock("SetSubnetChecker")
	b.subnet = fn
	b.mu.Unlock()
}

// SetSecretReader resolves VAULT image pull secrets through OCI Vault.
func (b *InMemoryBackend) SetSecretReader(fn SecretReader) {
	b.mu.Lock("SetSecretReader")
	b.secrets = fn
	b.mu.Unlock()
}

// SetEngine switches the backend to docker mode.
func (b *InMemoryBackend) SetEngine(e *Engine) {
	b.mu.Lock("SetEngine")
	b.engine = e
	b.mu.Unlock()
	e.sink = b
}

// Reset clears all state and stops every running container.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	e := b.engine
	b.resetMaps()
	b.mu.Unlock()
	b.Auth.Reset()

	if e != nil {
		e.HaltAll()
	}
}

// Close stops every running container.
func (b *InMemoryBackend) Close() {
	b.mu.RLock("Close")
	e := b.engine
	b.mu.RUnlock()

	if e != nil {
		e.Close()
	}
}

func (b *InMemoryBackend) nowUTC() time.Time { return b.now().UTC() }

// Now returns the backend clock (Sink).
func (b *InMemoryBackend) Now() time.Time {
	b.mu.RLock("Now")
	defer b.mu.RUnlock()

	return b.nowUTC()
}

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

// Sweep applies due time-based transitions against the backend clock.
func (b *InMemoryBackend) Sweep() {
	b.mu.Lock("Sweep")
	defer b.mu.Unlock()

	now := b.nowUTC()

	for _, in := range b.instances {
		if in.TransitionAt == nil || in.TransitionAt.After(now) {
			continue
		}

		b.setState(in, in.Target, "")
		in.Target, in.TransitionAt, in.Etag = "", nil, newEtag()
	}

	for _, wr := range b.workReqs {
		if wr.Status == wrInProgress && wr.FinishAt != nil && !wr.FinishAt.After(now) {
			wr.Status, wr.FinishedAt = wrSucceeded, wr.FinishAt
		}
	}
}

// setState moves the instance and, unless the engine drives them, its containers. Callers hold the write lock.
func (b *InMemoryBackend) setState(in *Instance, state, details string) {
	in.State, in.Details = state, details
	now := b.nowUTC()

	for _, c := range in.Containers {
		if b.engine != nil && state == stateActive {
			continue
		}

		if c.State == stateInactive && state == stateInactive {
			continue
		}

		c.State, c.Details, c.Updated = state, details, &now
	}
}

// Mutate runs fn on the instance under the write lock (Sink).
func (b *InMemoryBackend) Mutate(id string, fn func(*Instance)) {
	b.mu.Lock("Mutate")
	defer b.mu.Unlock()

	if in, ok := b.instances[id]; ok {
		fn(in)
	}
}

// newWR records a work request. Callers hold the write lock.
func (b *InMemoryBackend) newWR(in *Instance, typ, action string, finish *time.Time) *WorkRequest {
	wr := &WorkRequest{
		ID: newOCID("computecontainerinstanceworkrequest"), Type: typ, Compartment: in.Compartment, ResourceID: in.ID,
		ResourceType: "containerInstance", Action: action, Accepted: b.nowUTC(), Status: wrInProgress,
		FinishAt: finish, Seq: b.nextSeq(),
	}
	b.workReqs[wr.ID] = wr

	return wr
}

func (b *InMemoryBackend) finishAt() *time.Time {
	t := b.nowUTC().Add(b.transition)

	return &t
}
