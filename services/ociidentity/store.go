package ociidentity

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
	// Region is the home region of the emulated tenancy.
	Region = "us-ashburn-1"
	// TenancyID is the OCID of the emulated tenancy; it is also the root compartment id.
	TenancyID = "ocid1.tenancy.oc1..aaaaaaaagopherstackemulatedtenancy000000000000000000000000"

	tenancyName      = "gopherstack"
	defaultListLimit = 100
	ocidTailLen      = 52
	transitionWindow = 2 * time.Second
	recoverWindow    = 90 * 24 * time.Hour
)

// InMemoryBackend holds all IAM state under one lock.
type InMemoryBackend struct {
	mu          *lockmetrics.RWMutex
	now         func() time.Time
	created     time.Time
	compartment map[string]*Compartment
	users       map[string]*User
	groups      map[string]*Group
	members     map[string]*Membership
	dynGroups   map[string]*DynamicGroup
	policies    map[string]*Policy
	tagNS       map[string]*TagNamespace
	tagDefs     map[string]*TagDef
	tagDefaults map[string]*TagDefault
	workReqs    map[string]*WorkRequest
	// Auth is the API-key registry shared with the other OCI services.
	Auth *ociauth.Registry
	seq  int64
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{mu: lockmetrics.New("ociidentity"), now: time.Now, Auth: ociauth.NewRegistry()}
	b.initMaps()
	b.created = b.nowUTC()

	return b
}

func (b *InMemoryBackend) initMaps() {
	b.compartment = map[string]*Compartment{}
	b.users = map[string]*User{}
	b.groups = map[string]*Group{}
	b.members = map[string]*Membership{}
	b.dynGroups = map[string]*DynamicGroup{}
	b.policies = map[string]*Policy{}
	b.tagNS = map[string]*TagNamespace{}
	b.tagDefs = map[string]*TagDef{}
	b.tagDefaults = map[string]*TagDefault{}
	b.workReqs = map[string]*WorkRequest{}
}

// SetClock injects the time source used for lifecycle transitions.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	b.now = now
	b.mu.Unlock()
}

// Reset clears all state and the registered API keys.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	b.initMaps()
	b.created = b.nowUTC()
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
	return "ocid1." + kind + ".oc1.." + awsid.NewN("", ocidTailLen)
}

func nonNilTags(t Tags) Tags {
	if t.FreeformTags == nil {
		t.FreeformTags = map[string]string{}
	}

	if t.DefinedTags == nil {
		t.DefinedTags = map[string]map[string]any{}
	}

	return t
}

func checkEtag(current, ifMatch string) *APIError {
	if ifMatch != "" && ifMatch != current {
		return errEtag()
	}

	return nil
}

func (b *InMemoryBackend) newMeta(kind, compartment, name, desc string, tags Tags) Meta {
	return Meta{
		ID: newOCID(kind), CompartmentID: compartment, Name: name, Description: desc,
		TimeCreated: b.nowUTC(), LifecycleState: stateActive, Tags: nonNilTags(tags),
	}
}

func (b *InMemoryBackend) newInternal() Internal { return Internal{Etag: newEtag(), Seq: b.nextSeq()} }

// Sweep applies due lifecycle transitions.
func (b *InMemoryBackend) Sweep() {
	b.mu.Lock("Sweep")
	defer b.mu.Unlock()

	now := b.nowUTC()

	for _, c := range b.compartment {
		if c.LifecycleState == stateDeleting && c.Internal.TransitionAt != nil && !c.Internal.TransitionAt.After(now) {
			deletedAt := *c.Internal.TransitionAt
			c.LifecycleState, c.Internal.TransitionAt, c.Internal.TimeDeleted = stateDeleted, nil, &deletedAt
		}
	}

	for _, d := range b.tagDefs {
		if d.LifecycleState == stateDeleting && d.Internal.TransitionAt != nil && !d.Internal.TransitionAt.After(now) {
			d.LifecycleState, d.Internal.TransitionAt = stateDeleted, nil
		}
	}
}

// ListOpts carries the shared list query parameters.
type ListOpts struct {
	SortBy string
	Order  string
	Page   string
	Limit  int
}

func paginate[T any](items []T, o ListOpts, byName func(T) string, byTime func(T) (time.Time, int64)) page.Page[T] {
	byNameSort := strings.EqualFold(o.SortBy, "NAME")
	desc := strings.EqualFold(o.Order, "DESC")

	slices.SortStableFunc(items, func(a, c T) int {
		var r int

		if byNameSort {
			r = cmp.Compare(strs.Fold(byName(a)), strs.Fold(byName(c)))
		} else {
			ta, sa := byTime(a)
			tc, sc := byTime(c)
			r = cmp.Or(ta.Compare(tc), cmp.Compare(sa, sc))
		}

		if desc {
			return -r
		}

		return r
	})

	return page.New(items, o.Page, o.Limit, defaultListLimit)
}

func validListOpts(o ListOpts, sortKeys ...string) *APIError {
	if o.SortBy != "" && !slices.ContainsFunc(sortKeys, func(k string) bool { return strings.EqualFold(k, o.SortBy) }) {
		return errParam("sortBy must be one of " + strings.Join(sortKeys, ", "))
	}

	if o.Order != "" && !strings.EqualFold(o.Order, "ASC") && !strings.EqualFold(o.Order, "DESC") {
		return errParam("sortOrder must be ASC or DESC")
	}

	return nil
}

func validState(state string, allowed ...string) *APIError {
	if state != "" && !slices.ContainsFunc(allowed, func(a string) bool { return strings.EqualFold(a, state) }) {
		return errParam("lifecycleState must be one of " + strings.Join(allowed, ", "))
	}

	return nil
}

func matchState(have, want string) bool { return want == "" || strings.EqualFold(have, want) }

type listAccess[T any] struct {
	fields func(*T) (name, state string, created time.Time, seq int64)
}

// listNamed filters a name-keyed resource map by name and state, sorts and pages it.
func listNamed[T, W any](m map[string]*T, f NameFilter, o ListOpts, acc listAccess[T], view func(*T) W) page.Page[W] {
	var all []*T

	for _, v := range m {
		name, state, _, _ := acc.fields(v)
		if (f.Name == "" || name == f.Name) && matchState(state, f.State) {
			all = append(all, v)
		}
	}

	pg := paginate(all, o, func(v *T) string { return nameOf(acc, v) },
		func(v *T) (time.Time, int64) { return stampOf(acc, v) })

	out := make([]W, 0, len(pg.Data))
	for _, v := range pg.Data {
		out = append(out, view(v))
	}

	return page.Page[W]{Data: out, Next: pg.Next}
}

func nameOf[T any](acc listAccess[T], v *T) string {
	name, _, _, _ := acc.fields(v)

	return name
}

func stampOf[T any](acc listAccess[T], v *T) (time.Time, int64) {
	_, _, created, seq := acc.fields(v)

	return created, seq
}
