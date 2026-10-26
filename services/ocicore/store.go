package ocicore

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
	// RegionKey is the three letter key of the emulated region.
	RegionKey = "iad"

	transitionWindow = 2 * time.Second
	retention        = time.Hour
	defaultListLimit = 100
	ocidTailLen      = 52
	maxNameLen       = 255
)

// InMemoryBackend holds all Core networking state under one lock.
type InMemoryBackend struct {
	mu          *lockmetrics.RWMutex
	now         func() time.Time
	res         map[string]resource
	nsgRules    map[string]*NSGRule
	compartment func(string) bool
	// Auth is the API-key registry shared with the other OCI services.
	Auth    *ociauth.Registry
	seq     int64
	ipSeq   int64
	ipv6Seq int64
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{mu: lockmetrics.New("ocicore"), now: time.Now, Auth: ociauth.NewRegistry()}
	b.initMaps()

	return b
}

func (b *InMemoryBackend) initMaps() {
	b.res = map[string]resource{}
	b.nsgRules = map[string]*NSGRule{}
}

// SetClock injects the time source used for lifecycle transitions.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	b.now = now
	b.mu.Unlock()
}

// SetCompartmentChecker makes compartmentId arguments validated against another service (OCI Identity).
func (b *InMemoryBackend) SetCompartmentChecker(fn func(string) bool) {
	b.mu.Lock("SetCompartmentChecker")
	b.compartment = fn
	b.mu.Unlock()
}

// Reset clears all state and the registered API keys.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	b.initMaps()
	b.seq, b.ipSeq, b.ipv6Seq = 0, 0, 0
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
	return "ocid1." + kind + ".oc1." + RegionKey + "." + awsid.NewN("", ocidTailLen)
}

func ocidKind(id string) string {
	parts := strings.Split(id, ".")
	if len(parts) < 2 || parts[0] != "ocid1" {
		return ""
	}

	return parts[1]
}

func live(r resource) bool {
	switch r.base().LifecycleState {
	case statTerminating, statTerminated, statDetaching, statDetached, statDeleted:
		return false
	}

	return true
}

func terminal(state string) bool {
	return state == statTerminated || state == statDetached || state == statDeleted
}

// moveTo enters an interim state that settles into final after the transition window.
func (b *InMemoryBackend) moveTo(r resource, interim, final string) {
	bs := r.base()
	at := b.nowUTC().Add(transitionWindow)
	bs.LifecycleState, bs.Internal.TransitionAt, bs.Internal.NextState = interim, &at, final
	b.bump(r)
}

// TagsIn carries the tags accepted by create and update calls.
type TagsIn struct {
	FreeformTags map[string]string         `json:"freeformTags"`
	DefinedTags  map[string]map[string]any `json:"definedTags"`
}

func (b *InMemoryBackend) newBase(kind, compartment, name string, tags TagsIn) Base {
	now := b.nowUTC()
	if name == "" {
		name = kind + now.Format("20060102150405")
	}

	if tags.FreeformTags == nil {
		tags.FreeformTags = map[string]string{}
	}

	if tags.DefinedTags == nil {
		tags.DefinedTags = map[string]map[string]any{}
	}

	at := now.Add(transitionWindow)

	return Base{
		ID: newOCID(kind), CompartmentID: compartment, DisplayName: name, LifecycleState: statProvisioning,
		TimeCreated: now, FreeformTags: tags.FreeformTags, DefinedTags: tags.DefinedTags,
		Internal: &Internal{Etag: newEtag(), Seq: b.nextSeq(), TransitionAt: &at},
	}
}

func (b *InMemoryBackend) add(r resource) { b.res[r.base().ID] = r }

func validName(name string) *APIError {
	if len(name) > maxNameLen {
		return errParam("displayName must be at most 255 characters")
	}

	return nil
}

// checkCompartment verifies a compartment argument; callers hold the lock.
func (b *InMemoryBackend) checkCompartment(id string) *APIError {
	if id == "" {
		return errMissing("compartmentId is required")
	}

	if b.compartment != nil && !b.compartment(id) {
		return errNotFound(id)
	}

	return nil
}

func get[T resource](b *InMemoryBackend, id string) (T, *APIError) {
	var zero T

	r, ok := b.res[id].(T)
	if !ok || r.base().LifecycleState == "" {
		return zero, errNotFound(id)
	}

	return r, nil
}

// getLive is get for a resource that must still accept changes.
func getLive[T resource](b *InMemoryBackend, id string) (T, *APIError) {
	r, aerr := get[T](b, id)
	if aerr != nil {
		return r, aerr
	}

	if !live(r) {
		return r, errState("The resource " + id + " is " + r.base().LifecycleState)
	}

	return r, nil
}

func checkEtag(r resource, ifMatch string) *APIError {
	if ifMatch != "" && ifMatch != r.base().Internal.Etag {
		return errEtag()
	}

	return nil
}

func (b *InMemoryBackend) bump(r resource) { r.base().Internal.Etag = newEtag() }

// UpdateIn is the part of every update call shared by all resources.
type UpdateIn struct {
	DisplayName *string `json:"displayName"`
	TagsIn
}

// updateCommon applies name and tags then bumps the etag; callers hold the lock.
func (b *InMemoryBackend) updateCommon(r resource, in UpdateIn) {
	bs := r.base()
	if in.DisplayName != nil {
		bs.DisplayName = *in.DisplayName
	}

	if in.FreeformTags != nil {
		bs.FreeformTags = in.FreeformTags
	}

	if in.DefinedTags != nil {
		bs.DefinedTags = in.DefinedTags
	}

	b.bump(r)
}

// Sweep applies due lifecycle transitions and purges long-terminated resources.
func (b *InMemoryBackend) Sweep() {
	b.mu.Lock("Sweep")
	defer b.mu.Unlock()

	now := b.nowUTC()

	for id, r := range b.res {
		bs := r.base()
		in := bs.Internal

		if in.TransitionAt != nil && !in.TransitionAt.After(now) {
			at := *in.TransitionAt
			in.TransitionAt = nil

			switch {
			case in.NextState != "":
				bs.LifecycleState, in.NextState = in.NextState, ""
				if terminal(bs.LifecycleState) {
					in.TimeDeleted = &at
				}
			default:
				b.settle(r, at)
			}
		}

		if in.TimeDeleted != nil && now.Sub(*in.TimeDeleted) > retention {
			delete(b.res, id)
			b.dropRules(id)
		}
	}
}

func (b *InMemoryBackend) settle(r resource, at time.Time) {
	bs := r.base()

	switch bs.LifecycleState {
	case statProvisioning:
		bs.LifecycleState = statAvailable
		if p, ok := r.(*PublicIP); ok && p.AssignedEntityID != "" {
			p.LifecycleState = statAssigned
		}
	case statTerminating:
		bs.LifecycleState, bs.Internal.TimeDeleted = statTerminated, &at
	}
}

func (b *InMemoryBackend) dropRules(nsgID string) {
	for id, r := range b.nsgRules {
		if r.NsgID == nsgID {
			delete(b.nsgRules, id)
		}
	}
}

// ListOpts carries the shared list query parameters.
type ListOpts struct {
	Match       func(resource) bool
	Compartment string
	DisplayName string
	State       string
	VcnID       string
	SortBy      string
	Order       string
	Page        string
	Limit       int
}

func validListOpts(o ListOpts) *APIError {
	if o.SortBy != "" && !strings.EqualFold(o.SortBy, "TIMECREATED") && !strings.EqualFold(o.SortBy, "DISPLAYNAME") {
		return errParam("sortBy must be one of TIMECREATED, DISPLAYNAME")
	}

	if o.Order != "" && !strings.EqualFold(o.Order, "ASC") && !strings.EqualFold(o.Order, "DESC") {
		return errParam("sortOrder must be ASC or DESC")
	}

	return nil
}

func sortRes[T resource](items []T, o ListOpts) {
	byName := strings.EqualFold(o.SortBy, "DISPLAYNAME")
	desc := !byName
	if o.Order != "" {
		desc = strings.EqualFold(o.Order, "DESC")
	}

	slices.SortStableFunc(items, func(a, c T) int {
		ab, cb := a.base(), c.base()

		var r int
		if byName {
			r = cmp.Compare(strs.Fold(ab.DisplayName), strs.Fold(cb.DisplayName))
		} else {
			r = cmp.Or(ab.TimeCreated.Compare(cb.TimeCreated), cmp.Compare(ab.Internal.Seq, cb.Internal.Seq))
		}

		if desc {
			return -r
		}

		return r
	})
}

// list returns the resources of one kind in a compartment, filtered, sorted and paged.
func list[T resource](b *InMemoryBackend, o ListOpts) (page.Page[resource], *APIError) {
	if aerr := validListOpts(o); aerr != nil {
		return page.Page[resource]{}, aerr
	}

	b.mu.RLock("list")
	defer b.mu.RUnlock()

	if aerr := b.checkCompartment(o.Compartment); aerr != nil {
		return page.Page[resource]{}, aerr
	}

	var all []T

	for _, r := range b.res {
		t, ok := r.(T)
		if !ok {
			continue
		}

		bs := t.base()
		if bs.CompartmentID == o.Compartment && (o.DisplayName == "" || bs.DisplayName == o.DisplayName) &&
			(o.State == "" || strings.EqualFold(bs.LifecycleState, o.State)) &&
			(o.VcnID == "" || t.vcnRef() == o.VcnID) && (o.Match == nil || o.Match(t)) {
			all = append(all, t)
		}
	}

	sortRes(all, o)

	pg := page.New(all, o.Page, o.Limit, defaultListLimit)
	out := make([]resource, 0, len(pg.Data))

	for _, t := range pg.Data {
		out = append(out, t.view())
	}

	return page.Page[resource]{Data: out, Next: pg.Next}, nil
}

// getView returns one resource and its etag.
func getView[T resource](b *InMemoryBackend, id string) (resource, string, *APIError) {
	b.mu.RLock("get")
	defer b.mu.RUnlock()

	r, aerr := get[T](b, id)
	if aerr != nil {
		return nil, "", aerr
	}

	return r.view(), r.base().Internal.Etag, nil
}

// changeCompartment moves a resource to another compartment.
func changeCompartment[T resource](b *InMemoryBackend, id, compartment, ifMatch string) *APIError {
	b.mu.Lock("changeCompartment")
	defer b.mu.Unlock()

	r, aerr := getLive[T](b, id)
	if aerr != nil {
		return aerr
	}

	if aerr = b.checkCompartment(compartment); aerr != nil {
		return aerr
	}

	if aerr = checkEtag(r, ifMatch); aerr != nil {
		return aerr
	}

	r.base().CompartmentID = compartment
	b.bump(r)

	return nil
}

// deleteRes starts termination of a resource unless something still depends on it.
func deleteRes[T resource](b *InMemoryBackend, id, ifMatch string) *APIError {
	b.mu.Lock("delete")
	defer b.mu.Unlock()

	r, aerr := get[T](b, id)
	if aerr != nil {
		return aerr
	}

	switch r.base().LifecycleState {
	case statTerminated:
		return errNotFound(id)
	case statTerminating:
		return nil
	}

	if aerr = checkEtag(r, ifMatch); aerr != nil {
		return aerr
	}

	if aerr = b.deleteCheck(r); aerr != nil {
		return aerr
	}

	b.terminate(r)

	return nil
}

func (b *InMemoryBackend) markTerminating(r resource) {
	bs := r.base()
	at := b.nowUTC().Add(transitionWindow)
	bs.LifecycleState, bs.Internal.TransitionAt = statTerminating, &at
	b.bump(r)
}
