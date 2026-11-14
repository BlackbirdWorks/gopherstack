package ocidns

import (
	"cmp"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
	"github.com/blackbirdworks/gopherstack/pkgs/ociretry"
	"github.com/blackbirdworks/gopherstack/pkgs/ocitags"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	keyName        = "name"
	keyDisplayName = "displayName"
	keyTimeCreated = "timeCreated"
	keyZoneType    = "zoneType"

	// Version is the OCI DNS API version served.
	Version = "20180115"

	defaultListLimit = 100
	maxListLimit     = 1000
	ocidTailLen      = 52
)

// VcnInfo is a VCN a private resolver is attached to.
type VcnInfo struct {
	Created       time.Time
	ID            string
	CompartmentID string
	DisplayName   string
}

// SubnetInfo is the part of a subnet endpoints are validated against.
type SubnetInfo struct {
	VcnID string
	Cidr  string
}

// InMemoryBackend holds every resource under one lock.
type InMemoryBackend struct {
	Auth        *ociauth.Registry
	mu          *lockmetrics.RWMutex
	now         func() time.Time
	compartment func(string) bool
	vcns        func() []VcnInfo
	subnet      func(string) (SubnetInfo, bool)
	zones       map[string]*Zone
	views       map[string]*View
	resolvers   map[string]*Resolver
	endpoints   map[string]*Endpoint
	tsigs       map[string]*TsigKey
	policies    map[string]*SteeringPolicy
	attachments map[string]*Attachment
	retry       *ociretry.Store
	events      chan xferEvent
	attempts    map[string]time.Time
	tagLookup   ocitags.Lookup
	selfBase    string
	seq         int64
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		mu: lockmetrics.New(
			"ocidns",
		),
		now:    time.Now,
		Auth:   ociauth.NewRegistry(),
		events: make(chan xferEvent, eventQueue),
	}
	b.initMaps()

	return b
}

func (b *InMemoryBackend) initMaps() {
	b.zones = map[string]*Zone{}
	b.views = map[string]*View{}
	b.resolvers = map[string]*Resolver{}
	b.endpoints = map[string]*Endpoint{}
	b.tsigs = map[string]*TsigKey{}
	b.policies = map[string]*SteeringPolicy{}
	b.attachments = map[string]*Attachment{}
	b.retry = ociretry.New()
	b.attempts = map[string]time.Time{}
}

// SetTagLookup wires defined-tag validation against a tag namespace source (OCI Identity); nil accepts any namespace.
func (b *InMemoryBackend) SetTagLookup(fn ocitags.Lookup) {
	b.mu.Lock("SetTagLookup")
	b.tagLookup = fn
	b.mu.Unlock()
}

func (b *InMemoryBackend) checkTags(t Tags) *APIError {
	if err := ocitags.Validate(t.FreeformTags, t.DefinedTags, b.tagLookup); err != nil {
		return newErr(err.Status, err.Code, err.Message)
	}

	return nil
}

const headerRetry = "Opc-Retry-Token"

// SetClock injects the time source for timestamps.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	b.now = now
	b.mu.Unlock()
}

// SetCompartmentChecker validates compartmentId arguments against another service (OCI Identity).
func (b *InMemoryBackend) SetCompartmentChecker(fn func(string) bool) {
	b.mu.Lock("SetCompartmentChecker")
	b.compartment = fn
	b.mu.Unlock()
}

// SetVcnSource lists the VCNs that own a private resolver (OCI Core).
func (b *InMemoryBackend) SetVcnSource(fn func() []VcnInfo) {
	b.mu.Lock("SetVcnSource")
	b.vcns = fn
	b.mu.Unlock()
}

// SetSubnetResolver looks up subnets for resolver endpoints (OCI Core).
func (b *InMemoryBackend) SetSubnetResolver(fn func(string) (SubnetInfo, bool)) {
	b.mu.Lock("SetSubnetResolver")
	b.subnet = fn
	b.mu.Unlock()
}

// SetSelfBase sets the base URL rendered in the `self` field of resources.
func (b *InMemoryBackend) SetSelfBase(base string) {
	b.mu.Lock("SetSelfBase")
	b.selfBase = base
	b.mu.Unlock()
}

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	b.initMaps()
	b.seq = 0
	b.mu.Unlock()
	b.Auth.Reset()
}

func (b *InMemoryBackend) nowUTC() time.Time { return b.now().UTC().Truncate(time.Millisecond) }

func (b *InMemoryBackend) nextSeq() int64 {
	b.seq++

	return b.seq
}

func (b *InMemoryBackend) self(kind, id string) string {
	return b.selfBase + "/" + kind + "/" + id
}

func newOCID(kind string) string { return "ocid1." + kind + ".oc1.." + awsid.NewN("", ocidTailLen) }

func (b *InMemoryBackend) checkCompartment(id string) *APIError {
	if id == "" {
		return errMissing("compartmentId is required")
	}

	if b.compartment != nil && !b.compartment(id) {
		return errNotFound(id)
	}

	return nil
}

// cond carries the If-Match and If-Unmodified-Since request preconditions.
type cond struct {
	Unmodified time.Time
	IfMatch    string
}

func etagMatches(header, etag string) bool {
	for part := range strings.SplitSeq(header, ",") {
		part = strings.Trim(strings.TrimPrefix(strings.TrimSpace(part), "W/"), `"`)
		if part == "*" || part == etag {
			return true
		}
	}

	return false
}

func (c cond) check(id string, m meta) *APIError {
	if c.IfMatch != "" && !etagMatches(c.IfMatch, m.etag(id)) {
		return errEtag()
	}

	if !c.Unmodified.IsZero() && m.Modified.Truncate(time.Second).After(c.Unmodified) {
		return errUnmodified()
	}

	return nil
}

// Tag is the etag and modification time returned with a resource.
type Tag struct {
	Modified time.Time
	ETag     string
}

func tagOf(id string, m meta) Tag { return Tag{ETag: m.etag(id), Modified: m.Modified} }

// ListOpts carries the shared list query parameters.
type ListOpts struct {
	SortBy string
	Order  string
	Page   string
	Limit  int
}

type sorter[T any] map[string]func(a, c T) int

// sortPage sorts items by the requested key and slices out a page; total is the matching item count.
func sortPage[T any](items []T, o ListOpts, defBy string, defDesc bool, keys sorter[T]) (page.Page[T], *APIError) {
	by := defBy

	if o.SortBy != "" {
		by = o.SortBy
	}

	fn, ok := keys[by]
	if !ok {
		return page.Page[T]{}, errParam("sortBy is not a supported value")
	}

	desc := defDesc

	switch strings.ToUpper(o.Order) {
	case "":
	case "ASC":
		desc = false
	case "DESC":
		desc = true
	default:
		return page.Page[T]{}, errParam("sortOrder must be ASC or DESC")
	}

	if o.Limit < 0 || o.Limit > maxListLimit {
		return page.Page[T]{}, errParam("limit must be between 1 and 1000")
	}

	if err := page.ValidateToken(o.Page); err != nil {
		return page.Page[T]{}, errParam("page is not a valid page token")
	}

	slices.SortStableFunc(items, func(a, c T) int {
		if desc {
			return -fn(a, c)
		}

		return fn(a, c)
	})

	return page.New(items, o.Page, o.Limit, defaultListLimit), nil
}

func byCreated[T any](created func(T) time.Time, seq func(T) int64) func(a, c T) int {
	return func(a, c T) int {
		return cmp.Or(created(a).Compare(created(c)), cmp.Compare(seq(a), seq(c)))
	}
}

func byString[T any](get func(T) string, seq func(T) int64) func(a, c T) int {
	return func(a, c T) int { return cmp.Or(cmp.Compare(get(a), get(c)), cmp.Compare(seq(a), seq(c))) }
}

// ListResult is a page of items and the number of items that matched before paging.
type ListResult[T any] struct {
	Next  string
	Items []T
	Total int
}

func finish[T, W any](pg page.Page[T], total int, conv func(T) W) ListResult[W] {
	out := make([]W, 0, len(pg.Data))
	for _, it := range pg.Data {
		out = append(out, conv(it))
	}

	return ListResult[W]{Items: out, Next: pg.Next, Total: total}
}

func nonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}

	return s
}

func isOCID(s string) bool { return strings.HasPrefix(s, "ocid1.") }

func containsFold(s, sub string) bool { return strings.Contains(strs.Fold(s), strs.Fold(sub)) }
