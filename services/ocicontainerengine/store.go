package ocicontainerengine

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
)

const (
	// Region is the emulated region name.
	Region           = "us-ashburn-1"
	regionKey        = "iad"
	defaultListLimit = 100
	ocidTailLen      = 52
	shortTail        = 5
	longTail         = 11
	sourceImage      = "IMAGE"
	osOracleLinux    = "Oracle Linux"

	defaultTransition = 2 * time.Second
	maxName           = 255
)

// SubnetInfo is the slice of a subnet the service validates references against.
type SubnetInfo struct {
	ID                 string
	VcnID              string
	AvailabilityDomain string
	CIDR               string
}

// ShapeInfo is the slice of a compute shape node pools validate against.
type ShapeInfo struct {
	Name              string
	Arch              string
	Flexible          bool
	MinOcpus          float64
	MaxOcpus          float64
	MaxMemoryGBs      float64
	DefaultMemPerOcpu float64
}

// ImageInfo is the slice of a compute image node pools validate against.
type ImageInfo struct {
	ID        string
	Name      string
	OS        string
	OSVersion string
	Arch      string
}

// Network is the narrow view of OCI Core (VCNs, subnets, shapes, images) the service needs.
type Network interface {
	Subnet(id string) (SubnetInfo, bool)
	VCNExists(id string) bool
	Shape(name string) (ShapeInfo, bool)
	Shapes() []ShapeInfo
	Image(id string) (ImageInfo, bool)
	Images() []ImageInfo
}

// KeyChecker reports the lifecycle state of a KMS key OCID and whether it exists.
type KeyChecker func(keyID string) (state string, found bool)

// InMemoryBackend holds every cluster, node pool and work request under one lock.
type InMemoryBackend struct {
	mu          *lockmetrics.RWMutex
	now         func() time.Time
	clusters    map[string]*Cluster
	pools       map[string]*NodePool
	workReqs    map[string]*WorkRequest
	Auth        *ociauth.Registry
	net         Network
	keyChecker  KeyChecker
	compartment func(string) bool
	engine      *clusterEngine
	wg          *sync.WaitGroup
	runCtx      context.Context //nolint:containedctx // engine goroutines outlive any single request
	stop        context.CancelFunc
	transition  time.Duration
	seq         int64
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend() *InMemoryBackend {
	ctx, stop := context.WithCancel(context.Background())
	b := &InMemoryBackend{
		mu: lockmetrics.New("ocicontainerengine"), now: time.Now, transition: defaultTransition,
		wg: &sync.WaitGroup{}, runCtx: ctx, stop: stop,
	}
	b.resetMaps()
	b.Auth = ociauth.NewRegistry()

	return b
}

func (b *InMemoryBackend) resetMaps() {
	b.clusters, b.pools, b.workReqs = map[string]*Cluster{}, map[string]*NodePool{}, map[string]*WorkRequest{}
}

// SetClock injects the time source used for lifecycle transitions.
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

// SetNetwork wires subnet, shape and image validation against OCI Core; nil disconnects.
func (b *InMemoryBackend) SetNetwork(n Network) {
	b.mu.Lock("SetNetwork")
	b.net = n
	b.mu.Unlock()
}

// SetKeyChecker wires validation of kmsKeyId references against a Vault backend.
func (b *InMemoryBackend) SetKeyChecker(fn KeyChecker) {
	b.mu.Lock("SetKeyChecker")
	b.keyChecker = fn
	b.mu.Unlock()
}

// SetCompartmentChecker validates compartmentId arguments against another service (OCI Identity).
func (b *InMemoryBackend) SetCompartmentChecker(fn func(string) bool) {
	b.mu.Lock("SetCompartmentChecker")
	b.compartment = fn
	b.mu.Unlock()
}

// Reset clears all state and tears down engine clusters.
func (b *InMemoryBackend) Reset() {
	b.stop()
	b.wg.Wait()

	b.mu.Lock("Reset")
	lcs := b.detachAllLocked()
	b.resetMaps()
	b.runCtx, b.stop = context.WithCancel(context.Background())
	e := b.engine
	b.mu.Unlock()

	if e != nil {
		e.reap(lcs)
	}

	b.Auth.Reset()
}

// Close stops engine goroutines and releases cluster containers.
func (b *InMemoryBackend) Close() {
	b.stop()
	b.wg.Wait()
	b.closeEngine()
}

func (b *InMemoryBackend) nowUTC() time.Time { return b.now().UTC() }

func (b *InMemoryBackend) nextSeq() int64 {
	b.seq++

	return b.seq
}

func newEtag() string { return uuid.NewString() }

func newOCID(kind string) string {
	return "ocid1." + kind + ".oc1." + regionKey + "." + awsid.NewN("", ocidTailLen)
}

func ocidTail(id string, n int) string {
	if len(id) <= n {
		return id
	}

	return id[len(id)-n:]
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

func validListOpts(o ListOpts, sorts ...string) *APIError {
	if o.SortBy != "" && !slices.Contains(sorts, strings.ToUpper(o.SortBy)) {
		return errParam("sortBy must be one of " + strings.Join(sorts, ", "))
	}

	if o.Order != "" && !strings.EqualFold(o.Order, "ASC") && !strings.EqualFold(o.Order, "DESC") {
		return errParam("sortOrder must be ASC or DESC")
	}

	return nil
}

func paginate[T any](
	items []T, o ListOpts, id, name func(T) string, created func(T) (time.Time, int64),
) page.Page[T] {
	desc := true

	switch strings.ToUpper(o.Order) {
	case "ASC":
		desc = false
	case "DESC":
		desc = true
	}

	by := strings.ToUpper(o.SortBy)

	slices.SortStableFunc(items, func(a, c T) int {
		var r int

		switch by {
		case "ID":
			r = cmp.Compare(id(a), id(c))
		case "NAME":
			r = cmp.Compare(name(a), name(c))
		default:
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

func def[T comparable](v, d T) T {
	var zero T
	if v == zero {
		return d
	}

	return v
}

func firstErr(errs ...*APIError) *APIError {
	for _, e := range errs {
		if e != nil {
			return e
		}
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

func equalFoldStr(a, b string) bool { return strings.EqualFold(a, b) }

func containsFold(list []string, v string) bool {
	return slices.ContainsFunc(list, func(s string) bool { return strings.EqualFold(s, v) })
}

func orSlice[S ~[]E, E any](v, d S) S {
	if v != nil {
		return v
	}

	return d
}

func orMap[M ~map[K]V, K comparable, V any](v, d M) M {
	if v != nil {
		return v
	}

	return d
}

func orPtr[T any](v, d *T) *T {
	if v != nil {
		return v
	}

	return d
}
