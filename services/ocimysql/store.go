package ocimysql

import (
	"cmp"
	"context"
	"net/netip"
	"slices"
	"strconv"
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

	defaultTransition = 2 * time.Second
	maxName           = 255
	defaultAD         = "Uocm:US-ASHBURN-AD-1"
	defaultFD         = "FAULT-DOMAIN-1"
	firstHostOffset   = 5
)

// SubnetInfo is the slice of an OCI Core subnet the service validates references against.
type SubnetInfo struct {
	ID                 string
	VcnID              string
	AvailabilityDomain string
	CIDR               string
}

// Network is the narrow view of OCI Core the service needs.
type Network interface {
	Subnet(id string) (SubnetInfo, bool)
}

// InMemoryBackend holds every DB system, backup, channel, configuration and work request under one lock.
type InMemoryBackend struct {
	net         Network
	runCtx      context.Context
	eng         *dbEngine
	Auth        *ociauth.Registry
	channels    map[string]*Channel
	configs     map[string]*Configuration
	workReqs    map[string]*WorkRequest
	defaults    map[string]*Configuration
	stop        context.CancelFunc
	backups     map[string]*Backup
	systems     map[string]*DBSystem
	compartment func(string) bool
	mu          *lockmetrics.RWMutex
	wg          *sync.WaitGroup
	now         func() time.Time
	defaultList []*Configuration
	transition  time.Duration
	seq         int64
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend() *InMemoryBackend {
	ctx, stop := context.WithCancel(context.Background())
	b := &InMemoryBackend{
		mu: lockmetrics.New("ocimysql"), now: time.Now, transition: defaultTransition,
		wg: &sync.WaitGroup{}, runCtx: ctx, stop: stop, defaults: map[string]*Configuration{},
	}
	b.resetMaps()
	b.Auth = ociauth.NewRegistry()

	b.defaultList = defaultConfigs()
	for _, c := range b.defaultList {
		c.TimeCreated = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		c.TimeUpdated = c.TimeCreated
		b.defaults[c.ID] = c
	}

	return b
}

func (b *InMemoryBackend) resetMaps() {
	b.systems, b.backups, b.channels = map[string]*DBSystem{}, map[string]*Backup{}, map[string]*Channel{}
	b.configs, b.workReqs = map[string]*Configuration{}, map[string]*WorkRequest{}
}

// SetClock injects the time source used for lifecycle transitions, backup windows and retention.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	b.now = now
	b.mu.Unlock()
}

// SetTransition sets how long CREATING, UPDATING and DELETING states last without the docker engine.
func (b *InMemoryBackend) SetTransition(d time.Duration) {
	b.mu.Lock("SetTransition")
	b.transition = d
	b.mu.Unlock()
}

// SetNetwork wires subnet validation and endpoint address allocation against OCI Core; nil disconnects.
func (b *InMemoryBackend) SetNetwork(n Network) {
	b.mu.Lock("SetNetwork")
	b.net = n
	b.mu.Unlock()
}

// SetCompartmentChecker validates compartmentId arguments against another service (OCI Identity).
func (b *InMemoryBackend) SetCompartmentChecker(fn func(string) bool) {
	b.mu.Lock("SetCompartmentChecker")
	b.compartment = fn
	b.mu.Unlock()
}

// Reset clears all state and tears down engine servers.
func (b *InMemoryBackend) Reset() {
	b.stop()
	b.wg.Wait()

	b.mu.Lock("Reset")
	lives := b.detachAllLocked()
	b.resetMaps()
	b.runCtx, b.stop = context.WithCancel(context.Background())
	e := b.eng
	b.mu.Unlock()

	e.closeAll(lives, true)
	b.Auth.Reset()
}

// Close stops engine goroutines and removes the servers; data volumes are kept.
func (b *InMemoryBackend) Close() {
	b.stop()
	b.wg.Wait()

	b.mu.Lock("Close")
	lives := b.detachAllLocked()
	e := b.eng
	b.mu.Unlock()

	e.closeAll(lives, false)
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

func (b *InMemoryBackend) checkCompartmentLocked(id string) *APIError {
	b.mu.RLock("checkCompartment")
	defer b.mu.RUnlock()

	return b.checkCompartment(id)
}

// ListOpts carries the shared list query parameters.
type ListOpts struct {
	SortBy string
	Order  string
	Page   string
	Limit  int
}

func validListOpts(o ListOpts, sorts ...string) *APIError {
	if o.SortBy != "" && !containsFold(sorts, o.SortBy) {
		return errParam("sortBy must be one of " + strings.Join(sorts, ", "))
	}

	if o.Order != "" && !strings.EqualFold(o.Order, "ASC") && !strings.EqualFold(o.Order, "DESC") {
		return errParam("sortOrder must be ASC or DESC")
	}

	return nil
}

// sortKeys are the comparable fields of a listed resource.
type sortKeys struct {
	created time.Time
	updated time.Time
	name    string
	shape   string
	seq     int64
}

func paginate[T any](items []T, o ListOpts, keys func(T) sortKeys) page.Page[T] {
	desc := false

	switch strings.ToUpper(o.Order) {
	case "ASC":
		desc = false
	case "DESC":
		desc = true
	default:
		desc = !strings.EqualFold(o.SortBy, keyDisplayName) && !strings.EqualFold(o.SortBy, "shapeName")
	}

	slices.SortStableFunc(items, func(a, c T) int {
		ka, kc := keys(a), keys(c)

		var r int

		switch {
		case strings.EqualFold(o.SortBy, keyDisplayName):
			r = cmp.Compare(ka.name, kc.name)
		case strings.EqualFold(o.SortBy, "shapeName"):
			r = cmp.Compare(ka.shape, kc.shape)
		case strings.EqualFold(o.SortBy, keyTimeUpdated):
			r = ka.updated.Compare(kc.updated)
		default:
			r = ka.created.Compare(kc.created)
		}

		r = cmp.Or(r, cmp.Compare(ka.seq, kc.seq))
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

func validateName(name string) *APIError {
	if strings.TrimSpace(name) == "" {
		return errMissing("displayName must not be empty")
	}

	if len(name) > maxName {
		return errParam("displayName must be at most 255 characters")
	}

	return nil
}

func containsFold(list []string, v string) bool {
	return slices.ContainsFunc(list, func(s string) bool { return strings.EqualFold(s, v) })
}

func hostIn(cidr string, offset int) string {
	p, err := netip.ParsePrefix(cidr)
	if err != nil {
		return ""
	}

	a := p.Masked().Addr()
	for range offset {
		a = a.Next()
	}

	if !p.Contains(a) {
		return ""
	}

	return a.String()
}

// documentationAddr hands out addresses from the RFC 5737 documentation ranges when no subnet is known.
func documentationAddr(n int64) string {
	bases := []string{"203.0.113.", "198.51.100.", "192.0.2."}
	const hosts = 253

	i := (n - 1) % (hosts * int64(len(bases)))

	return bases[i/hosts] + strconv.FormatInt(i%hosts+1, 10)
}

func contextWithCancel() (context.Context, context.CancelFunc) {
	return context.WithCancel(context.Background())
}
