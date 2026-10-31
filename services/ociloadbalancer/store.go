package ociloadbalancer

import (
	"cmp"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"net/netip"
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
	publicPoolBase   = "198.18.0.0/15"
	reservedHosts    = 4
	addrBits         = 32
)

// InMemoryBackend holds every load balancer and work request under one lock.
type InMemoryBackend struct {
	mu            *lockmetrics.RWMutex
	now           func() time.Time
	lbs           map[string]*LoadBalancer
	workReqs      map[string]*WorkRequest
	keys          map[string]*tls.Certificate
	Auth          *ociauth.Registry
	compartment   func(string) bool
	subnet        func(string) (string, bool)
	dp            *dataPlane
	transition    time.Duration
	seq           int64
	ipSeq         int64
	allowExternal bool
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{mu: lockmetrics.New("ociloadbalancer"), now: time.Now, transition: defaultTransit}
	b.resetMaps()
	b.Auth = ociauth.NewRegistry()
	b.dp = newDataPlane(b)

	return b
}

func (b *InMemoryBackend) resetMaps() {
	b.lbs, b.workReqs, b.keys = map[string]*LoadBalancer{}, map[string]*WorkRequest{}, map[string]*tls.Certificate{}
}

// SetClock injects the time source used for lifecycle transitions and health checks.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	b.now = now
	b.mu.Unlock()
}

// SetTransition sets how long asynchronous operations take to complete.
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

// SetSubnetResolver validates subnetIds against another service (OCI Core) and returns each subnet's CIDR.
func (b *InMemoryBackend) SetSubnetResolver(fn func(string) (string, bool)) {
	b.mu.Lock("SetSubnetResolver")
	b.subnet = fn
	b.mu.Unlock()
}

// SetAllowExternal lets the data plane reach non-loopback backends.
func (b *InMemoryBackend) SetAllowExternal(allow bool) {
	b.mu.Lock("SetAllowExternal")
	b.allowExternal = allow
	b.mu.Unlock()
}

func (b *InMemoryBackend) externalAllowed() bool {
	b.mu.RLock("externalAllowed")
	defer b.mu.RUnlock()

	return b.allowExternal
}

// Reset clears all state and stops every data-plane listener.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	b.resetMaps()
	b.mu.Unlock()
	b.Auth.Reset()
	b.dp.reset()
}

// Close stops every data-plane listener.
func (b *InMemoryBackend) Close() { b.dp.reset() }

func (b *InMemoryBackend) nowUTC() time.Time { return b.now().UTC() }

func (b *InMemoryBackend) clock() time.Time {
	b.mu.RLock("clock")
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

func (b *InMemoryBackend) transitionAt() *time.Time {
	t := b.nowUTC().Add(b.transition)

	return &t
}

// clone deep-copies a load balancer; stored load balancers are immutable once published.
func (lb *LoadBalancer) clone() *LoadBalancer {
	raw, err := json.Marshal(lb)
	if err != nil {
		return lb
	}

	out := &LoadBalancer{}
	if json.Unmarshal(raw, out) != nil {
		return lb
	}

	out.fillMaps()

	return out
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

// Sweep applies lifecycle transitions against the backend clock and reconciles data-plane listeners.
func (b *InMemoryBackend) Sweep() {
	b.mu.Lock("Sweep")

	now := b.nowUTC()
	for id, lb := range b.lbs {
		if lb.TransitionAt == nil || lb.TransitionAt.After(now) {
			continue
		}

		next := lb.clone()
		next.TransitionAt = nil

		if lb.State == stateDeleting {
			next.State = stateDeleted
			b.dropKeys(id)
		} else {
			next.State = stateActive
		}

		b.lbs[id] = next
	}

	b.mu.Unlock()
	b.dp.sync()
}

func (b *InMemoryBackend) dropKeys(lbID string) {
	for k := range b.keys {
		if strings.HasPrefix(k, lbID+"/") {
			delete(b.keys, k)
		}
	}
}

// allocIP assigns the next address from the subnet CIDR, or from the emulator pool when none is known.
func (b *InMemoryBackend) allocIP(cidr string) string {
	b.ipSeq++

	prefix, err := netip.ParsePrefix(cidr)
	if err != nil || !prefix.Addr().Is4() {
		prefix = netip.MustParsePrefix(publicPoolBase)
	}

	hosts := int64(1) << (addrBits - prefix.Bits())
	if hosts <= reservedHosts+1 {
		return prefix.Masked().Addr().String()
	}

	off := reservedHosts + (b.ipSeq-1)%(hosts-reservedHosts-1)
	addr := prefix.Masked().Addr()
	raw := addr.As4()
	binary.BigEndian.PutUint32(raw[:], binary.BigEndian.Uint32(raw[:])+uint32(off)) //nolint:gosec // off < 2^32

	return netip.AddrFrom4(raw).String()
}
