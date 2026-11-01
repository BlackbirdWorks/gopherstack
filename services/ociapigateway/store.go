package ociapigateway

import (
	"cmp"
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
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	// Region is the fixed region baked into emulated OCIDs and gateway hostnames.
	Region           = "us-ashburn-1"
	defaultListLimit = 100
	ocidTailLen      = 52
	hostLabelLen     = 26

	defaultTransition = 2 * time.Second
)

// InMemoryBackend holds every gateway, deployment, certificate and work request under one lock.
type InMemoryBackend struct {
	mu            *lockmetrics.RWMutex
	now           func() time.Time
	gateways      map[string]*Gateway
	deployments   map[string]*Deployment
	certificates  map[string]*Certificate
	workReqs      map[string]*WorkRequest
	specs         map[string]*compiledSpec
	Auth          *ociauth.Registry
	compartment   func(string) bool
	subnet        func(string) bool
	functions     FunctionInvoker
	limiter       *rateLimiter
	client        *http.Client
	jwks          *jwksCache
	transition    time.Duration
	seq           int64
	allowExternal bool
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{mu: lockmetrics.New("ociapigateway"), now: time.Now, transition: defaultTransition}
	b.resetMaps()
	b.Auth = ociauth.NewRegistry()
	b.limiter = newRateLimiter()
	b.jwks = newJWKSCache()
	b.client = b.newBackendClient()

	return b
}

func (b *InMemoryBackend) resetMaps() {
	b.gateways, b.deployments = map[string]*Gateway{}, map[string]*Deployment{}
	b.certificates, b.workReqs = map[string]*Certificate{}, map[string]*WorkRequest{}
	b.specs = map[string]*compiledSpec{}
}

// newBackendClient builds the data-plane HTTP client. The dial guard rejects non-loopback peers
// unless external backends are allowed, and redirects are returned to the caller rather than followed.
func (b *InMemoryBackend) newBackendClient() *http.Client {
	tr := httpclient.NewTransport()
	tr.Proxy = nil
	tr.DialContext = b.guardedDialer().DialContext

	return &http.Client{
		Transport:     tr,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

// SetClock injects the time source used for lifecycle transitions, rate limits and token validation.
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

// SetCompartmentChecker validates compartmentId arguments against another service (OCI Identity).
func (b *InMemoryBackend) SetCompartmentChecker(fn func(string) bool) {
	b.mu.Lock("SetCompartmentChecker")
	b.compartment = fn
	b.mu.Unlock()
}

// SetSubnetChecker validates subnetId arguments against another service (OCI Core).
func (b *InMemoryBackend) SetSubnetChecker(fn func(string) bool) {
	b.mu.Lock("SetSubnetChecker")
	b.subnet = fn
	b.mu.Unlock()
}

// SetFunctionInvoker wires OCI Functions for ORACLE_FUNCTIONS_BACKEND and CUSTOM_AUTHENTICATION.
func (b *InMemoryBackend) SetFunctionInvoker(fn FunctionInvoker) {
	b.mu.Lock("SetFunctionInvoker")
	b.functions = fn
	b.mu.Unlock()
}

// SetAllowExternal lets HTTP backends and remote JWKS URIs reach non-loopback hosts.
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

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	b.resetMaps()
	b.mu.Unlock()
	b.Auth.Reset()
	b.limiter.reset()
	b.jwks.reset()
}

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

func newHostname() string {
	return awsid.NewN("", hostLabelLen) + ".apigateway." + Region + ".oci.customer-oci.com"
}

func checkEtag(current, ifMatch string) *APIError {
	if ifMatch != "" && ifMatch != current {
		return errEtag()
	}

	return nil
}

// checkMove validates the etag and the target compartment of a ChangeCompartment call.
func (b *InMemoryBackend) checkMove(current, ifMatch, target string) *APIError {
	if err := checkEtag(current, ifMatch); err != nil {
		return err
	}

	return b.checkCompartment(target)
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

// paginate sorts by displayName or timeCreated (the only sortBy values API Gateway documents) and pages.
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

func due(at *time.Time, now time.Time) bool { return at != nil && !at.After(now) }

// settleState applies a pending transition to a lifecycle state.
func settleState(state *string, at **time.Time, now time.Time) {
	if !due(*at, now) {
		return
	}

	*at = nil

	if *state == stateDeleting {
		*state = stateDeleted
	} else {
		*state = stateActive
	}
}

// Sweep applies lifecycle transitions against the backend clock.
func (b *InMemoryBackend) Sweep() {
	b.mu.Lock("Sweep")
	defer b.mu.Unlock()

	now := b.nowUTC()
	for _, g := range b.gateways {
		settleState(&g.State, &g.TransitionAt, now)
	}

	for id, d := range b.deployments {
		settleState(&d.State, &d.TransitionAt, now)

		if d.State == stateDeleted {
			delete(b.specs, id)
		}
	}

	for _, c := range b.certificates {
		settleState(&c.State, &c.TransitionAt, now)
	}
}
