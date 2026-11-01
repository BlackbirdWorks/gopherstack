package ociartifacts

import (
	"cmp"
	"context"
	"crypto/rand"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	// Region is the fixed region baked into emulated OCIDs.
	Region = "us-ashburn-1"
	// DefaultNamespace is the tenancy namespace served unless overridden.
	DefaultNamespace = "gopherstack"
	// DefaultTenancy is the root compartment used for repositories created by a first push.
	DefaultTenancy = "ocid1.tenancy.oc1..aaaaaaaagopherstackemulatedtenancy000000000000000000000000"

	defaultListLimit = 100
	maxListLimit     = 1000
	ocidTailLen      = 52
	secretBytes      = 32
	registryDirName  = "ociartifacts"
	sortByName       = "DISPLAYNAME"
)

// InMemoryBackend holds every resource under one lock; image blobs live in an embedded registry.
type InMemoryBackend struct {
	ctx          context.Context //nolint:containedctx // lifetime of the lazily built registry
	Auth         *ociauth.Registry
	compartment  func(string) bool
	within       func(id, ancestor string) bool
	mu           *lockmetrics.RWMutex
	now          func() time.Time
	raw          http.Handler
	app          http.Handler
	repos        map[string]*Repo
	images       map[string]*Image
	sigs         map[string]*Signature
	gens         map[string]*GenericRepo
	arts         map[string]*GenericArtifact
	namespace    string
	tenancy      string
	dataDir      string
	configEtag   string
	secret       []byte
	seq          int64
	createOnPush bool
}

// NewInMemoryBackend creates an empty backend that keeps image blobs in memory.
func NewInMemoryBackend() *InMemoryBackend {
	secret := make([]byte, secretBytes)
	_, _ = rand.Read(secret) // crypto/rand.Read never fails on supported platforms

	b := &InMemoryBackend{
		ctx: context.Background(), mu: lockmetrics.New("ociartifacts"), now: time.Now, Auth: ociauth.NewRegistry(),
		namespace: DefaultNamespace, tenancy: DefaultTenancy, secret: secret, createOnPush: true,
		configEtag: newEtag(),
	}
	b.initMaps()

	return b
}

func (b *InMemoryBackend) initMaps() {
	b.repos = map[string]*Repo{}
	b.images = map[string]*Image{}
	b.sigs = map[string]*Signature{}
	b.gens = map[string]*GenericRepo{}
	b.arts = map[string]*GenericArtifact{}
}

// SetClock injects the time source for timestamps and token expiry.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	b.now = now
	b.mu.Unlock()
}

// SetContext bounds the lifetime of the embedded registry.
func (b *InMemoryBackend) SetContext(ctx context.Context) {
	b.mu.Lock("SetContext")
	b.ctx = ctx
	b.mu.Unlock()
}

// SetDataDir keeps image blobs and manifests under dir; empty keeps them in memory.
func (b *InMemoryBackend) SetDataDir(dir string) {
	b.mu.Lock("SetDataDir")
	b.dataDir = dir
	b.mu.Unlock()
}

// SetNamespace overrides the tenancy namespace used in registry paths.
func (b *InMemoryBackend) SetNamespace(ns string) {
	b.mu.Lock("SetNamespace")
	b.namespace = ns
	b.mu.Unlock()
}

// SetTenancy sets the root compartment that owns repositories created by a first push.
func (b *InMemoryBackend) SetTenancy(id string) {
	b.mu.Lock("SetTenancy")
	b.tenancy = id
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

// Namespace returns the tenancy namespace.
func (b *InMemoryBackend) Namespace() string {
	b.mu.RLock("Namespace")
	defer b.mu.RUnlock()

	return b.namespace
}

// Reset clears all state and removes stored blobs.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	b.initMaps()
	b.createOnPush, b.configEtag = true, newEtag()
	b.releaseRegistry(true)
	b.mu.Unlock()
	b.Auth.Reset()
}

// Close releases the embedded registry.
func (b *InMemoryBackend) Close() {
	b.mu.Lock("Close")
	b.releaseRegistry(false)
	b.mu.Unlock()
}

func (b *InMemoryBackend) releaseRegistry(purge bool) {
	if b.raw != nil {
		ociregistry.Shutdown(b.raw)
		b.raw, b.app = nil, nil
	}

	if purge && b.dataDir != "" {
		_ = os.RemoveAll(filepath.Join(b.dataDir, registryDirName))
	}
}

// registry returns the embedded Docker Registry v2 handler, building it on first use.
func (b *InMemoryBackend) registry() http.Handler {
	b.mu.RLock("registry")
	app := b.app
	b.mu.RUnlock()

	if app != nil {
		return app
	}

	b.mu.Lock("registry")
	defer b.mu.Unlock()

	return b.registryLocked()
}

func (b *InMemoryBackend) registryLocked() http.Handler {
	if b.app == nil {
		dir := ""
		if b.dataDir != "" {
			dir = filepath.Join(b.dataDir, registryDirName)
		}

		b.raw = ociregistry.New(b.ctx, ociregistry.Options{InstanceID: "gopherstack-ociartifacts", RootDir: dir})
		b.app = ociregistry.Limit(b.raw, 0)
	}

	return b.app
}

func (b *InMemoryBackend) nowUTC() time.Time { return b.now().UTC().Truncate(time.Millisecond) }

func (b *InMemoryBackend) nextSeq() int64 {
	b.seq++

	return b.seq
}

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

func (b *InMemoryBackend) inCompartment(have, want string, subtree bool) bool {
	if have == want {
		return true
	}

	return subtree && b.within != nil && b.within(have, want)
}

// ListOpts carries the shared list query parameters.
type ListOpts struct {
	SortBy  string
	Order   string
	Page    string
	Limit   int
	Subtree bool
}

type sortKey struct {
	created time.Time
	name    string
	seq     int64
}

// paginate sorts and slices items; it returns the page and how many matching items follow it.
func paginate[T any](items []T, o ListOpts, key func(T) sortKey) (page.Page[T], int, *APIError) {
	by := strings.ToUpper(o.SortBy)
	if by != "" && by != "TIMECREATED" && by != sortByName {
		return page.Page[T]{}, 0, errParam("sortBy must be TIMECREATED or DISPLAYNAME")
	}

	order := strings.ToUpper(o.Order)
	if order != "" && order != "ASC" && order != "DESC" {
		return page.Page[T]{}, 0, errParam("sortOrder must be ASC or DESC")
	}

	if o.Limit < 0 || o.Limit > maxListLimit {
		return page.Page[T]{}, 0, errParam("limit must be between 1 and 1000")
	}

	if err := page.ValidateToken(o.Page); err != nil {
		return page.Page[T]{}, 0, errParam("page is not a valid page token")
	}

	desc := by != sortByName
	if order != "" {
		desc = order == "DESC"
	}

	slices.SortStableFunc(items, func(a, c T) int {
		ka, kc := key(a), key(c)

		var r int
		if by == sortByName {
			r = cmp.Or(cmp.Compare(ka.name, kc.name), cmp.Compare(ka.seq, kc.seq))
		} else {
			r = cmp.Or(ka.created.Compare(kc.created), cmp.Compare(ka.seq, kc.seq))
		}

		if desc {
			return -r
		}

		return r
	})

	pg := page.New(items, o.Page, o.Limit, defaultListLimit)
	remaining := max(0, len(items)-(page.DecodeToken(o.Page)+len(pg.Data)))

	return pg, remaining, nil
}

// matchWild matches a filter value that may end in `*`.
func matchWild(pattern, value string) bool {
	if prefix, ok := strings.CutSuffix(pattern, "*"); ok {
		return strings.HasPrefix(value, prefix)
	}

	return pattern == value
}

func str(p *string) string {
	if p == nil {
		return ""
	}

	return *p
}

// Compartments lists the tenancy root and every compartment that holds a repository.
func (b *InMemoryBackend) Compartments() []string {
	b.mu.RLock("Compartments")
	defer b.mu.RUnlock()

	seen := map[string]struct{}{b.tenancy: {}}

	for _, r := range b.repos {
		seen[r.CompartmentID] = struct{}{}
	}

	for _, g := range b.gens {
		seen[g.CompartmentID] = struct{}{}
	}

	return slices.Sorted(maps.Keys(seen))
}
