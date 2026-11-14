package ocifilestorage

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
	// Region is the fixed region key baked into emulated OCIDs.
	Region           = "iad"
	defaultListLimit = 100
	ocidTailLen      = 52
	defaultTransit   = 2 * time.Second
	deletedRetention = time.Hour
)

// Resource kinds embedded in OCIDs.
const (
	kindFS        = "filesystem"
	kindMT        = "mounttarget"
	kindExportSet = "exportset"
	kindExport    = "export"
	kindSnapshot  = "snapshot"
	kindPolicy    = "filesystemsnapshotpolicy"
	kindRepl      = "replication"
	kindReplTgt   = "replicationtarget"
	kindConnector = "outboundconnector"
	kindQuota     = "quotarule"
)

type entry[W resource] struct {
	W    W          `json:"w"`
	At   *time.Time `json:"at,omitempty"`
	Gone *time.Time `json:"gone,omitempty"`
	Etag string     `json:"etag"`
	To   string     `json:"to,omitempty"`
	Seq  int64      `json:"seq"`
}

type table[W resource] map[string]*entry[W]

func (t table[W]) get(id string) (*entry[W], *APIError) {
	e, ok := t[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return e, nil
}

// usable returns an entry that is neither absent nor deleted.
func (t table[W]) usable(id string) (*entry[W], *APIError) {
	e, aerr := t.get(id)
	if aerr != nil {
		return nil, aerr
	}

	if st := e.W.bs().State; st == stDeleted {
		return nil, errNotFound(id)
	}

	return e, nil
}

// SubnetInfo describes a subnet known to OCI Core.
type SubnetInfo struct {
	VcnID         string
	CIDR          string
	AD            string
	CompartmentID string
}

// NicRequest asks the network service for a VNIC on behalf of a mount target.
type NicRequest struct {
	CompartmentID string
	AD            string
	SubnetID      string
	DisplayName   string
	Hostname      string
	IPAddress     string
	NsgIDs        []string
}

// Nic is the VNIC and primary private IP that back a mount target.
type Nic struct {
	VnicID      string
	PrivateIPID string
	IPAddress   string
}

// Network is the in-process view of OCI Core that mount targets depend on.
type Network struct {
	Subnet    func(id string) (SubnetInfo, bool)
	CreateNic func(NicRequest) (Nic, error)
	DeleteNic func(vnicID string)
}

// SecretFunc returns the content of a vault secret version (nil version means current).
type SecretFunc func(id string, version *int) ([]byte, bool)

// InMemoryBackend holds every File Storage resource under one lock.
type InMemoryBackend struct {
	mu          *lockmetrics.RWMutex
	now         func() time.Time
	fss         table[*FileSystem]
	mts         table[*MountTarget]
	sets        table[*ExportSet]
	exps        table[*Export]
	snaps       table[*Snapshot]
	pols        table[*Policy]
	reps        table[*Replication]
	tgts        table[*ReplicationTarget]
	conns       table[*Connector]
	quotas      table[*QuotaRule]
	tokens      map[string]string
	Auth        *ociauth.Registry
	compartment func(string) bool
	net         *Network
	secret      SecretFunc
	keyState    func(string) (string, bool)
	dp          dataPlane
	onClose     func()
	transition  time.Duration
	seq         int64
}

// NewInMemoryBackend creates an empty backend running in stub mode.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		mu: lockmetrics.New("ocifilestorage"), now: time.Now, transition: defaultTransit, Auth: ociauth.NewRegistry(),
	}
	b.resetMaps()

	return b
}

func (b *InMemoryBackend) resetMaps() {
	b.fss, b.mts, b.sets, b.exps = table[*FileSystem]{}, table[*MountTarget]{}, table[*ExportSet]{}, table[*Export]{}
	b.snaps, b.pols, b.reps = table[*Snapshot]{}, table[*Policy]{}, table[*Replication]{}
	b.tgts, b.conns, b.quotas = table[*ReplicationTarget]{}, table[*Connector]{}, table[*QuotaRule]{}
	b.tokens = map[string]string{}
}

// SetClock injects the time source used for lifecycle transitions.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	b.now = now
	b.mu.Unlock()
}

// SetTransition sets how long asynchronous lifecycle changes take to complete.
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

// SetNetwork wires subnet validation and VNIC provisioning from OCI Core.
func (b *InMemoryBackend) SetNetwork(n Network) {
	b.mu.Lock("SetNetwork")
	b.net = &n
	b.mu.Unlock()
}

// SetSecretLookup wires Kerberos keytab and LDAP password secret validation from OCI Vault.
func (b *InMemoryBackend) SetSecretLookup(fn SecretFunc) {
	b.mu.Lock("SetSecretLookup")
	b.secret = fn
	b.mu.Unlock()
}

// SetCloser registers fn to run when the backend closes.
func (b *InMemoryBackend) SetCloser(fn func()) {
	b.mu.Lock("SetCloser")
	b.onClose = fn
	b.mu.Unlock()
}

// Reset clears all state, mount target servers and file system data.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	nics := b.nicIDs()
	b.resetMaps()
	dp := b.dp
	b.mu.Unlock()
	b.Auth.Reset()
	b.dropNics(nics)

	if dp != nil {
		dp.Reset()
	}
}

// Close stops the data plane.
func (b *InMemoryBackend) Close() {
	b.mu.RLock("Close")
	dp, fn := b.dp, b.onClose
	b.mu.RUnlock()

	if dp != nil {
		dp.Close()
	}

	if fn != nil {
		fn()
	}
}

func (b *InMemoryBackend) nowUTC() time.Time { return b.now().UTC() }

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

// tagsIn carries the tag fields shared by create and update bodies.
type tagsIn struct {
	Freeform map[string]string         `json:"freeformTags"`
	Defined  map[string]map[string]any `json:"definedTags"`
}

func (b *InMemoryBackend) newBase(kind string, t tagsIn, locks []lock) base {
	now := b.nowUTC()
	bs := base{
		ID: newOCID(kind), State: stCreating, TimeCreated: now, FreeformTags: t.Freeform, DefinedTags: t.Defined,
		Locks: []lock{},
	}

	if bs.FreeformTags == nil {
		bs.FreeformTags = map[string]string{}
	}

	if bs.DefinedTags == nil {
		bs.DefinedTags = map[string]map[string]any{}
	}

	for _, l := range locks {
		l.TimeCreated = &now
		bs.Locks = append(bs.Locks, l)
	}

	return bs
}

// put stores a new resource, scheduling its move from CREATING to ACTIVE (or settling it at once).
func put[W resource](b *InMemoryBackend, t table[W], w W, active string) *entry[W] {
	e := &entry[W]{W: w, Etag: newEtag(), Seq: b.nextSeq()}
	t[w.bs().ID] = e
	moveTo(b, e, active)

	return e
}

// moveTo schedules the transition of e to state after the configured delay, or applies it at once.
func moveTo[W resource](b *InMemoryBackend, e *entry[W], state string) {
	if b.transition <= 0 {
		e.W.bs().State, e.At, e.To = state, nil, ""
		stampDeleted(e, b.nowUTC())

		return
	}

	at := b.nowUTC().Add(b.transition)
	e.At, e.To = &at, state
}

func stampDeleted[W resource](e *entry[W], now time.Time) {
	if e.W.bs().State == stDeleted && e.Gone == nil {
		e.Gone = &now
	}
}

// begin sets an interim state that settles to final after the transition delay.
func begin[W resource](b *InMemoryBackend, e *entry[W], interim, final string) {
	e.W.bs().State = interim
	e.Etag = newEtag()
	moveTo(b, e, final)
}

func settleDue[W resource](t table[W], now time.Time, onSettle func(*entry[W])) {
	for id, e := range t {
		bs := e.W.bs()

		if e.At != nil && !e.At.After(now) {
			bs.State, e.At, e.To = e.To, nil, ""
			e.Etag = newEtag()
			stampDeleted(e, now)

			if onSettle != nil {
				onSettle(e)
			}
		}

		if bs.State == stDeleted && e.Gone != nil && now.Sub(*e.Gone) > deletedRetention {
			delete(t, id)
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

func paginate[T any](items []T, o ListOpts, name func(T) string, created func(T) (time.Time, int64)) page.Page[T] {
	byName := strings.EqualFold(o.SortBy, "displayName") || strings.EqualFold(o.SortBy, "path")
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

// listOf filters, sorts and pages a table, rendering each entry with render.
func listOf[W resource, V any](
	t table[W], keep func(*entry[W]) bool, o ListOpts, name func(W) string, render func(*entry[W]) V,
) page.Page[V] {
	var all []*entry[W]

	for _, e := range t {
		if keep(e) {
			all = append(all, e)
		}
	}

	pg := paginate(all, o,
		func(e *entry[W]) string { return name(e.W) },
		func(e *entry[W]) (time.Time, int64) { return e.W.bs().TimeCreated, e.Seq },
	)

	out := make([]V, 0, len(pg.Data))
	for _, e := range pg.Data {
		out = append(out, render(e))
	}

	return page.Page[V]{Data: out, Next: pg.Next}
}

// scope describes how a resource matches the compartment, availability domain and name filters of a list.
type scope[W resource] struct {
	fields func(W) (compartment, ad, name string)
	extra  func(W) bool
	render func(*entry[W]) map[string]any
}

// listScoped lists the resources of a compartment and availability domain. Callers hold the read lock.
func listScoped[W resource](
	b *InMemoryBackend, t table[W], f MTFilter, o ListOpts, sc scope[W],
) (page.Page[map[string]any], error) {
	if f.Compartment == "" || f.AD == "" {
		return page.Page[map[string]any]{}, errMissing("compartmentId and availabilityDomain are required")
	}

	if aerr := b.checkCompartment(f.Compartment); aerr != nil {
		return page.Page[map[string]any]{}, aerr
	}

	keep := func(e *entry[W]) bool {
		comp, ad, name := sc.fields(e.W)
		bs := e.W.bs()

		return comp == f.Compartment && ad == f.AD && (f.State == "" || bs.State == f.State) &&
			(f.ID == "" || bs.ID == f.ID) && (f.DisplayName == "" || name == f.DisplayName) &&
			(sc.extra == nil || sc.extra(e.W))
	}

	return listOf(t, keep, o, func(w W) string {
		_, _, n := sc.fields(w)

		return n
	}, sc.render), nil
}

// Sweep applies due time-based transitions and schedules against the backend clock.
func (b *InMemoryBackend) Sweep() {
	b.mu.Lock("Sweep")
	defer b.mu.Unlock()

	now := b.nowUTC()
	settleDue(b.fss, now, nil)
	settleDue(b.mts, now, b.mountTargetSettled)
	settleDue(b.sets, now, nil)
	settleDue(b.exps, now, nil)
	settleDue(b.snaps, now, nil)
	settleDue(b.pols, now, nil)
	settleDue(b.reps, now, nil)
	settleDue(b.tgts, now, nil)
	settleDue(b.conns, now, nil)
	settleDue(b.quotas, now, nil)
	b.sweepFileSystems(now)
	b.sweepMountTargets(now)
	b.sweepSnapshots(now)
	b.sweepPolicies(now)
	b.sweepReplications(now)
}
