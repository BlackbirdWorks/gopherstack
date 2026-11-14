package ocidatabase

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
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	// Region is the fixed region key baked into emulated OCIDs.
	Region           = "iad"
	regionName       = "us-ashburn-1"
	defaultListLimit = 100
	ocidTailLen      = 52
	uniqueIDLen      = 14
	defaultTransit   = 2 * time.Second
	actionUpdated    = "UPDATED"
	wrInProgress     = "IN_PROGRESS"
	wrSucceeded      = "SUCCEEDED"
	wrFailed         = "FAILED"
)

// SubnetInfo describes a subnet known to OCI Core.
type SubnetInfo struct {
	CIDR string
}

// InMemoryBackend holds every Autonomous Database, backup and work request under one lock.
type InMemoryBackend struct {
	mu          *lockmetrics.RWMutex
	now         func() time.Time
	runCtx      context.Context //nolint:containedctx // lifetime of the asynchronous engine work
	cancel      context.CancelFunc
	dbs         map[string]*ADB
	backups     map[string]*Backup
	workReqs    map[string]*WorkRequest
	tokens      map[string]string
	pki         map[string]*security
	Auth        *ociauth.Registry
	compartment func(string) bool
	subnet      func(string) (SubnetInfo, bool)
	eng         *dbEngine
	onClose     func()
	wg          sync.WaitGroup
	transition  time.Duration
	seq         int64
}

// NewInMemoryBackend creates an empty backend running in stub mode.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		mu: lockmetrics.New("ocidatabase"), now: time.Now, transition: defaultTransit, Auth: ociauth.NewRegistry(),
	}
	b.resetMaps()

	return b
}

func (b *InMemoryBackend) resetMaps() {
	b.runCtx, b.cancel = context.WithCancel(context.Background())
	b.dbs, b.backups, b.workReqs = map[string]*ADB{}, map[string]*Backup{}, map[string]*WorkRequest{}
	b.tokens, b.pki = map[string]string{}, map[string]*security{}
}

// SetClock injects the time source used for lifecycle transitions.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	b.now = now
	b.mu.Unlock()
}

// SetTransition sets how long asynchronous stub-mode operations take to complete.
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

// SetSubnetResolver validates subnetId arguments against another service (OCI Core).
func (b *InMemoryBackend) SetSubnetResolver(fn func(string) (SubnetInfo, bool)) {
	b.mu.Lock("SetSubnetResolver")
	b.subnet = fn
	b.mu.Unlock()
}

// Reset clears all state and stops every running database.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	b.cancel()
	live := b.detachAll()
	b.resetMaps()
	b.mu.Unlock()
	b.Auth.Reset()
	b.wg.Wait()
	closeAll(live)
}

// Close stops every running database.
func (b *InMemoryBackend) Close() {
	b.mu.Lock("Close")
	b.cancel()
	live := b.detachAll()
	b.mu.Unlock()
	b.wg.Wait()
	closeAll(live)

	if b.onClose != nil {
		b.onClose()
	}
}

// SetCloser registers fn to run when the backend closes.
func (b *InMemoryBackend) SetCloser(fn func()) {
	b.mu.Lock("SetCloser")
	b.onClose = fn
	b.mu.Unlock()
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

	for _, db := range b.dbs {
		if db.TransitionAt != nil && !db.TransitionAt.After(now) {
			b.settle(db, db.Target)
		}
	}

	for _, bk := range b.backups {
		if bk.TransitionAt != nil && !bk.TransitionAt.After(now) {
			b.settleBackup(bk)
		}
	}

	for _, wr := range b.workReqs {
		if wr.Status == wrInProgress && wr.FinishAt != nil && !wr.FinishAt.After(now) {
			wr.Status, wr.FinishedAt = wrSucceeded, wr.FinishAt
		}
	}
}

func (b *InMemoryBackend) settleBackup(bk *Backup) {
	now := b.nowUTC()

	switch bk.State {
	case backupCreating:
		bk.State, bk.Ended = backupActive, &now
	case backupDeleting:
		bk.State = backupDeleted
	}

	bk.TransitionAt = nil
}

// settle moves db to state and clears the pending transition. Callers hold the write lock.
func (b *InMemoryBackend) settle(db *ADB, state string) {
	prev := db.State
	db.State, db.Target, db.TransitionAt, db.Details, db.Etag = state, "", nil, "", newEtag()

	if prev == statePending && state == stateAvailable {
		b.addAutomaticBackup(db)
	}
}
