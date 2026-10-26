package gcpdatastore

import (
	"crypto/rand"
	"math/big"
	"slices"
	"time"

	"cloud.google.com/go/datastore/admin/apiv1/adminpb"
	"cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	historyWindow = time.Hour
	txnTTL        = 270 * time.Second
	txnIdleTTL    = 60 * time.Second
	sweepEvery    = time.Minute
	txnIDBytes    = 16
	idSpace       = 1_000_000_000_000_000
)

type entVer struct {
	at      time.Time
	created time.Time
	ent     *datastorepb.Entity
	version int64
}

// entRec is the version history of one entity; a nil ent marks a deletion.
type entRec struct {
	key  *datastorepb.Key
	vers []entVer
}

func (r *entRec) cur() *entVer {
	if len(r.vers) == 0 || r.vers[len(r.vers)-1].ent == nil {
		return nil
	}

	return &r.vers[len(r.vers)-1]
}

func (r *entRec) latest() time.Time {
	if len(r.vers) == 0 {
		return time.Time{}
	}

	return r.vers[len(r.vers)-1].at
}

func (r *entRec) at(t time.Time) *entVer {
	for _, v := range slices.Backward(r.vers) {
		if !v.at.After(t) {
			if v.ent == nil {
				return nil
			}

			return &v
		}
	}

	return nil
}

func (r *entRec) prune(now time.Time) {
	cutoff := now.Add(-historyWindow)
	keep := 0

	for i := 0; i+1 < len(r.vers); i++ {
		if !r.vers[i+1].at.After(cutoff) {
			keep = i + 1
		}
	}

	r.vers = r.vers[keep:]
}

// change records one committed write for optimistic conflict detection.
type change struct {
	at  time.Time
	key *datastorepb.Key
	ks  string
}

// queryScope is what a transactional query read: entities of kind (or any kind) under ancestor.
type queryScope struct {
	ancestor *datastorepb.Key
	part     partition
	kind     string
}

func (q queryScope) covers(k *datastorepb.Key) bool {
	if keyPartition(k) != q.part {
		return false
	}

	if q.kind != "" && keyKind(k) != q.kind {
		return false
	}

	return q.ancestor == nil || isDescendantOrSelf(k, q.ancestor)
}

type txnRec struct {
	created  time.Time
	lastUsed time.Time
	readTime time.Time
	reads    map[string]struct{}
	part     partition
	queries  []queryScope
	readOnly bool
}

// InMemoryBackend holds every entity, transaction, reserved id and index under one lock.
type InMemoryBackend struct {
	now            func() time.Time
	mu             *lockmetrics.RWMutex
	ents           map[string]*entRec
	txns           map[string]*txnRec
	ended          map[string]time.Time
	used           map[string]map[int64]struct{}
	indexes        map[string]*adminpb.Index
	ops            *gcplro.Store
	lastCommit     time.Time
	lastSweep      time.Time
	changes        []change
	nextIndex      int64
	enforceIndexes bool
}

// NewInMemoryBackend creates an empty backend that enforces composite indexes.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		mu:             lockmetrics.New("gcpdatastore"),
		now:            time.Now,
		ents:           make(map[string]*entRec),
		txns:           make(map[string]*txnRec),
		ended:          make(map[string]time.Time),
		used:           make(map[string]map[int64]struct{}),
		indexes:        make(map[string]*adminpb.Index),
		ops:            gcplro.New("gcpdatastore"),
		enforceIndexes: true,
	}
}

// SetClock replaces the backend clock.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.now = now
}

// SetEnforceIndexes toggles FAILED_PRECONDITION for queries that need a missing composite index.
func (b *InMemoryBackend) SetEnforceIndexes(on bool) {
	b.mu.Lock("SetEnforceIndexes")
	defer b.mu.Unlock()

	b.enforceIndexes = on
}

// Reset clears all entities, transactions, reserved ids, indexes and operations.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	b.ents = make(map[string]*entRec)
	b.txns = make(map[string]*txnRec)
	b.ended = make(map[string]time.Time)
	b.used = make(map[string]map[int64]struct{})
	b.indexes = make(map[string]*adminpb.Index)
	b.changes = nil
	b.nextIndex = 0
	b.lastCommit = time.Time{}
	b.mu.Unlock()
	b.ops.Reset()
}

func (b *InMemoryBackend) wallLocked() time.Time { return b.now().UTC().Truncate(time.Microsecond) }

// readNowLocked is the read time of a latest-state read; it never precedes a commit.
func (b *InMemoryBackend) readNowLocked() time.Time {
	t := b.wallLocked()
	if t.Before(b.lastCommit) {
		return b.lastCommit
	}

	return t
}

func (b *InMemoryBackend) sweepLocked(now time.Time) {
	if now.Sub(b.lastSweep) < sweepEvery {
		return
	}

	b.lastSweep = now

	for ks, rec := range b.ents {
		rec.prune(now)

		if rec.cur() == nil && len(rec.vers) == 1 && now.Sub(rec.latest()) > historyWindow {
			delete(b.ents, ks)
		}
	}

	for id, at := range b.ended {
		if now.Sub(at) > txnTTL {
			delete(b.ended, id)
		}
	}

	oldest := now
	for id, t := range b.txns {
		if now.Sub(t.created) > txnTTL {
			delete(b.txns, id)

			continue
		}

		if t.readTime.Before(oldest) {
			oldest = t.readTime
		}
	}

	b.changes = slices.DeleteFunc(b.changes, func(c change) bool { return c.at.Before(oldest.Add(-time.Second)) })
}

func newTxnID() (string, error) {
	buf := make([]byte, txnIDBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", errInvalid("transaction id generation failed: %v", err)
	}

	return string(buf), nil
}

func (b *InMemoryBackend) beginLocked(opts *datastorepb.TransactionOptions, part partition) (string, *txnRec, error) {
	id, err := newTxnID()
	if err != nil {
		return "", nil, err
	}

	now := b.wallLocked()
	t := &txnRec{created: now, lastUsed: now, readTime: b.readNowLocked(), reads: map[string]struct{}{}, part: part}

	if ro := opts.GetReadOnly(); ro != nil {
		t.readOnly = true

		if ro.GetReadTime() != nil {
			if t.readTime, err = b.checkReadTimeLocked(ro.GetReadTime()); err != nil {
				return "", nil, err
			}
		}
	}

	b.txns[id] = t

	return id, t, nil
}

func (b *InMemoryBackend) checkReadTimeLocked(ts *timestamppb.Timestamp) (time.Time, error) {
	t := ts.AsTime().UTC().Truncate(time.Microsecond)
	now := b.readNowLocked()

	if t.After(now) {
		return time.Time{}, errInvalid("The requested read time is in the future.")
	}

	if now.Sub(t) > historyWindow {
		return time.Time{}, errInvalid("The requested read time is more than one hour in the past.")
	}

	return t, nil
}

func (b *InMemoryBackend) txnLocked(id []byte, part partition) (*txnRec, error) {
	t, ok := b.txns[string(id)]
	if !ok {
		return nil, errBadTxn()
	}

	now := b.wallLocked()
	if now.Sub(t.created) > txnTTL || now.Sub(t.lastUsed) > txnIdleTTL {
		delete(b.txns, string(id))

		return nil, errBadTxn()
	}

	if t.part.project != part.project || t.part.database != part.database {
		return nil, errBadTxn()
	}

	t.lastUsed = now

	return t, nil
}

// readView is a consistent snapshot a read runs against.
type readView struct {
	at       time.Time
	txn      *txnRec
	newTxnID []byte
}

// read runs fn against the snapshot ro selects, holding the lock fn needs.
func (b *InMemoryBackend) read(ro *datastorepb.ReadOptions, part partition, fn func(rv *readView) error) error {
	switch ct := ro.GetConsistencyType().(type) {
	case *datastorepb.ReadOptions_Transaction:
		b.mu.Lock("ReadTxn")
		defer b.mu.Unlock()

		t, err := b.txnLocked(ct.Transaction, part)
		if err != nil {
			return err
		}

		return fn(&readView{at: t.readTime, txn: t})
	case *datastorepb.ReadOptions_NewTransaction:
		b.mu.Lock("ReadNewTxn")
		defer b.mu.Unlock()

		id, t, err := b.beginLocked(ct.NewTransaction, part)
		if err != nil {
			return err
		}

		return fn(&readView{at: t.readTime, txn: t, newTxnID: []byte(id)})
	case *datastorepb.ReadOptions_ReadTime:
		b.mu.RLock("ReadAt")
		defer b.mu.RUnlock()

		at, err := b.checkReadTimeLocked(ct.ReadTime)
		if err != nil {
			return err
		}

		return fn(&readView{at: at})
	default:
		b.mu.RLock("Read")
		defer b.mu.RUnlock()

		return fn(&readView{at: b.readNowLocked()})
	}
}

func (b *InMemoryBackend) entityAt(ks string, rv *readView) *entVer {
	rec := b.ents[ks]
	if rec == nil {
		return nil
	}

	return rec.at(rv.at)
}

// reserveIDLocked records an explicit numeric id so auto-allocation never hands it out.
func (b *InMemoryBackend) reserveIDLocked(p partition, id int64) {
	m := b.used[p.String()]
	if m == nil {
		m = make(map[int64]struct{})
		b.used[p.String()] = m
	}

	m[id] = struct{}{}
}

func (b *InMemoryBackend) allocIDLocked(p partition) (int64, error) {
	for range 100 {
		n, err := rand.Int(rand.Reader, big.NewInt(idSpace))
		if err != nil {
			return 0, errInvalid("id generation failed: %v", err)
		}

		id := n.Int64() + 1
		if _, taken := b.used[p.String()][id]; taken {
			continue
		}

		b.reserveIDLocked(p, id)

		return id, nil
	}

	return 0, errInvalid("could not allocate a unique id")
}

// endTxnLocked removes a finished transaction but remembers it so a late Rollback stays a no-op.
func (b *InMemoryBackend) endTxnLocked(id string) {
	if _, ok := b.txns[id]; ok {
		delete(b.txns, id)
		b.ended[id] = b.wallLocked()
	}
}
