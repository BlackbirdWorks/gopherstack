package gcpfirestore

import (
	"crypto/rand"
	"encoding/hex"
	"math/big"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	historyWindow = time.Hour
	txnTTL        = 270 * time.Second
	sweepEvery    = time.Minute
	txnIDBytes    = 16
)

type docVer struct {
	at  time.Time
	doc *firestorepb.Document
}

// docRec is the version history of one document; a nil doc marks a deletion.
type docRec struct {
	vers []docVer
}

func (r *docRec) cur() *firestorepb.Document {
	if len(r.vers) == 0 {
		return nil
	}

	return r.vers[len(r.vers)-1].doc
}

func (r *docRec) latest() time.Time {
	if len(r.vers) == 0 {
		return time.Time{}
	}

	return r.vers[len(r.vers)-1].at
}

func (r *docRec) at(t time.Time) *firestorepb.Document {
	if t.IsZero() {
		return r.cur()
	}

	for _, v := range slices.Backward(r.vers) {
		if !v.at.After(t) {
			return v.doc
		}
	}

	return nil
}

func (r *docRec) prune(now time.Time) {
	cutoff := now.Add(-historyWindow)
	keep := 0

	for i := 0; i+1 < len(r.vers); i++ {
		if !r.vers[i+1].at.After(cutoff) {
			keep = i + 1
		}
	}

	r.vers = r.vers[keep:]
}

type scope struct {
	parent       string
	collectionID string
	descendants  bool
}

type txnRec struct {
	created  time.Time
	readTime time.Time
	docs     map[string]struct{}
	scopes   []scope
	readOnly bool
}

// InMemoryBackend holds every document, transaction and listener under one lock.
type InMemoryBackend struct {
	now        func() time.Time
	mu         *lockmetrics.RWMutex
	docs       map[string]*docRec
	txns       map[string]*txnRec
	subs       map[chan struct{}]struct{}
	lastCommit time.Time
	lastSweep  time.Time
	observers  []DocumentObserver
	pending    []DocumentEvent
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		mu:   lockmetrics.New("gcpfirestore"),
		now:  time.Now,
		docs: make(map[string]*docRec),
		txns: make(map[string]*txnRec),
		subs: make(map[chan struct{}]struct{}),
	}
}

// SetClock replaces the backend clock.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.now = now
}

// Reset clears all documents and transactions.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.docs = make(map[string]*docRec)
	b.txns = make(map[string]*txnRec)
	b.lastCommit = time.Time{}
	b.notifyLocked()
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

// Subscribe returns a channel signalled after every state change and a cancel function.
func (b *InMemoryBackend) Subscribe() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)

	b.mu.Lock("Subscribe")
	b.subs[ch] = struct{}{}
	b.mu.Unlock()

	return ch, func() {
		b.mu.Lock("Unsubscribe")
		delete(b.subs, ch)
		b.mu.Unlock()
	}
}

func (b *InMemoryBackend) notifyLocked() {
	for ch := range b.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (b *InMemoryBackend) sweepLocked(now time.Time) {
	if now.Sub(b.lastSweep) < sweepEvery {
		return
	}

	b.lastSweep = now

	for name, rec := range b.docs {
		rec.prune(now)

		if rec.cur() == nil && len(rec.vers) == 1 && now.Sub(rec.latest()) > historyWindow {
			delete(b.docs, name)
		}
	}

	for id, t := range b.txns {
		if now.Sub(t.created) > txnTTL {
			delete(b.txns, id)
		}
	}
}

func newTxnID() (string, error) {
	buf := make([]byte, txnIDBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", errInvalid("transaction id generation failed: %v", err)
	}

	return hex.EncodeToString(buf), nil
}

func newDocID() string {
	out := make([]byte, autoIDLen)
	limit := big.NewInt(int64(len(autoIDAlphas)))

	for i := range out {
		n, err := rand.Int(rand.Reader, limit)
		if err != nil {
			n = big.NewInt(int64(i))
		}

		out[i] = autoIDAlphas[n.Int64()]
	}

	return string(out)
}

func (b *InMemoryBackend) beginLocked(opts *firestorepb.TransactionOptions) (string, *txnRec, error) {
	id, err := newTxnID()
	if err != nil {
		return "", nil, err
	}

	now := b.readNowLocked()
	b.lastCommit = now
	t := &txnRec{created: now, readTime: now, docs: map[string]struct{}{}}

	if ro := opts.GetReadOnly(); ro != nil {
		t.readOnly = true

		if rt := ro.GetReadTime(); rt != nil {
			if verr := rt.CheckValid(); verr != nil {
				return "", nil, errInvalid("Invalid read_time: %v", verr)
			}

			t.readTime = rt.AsTime()
		}
	}

	b.sweepLocked(now)
	b.txns[id] = t

	return id, t, nil
}

// BeginTransaction starts a read-write or read-only transaction.
func (b *InMemoryBackend) BeginTransaction(req *firestorepb.BeginTransactionRequest) ([]byte, error) {
	if err := validateDatabase(req.GetDatabase()); err != nil {
		return nil, err
	}

	b.mu.Lock("BeginTransaction")
	defer b.mu.Unlock()

	if old := req.GetOptions().GetReadWrite().GetRetryTransaction(); len(old) > 0 {
		delete(b.txns, string(old))
	}

	id, _, err := b.beginLocked(req.GetOptions())
	if err != nil {
		return nil, err
	}

	return []byte(id), nil
}

// Rollback discards a transaction.
func (b *InMemoryBackend) Rollback(req *firestorepb.RollbackRequest) error {
	if err := validateDatabase(req.GetDatabase()); err != nil {
		return err
	}

	b.mu.Lock("Rollback")
	defer b.mu.Unlock()

	if _, ok := b.txns[string(req.GetTransaction())]; !ok {
		return errInvalid("Transaction ID is invalid or has expired.")
	}

	delete(b.txns, string(req.GetTransaction()))

	return nil
}

type readSel struct {
	readTime *timestamppb.Timestamp
	newTxn   *firestorepb.TransactionOptions
	txn      []byte
}

func (s readSel) usesTxn() bool { return len(s.txn) > 0 || s.newTxn != nil }

// readView is the resolved snapshot a read executes against.
type readView struct {
	txn      *txnRec
	at       time.Time
	readTime time.Time
	newTxnID []byte
}

func (b *InMemoryBackend) resolve(sel readSel) (*readView, error) {
	switch {
	case len(sel.txn) > 0:
		t, ok := b.txns[string(sel.txn)]
		if !ok {
			return nil, errInvalid("Transaction ID is invalid or has expired.")
		}

		if t.readOnly {
			return &readView{at: t.readTime, readTime: t.readTime}, nil
		}

		return &readView{txn: t, readTime: b.readNowLocked()}, nil
	case sel.newTxn != nil:
		id, t, err := b.beginLocked(sel.newTxn)
		if err != nil {
			return nil, err
		}

		rv := &readView{newTxnID: []byte(id), readTime: t.readTime}
		if t.readOnly {
			rv.at = t.readTime
		} else {
			rv.txn = t
		}

		return rv, nil
	case sel.readTime != nil:
		if err := sel.readTime.CheckValid(); err != nil {
			return nil, errInvalid("Invalid read_time: %v", err)
		}

		t := sel.readTime.AsTime()
		if t.After(b.readNowLocked()) {
			return nil, errInvalid("read_time must not be in the future.")
		}

		return &readView{at: t, readTime: t}, nil
	}

	return &readView{readTime: b.readNowLocked()}, nil
}

// read resolves sel and runs fn; transaction-bound reads take the write lock because they record what they read.
func (b *InMemoryBackend) read(sel readSel, fn func(rv *readView) error) error {
	if sel.usesTxn() {
		b.mu.Lock("read")
		defer b.mu.Unlock()
	} else {
		b.mu.RLock("read")
		defer b.mu.RUnlock()
	}

	rv, err := b.resolve(sel)
	if err != nil {
		return err
	}

	return fn(rv)
}

func (rv *readView) noteDoc(name string) {
	if rv.txn != nil {
		rv.txn.docs[name] = struct{}{}
	}
}

func (rv *readView) noteScope(s scope) {
	if rv.txn != nil {
		rv.txn.scopes = append(rv.txn.scopes, s)
	}
}

func (b *InMemoryBackend) docAt(name string, rv *readView) *firestorepb.Document {
	rv.noteDoc(name)

	rec := b.docs[name]
	if rec == nil {
		return nil
	}

	return rec.at(rv.at)
}

func (b *InMemoryBackend) inScope(name string, s scope) bool {
	return matchesCollection(name, s.parent, s.collectionID, s.descendants)
}

// conflictLocked reports whether anything a read-write transaction read has changed since it began.
func (b *InMemoryBackend) conflictLocked(t *txnRec) bool {
	for name := range t.docs {
		if rec := b.docs[name]; rec != nil && rec.latest().After(t.readTime) {
			return true
		}
	}

	for name, rec := range b.docs {
		if !rec.latest().After(t.readTime) {
			continue
		}

		for _, s := range t.scopes {
			if b.inScope(name, s) {
				return true
			}
		}
	}

	return false
}

// matchesCollection reports whether the document name is a direct child of parent/collectionID, or
// (descendants) lives under parent in any collection named collectionID ("" matches any).
func matchesCollection(name, parent, collectionID string, descendants bool) bool {
	rest, ok := strings.CutPrefix(name, parent+"/")
	if !ok {
		return false
	}

	segs := strings.Split(rest, "/")
	if len(segs) < 2 { //nolint:mnd // collection/document pair
		return false
	}

	if !descendants {
		return len(segs) == 2 && segs[0] == collectionID
	}

	return collectionID == "" || segs[len(segs)-2] == collectionID
}
