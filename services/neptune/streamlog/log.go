// Package streamlog is the Neptune Streams commit log: an ordered, expiring record of committed graph changes.
package streamlog

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// Op values of a change record.
const (
	OpAdd    = "ADD"
	OpRemove = "REMOVE"
)

// Iterator types accepted by Read.
const (
	AtSequenceNumber    = "AT_SEQUENCE_NUMBER"
	AfterSequenceNumber = "AFTER_SEQUENCE_NUMBER"
	TrimHorizon         = "TRIM_HORIZON"
	Latest              = "LATEST"
)

// DefaultRetention is the documented default of neptune_streams_expiry_days (7 days).
const DefaultRetention = 7 * 24 * time.Hour

var (
	// ErrNotFound means the requested sequence number is past the newest record.
	ErrNotFound = errors.New("stream records not found")
	// ErrExpired means the requested sequence number was already trimmed.
	ErrExpired = errors.New("stream records expired")
	// ErrInvalid means the iterator or sequence number is malformed.
	ErrInvalid = errors.New("invalid stream request")
)

// Value is a PG_JSON typed value.
type Value struct {
	Value    any    `json:"value"`
	DataType string `json:"dataType"`
}

// Data is the serialised change: PG_JSON fields for property graphs, Stmt (an N-Quads line) for SPARQL.
type Data struct {
	Value *Value `json:"value,omitempty"`
	ID    string `json:"id,omitempty"`
	Type  string `json:"type,omitempty"`
	Key   string `json:"key,omitempty"`
	From  string `json:"from,omitempty"`
	To    string `json:"to,omitempty"`
	Stmt  string `json:"stmt,omitempty"`
}

// Change is one operation of a transaction, before it is numbered.
type Change struct {
	Op   string
	Data Data
}

// Record is a numbered change as stored and served.
type Record struct {
	Op          string `json:"op"`
	Data        Data   `json:"data"`
	CommitNum   int64  `json:"commitNum"`
	OpNum       int64  `json:"opNum"`
	TimestampMs int64  `json:"commitTimestamp"`
	IsLastOp    bool   `json:"isLastOp,omitempty"`
}

// Policy gates recording and sets retention; it mirrors neptune_streams and neptune_streams_expiry_days.
type Policy struct {
	Retention time.Duration
	Enabled   bool
}

// Log is a concurrency-safe commit log.
type Log struct {
	now     func() time.Time
	mu      *lockmetrics.RWMutex
	records []Record
	policy  Policy
	commit  int64
	trimmed int64
}

// New returns an empty, disabled log.
func New() *Log {
	return &Log{
		now: time.Now, mu: lockmetrics.New("neptune-streamlog"), policy: Policy{Retention: DefaultRetention},
	}
}

// SetPolicy replaces the recording policy.
func (l *Log) SetPolicy(p Policy) {
	if p.Retention <= 0 {
		p.Retention = DefaultRetention
	}

	l.mu.Lock("SetPolicy")
	l.policy = p
	l.mu.Unlock()
}

// SetClock replaces the time source.
func (l *Log) SetClock(now func() time.Time) {
	l.mu.Lock("SetClock")
	l.now = now
	l.mu.Unlock()
}

// Policy returns the current policy.
func (l *Log) Policy() Policy {
	l.mu.RLock("Policy")
	defer l.mu.RUnlock()

	return l.policy
}

// Enabled reports whether commits are being recorded.
func (l *Log) Enabled() bool { return l.Policy().Enabled }

// Commit records one transaction's changes under the next commit number; it is a no-op when disabled or empty.
func (l *Log) Commit(changes []Change) {
	if len(changes) == 0 {
		return
	}

	l.mu.Lock("Commit")
	defer l.mu.Unlock()

	if !l.policy.Enabled {
		return
	}

	l.trimLocked()
	l.commit++
	ts := l.now().UnixMilli()

	for i, c := range changes {
		l.records = append(l.records, Record{
			CommitNum: l.commit, OpNum: int64(i + 1), TimestampMs: ts, Op: c.Op, Data: c.Data,
			IsLastOp: i == len(changes)-1,
		})
	}
}

func (l *Log) trimLocked() {
	cutoff := l.now().Add(-l.policy.Retention).UnixMilli()
	n := 0

	for n < len(l.records) && l.records[n].TimestampMs < cutoff {
		l.trimmed = l.records[n].CommitNum
		n++
	}

	l.records = l.records[n:]
}

// Query selects where to start reading.
type Query struct {
	Iterator  string
	CommitNum int64
	OpNum     int64
	Limit     int
}

// Page is the result of a read: lastEvent is the id of the final record, or the newest record when empty.
type Page struct {
	Records    []Record
	LastCommit int64
	LastOp     int64
	LastTimeMs int64
}

// Read returns up to q.Limit records from the requested position.
func (l *Log) Read(q Query) (Page, error) {
	l.mu.Lock("Read")
	l.trimLocked()
	defer l.mu.Unlock()

	start, err := l.startLocked(q)
	if err != nil {
		return Page{}, err
	}

	end := min(len(l.records), start+max(q.Limit, 1))
	if q.Iterator == Latest {
		end = min(end, start+1)
	}

	var p Page

	p.Records = append([]Record(nil), l.records[start:end]...)

	last := Record{}
	switch {
	case len(p.Records) > 0:
		last = p.Records[len(p.Records)-1]
	case len(l.records) > 0:
		last = l.records[len(l.records)-1]
	}

	p.LastCommit, p.LastOp, p.LastTimeMs = last.CommitNum, last.OpNum, last.TimestampMs

	return p, nil
}

func (l *Log) startLocked(q Query) (int, error) {
	switch q.Iterator {
	case TrimHorizon:
		return 0, nil
	case Latest:
		return max(len(l.records)-1, 0), nil
	case AtSequenceNumber, AfterSequenceNumber:
		return l.seekLocked(q)
	default:
		return 0, fmt.Errorf("%w: iteratorType %q", ErrInvalid, q.Iterator)
	}
}

func (l *Log) checkRangeLocked(q Query) error {
	switch {
	case q.CommitNum < 1 || q.OpNum < 1:
		return fmt.Errorf("%w: commitNum and opNum must be positive", ErrInvalid)
	case q.CommitNum > l.commit:
		return fmt.Errorf("%w: %d:%d", ErrNotFound, q.CommitNum, q.OpNum)
	case q.CommitNum <= l.trimmed:
		return fmt.Errorf("%w: %d:%d", ErrExpired, q.CommitNum, q.OpNum)
	}

	return nil
}

func (l *Log) seekLocked(q Query) (int, error) {
	if err := l.checkRangeLocked(q); err != nil {
		return 0, err
	}

	idx := len(l.records)

	for i, r := range l.records {
		if r.CommitNum > q.CommitNum || (r.CommitNum == q.CommitNum && r.OpNum >= q.OpNum) {
			idx = i

			break
		}
	}

	exact := idx < len(l.records) && l.records[idx].CommitNum == q.CommitNum && l.records[idx].OpNum == q.OpNum

	switch {
	case q.Iterator == AfterSequenceNumber && exact:
		return idx + 1, nil
	case q.Iterator == AtSequenceNumber && !exact:
		return 0, fmt.Errorf("%w: %d:%d", ErrNotFound, q.CommitNum, q.OpNum)
	default:
		return idx, nil
	}
}

type dump struct {
	Records []Record `json:"records"`
	Commit  int64    `json:"commit"`
	Trimmed int64    `json:"trimmed"`
}

// Export serialises the log for persistence; nil when nothing was ever recorded.
func (l *Log) Export() ([]byte, error) {
	l.mu.RLock("Export")
	defer l.mu.RUnlock()

	if l.commit == 0 {
		return nil, nil
	}

	b, err := json.Marshal(dump{Records: l.records, Commit: l.commit, Trimmed: l.trimmed}, jsonx.V1())
	if err != nil {
		return nil, fmt.Errorf("encode stream log: %w", err)
	}

	return b, nil
}

// Import replaces the log contents with an Export dump; the policy is kept.
func (l *Log) Import(data []byte) error {
	var d dump
	if err := json.Unmarshal(data, &d, jsonx.V1()); err != nil {
		return fmt.Errorf("decode stream log: %w", err)
	}

	l.mu.Lock("Import")
	l.records, l.commit, l.trimmed = d.Records, d.Commit, d.Trimmed
	l.mu.Unlock()

	return nil
}

// Reset discards every record and restarts commit numbering.
func (l *Log) Reset() {
	l.mu.Lock("Reset")
	l.records, l.commit, l.trimmed = nil, 0, 0
	l.mu.Unlock()
}

// Size returns the retained record count and their serialised size in bytes.
func (l *Log) Size() (int, int) {
	l.mu.RLock("Size")
	defer l.mu.RUnlock()

	if len(l.records) == 0 {
		return 0, 0
	}

	b, err := json.Marshal(l.records, jsonx.V1())
	if err != nil {
		return len(l.records), 0
	}

	return len(l.records), len(b)
}
