package gcpbigquery

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpaudit"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/safemap"
	"github.com/blackbirdworks/gopherstack/services/gcpbigquery/bqsql"
)

// InMemoryBackend holds every dataset, table, row and job under one lock.
type InMemoryBackend struct {
	mu         *lockmetrics.RWMutex
	datasets   map[string]*Dataset
	jobs       map[string]*Job
	runs       *safemap.Map[string, *jobRun]
	res        *reservationState
	baseCancel context.CancelFunc
	startHook  func(ctx context.Context, phase, project, id string)
	objects    ObjectStore
	baseCtx    context.Context
	// Audit records Cloud Audit Logs entries for dataset, table and job changes.
	Audit gcpaudit.Emitter
	wg    sync.WaitGroup
	seq   int64
	// extractShardBytes is the logical size at which an extract shards (default 1 GiB).
	extractShardBytes int64
	loadMaxBytes      int64
	loadMaxFiles      int
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend() *InMemoryBackend {
	ctx, cancel := context.WithCancel(context.Background())

	return &InMemoryBackend{
		mu:         lockmetrics.New("gcpbigquery"),
		datasets:   make(map[string]*Dataset),
		jobs:       make(map[string]*Job),
		res:        newReservationState(),
		runs:       safemap.New[string, *jobRun]("gcpbigquery.runs"),
		baseCtx:    ctx,
		baseCancel: cancel,
	}
}

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.datasets = make(map[string]*Dataset)
	b.jobs = make(map[string]*Job)
	b.res = newReservationState()
	b.seq = 0
}

// SetExtractShardBytes sets the shard size of extract jobs; n <= 0 restores the 1 GiB default.
func (b *InMemoryBackend) SetExtractShardBytes(n int64) {
	b.mu.Lock("SetExtractShardBytes")
	defer b.mu.Unlock()

	b.extractShardBytes = n
}

// SetObjectStore wires the object store load and extract jobs read from and write to.
func (b *InMemoryBackend) SetObjectStore(s ObjectStore) {
	b.mu.Lock("SetObjectStore")
	defer b.mu.Unlock()

	b.objects = s
}

func dsKey(project, dataset string) string { return project + "/" + dataset }

func nowMs() int64 { return time.Now().UnixMilli() }

func msString(ms int64) string { return strconv.FormatInt(ms, 10) }

func etagOf(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "|")))

	return base64.StdEncoding.EncodeToString(sum[:12])
}

func datasetIDPattern() *regexp.Regexp { return regexp.MustCompile(`^[A-Za-z0-9_]+$`) }

func tableIDPattern() *regexp.Regexp {
	return regexp.MustCompile(`^[\p{L}\p{M}\p{N}\p{Pc}\p{Pd}\p{Zs}]+$`)
}

// nextSeq returns a strictly increasing counter used for etags and modification times.
func (b *InMemoryBackend) nextSeq() int64 {
	b.seq++

	return b.seq
}

// mergePatch applies an RFC 7396 style merge patch: null deletes a key, objects merge recursively.
func mergePatch(dst, patch map[string]any) {
	for k, v := range patch {
		if v == nil {
			delete(dst, k)

			continue
		}

		pm, isMap := v.(map[string]any)
		dm, dstMap := dst[k].(map[string]any)

		if isMap && dstMap {
			mergePatch(dm, pm)

			continue
		}

		dst[k] = v
	}
}

func cloneMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))

	for k, v := range m {
		if sub, ok := v.(map[string]any); ok {
			out[k] = cloneMap(sub)
		} else {
			out[k] = v
		}
	}

	return out
}

// catalog adapts the backend to the SQL engine; callers hold the backend lock.
type catalog struct {
	b *InMemoryBackend
}

func (c catalog) Table(ref bqsql.TableRef) (*bqsql.TableData, bool) {
	ds, ok := c.b.datasets[dsKey(ref.Project, ref.Dataset)]
	if !ok {
		return nil, false
	}

	t, ok := ds.live(ref.Table)
	if !ok {
		return nil, false
	}

	td := &bqsql.TableData{
		Ref:          ref,
		Fields:       t.Schema,
		Rows:         t.Rows,
		ViewQuery:    t.ViewQuery,
		Materialized: t.MV != nil,
		RowPolicies:  t.policyData(),
	}

	if t.isExternal() {
		externalTableData(c.b, t, td)
	} else if t.requiresPartitionFilter() {
		td.PartitionFilter = t.partitionColumns()
	}

	return td, true
}

// TableAt implements bqsql.TimeTravelCatalog.
func (c catalog) TableAt(ref bqsql.TableRef, at time.Time) (*bqsql.TableData, error) {
	ds, ok := c.b.datasets[dsKey(ref.Project, ref.Dataset)]
	if !ok {
		return nil, &bqsql.Error{
			Kind:    bqsql.ErrNotFound,
			Message: "Not found: Dataset " + ref.Project + ":" + ref.Dataset,
		}
	}

	t, ok := ds.live(ref.Table)
	if !ok {
		return nil, &bqsql.Error{Kind: bqsql.ErrNotFound, Message: "Not found: Table " + ref.String()}
	}

	now := nowMs()
	ms := at.UnixMilli()

	switch {
	case ms > now:
		return nil, &bqsql.Error{
			Kind:    bqsql.ErrInvalid,
			Message: "Invalid snapshot time for table " + ref.String() + ": it is in the future",
		}
	case ms < now-timeTravelWindow.Milliseconds():
		return nil, &bqsql.Error{
			Kind:    bqsql.ErrInvalid,
			Message: "Invalid snapshot time for table " + ref.String() + ": it is more than 7 days in the past",
		}
	}

	fields, rows, existed := t.stateAt(ms)
	if !existed {
		return nil, &bqsql.Error{
			Kind:    bqsql.ErrNotFound,
			Message: "Not found: Table " + ref.String() + " did not exist at the requested time",
		}
	}

	return &bqsql.TableData{Ref: ref, Fields: fields, Rows: rows}, nil
}

// Routine implements bqsql.RoutineCatalog.
func (c catalog) Routine(ref bqsql.TableRef) (*bqsql.RoutineData, bool) {
	ds, ok := c.b.datasets[dsKey(ref.Project, ref.Dataset)]
	if !ok {
		return nil, false
	}

	r, key, ok := ds.routine(ref.Table)
	if !ok {
		return nil, false
	}

	rd, err := routineData(ref.Project, ref.Dataset, key, r.Resource)

	return rd, err == nil
}

func (c catalog) DatasetExists(project, dataset string) bool {
	_, ok := c.b.datasets[dsKey(project, dataset)]

	return ok
}
