package gcpbigquery

import (
	"crypto/sha256"
	"encoding/base64"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/services/gcpbigquery/bqsql"
)

// InMemoryBackend holds every dataset, table, row and job under one lock.
type InMemoryBackend struct {
	mu       *lockmetrics.RWMutex
	datasets map[string]*Dataset
	jobs     map[string]*Job
	objects  ObjectStore
	seq      int64
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		mu:       lockmetrics.New("gcpbigquery"),
		datasets: make(map[string]*Dataset),
		jobs:     make(map[string]*Job),
	}
}

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.datasets = make(map[string]*Dataset)
	b.jobs = make(map[string]*Job)
	b.seq = 0
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

	t, ok := ds.Tables[ref.Table]
	if !ok {
		return nil, false
	}

	return &bqsql.TableData{Ref: ref, Fields: t.Schema, Rows: t.Rows, ViewQuery: t.ViewQuery}, true
}

func (c catalog) DatasetExists(project, dataset string) bool {
	_, ok := c.b.datasets[dsKey(project, dataset)]

	return ok
}
