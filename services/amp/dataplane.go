package amp

import (
	"cmp"
	"context"
	"math"
	"regexp"
	"slices"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
	"github.com/blackbirdworks/gopherstack/services/amp/promql"
)

var labelNameRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// IngestResult counts what a remote-write request stored.
type IngestResult struct {
	Samples   int
	Duplicate int
	Dropped   int
}

func (b *InMemoryBackend) dataWorkspaceLocked(sc scope, id string) error {
	w, err := b.workspaceLocked(sc, id)
	if err != nil {
		return err
	}

	if st := b.state(w.Pending); st != statusActive {
		return conflict(resWorkspace, id, "Workspace %s is %s", id, st)
	}

	return nil
}

func validateWriteSeries(ws writeSeries) error {
	if len(ws.Labels) == 0 {
		return restroute.Validation("a time series needs at least one label")
	}

	for name := range ws.Labels {
		if !labelNameRe.MatchString(name) {
			return restroute.Validation("invalid label name %q", name)
		}
	}

	return nil
}

// Ingest stores remote-write samples. Non-finite values (including stale markers) are dropped.
func (b *InMemoryBackend) Ingest(ctx context.Context, wsID string, series []writeSeries) (IngestResult, error) {
	for _, ws := range series {
		if err := validateWriteSeries(ws); err != nil {
			return IngestResult{}, err
		}
	}

	sc := b.scopeOf(ctx)

	b.mu.Lock("Ingest")
	defer b.mu.Unlock()

	b.purgeLocked()

	if err := b.dataWorkspaceLocked(sc, wsID); err != nil {
		return IngestResult{}, err
	}

	var res IngestResult

	for _, ws := range series {
		key := seriesKey(wsID, promql.Fingerprint(ws.Labels))

		rec, ok := b.series.Get(key)
		if !ok {
			rec = &SeriesRecord{Key: key, WorkspaceID: wsID, Labels: ws.Labels}
			b.series.Put(rec)
		}

		for _, s := range ws.Samples {
			if math.IsNaN(s.V) || math.IsInf(s.V, 0) {
				res.Dropped++

				continue
			}

			if insertSample(rec, s) {
				res.Samples++
			} else {
				res.Duplicate++
			}
		}
	}

	return res, nil
}

// insertSample keeps rec.Samples ordered by time; a repeated timestamp is a duplicate.
func insertSample(rec *SeriesRecord, s promql.Sample) bool {
	n := len(rec.Samples)
	if n == 0 || rec.Samples[n-1].T < s.T {
		rec.Samples = append(rec.Samples, s)

		return true
	}

	i, found := slices.BinarySearchFunc(
		rec.Samples,
		s.T,
		func(e promql.Sample, t int64) int { return cmp.Compare(e.T, t) },
	)
	if found {
		return false
	}

	rec.Samples = slices.Insert(rec.Samples, i, s)

	return true
}

// wsStorage adapts one workspace's series to promql.Storage; callers hold the backend read lock.
type wsStorage struct {
	b    *InMemoryBackend
	wsID string
}

func (s wsStorage) Select(ms []*promql.Matcher, minT, maxT int64) []promql.Series {
	var out []promql.Series

	for _, rec := range s.b.seriesByWS.Get(s.wsID) {
		if !promql.MatchesAll(rec.Labels, ms) {
			continue
		}

		lo := sort.Search(len(rec.Samples), func(i int) bool { return rec.Samples[i].T >= minT })
		hi := sort.Search(len(rec.Samples), func(i int) bool { return rec.Samples[i].T > maxT })

		if lo < hi {
			out = append(out, promql.Series{Labels: rec.Labels, Samples: rec.Samples[lo:hi]})
		}
	}

	return out
}

// withStorage runs fn against a workspace's samples under the read lock.
func (b *InMemoryBackend) withStorage(ctx context.Context, wsID string, fn func(promql.Storage)) error {
	b.mu.RLock("Query")
	defer b.mu.RUnlock()

	if err := b.dataWorkspaceLocked(b.scopeOf(ctx), wsID); err != nil {
		return err
	}

	fn(wsStorage{b: b, wsID: wsID})

	return nil
}

// SeriesLabels returns the label sets of series matching any selector with a sample in [minT, maxT].
func (b *InMemoryBackend) SeriesLabels(
	ctx context.Context, wsID string, selectors [][]*promql.Matcher, minT, maxT int64,
) ([]promql.Labels, error) {
	var out []promql.Labels

	err := b.withStorage(ctx, wsID, func(st promql.Storage) {
		seen := map[string]bool{}

		for _, ms := range selectors {
			for _, s := range st.Select(ms, minT, maxT) {
				if fp := promql.Fingerprint(s.Labels); !seen[fp] {
					seen[fp] = true

					out = append(out, s.Labels)
				}
			}
		}
	})

	return out, err
}
