package gcplogging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

var errBadSnapshot = errors.New("gcplogging: corrupt snapshot")

// snapshotVersion must be bumped when a change to the snapshot types would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type bucketSnap struct {
	DeleteRequested time.Time       `json:"deleteRequested"`
	Bucket          json.RawMessage `json:"bucket"`
	Entries         []int           `json:"entries,omitempty"`
}

type seriesSnap struct {
	Labels  map[string]string `json:"labels,omitempty"`
	Buckets []int64           `json:"buckets,omitempty"`
	Count   int64             `json:"count"`
	Sum     float64           `json:"sum"`
	Mean    float64           `json:"mean"`
	SSD     float64           `json:"ssd"`
}

type metricSnap struct {
	Metric json.RawMessage `json:"metric"`
	Series []seriesSnap    `json:"series,omitempty"`
}

type backendSnapshot struct {
	Buckets    map[string]bucketSnap      `json:"buckets"`
	Views      map[string]json.RawMessage `json:"views"`
	Sinks      map[string]json.RawMessage `json:"sinks"`
	Exclusions map[string]json.RawMessage `json:"exclusions"`
	Metrics    map[string]metricSnap      `json:"metrics"`
	Settings   map[string]json.RawMessage `json:"settings"`
	Cmek       map[string]json.RawMessage `json:"cmek"`
	Ops        map[string]json.RawMessage `json:"ops"`
	Entries    []json.RawMessage          `json:"entries"`
	Seeded     []string                   `json:"seeded"`
	Seq        uint64                     `json:"seq"`
	Version    int                        `json:"version"`
}

func marshalPB(m proto.Message) json.RawMessage {
	out, err := protojson.Marshal(m)
	if err != nil {
		return nil
	}

	return out
}

func unmarshalPB(raw json.RawMessage, m proto.Message) error {
	return protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(raw, m)
}

func snapMap[T proto.Message](in map[string]T) map[string]json.RawMessage {
	out := make(map[string]json.RawMessage, len(in))
	for k, v := range in {
		out[k] = marshalPB(v)
	}

	return out
}

// Snapshot serialises buckets with their entries, sinks, exclusions, views, metrics and settings.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Version:    snapshotVersion,
		Seq:        b.seq,
		Buckets:    make(map[string]bucketSnap, len(b.buckets)),
		Views:      snapMap(b.views),
		Sinks:      snapMap(b.sinks),
		Exclusions: snapMap(b.exclusions),
		Settings:   snapMap(b.settings),
		Cmek:       snapMap(b.cmek),
		Metrics:    make(map[string]metricSnap, len(b.metrics)),
		Ops:        b.ops.Snapshot(),
	}

	index := map[*loggingpb.LogEntry]int{}

	for name, bd := range b.buckets {
		bs := bucketSnap{Bucket: marshalPB(bd.pb), DeleteRequested: bd.deleteRequested}

		for _, e := range bd.entries {
			i, ok := index[e]
			if !ok {
				i = len(snap.Entries)
				index[e] = i
				snap.Entries = append(snap.Entries, marshalPB(e))
			}

			bs.Entries = append(bs.Entries, i)
		}

		snap.Buckets[name] = bs
	}

	for name, rec := range b.metrics {
		snap.Metrics[name] = snapMetric(rec)
	}

	for p := range b.seeded {
		snap.Seeded = append(snap.Seeded, p)
	}

	return persistence.MarshalSnapshot(ctx, "gcplogging", snap)
}

func snapMetric(rec *metricRec) metricSnap {
	ms := metricSnap{Metric: marshalPB(rec.pb)}
	for _, s := range rec.series {
		ms.Series = append(ms.Series, seriesSnap{
			Labels: s.labels, Buckets: s.buckets, Count: s.count, Sum: s.sum, Mean: s.mean, SSD: s.ssd,
		})
	}

	return ms
}

func restoreMap[T proto.Message](raw map[string]json.RawMessage, newT func() T) (map[string]T, error) {
	out := make(map[string]T, len(raw))

	for k, r := range raw {
		v := newT()
		if err := unmarshalPB(r, v); err != nil {
			return nil, fmt.Errorf("gcplogging: restore %q: %w", k, err)
		}

		out[k] = v
	}

	return out, nil
}

// Restore loads backend state from a snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "gcplogging", data, &snap); err != nil {
		return err
	}

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "gcplogging: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.mu.Lock("Restore")
		b.resetMaps()
		b.mu.Unlock()

		return nil
	}

	if err := b.ops.Restore(snap.Ops); err != nil {
		return fmt.Errorf("gcplogging: restore operations: %w", err)
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.resetMaps()

	if err := b.load(&snap); err != nil {
		b.resetMaps()

		return err
	}

	b.seq = max(b.seq, snap.Seq)

	return nil
}

func newExclusion() *loggingpb.LogExclusion { return &loggingpb.LogExclusion{} }
func newSettings() *loggingpb.Settings      { return &loggingpb.Settings{} }
func newCmek() *loggingpb.CmekSettings      { return &loggingpb.CmekSettings{} }
func newView() *loggingpb.LogView           { return &loggingpb.LogView{} }
func newSink() *loggingpb.LogSink           { return &loggingpb.LogSink{} }

// load fills the freshly reset maps from a decoded snapshot.
func (b *InMemoryBackend) load(snap *backendSnapshot) error {
	var err error

	if b.views, err = restoreMap(snap.Views, newView); err != nil {
		return err
	}

	if b.sinks, err = restoreMap(snap.Sinks, newSink); err != nil {
		return err
	}

	if b.exclusions, err = restoreMap(snap.Exclusions, newExclusion); err != nil {
		return err
	}

	if b.settings, err = restoreMap(snap.Settings, newSettings); err != nil {
		return err
	}

	if b.cmek, err = restoreMap(snap.Cmek, newCmek); err != nil {
		return err
	}

	entries, err := decodeEntries(snap.Entries)
	if err != nil {
		return err
	}

	if err = b.loadBuckets(snap.Buckets, entries); err != nil {
		return err
	}

	for _, p := range snap.Seeded {
		b.seeded[p] = true
	}

	return b.loadMetrics(snap.Metrics)
}

func decodeEntries(raw []json.RawMessage) ([]*loggingpb.LogEntry, error) {
	out := make([]*loggingpb.LogEntry, len(raw))

	for i, r := range raw {
		e := &loggingpb.LogEntry{}
		if err := unmarshalPB(r, e); err != nil {
			return nil, fmt.Errorf("gcplogging: restore entry %d: %w", i, err)
		}

		out[i] = e
	}

	return out, nil
}

func (b *InMemoryBackend) loadBuckets(in map[string]bucketSnap, entries []*loggingpb.LogEntry) error {
	for name, bs := range in {
		pb := &loggingpb.LogBucket{}
		if err := unmarshalPB(bs.Bucket, pb); err != nil {
			return fmt.Errorf("gcplogging: restore bucket %q: %w", name, err)
		}

		bd := newBucketData(pb)
		bd.deleteRequested = bs.DeleteRequested

		for _, i := range bs.Entries {
			if i < 0 || i >= len(entries) {
				return fmt.Errorf("%w: bucket %q entry index %d", errBadSnapshot, name, i)
			}

			bd.entries = append(bd.entries, entries[i])
			bd.keys[entryKey(entries[i])] = struct{}{}
		}

		b.buckets[name] = bd
	}

	return nil
}

func (b *InMemoryBackend) loadMetrics(in map[string]metricSnap) error {
	for name, ms := range in {
		pb := &loggingpb.LogMetric{}
		if err := unmarshalPB(ms.Metric, pb); err != nil {
			return fmt.Errorf("gcplogging: restore metric %q: %w", name, err)
		}

		cm, err := compileMetric(pb)
		if err != nil {
			return fmt.Errorf("gcplogging: restore metric %q: %w", name, err)
		}

		rec := &metricRec{pb: pb, compiled: cm, series: map[string]*seriesRec{}}

		for _, s := range ms.Series {
			sr := rec.seriesFor(s.Labels)
			sr.count, sr.sum, sr.mean, sr.ssd = s.Count, s.Sum, s.Mean, s.SSD

			if len(s.Buckets) == len(sr.buckets) {
				copy(sr.buckets, s.Buckets)
			}
		}

		b.metrics[name] = rec
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("gcplogging: restore snapshot: %w", err)
	}

	return nil
}
