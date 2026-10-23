package gcpmonitoring

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to the snapshot types would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type seriesSnap struct {
	Project string          `json:"project"`
	Unit    string          `json:"unit,omitempty"`
	Series  json.RawMessage `json:"series"`
}

type channelSnap struct {
	CodeAt    time.Time         `json:"codeAt"`
	Sensitive map[string]string `json:"sensitive,omitempty"`
	Code      string            `json:"code,omitempty"`
	Channel   json.RawMessage   `json:"channel"`
}

type uptimeSnap struct {
	LastRun time.Time       `json:"lastRun"`
	Config  json.RawMessage `json:"config"`
}

type backendSnapshot struct {
	Descriptors map[string]json.RawMessage `json:"descriptors"`
	Policies    map[string]json.RawMessage `json:"policies"`
	Groups      map[string]json.RawMessage `json:"groups"`
	Channels    map[string]channelSnap     `json:"channels"`
	Uptime      map[string]uptimeSnap      `json:"uptime"`
	Series      []seriesSnap               `json:"series"`
	Incidents   []Incident                 `json:"incidents"`
	Version     int                        `json:"version"`
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

func restoreMap[T proto.Message](raw map[string]json.RawMessage, newT func() T) (map[string]T, error) {
	out := make(map[string]T, len(raw))

	for k, r := range raw {
		v := newT()
		if err := unmarshalPB(r, v); err != nil {
			return nil, fmt.Errorf("gcpmonitoring: restore %q: %w", k, err)
		}

		out[k] = v
	}

	return out, nil
}

func (r *seriesRec) toSnap() seriesSnap {
	ts := &monitoringpb.TimeSeries{
		Metric: r.metric, Resource: r.resource, Metadata: r.metadata, MetricKind: r.kind, ValueType: r.vtype,
		Points: r.points,
	}

	return seriesSnap{Project: r.project, Unit: r.unit, Series: marshalPB(ts)}
}

// Snapshot serialises descriptors, series, policies, channels, uptime checks, groups and incidents.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Version:     snapshotVersion,
		Descriptors: snapMap(b.descriptors),
		Policies:    snapMap(b.policies),
		Groups:      snapMap(b.groups),
		Channels:    make(map[string]channelSnap, len(b.channels)),
		Uptime:      make(map[string]uptimeSnap, len(b.uptime)),
	}

	for k, c := range b.channels {
		snap.Channels[k] = channelSnap{Channel: marshalPB(c.pb), Sensitive: c.sensitive, Code: c.code, CodeAt: c.codeAt}
	}

	for k, u := range b.uptime {
		snap.Uptime[k] = uptimeSnap{Config: marshalPB(u.pb), LastRun: u.lastRun}
	}

	for _, key := range sortedKeys(b.series) {
		snap.Series = append(snap.Series, b.series[key].toSnap())
	}

	for _, id := range sortedKeys(b.incidents) {
		snap.Incidents = append(snap.Incidents, cloneIncident(b.incidents[id]))
	}

	return persistence.MarshalSnapshot(ctx, "gcpmonitoring", snap)
}

// Restore loads backend state from a snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "gcpmonitoring", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "gcpmonitoring: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.resetMaps()

		return nil
	}

	b.resetMaps()

	if err := b.load(&snap); err != nil {
		b.resetMaps()

		return err
	}

	return nil
}

func (b *InMemoryBackend) load(snap *backendSnapshot) error {
	var err error

	if b.descriptors, err = restoreMap(
		snap.Descriptors,
		func() *metricpb.MetricDescriptor { return &metricpb.MetricDescriptor{} },
	); err != nil {
		return err
	}

	if b.policies, err = restoreMap(
		snap.Policies,
		func() *monitoringpb.AlertPolicy { return &monitoringpb.AlertPolicy{} },
	); err != nil {
		return err
	}

	if b.groups, err = restoreMap(
		snap.Groups,
		func() *monitoringpb.Group { return &monitoringpb.Group{} },
	); err != nil {
		return err
	}

	if err = b.loadChannels(snap.Channels); err != nil {
		return err
	}

	if err = b.loadUptime(snap.Uptime); err != nil {
		return err
	}

	for i := range snap.Incidents {
		inc := snap.Incidents[i]
		b.incidents[inc.ID] = &inc
	}

	return b.loadSeries(snap.Series)
}

func (b *InMemoryBackend) loadChannels(in map[string]channelSnap) error {
	for k, cs := range in {
		pb := &monitoringpb.NotificationChannel{}
		if err := unmarshalPB(cs.Channel, pb); err != nil {
			return fmt.Errorf("gcpmonitoring: restore channel %q: %w", k, err)
		}

		sens := cs.Sensitive
		if sens == nil {
			sens = map[string]string{}
		}

		b.channels[k] = &channelRec{pb: pb, sensitive: sens, code: cs.Code, codeAt: cs.CodeAt}
	}

	return nil
}

func (b *InMemoryBackend) loadUptime(in map[string]uptimeSnap) error {
	for k, us := range in {
		pb := &monitoringpb.UptimeCheckConfig{}
		if err := unmarshalPB(us.Config, pb); err != nil {
			return fmt.Errorf("gcpmonitoring: restore uptime check %q: %w", k, err)
		}

		b.uptime[k] = &uptimeRec{pb: pb, lastRun: us.LastRun}
	}

	return nil
}

func (b *InMemoryBackend) loadSeries(in []seriesSnap) error {
	for i, ss := range in {
		ts := &monitoringpb.TimeSeries{}
		if err := unmarshalPB(ss.Series, ts); err != nil {
			return fmt.Errorf("gcpmonitoring: restore series %d: %w", i, err)
		}

		b.series[seriesKey(ss.Project, ts.GetMetric(), ts.GetResource())] = &seriesRec{
			project: ss.Project, unit: ss.Unit, metric: ts.GetMetric(), resource: ts.GetResource(),
			metadata: ts.GetMetadata(), kind: ts.GetMetricKind(), vtype: ts.GetValueType(), points: ts.GetPoints(),
		}
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("gcpmonitoring: restore snapshot: %w", err)
	}

	return nil
}
