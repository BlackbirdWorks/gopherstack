package rum

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

const (
	maxEventsPerBatch = 200
	eventVersion      = "1.0.0"
	millisThreshold   = 1e11
	msPerSecond       = 1000
	maxTimeBound      = 1 << 62
)

// WireRumEvent is one event of PutRumEvents.
type WireRumEvent struct {
	Details   string  `json:"details"`
	ID        string  `json:"id"`
	Metadata  string  `json:"metadata"`
	Type      string  `json:"type"`
	Timestamp float64 `json:"timestamp"`
}

// PutEventsInput holds PutRumEvents parameters.
type PutEventsInput struct {
	AppMonitorDetails *struct {
		ID      string `json:"id"`
		Name    string `json:"name"`
		Version string `json:"version"`
	} `json:"AppMonitorDetails"`
	UserDetails *struct {
		SessionID string `json:"sessionId"`
		UserID    string `json:"userId"`
	} `json:"UserDetails"`
	BatchID   string         `json:"BatchId"`
	Alias     string         `json:"Alias"`
	AppID     string         `json:"-"`
	RumEvents []WireRumEvent `json:"RumEvents"`
}

type delivery struct {
	region string
	group  string
	stream string
	logs   []LogEvent
	points []cwmetric.Point
}

func (b *InMemoryBackend) monitorByIDLocked(id string) (*AppMonitor, error) {
	m, ok := b.monitors.Get(id)
	if !ok {
		return nil, notFound(id, "App monitor with ID %s does not exist", id)
	}

	return m, nil
}

func validateEvents(in *PutEventsInput) error {
	if in.BatchID == "" || in.AppMonitorDetails == nil || in.UserDetails == nil {
		return invalid("BatchId, AppMonitorDetails and UserDetails are required")
	}

	if len(in.RumEvents) == 0 || len(in.RumEvents) > maxEventsPerBatch {
		return invalid("RumEvents must contain between 1 and %d events", maxEventsPerBatch)
	}

	for i, e := range in.RumEvents {
		if e.ID == "" || e.Type == "" || e.Timestamp <= 0 {
			return invalid("RumEvents[%d] requires id, type and a timestamp", i)
		}

		if !json.Valid([]byte(e.Details)) {
			return invalid("RumEvents[%d].details must be valid JSON", i)
		}

		if e.Metadata != "" && !json.Valid([]byte(e.Metadata)) {
			return invalid("RumEvents[%d].metadata must be valid JSON", i)
		}
	}

	return nil
}

func decodeJSON(s string) any {
	if s == "" {
		return nil
	}

	var v any

	_ = json.Unmarshal([]byte(s), &v)

	return v
}

func eventDoc(m *AppMonitor, in *PutEventsInput, e WireRumEvent) map[string]any {
	doc := map[string]any{
		"account_id":       m.Account,
		"application_Id":   m.ID,
		"application_name": m.Name,
		"batch_id":         in.BatchID,
		keyEventDetails:    decodeJSON(e.Details),
		"event_id":         e.ID,
		"event_timestamp":  e.Timestamp,
		"event_type":       e.Type,
		"event_version":    eventVersion,
		"log_stream":       m.ID,
		"user_details":     map[string]any{"sessionId": in.UserDetails.SessionID, "userId": in.UserDetails.UserID},
	}

	if v := in.AppMonitorDetails.Version; v != "" {
		doc["application_version"] = v
	}

	if in.UserDetails.SessionID != "" {
		doc["sessionId"] = in.UserDetails.SessionID
	}

	if in.UserDetails.UserID != "" {
		doc["userId"] = in.UserDetails.UserID
	}

	if md := decodeJSON(e.Metadata); md != nil {
		doc["metadata"] = md
	}

	return doc
}

func stringAt(doc map[string]any, path string) string {
	s, _ := pathString(doc, path)

	return s
}

// PutRumEvents validates and stores a batch of events, then forwards it to CloudWatch Logs and metrics.
func (b *InMemoryBackend) PutRumEvents(ctx context.Context, in *PutEventsInput) error {
	if err := validateEvents(in); err != nil {
		return err
	}

	d, err := b.storeEvents(in)
	if err != nil {
		return err
	}

	b.mu.RLock("PutRumEvents")
	sink, emitter := b.logs, b.metrics
	b.mu.RUnlock()

	if sink != nil && d.group != "" {
		if lerr := sink.PutLogEvents(ctx, d.region, d.group, d.stream, d.logs); lerr != nil {
			logger.Load(ctx).WarnContext(ctx, "rum: delivering events to CloudWatch Logs failed", "error", lerr)
		}
	}

	if emitter != nil {
		for _, p := range d.points {
			if merr := emitter.EmitMetric(p); merr != nil {
				logger.Load(ctx).WarnContext(ctx, "rum: publishing a metric failed", "error", merr)
			}
		}
	}

	return nil
}

func (b *InMemoryBackend) storeEvents(in *PutEventsInput) (delivery, error) {
	b.mu.Lock("PutRumEvents")
	defer b.mu.Unlock()

	m, err := b.monitorByIDLocked(in.AppID)
	if err != nil {
		return delivery{}, err
	}

	cp := *m
	cp.Active = true
	b.monitors.Put(&cp)

	d := delivery{region: m.Region}
	if m.CwLogEnabled {
		d.group, d.stream = logGroupOf(m), m.ID
	}

	defs := b.cloudWatchDefsLocked(m.ID)

	for _, e := range in.RumEvents {
		doc := eventDoc(m, in, e)

		raw, merr := json.Marshal(doc)
		if merr != nil {
			return delivery{}, internalError("could not encode event %s", e.ID)
		}

		md, _ := doc["metadata"].(map[string]any)
		seq := b.nextSeqLocked()

		b.events.Put(&Event{
			Key: eventKeyOf(m.ID, seq), AppID: m.ID, Raw: string(raw), Type: e.Type, Timestamp: e.Timestamp, Seq: seq,
			Browser: stringAt(md, "browserName"), Device: stringAt(md, "deviceType"), OS: stringAt(md, "osName"),
			Page: stringAt(md, "pageId"), Country: stringAt(md, "countryCode"),
		})

		d.logs = append(d.logs, LogEvent{Message: string(raw), Timestamp: int64(e.Timestamp * msPerSecond)})
		d.points = append(d.points, metricPoints(m, doc, defs)...)
	}

	slices.SortStableFunc(d.logs, func(a, z LogEvent) int { return cmp.Compare(a.Timestamp, z.Timestamp) })
	b.trimEventsLocked(m.ID)

	return d, nil
}

func eventKeyOf(appID string, seq int64) string {
	return appID + "|" + fmt.Sprintf("%020d", seq)
}

// trimEventsLocked drops events older than the retention window and keeps at most maxEventsPerApp.
func (b *InMemoryBackend) trimEventsLocked(appID string) {
	events := slices.Clone(b.eventsByApp.Get(appID))
	slices.SortFunc(events, func(a, z *Event) int { return cmp.Compare(a.Seq, z.Seq) })

	cutoff := float64(b.now().Add(-maxEventAge).Unix())
	excess := len(events) - maxEventsPerApp

	for i, e := range events {
		if e.Timestamp < cutoff || i < excess {
			b.events.Delete(e.Key)
		}
	}
}

func (b *InMemoryBackend) cloudWatchDefsLocked(appID string) []*MetricDef {
	var out []*MetricDef

	for _, d := range b.defsByApp.Get(appID) {
		if d.Destination == destCloudWatch {
			out = append(out, d)
		}
	}

	slices.SortFunc(out, func(a, z *MetricDef) int { return cmp.Compare(a.Order, z.Order) })

	return out
}

// metricPoints returns the default AWS/RUM metrics and the custom or extended definitions matched by the event.
func metricPoints(m *AppMonitor, doc map[string]any, defs []*MetricDef) []cwmetric.Point {
	var points []cwmetric.Point

	for _, name := range extendedNames() {
		spec := extendedSpecs[name]
		if matchPattern(spec.pattern, doc) {
			points = append(
				points,
				point(m, namespaceRUM, name, spec.unit, valueOf(doc, spec.valueKey), appDims(m, nil, doc)),
			)
		}
	}

	for _, d := range defs {
		pattern := map[string]any{}
		if d.EventPattern != "" {
			pattern, _ = parseObject(d.EventPattern)
		}

		if !matchPattern(pattern, doc) {
			continue
		}

		ns := namespaceRUM
		if d.Namespace != "" && d.Namespace != namespaceRUM {
			ns = namespaceCustom + d.Namespace
		}

		unit := cmp.Or(d.UnitLabel, defaultUnit(d))
		points = append(points, point(m, ns, d.Name, unit, valueOf(doc, d.ValueKey), appDims(m, d.DimensionKeys, doc)))
	}

	return points
}

func defaultUnit(d *MetricDef) string {
	if spec, ok := extendedSpecs[d.Name]; ok && (d.Namespace == "" || d.Namespace == namespaceRUM) {
		return spec.unit
	}

	return unitCount
}

func point(m *AppMonitor, ns, name, unit string, value float64, dims []cwmetric.Dimension) cwmetric.Point {
	return cwmetric.Point{Region: m.Region, Namespace: ns, Name: name, Unit: unit, Value: value, Dimensions: dims}
}

func valueOf(doc map[string]any, valueKey string) float64 {
	if valueKey == "" {
		return 1
	}

	s, ok := pathString(doc, valueKey)
	if !ok {
		return 1
	}

	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 1
	}

	return f
}

func appDims(m *AppMonitor, keys map[string]string, doc map[string]any) []cwmetric.Dimension {
	dims := []cwmetric.Dimension{{Name: dimApplicationName, Value: m.Name}}

	paths := make([]string, 0, len(keys))
	for p := range keys {
		paths = append(paths, p)
	}

	slices.Sort(paths)

	for _, p := range paths {
		if v, ok := pathString(doc, p); ok {
			dims = append(dims, cwmetric.Dimension{Name: keys[p], Value: v})
		}
	}

	return dims
}

// QueryFilter is the QueryFilter structure of GetAppMonitorData.
type QueryFilter struct {
	Name   string   `json:"Name"`
	Values []string `json:"Values"`
}

// TimeRange is the TimeRange structure; values are epoch seconds, or milliseconds when above 1e11.
type TimeRange struct {
	After  int64 `json:"After"`
	Before int64 `json:"Before"`
}

func (t TimeRange) bounds() (float64, float64) {
	norm := func(v int64) float64 {
		if float64(v) > millisThreshold {
			return float64(v) / msPerSecond
		}

		return float64(v)
	}

	after, before := norm(t.After), norm(t.Before)
	if t.Before == 0 {
		before = float64(maxTimeBound)
	}

	return after, before
}

// filterNames lists the QueryFilter names; Invert is last.
func filterNames() []string {
	return []string{"Browser", "Device", "Country", "Page", "OS", "EventType", "Invert"}
}

func eventAttr(e *Event, name string) string {
	switch name {
	case "Browser":
		return e.Browser
	case "Device":
		return e.Device
	case "Country":
		return e.Country
	case "Page":
		return e.Page
	case "OS":
		return e.OS
	default:
		return e.Type
	}
}

func eventMatches(e *Event, filters []QueryFilter) bool {
	for _, f := range filters {
		if f.Name == "Invert" {
			for _, n := range filterNames()[:len(filterNames())-1] {
				if slices.Contains(f.Values, eventAttr(e, n)) && eventAttr(e, n) != "" {
					return false
				}
			}

			continue
		}

		if !slices.Contains(f.Values, eventAttr(e, f.Name)) {
			return false
		}
	}

	return true
}

// GetAppMonitorData returns the stored raw events within the time range that match every filter.
func (b *InMemoryBackend) GetAppMonitorData(
	ctx context.Context, name string, tr *TimeRange, filters []QueryFilter, token string, maxResults int,
) (page.Page[string], error) {
	if tr == nil || tr.After == 0 {
		return page.Page[string]{}, invalid("TimeRange.After is required")
	}

	for _, f := range filters {
		if !slices.Contains(filterNames(), f.Name) {
			return page.Page[string]{}, invalid(
				"Filters[].Name must be one of Browser, Device, Country, Page, OS, EventType, Invert",
			)
		}
	}

	sc := b.scopeOf(ctx)

	b.mu.RLock("GetAppMonitorData")
	defer b.mu.RUnlock()

	m, err := b.monitorByNameLocked(sc, name)
	if err != nil {
		return page.Page[string]{}, err
	}

	after, before := tr.bounds()
	cutoff := float64(b.now().Add(-maxEventAge).Unix())

	var matched []*Event

	for _, e := range b.eventsByApp.Get(m.ID) {
		if e.Timestamp >= max(after, cutoff) && e.Timestamp <= before && eventMatches(e, filters) {
			matched = append(matched, e)
		}
	}

	slices.SortFunc(matched, func(a, z *Event) int {
		return cmp.Or(cmp.Compare(a.Timestamp, z.Timestamp), cmp.Compare(a.Seq, z.Seq))
	})

	raws := make([]string, len(matched))
	for i, e := range matched {
		raws[i] = e.Raw
	}

	return restroute.Paginate(raws, token, maxResults, defaultPageSize, maxPageSize)
}
