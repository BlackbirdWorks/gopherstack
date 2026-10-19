package rum

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

// WireMetricRequest is the MetricDefinitionRequest structure.
type WireMetricRequest struct {
	DimensionKeys map[string]string `json:"DimensionKeys,omitempty"`
	Name          string            `json:"Name"`
	EventPattern  string            `json:"EventPattern,omitempty"`
	Namespace     string            `json:"Namespace,omitempty"`
	UnitLabel     string            `json:"UnitLabel,omitempty"`
	ValueKey      string            `json:"ValueKey,omitempty"`
}

// WireMetricDef is the MetricDefinition structure.
type WireMetricDef struct {
	DimensionKeys      map[string]string `json:"DimensionKeys,omitempty"`
	MetricDefinitionID string            `json:"MetricDefinitionId"`
	Name               string            `json:"Name"`
	EventPattern       string            `json:"EventPattern,omitempty"`
	Namespace          string            `json:"Namespace,omitempty"`
	UnitLabel          string            `json:"UnitLabel,omitempty"`
	ValueKey           string            `json:"ValueKey,omitempty"`
}

// WireCreateError reports one metric definition rejected by BatchCreateRumMetricDefinitions.
type WireCreateError struct {
	MetricDefinition WireMetricRequest `json:"MetricDefinition"`
	ErrorCode        string            `json:"ErrorCode"`
	ErrorMessage     string            `json:"ErrorMessage"`
}

// WireDeleteError reports one metric definition ID rejected by BatchDeleteRumMetricDefinitions.
type WireDeleteError struct {
	MetricDefinitionID string `json:"MetricDefinitionId"`
	ErrorCode          string `json:"ErrorCode"`
	ErrorMessage       string `json:"ErrorMessage"`
}

// WireDestination is the MetricDestinationSummary structure.
type WireDestination struct {
	Destination    string `json:"Destination"`
	DestinationARN string `json:"DestinationArn,omitempty"`
	IAMRoleARN     string `json:"IamRoleArn,omitempty"`
}

func wireDef(d *MetricDef) WireMetricDef {
	return WireMetricDef{
		DimensionKeys: d.DimensionKeys, MetricDefinitionID: d.ID, Name: d.Name, EventPattern: d.EventPattern,
		Namespace: d.Namespace, UnitLabel: d.UnitLabel, ValueKey: d.ValueKey,
	}
}

func validateDestination(dest, destARN string) error {
	switch dest {
	case destCloudWatch:
		if destARN != "" {
			return invalid("DestinationArn is only valid when Destination is %s", destEvidently)
		}
	case destEvidently:
		if destARN == "" {
			return invalid("DestinationArn is required when Destination is %s", destEvidently)
		}
	default:
		return invalid("Destination must be %s or %s", destCloudWatch, destEvidently)
	}

	return nil
}

func (b *InMemoryBackend) destinationLocked(m *AppMonitor, dest, destARN string) (*Destination, error) {
	d, ok := b.destinations.Get(destinationKey(&Destination{AppID: m.ID, Destination: dest, DestinationARN: destARN}))
	if !ok {
		return nil, notFound(m.Name, "App monitor %s has no %s metrics destination", m.Name, dest)
	}

	return d, nil
}

// PutRumMetricsDestination creates or updates a metrics destination.
func (b *InMemoryBackend) PutRumMetricsDestination(ctx context.Context, app, dest, destARN, roleARN string) error {
	if err := validateDestination(dest, destARN); err != nil {
		return err
	}

	if dest == destEvidently && roleARN == "" {
		return invalid("IamRoleArn is required when Destination is %s", destEvidently)
	}

	sc := b.scopeOf(ctx)

	b.mu.Lock("PutRumMetricsDestination")
	defer b.mu.Unlock()

	m, err := b.monitorByNameLocked(sc, app)
	if err != nil {
		return err
	}

	b.destinations.Put(&Destination{AppID: m.ID, Destination: dest, DestinationARN: destARN, IAMRoleARN: roleARN})

	return nil
}

// DeleteRumMetricsDestination removes a destination and the metric definitions sent to it.
func (b *InMemoryBackend) DeleteRumMetricsDestination(ctx context.Context, app, dest, destARN string) error {
	if err := validateDestination(dest, destARN); err != nil {
		return err
	}

	sc := b.scopeOf(ctx)

	b.mu.Lock("DeleteRumMetricsDestination")
	defer b.mu.Unlock()

	m, err := b.monitorByNameLocked(sc, app)
	if err != nil {
		return err
	}

	d, err := b.destinationLocked(m, dest, destARN)
	if err != nil {
		return err
	}

	for _, def := range b.defsByApp.Get(m.ID) {
		if def.Destination == dest && def.DestinationARN == destARN {
			b.definitions.Delete(def.ID)
		}
	}

	b.destinations.Delete(destinationKey(d))

	return nil
}

// ListRumMetricsDestinations pages the destinations of an app monitor.
func (b *InMemoryBackend) ListRumMetricsDestinations(
	ctx context.Context, app, token string, maxResults int,
) (page.Page[WireDestination], error) {
	sc := b.scopeOf(ctx)

	b.mu.RLock("ListRumMetricsDestinations")
	defer b.mu.RUnlock()

	m, err := b.monitorByNameLocked(sc, app)
	if err != nil {
		return page.Page[WireDestination]{}, err
	}

	var all []*Destination

	b.destinations.Range(func(d *Destination) bool {
		if d.AppID == m.ID {
			all = append(all, d)
		}

		return true
	})

	slices.SortFunc(all, func(a, z *Destination) int {
		return cmp.Or(cmp.Compare(a.Destination, z.Destination), cmp.Compare(a.DestinationARN, z.DestinationARN))
	})

	out := make([]WireDestination, len(all))
	for i, d := range all {
		out[i] = WireDestination{Destination: d.Destination, DestinationARN: d.DestinationARN, IAMRoleARN: d.IAMRoleARN}
	}

	return restroute.Paginate(out, token, maxResults, defaultPageSize, maxPageSize)
}

func jsonObjectOf(s string) (map[string]any, error) {
	var out map[string]any
	if err := json.Unmarshal([]byte(s), &out); err != nil || out == nil {
		return nil, invalid("EventPattern must be a JSON object")
	}

	return out, nil
}

func patternHasPath(pattern map[string]any, path string) bool {
	_, ok := lookupPath(pattern, path)

	return ok
}

func patternHasEventType(pattern map[string]any, want string) bool {
	list, ok := pattern[keyEventType].([]any)

	return ok && slices.Contains(list, any(want))
}

// validateMetricRequest applies the extended-metric and custom-metric rules of the RUM API.
func validateMetricRequest(r WireMetricRequest) error {
	if r.Name == "" || len(r.Name) > maxNameLen {
		return invalid("Name must be 1-%d characters", maxNameLen)
	}

	if r.Namespace == "" || r.Namespace == namespaceRUM {
		return validateExtendedMetric(r)
	}

	return validateCustomMetric(r)
}

func validateExtendedMetric(r WireMetricRequest) error {
	spec, ok := extendedSpecs[r.Name]
	if !ok {
		return invalid("extended metric Name must be one of %s", strings.Join(extendedNames(), ", "))
	}

	if r.EventPattern == "" {
		return invalid("EventPattern is required for extended metrics")
	}

	pattern, err := jsonObjectOf(r.EventPattern)
	if err != nil {
		return err
	}

	if !patternHasEventType(pattern, spec.eventType) {
		return invalid("EventPattern must include event_type [%q] for metric %s", spec.eventType, r.Name)
	}

	for _, key := range spec.detailsRequired {
		if !patternHasPath(pattern, keyEventDetails+"."+key) {
			return invalid("EventPattern must include event_details.%s for metric %s", key, r.Name)
		}
	}

	if r.ValueKey != spec.valueKey {
		return invalid("ValueKey for metric %s must be %q", r.Name, spec.valueKey)
	}

	for path, dim := range r.DimensionKeys {
		if extendedDimensions[path] != dim {
			return invalid("DimensionKeys entry %q is not a valid extended metric dimension", path)
		}

		if !patternHasPath(pattern, path) {
			return invalid("dimension %q must also be included in EventPattern", path)
		}
	}

	return nil
}

func validateCustomMetric(r WireMetricRequest) error {
	if strings.HasPrefix(r.Namespace, "AWS/") {
		return invalid("custom metric Namespace cannot start with AWS/")
	}

	var pattern map[string]any

	if r.EventPattern != "" {
		p, err := jsonObjectOf(r.EventPattern)
		if err != nil {
			return err
		}

		if err = validateCustomPattern(p); err != nil {
			return err
		}

		pattern = p
	}

	if r.ValueKey != "" && !validTopLevel(r.ValueKey) {
		return invalid("ValueKey %q does not start with a valid RUM event field", r.ValueKey)
	}

	for path := range r.DimensionKeys {
		if !validTopLevel(path) {
			return invalid("DimensionKeys entry %q does not start with a valid RUM event field", path)
		}

		if !patternHasPath(pattern, path) {
			return invalid("dimension %q must also be included in EventPattern", path)
		}
	}

	return nil
}

func validateCustomPattern(pattern map[string]any) error {
	for k := range pattern {
		if !validTopLevel(k) {
			return invalid("EventPattern key %q is not a valid RUM event field", k)
		}
	}

	_, hasDetails := pattern[keyEventDetails]
	_, hasType := pattern[keyEventType]

	if hasDetails && !hasType {
		return invalid("EventPattern with event_details must also contain event_type")
	}

	return nil
}

func (b *InMemoryBackend) metricScopeLocked(
	sc scope, app, dest, destARN string,
) (*AppMonitor, error) {
	if err := validateDestination(dest, destARN); err != nil {
		return nil, err
	}

	m, err := b.monitorByNameLocked(sc, app)
	if err != nil {
		return nil, err
	}

	if _, err = b.destinationLocked(m, dest, destARN); err != nil {
		return nil, err
	}

	return m, nil
}

func (b *InMemoryBackend) definitionCountLocked(appID, dest, destARN string) int {
	n := 0

	for _, d := range b.defsByApp.Get(appID) {
		if d.Destination == dest && d.DestinationARN == destARN {
			n++
		}
	}

	return n
}

// BatchCreateRumMetricDefinitions creates the valid definitions and reports the rest as errors.
func (b *InMemoryBackend) BatchCreateRumMetricDefinitions(
	ctx context.Context, app, dest, destARN string, reqs []WireMetricRequest,
) ([]WireMetricDef, []WireCreateError, error) {
	if len(reqs) == 0 || len(reqs) > maxMetricBatch {
		return nil, nil, invalid("MetricDefinitions must contain between 1 and %d items", maxMetricBatch)
	}

	sc := b.scopeOf(ctx)

	b.mu.Lock("BatchCreateRumMetricDefinitions")
	defer b.mu.Unlock()

	m, err := b.metricScopeLocked(sc, app, dest, destARN)
	if err != nil {
		return nil, nil, err
	}

	created := []WireMetricDef{}
	failed := []WireCreateError{}
	count := b.definitionCountLocked(m.ID, dest, destARN)

	for _, r := range reqs {
		if verr := validateMetricRequest(r); verr != nil {
			failed = append(
				failed,
				WireCreateError{MetricDefinition: r, ErrorCode: "ValidationException", ErrorMessage: errMessage(verr)},
			)

			continue
		}

		if count >= maxMetricsPerDst {
			failed = append(failed, WireCreateError{
				MetricDefinition: r, ErrorCode: "ServiceQuotaExceededException",
				ErrorMessage: "a destination can hold at most 2000 metric definitions",
			})

			continue
		}

		d := &MetricDef{
			ID: uuid.NewString(), AppID: m.ID, Destination: dest, DestinationARN: destARN, Name: r.Name,
			Namespace: r.Namespace, EventPattern: r.EventPattern, UnitLabel: r.UnitLabel, ValueKey: r.ValueKey,
			DimensionKeys: r.DimensionKeys, Order: b.nextSeqLocked(),
		}
		b.definitions.Put(d)

		count++

		created = append(created, wireDef(d))
	}

	return created, failed, nil
}

func errMessage(err error) string {
	if re, ok := errors.AsType[*restroute.Error](err); ok {
		return re.Message
	}

	return err.Error()
}

// BatchDeleteRumMetricDefinitions deletes the definitions that exist and reports the rest as errors.
func (b *InMemoryBackend) BatchDeleteRumMetricDefinitions(
	ctx context.Context, app, dest, destARN string, ids []string,
) ([]string, []WireDeleteError, error) {
	if len(ids) == 0 || len(ids) > maxMetricBatch {
		return nil, nil, invalid("MetricDefinitionIds must contain between 1 and %d items", maxMetricBatch)
	}

	sc := b.scopeOf(ctx)

	b.mu.Lock("BatchDeleteRumMetricDefinitions")
	defer b.mu.Unlock()

	m, err := b.metricScopeLocked(sc, app, dest, destARN)
	if err != nil {
		return nil, nil, err
	}

	deleted := []string{}
	failed := []WireDeleteError{}

	for _, id := range ids {
		d, ok := b.definitions.Get(id)
		if !ok || d.AppID != m.ID || d.Destination != dest || d.DestinationARN != destARN {
			failed = append(failed, WireDeleteError{
				MetricDefinitionID: id, ErrorCode: "ResourceNotFoundException",
				ErrorMessage: "metric definition " + id + " does not exist",
			})

			continue
		}

		b.definitions.Delete(id)

		deleted = append(deleted, id)
	}

	return deleted, failed, nil
}

// BatchGetRumMetricDefinitions pages the definitions of one destination.
func (b *InMemoryBackend) BatchGetRumMetricDefinitions(
	ctx context.Context, app, dest, destARN, token string, maxResults int,
) (page.Page[WireMetricDef], error) {
	sc := b.scopeOf(ctx)

	b.mu.RLock("BatchGetRumMetricDefinitions")
	defer b.mu.RUnlock()

	m, err := b.metricScopeLocked(sc, app, dest, destARN)
	if err != nil {
		return page.Page[WireMetricDef]{}, err
	}

	var all []*MetricDef

	for _, d := range b.defsByApp.Get(m.ID) {
		if d.Destination == dest && d.DestinationARN == destARN {
			all = append(all, d)
		}
	}

	slices.SortFunc(all, func(a, z *MetricDef) int { return cmp.Compare(a.Order, z.Order) })

	out := make([]WireMetricDef, len(all))
	for i, d := range all {
		out[i] = wireDef(d)
	}

	return restroute.Paginate(out, token, maxResults, defaultPageSize, maxPageSize)
}

// UpdateRumMetricDefinition replaces one metric definition.
func (b *InMemoryBackend) UpdateRumMetricDefinition(
	ctx context.Context, app, dest, destARN, id string, r WireMetricRequest,
) error {
	if err := validateMetricRequest(r); err != nil {
		return err
	}

	sc := b.scopeOf(ctx)

	b.mu.Lock("UpdateRumMetricDefinition")
	defer b.mu.Unlock()

	m, err := b.metricScopeLocked(sc, app, dest, destARN)
	if err != nil {
		return err
	}

	d, ok := b.definitions.Get(id)
	if !ok || d.AppID != m.ID || d.Destination != dest || d.DestinationARN != destARN {
		return notFound(app, "metric definition %s does not exist", id)
	}

	cp := *d
	cp.Name, cp.Namespace, cp.EventPattern, cp.UnitLabel = r.Name, r.Namespace, r.EventPattern, r.UnitLabel
	cp.ValueKey, cp.DimensionKeys = r.ValueKey, r.DimensionKeys
	b.definitions.Put(&cp)

	return nil
}
