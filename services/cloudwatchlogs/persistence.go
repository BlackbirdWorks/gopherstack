package cloudwatchlogs

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type backendSnapshot struct {
	Groups                 map[string]map[string]*LogGroup                    `json:"groups"`
	Streams                map[string]map[string]map[string]*LogStream        `json:"streams"`
	Events                 map[string]map[string]map[string][]*OutputLogEvent `json:"events"`
	SubscriptionFilters    map[string]map[string][]*SubscriptionFilter        `json:"subscriptionFilters"`
	ExportTasks            map[string]*ExportTask                             `json:"exportTasks,omitempty"`
	ImportTasks            map[string]*ImportTask                             `json:"importTasks,omitempty"`
	Deliveries             map[string]*Delivery                               `json:"deliveries,omitempty"`
	LogAnomalyDetectors    map[string]*LogAnomalyDetector                     `json:"logAnomalyDetectors,omitempty"`
	ScheduledQueries       map[string]*ScheduledQuery                         `json:"scheduledQueries,omitempty"`
	AccountPolicies        map[string]*AccountPolicy                          `json:"accountPolicies,omitempty"`
	KmsKeys                map[string]string                                  `json:"kmsKeys,omitempty"`
	S3TableIntegrations    map[string]string                                  `json:"s3TableIntegrations,omitempty"`
	MetricFilters          map[string]map[string]map[string]*MetricFilter     `json:"metricFilters,omitempty"`
	QueryDefinitions       map[string]*QueryDefinition                        `json:"queryDefinitions,omitempty"`
	DataProtectionPolicies map[string]string                                  `json:"dataProtectionPolicies,omitempty"`
	ResourcePolicies       map[string]ResourcePolicy                          `json:"resourcePolicies,omitempty"`
	DeliveryDestinations   map[string]DeliveryDestination                     `json:"deliveryDestinations,omitempty"`
	DeliverySources        map[string]DeliverySource                          `json:"deliverySources,omitempty"`
	Destinations           map[string]CWLDestination                          `json:"destinations,omitempty"`
	IndexPolicies          map[string]IndexPolicy                             `json:"indexPolicies,omitempty"`
	Transformers           map[string]Transformer                             `json:"transformers,omitempty"`
	Integrations           map[string]CWLIntegration                          `json:"integrations,omitempty"`
	DeletionProtected      map[string]bool                                    `json:"deletionProtected,omitempty"`
	AccountID              string                                             `json:"accountID"`
	Region                 string                                             `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Groups:                 b.groups,
		Streams:                b.streams,
		Events:                 b.events,
		SubscriptionFilters:    b.subscriptionFilters,
		ExportTasks:            b.exportTasks,
		ImportTasks:            b.importTasks,
		Deliveries:             b.deliveries,
		LogAnomalyDetectors:    b.logAnomalyDetectors,
		ScheduledQueries:       b.scheduledQueries,
		AccountPolicies:        b.accountPolicies,
		KmsKeys:                b.kmsKeys,
		S3TableIntegrations:    b.s3TableIntegrations,
		MetricFilters:          b.metricFilters,
		QueryDefinitions:       b.queryDefinitions,
		DataProtectionPolicies: b.dataProtectionPolicies,
		ResourcePolicies:       b.resourcePolicies,
		DeliveryDestinations:   b.deliveryDestinations,
		DeliverySources:        b.deliverySources,
		Destinations:           b.destinations,
		IndexPolicies:          b.indexPolicies,
		Transformers:           b.transformers,
		Integrations:           b.integrations,
		DeletionProtected:      b.deletionProtected,
		AccountID:              b.accountID,
		Region:                 b.region,
	}

	return persistence.MarshalSnapshot(ctx, "cloudwatchlogs", snap)
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "cloudwatchlogs", data, &snap); err != nil {
		return err
	}

	initSnapshotNilMaps(&snap)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.groups = snap.Groups
	b.streams = snap.Streams
	b.events = snap.Events
	b.subscriptionFilters = snap.SubscriptionFilters
	b.exportTasks = snap.ExportTasks
	b.importTasks = snap.ImportTasks
	b.deliveries = snap.Deliveries
	b.logAnomalyDetectors = snap.LogAnomalyDetectors
	b.scheduledQueries = snap.ScheduledQueries
	b.accountPolicies = snap.AccountPolicies
	b.kmsKeys = snap.KmsKeys
	b.s3TableIntegrations = snap.S3TableIntegrations
	b.metricFilters = snap.MetricFilters
	b.queryDefinitions = snap.QueryDefinitions
	b.dataProtectionPolicies = snap.DataProtectionPolicies
	b.resourcePolicies = snap.ResourcePolicies
	b.deliveryDestinations = snap.DeliveryDestinations
	b.deliverySources = snap.DeliverySources
	b.destinations = snap.Destinations
	b.indexPolicies = snap.IndexPolicies
	b.transformers = snap.Transformers
	b.integrations = snap.Integrations
	b.deletionProtected = snap.DeletionProtected
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// initSnapshotNilMaps replaces any nil map in snap with an empty map so the
// restored backend never contains a nil map reference.
func initSnapshotNilMaps(snap *backendSnapshot) {
	initCoreNilMaps(snap)
	initCompletenessNilMaps(snap)
}

func initCoreNilMaps(snap *backendSnapshot) {
	if snap.Groups == nil {
		snap.Groups = make(map[string]map[string]*LogGroup)
	}
	if snap.Streams == nil {
		snap.Streams = make(map[string]map[string]map[string]*LogStream)
	}
	if snap.Events == nil {
		snap.Events = make(map[string]map[string]map[string][]*OutputLogEvent)
	}
	if snap.SubscriptionFilters == nil {
		snap.SubscriptionFilters = make(map[string]map[string][]*SubscriptionFilter)
	}
	if snap.ExportTasks == nil {
		snap.ExportTasks = make(map[string]*ExportTask)
	}
	if snap.ImportTasks == nil {
		snap.ImportTasks = make(map[string]*ImportTask)
	}
	if snap.Deliveries == nil {
		snap.Deliveries = make(map[string]*Delivery)
	}
	if snap.LogAnomalyDetectors == nil {
		snap.LogAnomalyDetectors = make(map[string]*LogAnomalyDetector)
	}
	if snap.ScheduledQueries == nil {
		snap.ScheduledQueries = make(map[string]*ScheduledQuery)
	}
	if snap.AccountPolicies == nil {
		snap.AccountPolicies = make(map[string]*AccountPolicy)
	}
	if snap.KmsKeys == nil {
		snap.KmsKeys = make(map[string]string)
	}
	if snap.S3TableIntegrations == nil {
		snap.S3TableIntegrations = make(map[string]string)
	}
}

func initCompletenessNilMaps(snap *backendSnapshot) {
	if snap.MetricFilters == nil {
		snap.MetricFilters = make(map[string]map[string]map[string]*MetricFilter)
	}
	if snap.QueryDefinitions == nil {
		snap.QueryDefinitions = make(map[string]*QueryDefinition)
	}
	if snap.DataProtectionPolicies == nil {
		snap.DataProtectionPolicies = make(map[string]string)
	}
	if snap.ResourcePolicies == nil {
		snap.ResourcePolicies = make(map[string]ResourcePolicy)
	}
	if snap.DeliveryDestinations == nil {
		snap.DeliveryDestinations = make(map[string]DeliveryDestination)
	}
	if snap.DeliverySources == nil {
		snap.DeliverySources = make(map[string]DeliverySource)
	}
	if snap.Destinations == nil {
		snap.Destinations = make(map[string]CWLDestination)
	}
	if snap.IndexPolicies == nil {
		snap.IndexPolicies = make(map[string]IndexPolicy)
	}
	if snap.Transformers == nil {
		snap.Transformers = make(map[string]Transformer)
	}
	if snap.Integrations == nil {
		snap.Integrations = make(map[string]CWLIntegration)
	}
	if snap.DeletionProtected == nil {
		snap.DeletionProtected = make(map[string]bool)
	}
}

// handlerSnapshot is the full persisted state for a Handler, combining both
// backend state and the handler-level tag data that lives outside the backend.
type handlerSnapshot struct {
	Tags    map[string]map[string]string `json:"tags,omitempty"`
	Backend []byte                       `json:"backend"`
}

// Snapshot implements persistence.Persistable by serialising both the backend
// state and the handler-owned tag data.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	type snapshotter interface {
		Snapshot(ctx context.Context) []byte
	}

	var backendData []byte
	if s, ok := h.Backend.(snapshotter); ok {
		backendData = s.Snapshot(ctx)
	}

	// Collect tags outside the backend lock.
	h.tagsMu.RLock("Snapshot")
	tagMap := make(map[string]map[string]string, len(h.tags))
	for k, t := range h.tags {
		tagMap[k] = t.Clone()
	}
	h.tagsMu.RUnlock()

	snap := handlerSnapshot{
		Backend: backendData,
		Tags:    tagMap,
	}

	return persistence.MarshalSnapshot(ctx, "cloudwatchlogs", snap)
}

// Restore implements persistence.Persistable by restoring both the backend
// state and the handler-owned tag data.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	// Attempt to decode as the combined handlerSnapshot format first.
	var snap handlerSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "cloudwatchlogs", data, &snap); err != nil {
		return err
	}

	if err := h.restoreBackend(ctx, snap.Backend, data); err != nil {
		return err
	}

	h.restoreTags(snap.Tags)

	return nil
}

// restoreBackend restores backend state from the snapshot.
// If backendData is non-nil it came from the new combined format; otherwise the
// caller should fall back to the raw data (legacy bare-backend format).
func (h *Handler) restoreBackend(ctx context.Context, backendData, rawData []byte) error {
	type restorer interface {
		Restore(context.Context, []byte) error
	}

	r, ok := h.Backend.(restorer)
	if !ok {
		return nil
	}

	src := backendData
	if src == nil {
		src = rawData
	}

	return r.Restore(ctx, src)
}

// restoreTags replaces the handler's tag store with the persisted tag map.
// All existing tags are discarded and replaced with the snapshot values.
func (h *Handler) restoreTags(tagMap map[string]map[string]string) {
	h.tagsMu.Lock("Restore")
	defer h.tagsMu.Unlock()

	// Close existing tag collections to prevent Prometheus metric registry leaks.
	for _, t := range h.tags {
		t.Close()
	}

	// Replace with a fresh map seeded from the snapshot.
	h.tags = make(map[string]*tags.Tags, len(tagMap))

	for resourceID, kv := range tagMap {
		t := tags.New("cwl." + resourceID + ".tags")
		t.Merge(kv)
		h.tags[resourceID] = t
	}
}
