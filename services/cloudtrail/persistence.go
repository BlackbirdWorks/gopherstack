package cloudtrail

import (
	"context"
	"encoding/json"
)

type backendSnapshot struct {
	Trails           map[string]*Trail          `json:"trails"`
	TrailsByARN      map[string]string          `json:"trailsByArn"`
	Channels         map[string]*Channel        `json:"channels,omitempty"`
	Dashboards       map[string]*Dashboard      `json:"dashboards,omitempty"`
	EventDataStores  map[string]*EventDataStore `json:"eventDataStores,omitempty"`
	Queries          map[string]*Query          `json:"queries,omitempty"`
	ResourcePolicies map[string]*ResourcePolicy `json:"resourcePolicies,omitempty"`
	Imports          map[string]*Import         `json:"imports,omitempty"`
	AccountID        string                     `json:"accountID"`
	Region           string                     `json:"region"`
	ChannelCounter   int                        `json:"channelCounter"`
	DashboardCounter int                        `json:"dashboardCounter"`
	EDSCounter       int                        `json:"edsCounter"`
	QueryCounter     int                        `json:"queryCounter"`
	ImportCounter    int                        `json:"importCounter"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Trails:           b.trails,
		TrailsByARN:      b.trailsByARN,
		Channels:         b.channels,
		Dashboards:       b.dashboards,
		EventDataStores:  b.eventDataStores,
		Queries:          b.queries,
		ResourcePolicies: b.resourcePolicies,
		Imports:          b.imports,
		AccountID:        b.accountID,
		Region:           b.region,
		ChannelCounter:   b.channelCounter,
		DashboardCounter: b.dashboardCounter,
		EDSCounter:       b.edsCounter,
		QueryCounter:     b.queryCounter,
		ImportCounter:    b.importCounter,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	ensureSnapMaps(&snap)

	b.trails = snap.Trails
	b.trailsByARN = snap.TrailsByARN
	b.channels = snap.Channels
	b.dashboards = snap.Dashboards
	b.eventDataStores = snap.EventDataStores
	b.queries = snap.Queries
	b.resourcePolicies = snap.ResourcePolicies
	b.imports = snap.Imports
	b.accountID = snap.AccountID
	b.region = snap.Region
	b.channelCounter = snap.ChannelCounter
	b.dashboardCounter = snap.DashboardCounter
	b.edsCounter = snap.EDSCounter
	b.queryCounter = snap.QueryCounter
	b.importCounter = snap.ImportCounter

	// Rebuild secondary indexes from restored state.
	b.channelsByARN = make(map[string]string, len(b.channels))
	b.channelsByName = make(map[string]string, len(b.channels))
	for id, ch := range b.channels {
		b.channelsByARN[ch.ChannelARN] = id
		b.channelsByName[ch.Name] = id
	}
	b.dashboardsByARN = make(map[string]string, len(b.dashboards))
	b.dashboardsByName = make(map[string]string, len(b.dashboards))
	for id, d := range b.dashboards {
		b.dashboardsByARN[d.DashboardARN] = id
		b.dashboardsByName[d.Name] = id
	}
	b.edsByARN = make(map[string]string, len(b.eventDataStores))
	b.edsByName = make(map[string]string, len(b.eventDataStores))
	for id, eds := range b.eventDataStores {
		b.edsByARN[eds.EventDataStoreARN] = id
		b.edsByName[eds.Name] = id
	}

	return nil
}

func ensureSnapMaps(snap *backendSnapshot) {
	if snap.Trails == nil {
		snap.Trails = make(map[string]*Trail)
	}
	if snap.TrailsByARN == nil {
		snap.TrailsByARN = make(map[string]string)
	}
	if snap.Channels == nil {
		snap.Channels = make(map[string]*Channel)
	}
	if snap.Dashboards == nil {
		snap.Dashboards = make(map[string]*Dashboard)
	}
	if snap.EventDataStores == nil {
		snap.EventDataStores = make(map[string]*EventDataStore)
	}
	if snap.Queries == nil {
		snap.Queries = make(map[string]*Query)
	}
	if snap.ResourcePolicies == nil {
		snap.ResourcePolicies = make(map[string]*ResourcePolicy)
	}
	if snap.Imports == nil {
		snap.Imports = make(map[string]*Import)
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
