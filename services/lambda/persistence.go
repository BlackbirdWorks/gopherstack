package lambda

import (
	"encoding/json"
	"log/slog"
)

type backendSnapshot struct {
	Functions             map[string]*FunctionConfiguration                      `json:"functions"`
	EventSourceMappings   map[string]*EventSourceMapping                         `json:"eventSourceMappings"`
	Aliases               map[string]map[string]*FunctionAlias                   `json:"aliases"`
	Versions              map[string][]*FunctionVersion                          `json:"versions"`
	FunctionURLConfigs    map[string]*FunctionURLConfig                          `json:"functionURLConfigs"`
	VersionCounters       map[string]int                                         `json:"versionCounters"`
	Layers                map[string][]*LayerVersion                             `json:"layers"`
	LayerVersionCounters  map[string]int64                                       `json:"layerVersionCounters"`
	LayerPolicies         map[string]map[int64]map[string]*LayerVersionStatement `json:"layerPolicies"`
	EventInvokeConfigs    map[string]*FunctionEventInvokeConfig                  `json:"eventInvokeConfigs"`
	FunctionConcurrencies map[string]int                                         `json:"functionConcurrencies"`
	AccountID             string                                                 `json:"accountID"`
	Region                string                                                 `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
// ZipData (code bytes) are not serialised — code must be re-deployed after restore.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Functions:             b.functions,
		EventSourceMappings:   b.eventSourceMappings,
		Aliases:               b.aliases,
		Versions:              b.versions,
		FunctionURLConfigs:    b.functionURLConfigs,
		VersionCounters:       b.versionCounters,
		Layers:                b.layers,
		LayerVersionCounters:  b.layerVersionCounters,
		LayerPolicies:         b.layerPolicies,
		EventInvokeConfigs:    b.eventInvokeConfigs,
		FunctionConcurrencies: b.functionConcurrencies,
		AccountID:             b.accountID,
		Region:                b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("persistence: snapshot marshal failed", "service", "lambda", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
// Code bytes are not restored — functions must be re-deployed after restore.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	normalizeSnapshot(&snap)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	restoreSnapshotFunctions(snap.Functions, snap.FunctionConcurrencies)
	restoreSnapshotLayers(snap.Layers)

	b.functions = snap.Functions
	b.eventSourceMappings = snap.EventSourceMappings
	b.aliases = snap.Aliases
	b.versions = snap.Versions
	b.functionURLConfigs = snap.FunctionURLConfigs
	b.versionCounters = snap.VersionCounters
	b.layers = snap.Layers
	b.layerVersionCounters = snap.LayerVersionCounters
	b.layerPolicies = snap.LayerPolicies
	b.eventInvokeConfigs = snap.EventInvokeConfigs
	b.functionConcurrencies = snap.FunctionConcurrencies
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// normalizeSnapshot initialises nil maps in a snapshot to empty maps so callers
// never need to nil-check after a Restore.
func normalizeSnapshot(snap *backendSnapshot) {
	if snap.Functions == nil {
		snap.Functions = make(map[string]*FunctionConfiguration)
	}

	if snap.EventSourceMappings == nil {
		snap.EventSourceMappings = make(map[string]*EventSourceMapping)
	}

	if snap.Aliases == nil {
		snap.Aliases = make(map[string]map[string]*FunctionAlias)
	}

	if snap.Versions == nil {
		snap.Versions = make(map[string][]*FunctionVersion)
	}

	if snap.FunctionURLConfigs == nil {
		snap.FunctionURLConfigs = make(map[string]*FunctionURLConfig)
	}

	if snap.VersionCounters == nil {
		snap.VersionCounters = make(map[string]int)
	}

	if snap.Layers == nil {
		snap.Layers = make(map[string][]*LayerVersion)
	}

	if snap.LayerVersionCounters == nil {
		snap.LayerVersionCounters = make(map[string]int64)
	}

	if snap.LayerPolicies == nil {
		snap.LayerPolicies = make(map[string]map[int64]map[string]*LayerVersionStatement)
	}

	if snap.EventInvokeConfigs == nil {
		snap.EventInvokeConfigs = make(map[string]*FunctionEventInvokeConfig)
	}

	if snap.FunctionConcurrencies == nil {
		snap.FunctionConcurrencies = make(map[string]int)
	}
}

// restoreSnapshotFunctions clears transient fields on restored function configurations
// and re-links ReservedConcurrentExecutions from the concurrency map.
func restoreSnapshotFunctions(fns map[string]*FunctionConfiguration, concurrencies map[string]int) {
	for name, fn := range fns {
		fn.ZipData = nil

		if fn.LastUpdateStatus == "" {
			fn.LastUpdateStatus = LastUpdateStatusSuccessful
		}

		if reserved, ok := concurrencies[name]; ok {
			v := reserved
			fn.ReservedConcurrentExecutions = &v
		} else {
			fn.ReservedConcurrentExecutions = nil
		}
	}
}

// restoreSnapshotLayers clears zip data from restored layer versions.
func restoreSnapshotLayers(layers map[string][]*LayerVersion) {
	for _, versions := range layers {
		for _, lv := range versions {
			lv.ZipData = nil
		}
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	type snapshotter interface{ Snapshot() []byte }
	if s, ok := h.Backend.(snapshotter); ok {
		return s.Snapshot()
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	type restorer interface{ Restore([]byte) error }
	if r, ok := h.Backend.(restorer); ok {
		return r.Restore(data)
	}

	return nil
}
