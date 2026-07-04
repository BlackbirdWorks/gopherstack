package eventbridge

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type backendSnapshot struct {
	Buses           map[string]map[string]*EventBus           `json:"buses"`
	Rules           map[string]map[string]map[string]*Rule    `json:"rules"`
	Targets         map[string]map[string]map[string]*Target  `json:"targets"`
	EventSources    map[string]map[string]*EventSource        `json:"eventSources,omitempty"`
	Replays         map[string]map[string]*Replay             `json:"replays,omitempty"`
	APIDestinations map[string]map[string]*APIDestination     `json:"apiDestinations,omitempty"`
	Archives        map[string]map[string]*Archive            `json:"archives,omitempty"`
	Connections     map[string]map[string]*Connection         `json:"connections,omitempty"`
	Endpoints       map[string]map[string]*Endpoint           `json:"endpoints,omitempty"`
	PartnerSources  map[string]map[string]*PartnerEventSource `json:"partnerSources,omitempty"`
	AccountID       string                                    `json:"accountID"`
	Region          string                                    `json:"region"`
	EventLog        []EventLogEntry                           `json:"eventLog"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Buses:           b.buses,
		Rules:           b.rules,
		Targets:         b.targets,
		EventSources:    b.eventSources,
		Replays:         b.replays,
		APIDestinations: b.apiDestinations,
		Archives:        b.archives,
		Connections:     b.connections,
		Endpoints:       b.endpoints,
		PartnerSources:  b.partnerSources,
		EventLog:        b.eventLog,
		AccountID:       b.accountID,
		Region:          b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).
			WarnContext(ctx, "persistence: snapshot marshal failed", "service", "eventbridge", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
// The logger and delivery targets are not restored — they are re-wired by the CLI.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "eventbridge", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	ensureBackendSnapshotMaps(&snap)

	b.buses = snap.Buses
	b.rules = snap.Rules
	b.targets = snap.Targets
	b.eventSources = snap.EventSources
	b.replays = snap.Replays
	b.apiDestinations = snap.APIDestinations
	b.archives = snap.Archives
	b.connections = snap.Connections
	b.endpoints = snap.Endpoints
	b.partnerSources = snap.PartnerSources
	b.eventLog = snap.EventLog
	b.accountID = snap.AccountID
	b.region = snap.Region
	b.ruleIndex = make(map[string]map[string]map[ruleIndexKey]map[string]*Rule)
	b.targetsByARN = make(map[string]map[string]map[string]struct{})
	b.patternCache = sync.Map{}

	if err := b.rebuildRuleIndexesLocked(); err != nil {
		return err
	}

	b.rebuildTargetsByARNLocked()

	return nil
}

func ensureBackendSnapshotMaps(snap *backendSnapshot) {
	if snap.Buses == nil {
		snap.Buses = make(map[string]map[string]*EventBus)
	}
	if snap.Rules == nil {
		snap.Rules = make(map[string]map[string]map[string]*Rule)
	}
	if snap.Targets == nil {
		snap.Targets = make(map[string]map[string]map[string]*Target)
	}
	if snap.EventSources == nil {
		snap.EventSources = make(map[string]map[string]*EventSource)
	}
	if snap.Replays == nil {
		snap.Replays = make(map[string]map[string]*Replay)
	}
	if snap.APIDestinations == nil {
		snap.APIDestinations = make(map[string]map[string]*APIDestination)
	}
	if snap.Archives == nil {
		snap.Archives = make(map[string]map[string]*Archive)
	}
	if snap.Connections == nil {
		snap.Connections = make(map[string]map[string]*Connection)
	}
	if snap.Endpoints == nil {
		snap.Endpoints = make(map[string]map[string]*Endpoint)
	}
	if snap.PartnerSources == nil {
		snap.PartnerSources = make(map[string]map[string]*PartnerEventSource)
	}
}

func (b *InMemoryBackend) rebuildRuleIndexesLocked() error {
	for region, regRules := range b.rules {
		for busKey, busRules := range regRules {
			for _, rule := range busRules {
				if rule.EventPattern != "" {
					compiled, err := b.getOrCompilePattern(rule.EventPattern)
					if err != nil {
						return err
					}
					rule.compiledPattern = compiled
				}
				b.addRuleToIndex(region, busKey, rule)
			}
		}
	}

	return nil
}

// rebuildTargetsByARNLocked rebuilds the targetsByARN index from b.targets.
// Must be called with the write lock held.
func (b *InMemoryBackend) rebuildTargetsByARNLocked() {
	for region, regionTargets := range b.targets {
		for targetKey, tMap := range regionTargets {
			for _, t := range tMap {
				b.arnIndexAdd(region, t.Arn, targetKey)
			}
		}
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

	return persistence.MarshalSnapshot(ctx, "eventbridge", snap)
}

// Restore implements persistence.Persistable by restoring both the backend
// state and the handler-owned tag data.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	// Attempt to decode as the combined handlerSnapshot format first.
	var snap handlerSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "eventbridge", data, &snap); err != nil {
		return err
	}

	if err := h.restoreBackend(ctx, snap.Backend, data); err != nil {
		return err
	}

	h.restoreTags(snap.Tags)

	return nil
}

// restoreBackend restores backend state from the snapshot.
// If backendData is non-nil it came from the new combined format; otherwise
// the caller falls back to the raw data (legacy bare-backend format).
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
// All existing tag collections are closed to prevent Prometheus metric
// registry leaks, then replaced with values from the snapshot.
func (h *Handler) restoreTags(tagMap map[string]map[string]string) {
	h.tagsMu.Lock("Restore")
	defer h.tagsMu.Unlock()

	for _, t := range h.tags {
		t.Close()
	}

	h.tags = make(map[string]*svcTags.Tags, len(tagMap))

	for resourceID, kv := range tagMap {
		t := svcTags.New("eb." + resourceID + ".tags")
		t.Merge(kv)
		h.tags[resourceID] = t
	}
}
