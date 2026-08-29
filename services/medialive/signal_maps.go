package medialive

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// --- Signal Map operations ---

// findSignalMap locates a signal map by ID or ARN or name.
func (b *InMemoryBackend) findSignalMap(identifier string) (*storedSignalMap, bool) {
	for _, sm := range b.signalMaps.All() {
		if sm.ID == identifier || sm.Arn == identifier || sm.Name == identifier {
			return sm, true
		}
	}

	return nil, false
}

// CreateSignalMap creates a new signal map.
func (b *InMemoryBackend) CreateSignalMap(
	name, description, discoveryEntryPointArn string,
	cwGroupIDs, ebGroupIDs []string,
	tags map[string]string,
) (*SignalMap, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name required", ErrInvalidParameter)
	}

	id := newID()
	now := time.Now().UTC()
	sm := &storedSignalMap{
		Tags:                            copyTags(tags),
		CloudWatchAlarmTemplateGroupIDs: append([]string{}, cwGroupIDs...),
		EventBridgeRuleTemplateGroupIDs: append([]string{}, ebGroupIDs...),
		Arn:                             b.signalMapARN(id),
		ID:                              id,
		Name:                            name,
		Description:                     description,
		DiscoveryEntryPointArn:          discoveryEntryPointArn,
		Status:                          "CREATE_COMPLETE",
		MonitorDeploymentStatus:         "NOT_DEPLOYED",
		CreatedAt:                       now,
		ModifiedAt:                      now,
	}

	b.mu.Lock("CreateSignalMap")
	defer b.mu.Unlock()
	b.signalMaps.Put(sm)

	return sm.toSignalMap(), nil
}

// GetSignalMap returns a signal map by identifier.
func (b *InMemoryBackend) GetSignalMap(identifier string) (*SignalMap, error) {
	b.mu.RLock("GetSignalMap")
	defer b.mu.RUnlock()
	sm, ok := b.findSignalMap(identifier)
	if !ok {
		return nil, fmt.Errorf("%w: signal map %s not found", ErrNotFound, identifier)
	}

	return sm.toSignalMap(), nil
}

// ListSignalMaps returns all signal maps.
// ListSignalMaps returns signal maps referencing cwGroupIdentifier and/or
// ebGroupIdentifier when set (api_op_ListSignalMaps.go's
// CloudWatchAlarmTemplateGroupIdentifier/EventBridgeRuleTemplateGroupIdentifier,
// matched against each signal map's own stored group-identifier lists via
// groupMatchesIdentifierList, cloudwatch_alarm_templates.go). Both filters
// apply (AND) when both are set.
func (b *InMemoryBackend) ListSignalMaps(
	maxResults int,
	nextToken string,
	cwGroupIdentifier, ebGroupIdentifier string,
) ([]*SignalMap, string, error) {
	b.mu.RLock("ListSignalMaps")
	defer b.mu.RUnlock()
	all := b.signalMaps.All()

	if cwGroupIdentifier != "" {
		id, arn, name := cwGroupIdentifier, cwGroupIdentifier, cwGroupIdentifier
		if g, ok := b.findCWAlarmTemplateGroup(cwGroupIdentifier); ok {
			id, arn, name = g.ID, g.Arn, g.Name
		}

		filtered := make([]*storedSignalMap, 0, len(all))

		for _, sm := range all {
			if groupMatchesIdentifierList(id, arn, name, sm.CloudWatchAlarmTemplateGroupIDs) {
				filtered = append(filtered, sm)
			}
		}

		all = filtered
	}

	if ebGroupIdentifier != "" {
		id, arn, name := ebGroupIdentifier, ebGroupIdentifier, ebGroupIdentifier
		if g, ok := b.findEBRuleTemplateGroup(ebGroupIdentifier); ok {
			id, arn, name = g.ID, g.Arn, g.Name
		}

		filtered := make([]*storedSignalMap, 0, len(all))

		for _, sm := range all {
			if groupMatchesIdentifierList(id, arn, name, sm.EventBridgeRuleTemplateGroupIDs) {
				filtered = append(filtered, sm)
			}
		}

		all = filtered
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })
	pg := page.New(all, nextToken, maxResults, defaultMaxResults)
	result := make([]*SignalMap, 0, len(pg.Data))
	for _, sm := range pg.Data {
		result = append(result, sm.toSignalMap())
	}

	return result, pg.Next, nil
}

// DeleteSignalMap deletes a signal map.
func (b *InMemoryBackend) DeleteSignalMap(identifier string) error {
	b.mu.Lock("DeleteSignalMap")
	defer b.mu.Unlock()
	sm, ok := b.findSignalMap(identifier)
	if !ok {
		return fmt.Errorf("%w: signal map %s not found", ErrNotFound, identifier)
	}
	b.signalMaps.Delete(sm.ID)
	delete(b.tags, sm.Arn)

	return nil
}

// StartUpdateSignalMap updates a signal map's configuration.
func (b *InMemoryBackend) StartUpdateSignalMap(
	identifier, name, description string,
	cwGroupIDs, ebGroupIDs []string,
) (*SignalMap, error) {
	b.mu.Lock("StartUpdateSignalMap")
	defer b.mu.Unlock()
	sm, ok := b.findSignalMap(identifier)
	if !ok {
		return nil, fmt.Errorf("%w: signal map %s not found", ErrNotFound, identifier)
	}
	if name != "" {
		sm.Name = name
	}
	if description != "" {
		sm.Description = description
	}
	if cwGroupIDs != nil {
		sm.CloudWatchAlarmTemplateGroupIDs = append([]string{}, cwGroupIDs...)
	}
	if ebGroupIDs != nil {
		sm.EventBridgeRuleTemplateGroupIDs = append([]string{}, ebGroupIDs...)
	}
	sm.Status = "UPDATE_COMPLETE"
	sm.ModifiedAt = time.Now().UTC()

	return sm.toSignalMap(), nil
}

// StartMonitorDeployment deploys monitoring for a signal map.
func (b *InMemoryBackend) StartMonitorDeployment(identifier string) (*SignalMap, error) {
	b.mu.Lock("StartMonitorDeployment")
	defer b.mu.Unlock()
	sm, ok := b.findSignalMap(identifier)
	if !ok {
		return nil, fmt.Errorf("%w: signal map %s not found", ErrNotFound, identifier)
	}
	sm.MonitorDeploymentStatus = "DEPLOYMENT_COMPLETE"
	sm.ModifiedAt = time.Now().UTC()

	return sm.toSignalMap(), nil
}

// --- SignalMap monitor deployment teardown ---

// StartDeleteMonitorDeployment tears down the monitor deployment for a signal map.
func (b *InMemoryBackend) StartDeleteMonitorDeployment(identifier string) (*SignalMap, error) {
	b.mu.Lock("StartDeleteMonitorDeployment")
	defer b.mu.Unlock()

	sm, ok := b.findSignalMap(identifier)
	if !ok {
		return nil, fmt.Errorf("%w: signalMap %s not found", ErrNotFound, identifier)
	}

	sm.MonitorDeploymentStatus = "DELETE_COMPLETE"
	sm.ModifiedAt = time.Now().UTC()

	return sm.toSignalMap(), nil
}
