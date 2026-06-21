package dms

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// backendSnapshot mirrors the region-nested backend maps (outer key = region).
type backendSnapshot struct {
	ReplicationInstances   map[string]map[string]*ReplicationInstance   `json:"replicationInstances"`
	Endpoints              map[string]map[string]*Endpoint              `json:"endpoints"`
	ReplicationTasks       map[string]map[string]*ReplicationTask       `json:"replicationTasks"`
	DataMigrations         map[string]map[string]*DataMigration         `json:"dataMigrations"`
	DataProviders          map[string]map[string]*DataProvider          `json:"dataProviders"`
	EventSubscriptions     map[string]map[string]*EventSubscription     `json:"eventSubscriptions"`
	FleetAdvisorCollectors map[string]map[string]*FleetAdvisorCollector `json:"fleetAdvisorCollectors"`
	InstanceProfiles       map[string]map[string]*InstanceProfile       `json:"instanceProfiles"`
	AccountID              string                                       `json:"accountID"`
	Region                 string                                       `json:"region"`
}

func (s *backendSnapshot) ensureNonNil() {
	if s.ReplicationInstances == nil {
		s.ReplicationInstances = make(map[string]map[string]*ReplicationInstance)
	}
	if s.Endpoints == nil {
		s.Endpoints = make(map[string]map[string]*Endpoint)
	}
	if s.ReplicationTasks == nil {
		s.ReplicationTasks = make(map[string]map[string]*ReplicationTask)
	}
	if s.DataMigrations == nil {
		s.DataMigrations = make(map[string]map[string]*DataMigration)
	}
	if s.DataProviders == nil {
		s.DataProviders = make(map[string]map[string]*DataProvider)
	}
	if s.EventSubscriptions == nil {
		s.EventSubscriptions = make(map[string]map[string]*EventSubscription)
	}
	if s.FleetAdvisorCollectors == nil {
		s.FleetAdvisorCollectors = make(map[string]map[string]*FleetAdvisorCollector)
	}
	if s.InstanceProfiles == nil {
		s.InstanceProfiles = make(map[string]map[string]*InstanceProfile)
	}
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		ReplicationInstances:   b.replicationInstances,
		Endpoints:              b.endpoints,
		ReplicationTasks:       b.replicationTasks,
		DataMigrations:         b.dataMigrations,
		DataProviders:          b.dataProviders,
		EventSubscriptions:     b.eventSubscriptions,
		FleetAdvisorCollectors: b.fleetAdvisorCollectors,
		InstanceProfiles:       b.instanceProfiles,
		AccountID:              b.accountID,
		Region:                 b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "dms: Snapshot marshal failure", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot produced by Snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	snap.ensureNonNil()

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.replicationInstances = snap.ReplicationInstances
	b.endpoints = snap.Endpoints
	b.replicationTasks = snap.ReplicationTasks
	b.dataMigrations = snap.DataMigrations
	b.dataProviders = snap.DataProviders
	b.eventSubscriptions = snap.EventSubscriptions
	b.fleetAdvisorCollectors = snap.FleetAdvisorCollectors
	b.instanceProfiles = snap.InstanceProfiles
	b.accountID = snap.AccountID
	b.region = snap.Region

	b.rebuildARNIndexes(&snap)

	return nil
}

// rebuildARNIndexes reconstructs all ARN-keyed maps (region-nested) and
// reinitialises nil tag registries.
func (b *InMemoryBackend) rebuildARNIndexes(snap *backendSnapshot) {
	b.replicationInstancesByARN = make(map[string]map[string]*ReplicationInstance, len(snap.ReplicationInstances))
	for region, m := range snap.ReplicationInstances {
		b.replicationInstancesByARN[region] = rebuildRI(m)
	}

	b.endpointsByARN = make(map[string]map[string]*Endpoint, len(snap.Endpoints))
	for region, m := range snap.Endpoints {
		b.endpointsByARN[region] = rebuildEP(m)
	}

	b.replicationTasksByARN = make(map[string]map[string]*ReplicationTask, len(snap.ReplicationTasks))
	for region, m := range snap.ReplicationTasks {
		b.replicationTasksByARN[region] = rebuildRT(m)
	}

	b.dataMigrationsByARN = make(map[string]map[string]*DataMigration, len(snap.DataMigrations))
	for region, m := range snap.DataMigrations {
		b.dataMigrationsByARN[region] = rebuildDM(m)
	}

	b.dataProvidersByARN = make(map[string]map[string]*DataProvider, len(snap.DataProviders))
	for region, m := range snap.DataProviders {
		b.dataProvidersByARN[region] = rebuildDP(m)
	}

	for _, m := range snap.EventSubscriptions {
		initEventSubscriptionTags(m)
	}
	for _, m := range snap.FleetAdvisorCollectors {
		initCollectorTags(m)
	}

	b.instanceProfilesByARN = make(map[string]map[string]*InstanceProfile, len(snap.InstanceProfiles))
	for region, m := range snap.InstanceProfiles {
		b.instanceProfilesByARN[region] = rebuildIP(m)
	}
}

func rebuildRI(m map[string]*ReplicationInstance) map[string]*ReplicationInstance {
	idx := make(map[string]*ReplicationInstance, len(m))
	for _, ri := range m {
		if ri.Tags == nil {
			ri.Tags = tags.New("dms.replication-instance." + ri.ReplicationInstanceIdentifier + ".tags")
		}
		idx[ri.ReplicationInstanceArn] = ri
	}

	return idx
}

func rebuildEP(m map[string]*Endpoint) map[string]*Endpoint {
	idx := make(map[string]*Endpoint, len(m))
	for _, ep := range m {
		if ep.Tags == nil {
			ep.Tags = tags.New("dms.endpoint." + ep.EndpointIdentifier + ".tags")
		}
		idx[ep.EndpointArn] = ep
	}

	return idx
}

func rebuildRT(m map[string]*ReplicationTask) map[string]*ReplicationTask {
	idx := make(map[string]*ReplicationTask, len(m))
	for _, rt := range m {
		if rt.Tags == nil {
			rt.Tags = tags.New("dms.task." + rt.ReplicationTaskIdentifier + ".tags")
		}
		idx[rt.ReplicationTaskArn] = rt
	}

	return idx
}

func rebuildDM(m map[string]*DataMigration) map[string]*DataMigration {
	idx := make(map[string]*DataMigration, len(m))
	for _, dm := range m {
		if dm.Tags == nil {
			dm.Tags = tags.New("dms.data-migration." + dm.DataMigrationName + ".tags")
		}
		idx[dm.DataMigrationArn] = dm
	}

	return idx
}

func rebuildDP(m map[string]*DataProvider) map[string]*DataProvider {
	idx := make(map[string]*DataProvider, len(m))
	for _, dp := range m {
		if dp.Tags == nil {
			dp.Tags = tags.New("dms.data-provider." + dp.DataProviderName + ".tags")
		}
		idx[dp.DataProviderArn] = dp
	}

	return idx
}

func initEventSubscriptionTags(m map[string]*EventSubscription) {
	for _, es := range m {
		if es.Tags == nil {
			es.Tags = tags.New("dms.event-subscription." + es.SubscriptionName + ".tags")
		}
	}
}

func initCollectorTags(m map[string]*FleetAdvisorCollector) {
	for _, col := range m {
		if col.Tags == nil {
			col.Tags = tags.New("dms.fleet-advisor-collector." + col.CollectorName + ".tags")
		}
	}
}

func rebuildIP(m map[string]*InstanceProfile) map[string]*InstanceProfile {
	idx := make(map[string]*InstanceProfile, len(m))
	for _, ip := range m {
		if ip.Tags == nil {
			ip.Tags = tags.New("dms.instance-profile." + ip.InstanceProfileName + ".tags")
		}
		idx[ip.InstanceProfileArn] = ip
	}

	return idx
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
