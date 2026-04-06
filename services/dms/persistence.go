package dms

import (
	"encoding/json"
	"log/slog"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type backendSnapshot struct {
	ReplicationInstances   map[string]*ReplicationInstance   `json:"replicationInstances"`
	Endpoints              map[string]*Endpoint              `json:"endpoints"`
	ReplicationTasks       map[string]*ReplicationTask       `json:"replicationTasks"`
	DataMigrations         map[string]*DataMigration         `json:"dataMigrations"`
	DataProviders          map[string]*DataProvider          `json:"dataProviders"`
	EventSubscriptions     map[string]*EventSubscription     `json:"eventSubscriptions"`
	FleetAdvisorCollectors map[string]*FleetAdvisorCollector `json:"fleetAdvisorCollectors"`
	InstanceProfiles       map[string]*InstanceProfile       `json:"instanceProfiles"`
	AccountID              string                            `json:"accountID"`
	Region                 string                            `json:"region"`
}

func (s *backendSnapshot) ensureNonNil() {
	if s.ReplicationInstances == nil {
		s.ReplicationInstances = make(map[string]*ReplicationInstance)
	}
	if s.Endpoints == nil {
		s.Endpoints = make(map[string]*Endpoint)
	}
	if s.ReplicationTasks == nil {
		s.ReplicationTasks = make(map[string]*ReplicationTask)
	}
	if s.DataMigrations == nil {
		s.DataMigrations = make(map[string]*DataMigration)
	}
	if s.DataProviders == nil {
		s.DataProviders = make(map[string]*DataProvider)
	}
	if s.EventSubscriptions == nil {
		s.EventSubscriptions = make(map[string]*EventSubscription)
	}
	if s.FleetAdvisorCollectors == nil {
		s.FleetAdvisorCollectors = make(map[string]*FleetAdvisorCollector)
	}
	if s.InstanceProfiles == nil {
		s.InstanceProfiles = make(map[string]*InstanceProfile)
	}
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
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
		slog.Default().Warn("dms: Snapshot marshal failure", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot produced by Snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
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

// rebuildARNIndexes reconstructs all ARN-keyed maps and reinitialises nil tag registries.
func (b *InMemoryBackend) rebuildARNIndexes(snap *backendSnapshot) {
	b.replicationInstancesByARN = rebuildRI(snap.ReplicationInstances)
	b.endpointsByARN = rebuildEP(snap.Endpoints)
	b.replicationTasksByARN = rebuildRT(snap.ReplicationTasks)
	b.dataMigrationsByARN = rebuildDM(snap.DataMigrations)
	b.dataProvidersByARN = rebuildDP(snap.DataProviders)
	initEventSubscriptionTags(snap.EventSubscriptions)
	initCollectorTags(snap.FleetAdvisorCollectors)
	b.instanceProfilesByARN = rebuildIP(snap.InstanceProfiles)
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
func (h *Handler) Snapshot() []byte { return h.Backend.Snapshot() }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error { return h.Backend.Restore(data) }
