package dms

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/google/uuid"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
// DMS resources are isolated per region: every backend operation resolves the
// caller's region from the request context and operates only on that region's
// nested store. DMS replication is inherently single-region (the source and
// target endpoints and the replication instance all live in the same region),
// so cross-region references never occur and isolation is always safe.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

const (
	statusActive         = "active"
	statusReady          = "ready"
	statusRunning        = "running"
	statusStopped        = "stopped"
	statusAvailable      = "available"
	statusCancelling     = "cancelling"
	statusSuccessful     = "successful"
	statusCreated        = "created"
	defaultEngineVersion = "3.5.3"

	eventCategoryCreation    = "creation"
	eventCategoryDeletion    = "deletion"
	eventCategoryStateChange = "state-change"
)

// InMemoryBackend is the in-memory store for AWS DMS resources.
//
// Every named resource collection is a *store.Table[T] keyed by a composite
// "<region>|<identifier-or-ARN>" primary key (see store_setup.go's
// [regionKey]), so that same-named resources stay isolated across regions
// exactly as the pre-Phase-3.3 per-region nested maps did. ByARN/ByID
// lookups and region-scoped listing go through the accompanying
// *store.Index[T] fields. Callers must hold b.mu while accessing any table
// or index.
type InMemoryBackend struct {
	registry               *store.Registry
	replicationInstances   *store.Table[ReplicationInstance]
	endpoints              *store.Table[Endpoint]
	replicationTasks       *store.Table[ReplicationTask]
	dataMigrations         *store.Table[DataMigration]
	dataProviders          *store.Table[DataProvider]
	eventSubscriptions     *store.Table[EventSubscription]
	fleetAdvisorCollectors *store.Table[FleetAdvisorCollector]
	// fleetAdvisorCollectorsByID indexes collectors by CollectorReferencedID (UUID) for O(1) delete by ID.
	fleetAdvisorCollectorsByID *store.Index[FleetAdvisorCollector]
	instanceProfiles           *store.Table[InstanceProfile]
	replicationInstancesByARN  *store.Index[ReplicationInstance]
	endpointsByARN             *store.Index[Endpoint]
	replicationTasksByARN      *store.Index[ReplicationTask]
	// tasksByInstanceARN indexes task ARNs by the instance ARN they are attached to,
	// enabling O(1) checks in DeleteReplicationInstance instead of scanning all tasks.
	tasksByInstanceARN            map[string]map[string]struct{}
	dataMigrationsByARN           *store.Index[DataMigration]
	dataProvidersByARN            *store.Index[DataProvider]
	instanceProfilesByARN         *store.Index[InstanceProfile]
	certificates                  *store.Table[Certificate]
	certificatesByARN             *store.Index[Certificate]
	replicationSubnetGroups       *store.Table[ReplicationSubnetGroup]
	replicationSubnetGroupsByARN  *store.Index[ReplicationSubnetGroup]
	migrationProjects             *store.Table[MigrationProject]
	migrationProjectsByARN        *store.Index[MigrationProject]
	replicationConfigs            *store.Table[ReplicationConfig]
	replicationConfigsByARN       *store.Index[ReplicationConfig]
	connections                   *store.Table[Connection] // primary key: "<region>|<riArn>:<epArn>"
	connectionsByRegion           *store.Index[Connection]
	assessmentRuns                *store.Table[AssessmentRun] // primary key: "<region>|<ARN>"
	assessmentRunsByRegion        *store.Index[AssessmentRun]
	events                        map[string][]*Event                // region → events
	recommendations               map[string][]*Recommendation       // region → recommendations
	fleetAdvisorDatabases         *store.Table[FleetAdvisorDatabase] // primary key: "<region>|<id>"
	fleetAdvisorDatabasesByRegion *store.Index[FleetAdvisorDatabase]
	endpointSchemas               map[string]map[string][]string // region → endpointARN → schemas
	// metadataModelRequests tracks pending metadata model operations per project per region,
	// primary key "<region>|<projectARN>|<reqID>".
	metadataModelRequests           *store.Table[MetadataModelRequest]
	metadataModelRequestsByProject  *store.Index[MetadataModelRequest]
	replicationInstancesByRegion    *store.Index[ReplicationInstance]
	endpointsByRegion               *store.Index[Endpoint]
	replicationTasksByRegion        *store.Index[ReplicationTask]
	dataMigrationsByRegion          *store.Index[DataMigration]
	dataProvidersByRegion           *store.Index[DataProvider]
	eventSubscriptionsByRegion      *store.Index[EventSubscription]
	fleetAdvisorCollectorsByRegion  *store.Index[FleetAdvisorCollector]
	instanceProfilesByRegion        *store.Index[InstanceProfile]
	certificatesByRegion            *store.Index[Certificate]
	replicationSubnetGroupsByRegion *store.Index[ReplicationSubnetGroup]
	migrationProjectsByRegion       *store.Index[MigrationProject]
	replicationConfigsByRegion      *store.Index[ReplicationConfig]
	// tasksByEndpointARN indexes task ARNs by endpoint ARN (source or target) for O(1) in-use check.
	tasksByEndpointARN map[string]map[string]struct{} // endpointARN → taskARN set
	mu                 *lockmetrics.RWMutex
	accountID          string
	region             string
	paginationSecret   string
}

// NewInMemoryBackend creates a new in-memory DMS backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:           store.NewRegistry(),
		tasksByInstanceARN: make(map[string]map[string]struct{}),
		events:             make(map[string][]*Event),
		recommendations:    make(map[string][]*Recommendation),
		endpointSchemas:    make(map[string]map[string][]string),
		tasksByEndpointARN: make(map[string]map[string]struct{}),
		accountID:          accountID,
		region:             region,
		paginationSecret:   uuid.NewString(),
		mu:                 lockmetrics.New("dms"),
	}

	registerAllTables(b)

	return b
}

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// closeAllTags closes the Tags registry on every value currently in t. It
// stays generic over the concrete resource type via a closer callback.
func closeAllTags[T any](t *store.Table[T], closer func(*T)) {
	for _, v := range t.All() {
		closer(v)
	}
}

// Reset clears all backend state and closes all tag registries across all regions.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	closeAllTags(b.replicationInstances, func(ri *ReplicationInstance) { ri.Tags.Close() })
	closeAllTags(b.endpoints, func(ep *Endpoint) { ep.Tags.Close() })
	closeAllTags(b.replicationTasks, func(rt *ReplicationTask) { rt.Tags.Close() })
	closeAllTags(b.dataMigrations, func(dm *DataMigration) { dm.Tags.Close() })
	closeAllTags(b.dataProviders, func(dp *DataProvider) { dp.Tags.Close() })
	closeAllTags(b.eventSubscriptions, func(es *EventSubscription) { es.Tags.Close() })
	closeAllTags(b.fleetAdvisorCollectors, func(col *FleetAdvisorCollector) { col.Tags.Close() })
	closeAllTags(b.instanceProfiles, func(ip *InstanceProfile) { ip.Tags.Close() })
	closeAllTags(b.migrationProjects, func(mp *MigrationProject) { mp.Tags.Close() })
	closeAllTags(b.replicationSubnetGroups, func(sg *ReplicationSubnetGroup) { sg.Tags.Close() })
	closeAllTags(b.replicationConfigs, func(rc *ReplicationConfig) { rc.Tags.Close() })

	b.registry.ResetAll()

	b.tasksByInstanceARN = make(map[string]map[string]struct{})
	b.events = make(map[string][]*Event)
	b.recommendations = make(map[string][]*Recommendation)
	b.endpointSchemas = make(map[string]map[string][]string)
	b.tasksByEndpointARN = make(map[string]map[string]struct{})
}

// PaginationSecret returns the HMAC secret for pagination tokens.
func (b *InMemoryBackend) PaginationSecret() string { return b.paginationSecret }
