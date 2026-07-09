// Package dms provides an in-memory implementation of the AWS Database Migration Service.
package dms

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
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
	defaultEngineVersion = "3.5.3"

	eventCategoryCreation    = "creation"
	eventCategoryDeletion    = "deletion"
	eventCategoryStateChange = "state-change"
)

var (
	// ErrNotFound is returned when a requested DMS resource cannot be found.
	ErrNotFound = awserr.New("ResourceNotFoundFault", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a DMS resource already exists.
	ErrAlreadyExists = awserr.New("ResourceAlreadyExistsFault", awserr.ErrConflict)
	// ErrInvalidState is returned when a DMS resource is in an invalid state for the requested operation.
	ErrInvalidState = awserr.New("InvalidResourceStateFault", awserr.ErrInvalidParameter)
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)

const defaultAllocatedStorage int32 = 50

// DataMigration represents an AWS DMS data migration.
type DataMigration struct {
	CreationTime         time.Time  `json:"creationTime"`
	Tags                 *tags.Tags `json:"-"`
	DataMigrationName    string     `json:"dataMigrationName"`
	DataMigrationArn     string     `json:"dataMigrationArn"`
	MigrationProjectArn  string     `json:"migrationProjectArn"`
	DataMigrationType    string     `json:"dataMigrationType"`
	ServiceAccessRoleArn string     `json:"serviceAccessRoleArn"`
	DataMigrationStatus  string     `json:"dataMigrationStatus"`
	AccountID            string     `json:"accountId"`
	Region               string     `json:"region"`
	SelectionRules       string     `json:"selectionRules,omitempty"`
	NumberOfJobs         int32      `json:"numberOfJobs"`
	EnableCloudwatchLogs bool       `json:"enableCloudwatchLogs"`
}

// DataProvider represents an AWS DMS data provider.
type DataProvider struct {
	CreationTime     time.Time  `json:"creationTime"`
	Tags             *tags.Tags `json:"-"`
	DataProviderName string     `json:"dataProviderName"`
	DataProviderArn  string     `json:"dataProviderArn"`
	Engine           string     `json:"engine"`
	Description      string     `json:"description,omitempty"`
	AccountID        string     `json:"accountId"`
	Region           string     `json:"region"`
}

// EventSubscription represents an AWS DMS event notification subscription.
type EventSubscription struct {
	CreationTime     time.Time  `json:"creationTime"`
	Tags             *tags.Tags `json:"-"`
	SubscriptionName string     `json:"subscriptionName"`
	SnsTopicArn      string     `json:"snsTopicArn"`
	SourceType       string     `json:"sourceType,omitempty"`
	Status           string     `json:"status"`
	AccountID        string     `json:"accountId"`
	Region           string     `json:"region"`
	SourceIDsList    []string   `json:"sourceIdsList,omitempty"`
	EventCategories  []string   `json:"eventCategories,omitempty"`
	Enabled          bool       `json:"enabled"`
}

// FleetAdvisorCollector represents an AWS DMS Fleet Advisor collector.
type FleetAdvisorCollector struct {
	CreatedDate           time.Time  `json:"createdDate"`
	Tags                  *tags.Tags `json:"-"`
	CollectorName         string     `json:"collectorName"`
	CollectorReferencedID string     `json:"collectorReferencedId"`
	CollectorVersion      string     `json:"collectorVersion"`
	Description           string     `json:"description,omitempty"`
	ServiceAccessRoleArn  string     `json:"serviceAccessRoleArn"`
	S3BucketName          string     `json:"s3BucketName"`
	CollectorHealthCheck  string     `json:"collectorHealthCheck"`
	LastDataReceived      string     `json:"lastDataReceived,omitempty"`
	RegisteredDate        string     `json:"registeredDate,omitempty"`
	ModifiedDate          string     `json:"modifiedDate,omitempty"`
	AccountID             string     `json:"accountId"`
	Region                string     `json:"region"`
}

// InstanceProfile represents an AWS DMS instance profile.
type InstanceProfile struct {
	CreationTime          time.Time  `json:"creationTime"`
	Tags                  *tags.Tags `json:"-"`
	InstanceProfileName   string     `json:"instanceProfileName"`
	InstanceProfileArn    string     `json:"instanceProfileArn"`
	AvailabilityZone      string     `json:"availabilityZone,omitempty"`
	KmsKeyArn             string     `json:"kmsKeyArn,omitempty"`
	NetworkType           string     `json:"networkType,omitempty"`
	Description           string     `json:"description,omitempty"`
	SubnetGroupIdentifier string     `json:"subnetGroupIdentifier,omitempty"`
	AccountID             string     `json:"accountId"`
	Region                string     `json:"region"`
	PubliclyAccessible    bool       `json:"publiclyAccessible"`
}

// ReplicationInstance represents an AWS DMS replication instance.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via AddTagsToResource or CreateReplicationInstance.
type ReplicationInstance struct {
	CreationTime                  time.Time  `json:"creationTime"`
	Tags                          *tags.Tags `json:"-"`
	ReplicationInstanceIdentifier string     `json:"replicationInstanceIdentifier"`
	ReplicationInstanceArn        string     `json:"replicationInstanceArn"`
	ReplicationInstanceClass      string     `json:"replicationInstanceClass"`
	EngineVersion                 string     `json:"engineVersion"`
	AvailabilityZone              string     `json:"availabilityZone"`
	ReplicationInstanceStatus     string     `json:"replicationInstanceStatus"`
	PrivateIPAddress              string     `json:"privateIpAddress"`
	AccountID                     string     `json:"accountId"`
	Region                        string     `json:"region"`
	AllocatedStorage              int32      `json:"allocatedStorage"`
	MultiAZ                       bool       `json:"multiAZ"`
	AutoMinorVersionUpgrade       bool       `json:"autoMinorVersionUpgrade"`
	PubliclyAccessible            bool       `json:"publiclyAccessible"`
}

// Endpoint represents an AWS DMS endpoint.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via AddTagsToResource or CreateEndpoint.
type Endpoint struct {
	CreationTime       time.Time  `json:"creationTime"`
	Tags               *tags.Tags `json:"-"`
	EndpointIdentifier string     `json:"endpointIdentifier"`
	EndpointArn        string     `json:"endpointArn"`
	EndpointType       string     `json:"endpointType"`
	EngineName         string     `json:"engineName"`
	ServerName         string     `json:"serverName,omitempty"`
	DatabaseName       string     `json:"databaseName,omitempty"`
	Username           string     `json:"username,omitempty"`
	Status             string     `json:"status"`
	AccountID          string     `json:"accountId"`
	Region             string     `json:"region"`
	Port               int32      `json:"port,omitempty"`
}

// ReplicationTask represents an AWS DMS replication task.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via AddTagsToResource or CreateReplicationTask.
type ReplicationTask struct {
	CreationTime              time.Time  `json:"creationTime"`
	Tags                      *tags.Tags `json:"-"`
	ReplicationTaskIdentifier string     `json:"replicationTaskIdentifier"`
	ReplicationTaskArn        string     `json:"replicationTaskArn"`
	SourceEndpointArn         string     `json:"sourceEndpointArn"`
	TargetEndpointArn         string     `json:"targetEndpointArn"`
	ReplicationInstanceArn    string     `json:"replicationInstanceArn"`
	MigrationType             string     `json:"migrationType"`
	TableMappings             string     `json:"tableMappings,omitempty"`
	ReplicationTaskSettings   string     `json:"replicationTaskSettings,omitempty"`
	Status                    string     `json:"status"`
	AccountID                 string     `json:"accountId"`
	Region                    string     `json:"region"`
}

// Certificate represents a DMS certificate.
type Certificate struct {
	CertificateIdentifier string
	CertificateArn        string
	CertificatePem        string
	AccountID             string
	Region                string
}

// ReplicationSubnetGroup represents a DMS replication subnet group.
type ReplicationSubnetGroup struct {
	Tags                              *tags.Tags `json:"-"`
	ReplicationSubnetGroupIdentifier  string
	ReplicationSubnetGroupArn         string
	ReplicationSubnetGroupDescription string
	VpcID                             string
	AccountID                         string
	Region                            string
}

// MigrationProject represents a DMS migration project.
type MigrationProject struct {
	Tags                       *tags.Tags `json:"-"`
	MigrationProjectName       string
	MigrationProjectArn        string
	MigrationProjectIdentifier string
	Description                string
	AccountID                  string
	Region                     string
}

// ReplicationConfig represents a DMS replication config.
type ReplicationConfig struct {
	Tags                        *tags.Tags `json:"-"`
	ReplicationConfigIdentifier string
	ReplicationConfigArn        string
	ReplicationType             string
	SourceEndpointArn           string
	TargetEndpointArn           string
	AccountID                   string
	Region                      string
}

// AssessmentRun represents a DMS pre-migration assessment run.
//
// Region supports Phase 3.3's store.Table keying (see store_setup.go) --
// AssessmentRun carries no other region-derived field, so the value needs
// its own copy to serve as a pure store.Table/store.Index key input.
type AssessmentRun struct {
	ReplicationTaskAssessmentRunArn string
	ReplicationTaskArn              string
	AssessmentRunName               string
	Status                          string
	Region                          string
}

// Connection represents a DMS connection between a replication instance and an endpoint.
//
// Region supports Phase 3.3's store.Table keying (see store_setup.go) --
// Connection carries no other region-derived field, so the value needs its
// own copy to serve as a pure store.Table/store.Index key input.
type Connection struct {
	ReplicationInstanceArn        string
	ReplicationInstanceIdentifier string
	EndpointArn                   string
	EndpointIdentifier            string
	Status                        string
	LastFailureMessage            string
	Region                        string
}

// Event records an operational event emitted by a DMS resource.
type Event struct {
	SourceIdentifier string
	SourceType       string
	Message          string
	Date             string
	EventCategories  []string
}

// Recommendation is a target-engine recommendation from Fleet Advisor.
type Recommendation struct {
	DatabaseID string
	EngineName string
	Status     string
}

// FleetAdvisorDatabase is a database discovered by a Fleet Advisor collector.
//
// Region supports Phase 3.3's store.Table keying (see store_setup.go) --
// FleetAdvisorDatabase carries no other region-derived field, so the value
// needs its own copy to serve as a pure store.Table/store.Index key input.
type FleetAdvisorDatabase struct {
	DatabaseID            string
	DatabaseName          string
	IPAddress             string
	EngineName            string
	CollectorReferencedID string
	Region                string
}

// MetadataModelRequest tracks a metadata model operation (assessment, conversion, etc.).
//
// Region supports Phase 3.3's store.Table keying (see store_setup.go) --
// MetadataModelRequest carries no other region-derived field, so the value
// needs its own copy to serve as a pure store.Table/store.Index key input.
type MetadataModelRequest struct {
	RequestIdentifier          string
	MigrationProjectIdentifier string
	Status                     string
	RequestType                string
	SelectionRules             string
	Region                     string
}

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

// resolveProjectARN returns the project ARN for a name-or-ARN identifier.
// Callers must hold b.mu.
func (b *InMemoryBackend) resolveProjectARN(region, identifier string) string {
	if strings.HasPrefix(identifier, "arn:") {
		return identifier
	}

	if mp, ok := b.migrationProjects.Get(regionKey(region, identifier)); ok {
		return mp.MigrationProjectArn
	}

	return identifier
}

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// mustDescribeReplicationInstances returns all replication instances without error (for internal use).
func (b *InMemoryBackend) mustDescribeReplicationInstances(ctx context.Context) []*ReplicationInstance {
	list, _ := b.DescribeReplicationInstances(ctx, "")

	return list
}

// mustDescribeEndpoints returns all endpoints without error (for internal use).
func (b *InMemoryBackend) mustDescribeEndpoints(ctx context.Context) []*Endpoint {
	list, _ := b.DescribeEndpoints(ctx, "")

	return list
}

// mustDescribeReplicationTasks returns all replication tasks without error (for internal use).
func (b *InMemoryBackend) mustDescribeReplicationTasks(ctx context.Context) []*ReplicationTask {
	list, _ := b.DescribeReplicationTasks(ctx, "")

	return list
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// CreateReplicationInstance creates a new DMS replication instance.
func (b *InMemoryBackend) CreateReplicationInstance(
	ctx context.Context,
	identifier, class, engineVersion, availabilityZone string,
	allocatedStorage int32,
	multiAZ, autoMinorVersionUpgrade, publiclyAccessible bool,
	kv map[string]string,
) (*ReplicationInstance, error) {
	b.mu.Lock("CreateReplicationInstance")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if b.replicationInstances.Has(regionKey(region, identifier)) {
		return nil, fmt.Errorf(
			"%w: replication instance %s already exists",
			ErrAlreadyExists,
			identifier,
		)
	}

	instanceARN := arn.Build("dms", region, b.accountID, "rep:"+identifier)
	t := tags.New("dms.replication-instance." + identifier + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}

	if engineVersion == "" {
		engineVersion = defaultEngineVersion
	}

	if allocatedStorage == 0 {
		allocatedStorage = defaultAllocatedStorage
	}

	ri := &ReplicationInstance{
		ReplicationInstanceIdentifier: identifier,
		ReplicationInstanceArn:        instanceARN,
		ReplicationInstanceClass:      class,
		EngineVersion:                 engineVersion,
		AvailabilityZone:              availabilityZone,
		AllocatedStorage:              allocatedStorage,
		MultiAZ:                       multiAZ,
		AutoMinorVersionUpgrade:       autoMinorVersionUpgrade,
		PubliclyAccessible:            publiclyAccessible,
		ReplicationInstanceStatus:     statusAvailable,
		PrivateIPAddress:              "10.0.0.1",
		AccountID:                     b.accountID,
		Region:                        region,
		CreationTime:                  time.Now().UTC(),
		Tags:                          t,
	}
	b.replicationInstances.Put(ri)
	cp := *ri

	return &cp, nil
}

// DescribeReplicationInstances returns replication instances, optionally filtered by identifier or ARN.
func (b *InMemoryBackend) DescribeReplicationInstances(
	ctx context.Context,
	identifierOrArn string,
) ([]*ReplicationInstance, error) {
	b.mu.RLock("DescribeReplicationInstances")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return describeByIdentifierOrARN(
		b.replicationInstances, b.replicationInstancesByARN, b.replicationInstancesByRegion, region, identifierOrArn,
	), nil
}

// DeleteReplicationInstance deletes a replication instance by ARN or identifier.
// AWS requires all replication tasks on the instance to be deleted first.
func (b *InMemoryBackend) DeleteReplicationInstance(ctx context.Context, arnOrID string) error {
	b.mu.Lock("DeleteReplicationInstance")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	deleteInstance := func(ri *ReplicationInstance, id string) error {
		// O(1) check via reverse index instead of scanning all tasks.
		if len(b.tasksByInstanceARN[ri.ReplicationInstanceArn]) > 0 {
			return fmt.Errorf(
				"%w: replication instance %s has tasks attached; delete all tasks first",
				ErrInvalidState,
				arnOrID,
			)
		}
		ri.Tags.Close()
		delete(b.tasksByInstanceARN, ri.ReplicationInstanceArn)
		b.replicationInstances.Delete(regionKey(region, id))

		return nil
	}

	// Try by identifier first, then by ARN index.
	if ri, ok := b.replicationInstances.Get(regionKey(region, arnOrID)); ok {
		return deleteInstance(ri, arnOrID)
	}
	if ri, ok := lookupUnique(b.replicationInstancesByARN, regionKey(region, arnOrID)); ok {
		return deleteInstance(ri, ri.ReplicationInstanceIdentifier)
	}

	return fmt.Errorf("%w: replication instance %s not found", ErrNotFound, arnOrID)
}

// CreateEndpoint creates a new DMS endpoint.
func (b *InMemoryBackend) CreateEndpoint(
	ctx context.Context,
	identifier, endpointType, engineName, serverName, databaseName, username string,
	port int32,
	kv map[string]string,
) (*Endpoint, error) {
	b.mu.Lock("CreateEndpoint")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if b.endpoints.Has(regionKey(region, identifier)) {
		return nil, fmt.Errorf("%w: endpoint %s already exists", ErrAlreadyExists, identifier)
	}

	endpointID := uuid.NewString()
	endpointARN := arn.Build("dms", region, b.accountID, "endpoint:"+endpointID)
	t := tags.New("dms.endpoint." + identifier + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}

	ep := &Endpoint{
		EndpointIdentifier: identifier,
		EndpointArn:        endpointARN,
		EndpointType:       endpointType,
		EngineName:         engineName,
		ServerName:         serverName,
		DatabaseName:       databaseName,
		Username:           username,
		Port:               port,
		Status:             statusActive,
		AccountID:          b.accountID,
		Region:             region,
		CreationTime:       time.Now().UTC(),
		Tags:               t,
	}
	b.endpoints.Put(ep)
	b.appendEvent(
		region, endpointARN, "replication-instance",
		"Endpoint "+identifier+" created", []string{eventCategoryCreation},
	)
	cp := *ep

	return &cp, nil
}

// DescribeEndpoints returns endpoints, optionally filtered by identifier or ARN.
func (b *InMemoryBackend) DescribeEndpoints(ctx context.Context, identifierOrArn string) ([]*Endpoint, error) {
	b.mu.RLock("DescribeEndpoints")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return describeByIdentifierOrARN(b.endpoints, b.endpointsByARN, b.endpointsByRegion, region, identifierOrArn), nil
}

// DeleteEndpoint deletes an endpoint by ARN or identifier.
// Real AWS rejects deletion if the endpoint is still referenced by any replication task.
func (b *InMemoryBackend) DeleteEndpoint(ctx context.Context, arnOrID string) (*Endpoint, error) {
	b.mu.Lock("DeleteEndpoint")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	deleteEndpoint := func(ep *Endpoint, id string) (*Endpoint, error) {
		// O(1) check using tasksByEndpointARN index (#performance).
		if tasks := b.tasksByEndpointARN[ep.EndpointArn]; len(tasks) > 0 {
			for taskARN := range tasks {
				taskID := taskARN
				if rt, ok := lookupUnique(b.replicationTasksByARN, regionKey(region, taskARN)); ok {
					taskID = rt.ReplicationTaskIdentifier
				}

				return nil, fmt.Errorf(
					"%w: endpoint %s is in use by replication task %s; delete the task first",
					ErrInvalidState,
					arnOrID,
					taskID,
				)
			}
		}
		cp := *ep
		ep.Tags.Close()
		b.endpoints.Delete(regionKey(region, id))
		b.appendEvent(
			region, ep.EndpointArn, "replication-instance",
			"Endpoint "+id+" deleted", []string{eventCategoryDeletion},
		)

		return &cp, nil
	}

	// Try by identifier first.
	if ep, ok := b.endpoints.Get(regionKey(region, arnOrID)); ok {
		return deleteEndpoint(ep, arnOrID)
	}
	// Try by ARN index.
	if ep, ok := lookupUnique(b.endpointsByARN, regionKey(region, arnOrID)); ok {
		return deleteEndpoint(ep, ep.EndpointIdentifier)
	}

	return nil, fmt.Errorf("%w: endpoint %s not found", ErrNotFound, arnOrID)
}

// CreateReplicationTask creates a new DMS replication task.
func (b *InMemoryBackend) CreateReplicationTask(
	ctx context.Context,
	identifier, sourceEndpointArn, targetEndpointArn, replicationInstanceArn,
	migrationType, tableMappings, settings string,
	kv map[string]string,
) (*ReplicationTask, error) {
	b.mu.Lock("CreateReplicationTask")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if b.replicationTasks.Has(regionKey(region, identifier)) {
		return nil, fmt.Errorf(
			"%w: replication task %s already exists",
			ErrAlreadyExists,
			identifier,
		)
	}

	// Validate referenced resources exist (real AWS returns ResourceNotFoundFault).
	if _, ok := lookupUnique(b.endpointsByARN, regionKey(region, sourceEndpointArn)); !ok {
		return nil, fmt.Errorf("%w: source endpoint %s not found", ErrNotFound, sourceEndpointArn)
	}

	if _, ok := lookupUnique(b.endpointsByARN, regionKey(region, targetEndpointArn)); !ok {
		return nil, fmt.Errorf("%w: target endpoint %s not found", ErrNotFound, targetEndpointArn)
	}

	if _, ok := lookupUnique(b.replicationInstancesByARN, regionKey(region, replicationInstanceArn)); !ok {
		return nil, fmt.Errorf(
			"%w: replication instance %s not found",
			ErrNotFound,
			replicationInstanceArn,
		)
	}

	taskARN := arn.Build("dms", region, b.accountID, "task:"+uuid.NewString())
	t := tags.New("dms.task." + identifier + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}

	rt := &ReplicationTask{
		ReplicationTaskIdentifier: identifier,
		ReplicationTaskArn:        taskARN,
		SourceEndpointArn:         sourceEndpointArn,
		TargetEndpointArn:         targetEndpointArn,
		ReplicationInstanceArn:    replicationInstanceArn,
		MigrationType:             migrationType,
		TableMappings:             tableMappings,
		ReplicationTaskSettings:   settings,
		Status:                    statusReady,
		AccountID:                 b.accountID,
		Region:                    region,
		CreationTime:              time.Now().UTC(),
		Tags:                      t,
	}
	b.replicationTasks.Put(rt)
	if b.tasksByInstanceARN[replicationInstanceArn] == nil {
		b.tasksByInstanceARN[replicationInstanceArn] = make(map[string]struct{})
	}
	b.tasksByInstanceARN[replicationInstanceArn][taskARN] = struct{}{}
	if b.tasksByEndpointARN[sourceEndpointArn] == nil {
		b.tasksByEndpointARN[sourceEndpointArn] = make(map[string]struct{})
	}
	b.tasksByEndpointARN[sourceEndpointArn][taskARN] = struct{}{}
	if b.tasksByEndpointARN[targetEndpointArn] == nil {
		b.tasksByEndpointARN[targetEndpointArn] = make(map[string]struct{})
	}
	b.tasksByEndpointARN[targetEndpointArn][taskARN] = struct{}{}
	cp := *rt

	return &cp, nil
}

// DescribeReplicationTasks returns replication tasks, optionally filtered by ARN or identifier.
func (b *InMemoryBackend) DescribeReplicationTasks(ctx context.Context, arnOrID string) ([]*ReplicationTask, error) {
	b.mu.RLock("DescribeReplicationTasks")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return describeByIdentifierOrARN(
		b.replicationTasks, b.replicationTasksByARN, b.replicationTasksByRegion, region, arnOrID,
	), nil
}

// StartReplicationTask transitions a replication task to running status.
func (b *InMemoryBackend) StartReplicationTask(ctx context.Context, arnOrID string) (*ReplicationTask, error) {
	b.mu.Lock("StartReplicationTask")
	defer b.mu.Unlock()

	rt := b.findTask(ctx, arnOrID)
	if rt == nil {
		return nil, fmt.Errorf("%w: replication task %s not found", ErrNotFound, arnOrID)
	}

	if rt.Status == statusRunning {
		return nil, fmt.Errorf(
			"%w: replication task %s is already running",
			ErrInvalidState,
			arnOrID,
		)
	}

	rt.Status = statusRunning
	b.appendEvent(
		getRegion(ctx, b.region), rt.ReplicationTaskArn, "replication-task",
		"Replication task "+rt.ReplicationTaskIdentifier+" started", []string{eventCategoryStateChange},
	)
	cp := *rt

	return &cp, nil
}

// StopReplicationTask transitions a replication task to stopped status.
// Real AWS rejects stopping a task that is not currently running.
func (b *InMemoryBackend) StopReplicationTask(ctx context.Context, arnOrID string) (*ReplicationTask, error) {
	b.mu.Lock("StopReplicationTask")
	defer b.mu.Unlock()

	rt := b.findTask(ctx, arnOrID)
	if rt == nil {
		return nil, fmt.Errorf("%w: replication task %s not found", ErrNotFound, arnOrID)
	}

	if rt.Status != statusRunning {
		return nil, fmt.Errorf(
			"%w: replication task %s cannot be stopped; current status is %s",
			ErrInvalidState,
			arnOrID,
			rt.Status,
		)
	}

	rt.Status = statusStopped
	b.appendEvent(
		getRegion(ctx, b.region), rt.ReplicationTaskArn, "replication-task",
		"Replication task "+rt.ReplicationTaskIdentifier+" stopped", []string{eventCategoryStateChange},
	)
	cp := *rt

	return &cp, nil
}

// DeleteReplicationTask deletes a replication task by ARN or identifier.
// AWS does not allow deleting a task while it is running.
func (b *InMemoryBackend) DeleteReplicationTask(ctx context.Context, arnOrID string) (*ReplicationTask, error) {
	b.mu.Lock("DeleteReplicationTask")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	deleteTask := func(rt *ReplicationTask, id string) (*ReplicationTask, error) {
		if rt.Status == statusRunning {
			return nil, fmt.Errorf(
				"%w: replication task %s cannot be deleted while running; stop it first",
				ErrInvalidState,
				arnOrID,
			)
		}
		cp := *rt
		rt.Tags.Close()
		b.replicationTasks.Delete(regionKey(region, id))
		// Remove from reverse instance→tasks index.
		if instTasks := b.tasksByInstanceARN[rt.ReplicationInstanceArn]; instTasks != nil {
			delete(instTasks, rt.ReplicationTaskArn)
		}
		// Remove from reverse endpoint→tasks index.
		if epTasks := b.tasksByEndpointARN[rt.SourceEndpointArn]; epTasks != nil {
			delete(epTasks, rt.ReplicationTaskArn)
		}
		if epTasks := b.tasksByEndpointARN[rt.TargetEndpointArn]; epTasks != nil {
			delete(epTasks, rt.ReplicationTaskArn)
		}

		return &cp, nil
	}

	// Try by identifier first, then by ARN index.
	if rt, ok := b.replicationTasks.Get(regionKey(region, arnOrID)); ok {
		return deleteTask(rt, arnOrID)
	}
	if rt, ok := lookupUnique(b.replicationTasksByARN, regionKey(region, arnOrID)); ok {
		return deleteTask(rt, rt.ReplicationTaskIdentifier)
	}

	return nil, fmt.Errorf("%w: replication task %s not found", ErrNotFound, arnOrID)
}

// findTask locates a replication task by identifier or ARN within the request
// region (must hold a lock).
func (b *InMemoryBackend) findTask(ctx context.Context, arnOrID string) *ReplicationTask {
	region := getRegion(ctx, b.region)
	if rt, ok := b.replicationTasks.Get(regionKey(region, arnOrID)); ok {
		return rt
	}

	if rt, ok := lookupUnique(b.replicationTasksByARN, regionKey(region, arnOrID)); ok {
		return rt
	}

	return nil
}

// AddTagsToResource adds tags to a DMS resource by ARN.
func (b *InMemoryBackend) AddTagsToResource(ctx context.Context, resourceArn string, kv map[string]string) error {
	b.mu.Lock("AddTagsToResource")
	defer b.mu.Unlock()

	t := b.findResourceTags(getRegion(ctx, b.region), resourceArn)
	if t == nil {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceArn)
	}

	t.Merge(kv)

	return nil
}

// ListTagsForResource returns tags for a DMS resource by ARN.
func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	t := b.findResourceTags(getRegion(ctx, b.region), resourceArn)
	if t == nil {
		return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceArn)
	}

	return t.Clone(), nil
}

// ApplyPendingMaintenanceAction applies a pending maintenance action to a replication instance.
func (b *InMemoryBackend) ApplyPendingMaintenanceAction(
	ctx context.Context,
	replicationInstanceArn, applyAction, optInType string,
) (*ReplicationInstance, error) {
	b.mu.Lock("ApplyPendingMaintenanceAction")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	ri, ok := lookupUnique(b.replicationInstancesByARN, regionKey(region, replicationInstanceArn))
	if !ok {
		ri, ok = b.replicationInstances.Get(regionKey(region, replicationInstanceArn))
	}
	if ok {
		// In-memory: mark the action as applied by updating the engine version
		// for "os-upgrade" / "db-upgrade" or just acknowledge for others.
		_ = applyAction
		_ = optInType
		cp := *ri

		return &cp, nil
	}

	return nil, fmt.Errorf(
		"%w: replication instance %s not found",
		ErrNotFound,
		replicationInstanceArn,
	)
}

// BatchStartRecommendations starts the analysis to generate recommendations.
// In-memory: always returns an empty error list (all successful).
// BatchStartRecommendations seeds target-engine recommendations based on existing source endpoints.
func (b *InMemoryBackend) BatchStartRecommendations(ctx context.Context) error {
	b.mu.Lock("BatchStartRecommendations")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	for _, ep := range b.endpointsByRegion.Get(region) {
		if ep.EndpointType == "source" {
			b.recommendations[region] = append(b.recommendations[region], &Recommendation{
				DatabaseID: ep.EndpointArn,
				EngineName: "aurora-mysql",
				Status:     "active",
			})
		}
	}

	return nil
}

// CancelMetadataModelConversion cancels a pending metadata model conversion task.
func (b *InMemoryBackend) CancelMetadataModelConversion(
	ctx context.Context,
	migrationProjectIdentifier, requestIdentifier string,
) (string, error) {
	if migrationProjectIdentifier == "" {
		return "", fmt.Errorf("%w: MigrationProjectIdentifier is required", ErrValidation)
	}

	if requestIdentifier == "" {
		return "", fmt.Errorf("%w: RequestIdentifier is required", ErrValidation)
	}

	b.mu.Lock("CancelMetadataModelConversion")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	projectARN := b.resolveProjectARN(region, migrationProjectIdentifier)

	if req, ok := b.metadataModelRequests.Get(metadataModelRequestKey(region, projectARN, requestIdentifier)); ok {
		req.Status = statusCancelling
	}

	return requestIdentifier, nil
}

// CancelMetadataModelCreation cancels a pending metadata model creation task.
func (b *InMemoryBackend) CancelMetadataModelCreation(
	ctx context.Context,
	migrationProjectIdentifier, requestIdentifier string,
) (string, error) {
	if migrationProjectIdentifier == "" {
		return "", fmt.Errorf("%w: MigrationProjectIdentifier is required", ErrValidation)
	}

	if requestIdentifier == "" {
		return "", fmt.Errorf("%w: RequestIdentifier is required", ErrValidation)
	}

	b.mu.Lock("CancelMetadataModelCreation")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	projectARN := b.resolveProjectARN(region, migrationProjectIdentifier)

	if req, ok := b.metadataModelRequests.Get(metadataModelRequestKey(region, projectARN, requestIdentifier)); ok {
		req.Status = statusCancelling
	}

	return requestIdentifier, nil
}

// CancelReplicationTaskAssessmentRun cancels a single premigration assessment run.
func (b *InMemoryBackend) CancelReplicationTaskAssessmentRun(
	ctx context.Context,
	replicationTaskAssessmentRunArn string,
) error {
	if replicationTaskAssessmentRunArn == "" {
		return fmt.Errorf("%w: ReplicationTaskAssessmentRunArn is required", ErrValidation)
	}

	b.mu.Lock("CancelReplicationTaskAssessmentRun")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	run, ok := b.assessmentRuns.Get(regionKey(region, replicationTaskAssessmentRunArn))
	if !ok {
		return fmt.Errorf(
			"%w: assessment run %s not found",
			ErrNotFound,
			replicationTaskAssessmentRunArn,
		)
	}

	run.Status = statusCancelling

	return nil
}

// StartAssessmentRun creates and stores a new premigration assessment run.
func (b *InMemoryBackend) StartAssessmentRun(
	ctx context.Context,
	taskArn, _, _, assessmentRunName string,
) (*AssessmentRun, error) {
	b.mu.Lock("StartAssessmentRun")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := lookupUnique(b.replicationTasksByARN, regionKey(region, taskArn)); !ok {
		return nil, fmt.Errorf("%w: replication task %s not found", ErrNotFound, taskArn)
	}

	runARN := arn.Build("dms", region, b.accountID, "assessment-run:"+uuid.NewString())
	run := &AssessmentRun{
		ReplicationTaskAssessmentRunArn: runARN,
		ReplicationTaskArn:              taskArn,
		AssessmentRunName:               assessmentRunName,
		Status:                          statusRunning,
		Region:                          region,
	}
	b.assessmentRuns.Put(run)
	cp := *run

	return &cp, nil
}

// DeleteAssessmentRun removes a stored assessment run.
func (b *InMemoryBackend) DeleteAssessmentRun(ctx context.Context, runArn string) (*AssessmentRun, error) {
	b.mu.Lock("DeleteAssessmentRun")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	run, ok := b.assessmentRuns.Get(regionKey(region, runArn))
	if !ok {
		return nil, fmt.Errorf("%w: assessment run %s not found", ErrNotFound, runArn)
	}

	cp := *run
	b.assessmentRuns.Delete(regionKey(region, runArn))

	return &cp, nil
}

// DescribeAssessmentRuns returns stored assessment runs, optionally filtered by task ARN.
func (b *InMemoryBackend) DescribeAssessmentRuns(ctx context.Context, taskArn string) ([]*AssessmentRun, error) {
	b.mu.RLock("DescribeAssessmentRuns")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	items := b.assessmentRunsByRegion.Get(region)
	list := make([]*AssessmentRun, 0, len(items))

	for _, run := range items {
		if taskArn != "" && run.ReplicationTaskArn != taskArn {
			continue
		}

		cp := *run
		list = append(list, &cp)
	}

	return list, nil
}

func isValidMigrationType(s string) bool {
	return s == "full-load" || s == "cdc" || s == "full-load-and-cdc"
}

// copyStringsOrEmpty returns a copy of src, guaranteeing a non-nil slice.
func copyStringsOrEmpty(src []string) []string {
	out := make([]string, len(src))
	copy(out, src)

	return out
}

// CreateDataMigration creates a new data migration.
func (b *InMemoryBackend) CreateDataMigration(
	ctx context.Context,
	name, migrationProjectArn, migrationType, serviceAccessRoleArn, selectionRules string,
	numberOfJobs int32,
	enableCloudwatchLogs bool,
	kv map[string]string,
) (*DataMigration, error) {
	b.mu.Lock("CreateDataMigration")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if b.dataMigrations.Has(regionKey(region, name)) {
		return nil, fmt.Errorf("%w: data migration %s already exists", ErrAlreadyExists, name)
	}

	if !isValidMigrationType(migrationType) {
		return nil, fmt.Errorf(
			"%w: invalid DataMigrationType %q; valid: full-load, cdc, full-load-and-cdc",
			ErrValidation,
			migrationType,
		)
	}

	migrationARN := arn.Build("dms", region, b.accountID, "data-migration:"+uuid.NewString())
	t := tags.New("dms.data-migration." + name + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}

	if numberOfJobs == 0 {
		numberOfJobs = 1
	}

	dm := &DataMigration{
		DataMigrationName:    name,
		DataMigrationArn:     migrationARN,
		MigrationProjectArn:  migrationProjectArn,
		DataMigrationType:    migrationType,
		ServiceAccessRoleArn: serviceAccessRoleArn,
		SelectionRules:       selectionRules,
		NumberOfJobs:         numberOfJobs,
		EnableCloudwatchLogs: enableCloudwatchLogs,
		DataMigrationStatus:  statusReady,
		AccountID:            b.accountID,
		Region:               region,
		CreationTime:         time.Now().UTC(),
		Tags:                 t,
	}
	b.dataMigrations.Put(dm)
	cp := *dm

	return &cp, nil
}

// CreateDataProvider creates a new data provider.
func (b *InMemoryBackend) CreateDataProvider(
	ctx context.Context,
	name, engine, description string,
	kv map[string]string,
) (*DataProvider, error) {
	b.mu.Lock("CreateDataProvider")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if b.dataProviders.Has(regionKey(region, name)) {
		return nil, fmt.Errorf("%w: data provider %s already exists", ErrAlreadyExists, name)
	}

	providerARN := arn.Build("dms", region, b.accountID, "data-provider:"+uuid.NewString())
	t := tags.New("dms.data-provider." + name + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}

	now := time.Now().UTC()
	dp := &DataProvider{
		DataProviderName: name,
		DataProviderArn:  providerARN,
		Engine:           engine,
		Description:      description,
		AccountID:        b.accountID,
		Region:           region,
		CreationTime:     now,
		Tags:             t,
	}
	b.dataProviders.Put(dp)
	cp := *dp

	return &cp, nil
}

// CreateEventSubscription creates a new event subscription.
func (b *InMemoryBackend) CreateEventSubscription(
	ctx context.Context,
	subscriptionName, snsTopicArn, sourceType string,
	sourceIDs, eventCategories []string,
	enabled bool,
	kv map[string]string,
) (*EventSubscription, error) {
	b.mu.Lock("CreateEventSubscription")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if b.eventSubscriptions.Has(regionKey(region, subscriptionName)) {
		return nil, fmt.Errorf(
			"%w: event subscription %s already exists",
			ErrAlreadyExists,
			subscriptionName,
		)
	}

	t := tags.New("dms.event-subscription." + subscriptionName + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}

	sourceIDsCopy := copyStringsOrEmpty(sourceIDs)
	eventCategoriesCopy := copyStringsOrEmpty(eventCategories)

	es := &EventSubscription{
		SubscriptionName: subscriptionName,
		SnsTopicArn:      snsTopicArn,
		SourceType:       sourceType,
		SourceIDsList:    sourceIDsCopy,
		EventCategories:  eventCategoriesCopy,
		Enabled:          enabled,
		Status:           statusActive,
		AccountID:        b.accountID,
		Region:           region,
		CreationTime:     time.Now().UTC(),
		Tags:             t,
	}
	b.eventSubscriptions.Put(es)
	cp := *es
	cp.SourceIDsList = copyStringsOrEmpty(es.SourceIDsList)
	cp.EventCategories = copyStringsOrEmpty(es.EventCategories)

	return &cp, nil
}

// CreateFleetAdvisorCollector creates a new Fleet Advisor collector.
func (b *InMemoryBackend) CreateFleetAdvisorCollector(
	ctx context.Context,
	collectorName, description, serviceAccessRoleArn, s3BucketName string,
) (*FleetAdvisorCollector, error) {
	b.mu.Lock("CreateFleetAdvisorCollector")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if b.fleetAdvisorCollectors.Has(regionKey(region, collectorName)) {
		return nil, fmt.Errorf(
			"%w: Fleet Advisor collector %s already exists",
			ErrAlreadyExists,
			collectorName,
		)
	}

	collectorID := uuid.NewString()
	t := tags.New("dms.fleet-advisor-collector." + collectorName + ".tags")
	col := &FleetAdvisorCollector{
		CollectorName:         collectorName,
		CollectorReferencedID: collectorID,
		CollectorVersion:      "1.0.0",
		Description:           description,
		ServiceAccessRoleArn:  serviceAccessRoleArn,
		S3BucketName:          s3BucketName,
		CollectorHealthCheck:  "HEALTHY",
		AccountID:             b.accountID,
		Region:                region,
		CreatedDate:           time.Now().UTC(),
		Tags:                  t,
	}
	b.fleetAdvisorCollectors.Put(col)

	// Seed two discovered databases per collector to emulate Fleet Advisor discovery.
	for _, seed := range []struct{ name, engine, ip string }{
		{collectorName + "-mysql-db", "mysql", "10.0.1.10"},
		{collectorName + "-pg-db", "postgresql", "10.0.1.11"},
	} {
		dbID := uuid.NewString()
		b.fleetAdvisorDatabases.Put(&FleetAdvisorDatabase{
			DatabaseID:            dbID,
			DatabaseName:          seed.name,
			IPAddress:             seed.ip,
			EngineName:            seed.engine,
			CollectorReferencedID: collectorID,
			Region:                region,
		})
	}

	cp := *col

	return &cp, nil
}

func isValidNetworkType(s string) bool {
	return s == "" || s == "IPV4" || s == "IPV6" || s == "DUAL"
}

// CreateInstanceProfile creates a new instance profile.
func (b *InMemoryBackend) CreateInstanceProfile(
	ctx context.Context,
	instanceProfileName, availabilityZone, kmsKeyArn, networkType, description, subnetGroupIdentifier string,
	publiclyAccessible bool,
	kv map[string]string,
) (*InstanceProfile, error) {
	b.mu.Lock("CreateInstanceProfile")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	key := instanceProfileName
	if key == "" {
		key = uuid.NewString()
	}

	if b.instanceProfiles.Has(regionKey(region, key)) {
		return nil, fmt.Errorf("%w: instance profile %s already exists", ErrAlreadyExists, key)
	}

	if !isValidNetworkType(networkType) {
		return nil, fmt.Errorf(
			"%w: invalid NetworkType %q; valid: IPV4, IPV6, DUAL",
			ErrValidation,
			networkType,
		)
	}

	profileARN := arn.Build("dms", region, b.accountID, "instance-profile:"+uuid.NewString())
	t := tags.New("dms.instance-profile." + key + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}

	if instanceProfileName == "" {
		instanceProfileName = key
	}

	ip := &InstanceProfile{
		InstanceProfileName:   instanceProfileName,
		InstanceProfileArn:    profileARN,
		AvailabilityZone:      availabilityZone,
		KmsKeyArn:             kmsKeyArn,
		NetworkType:           networkType,
		Description:           description,
		SubnetGroupIdentifier: subnetGroupIdentifier,
		PubliclyAccessible:    publiclyAccessible,
		AccountID:             b.accountID,
		Region:                region,
		CreationTime:          time.Now().UTC(),
		Tags:                  t,
	}
	b.instanceProfiles.Put(ip)
	cp := *ip

	return &cp, nil
}

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

// appendEvent records a DMS operational event. Caller must hold b.mu.
func (b *InMemoryBackend) appendEvent(region, sourceID, sourceType, msg string, cats []string) {
	b.events[region] = append(b.events[region], &Event{
		SourceIdentifier: sourceID,
		SourceType:       sourceType,
		Message:          msg,
		EventCategories:  cats,
		Date:             time.Now().UTC().Format(time.RFC3339),
	})
}

// DescribeEvents returns all recorded DMS events for the request region.
func (b *InMemoryBackend) DescribeEvents(ctx context.Context) ([]*Event, error) {
	b.mu.RLock("DescribeEvents")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	list := b.events[region]
	result := make([]*Event, len(list))
	for i, e := range list {
		cp := *e
		result[i] = &cp
	}

	return result, nil
}

// DescribeRecommendations returns Fleet Advisor target recommendations for the request region.
func (b *InMemoryBackend) DescribeRecommendations(ctx context.Context) ([]*Recommendation, error) {
	b.mu.RLock("DescribeRecommendations")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	list := b.recommendations[region]
	result := make([]*Recommendation, len(list))
	for i, r := range list {
		cp := *r
		result[i] = &cp
	}

	return result, nil
}

// DescribeFleetAdvisorDatabases returns databases discovered by Fleet Advisor collectors.
func (b *InMemoryBackend) DescribeFleetAdvisorDatabases(ctx context.Context) ([]*FleetAdvisorDatabase, error) {
	b.mu.RLock("DescribeFleetAdvisorDatabases")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	items := b.fleetAdvisorDatabasesByRegion.Get(region)
	result := make([]*FleetAdvisorDatabase, 0, len(items))

	for _, db := range items {
		cp := *db
		result = append(result, &cp)
	}

	return result, nil
}

// DeleteFleetAdvisorDatabases removes Fleet Advisor databases by ID and returns the deleted IDs.
func (b *InMemoryBackend) DeleteFleetAdvisorDatabases(ctx context.Context, ids []string) ([]string, error) {
	b.mu.Lock("DeleteFleetAdvisorDatabases")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	deleted := make([]string, 0, len(ids))

	for _, id := range ids {
		if b.fleetAdvisorDatabases.Delete(regionKey(region, id)) {
			deleted = append(deleted, id)
		}
	}

	return deleted, nil
}

// endpointSchemasStore returns the per-region inner map, lazily creating it.
// endpointSchemas is deliberately left a plain map (not converted to
// store.Table) because its values are []string, not *T -- see
// store_setup.go's registerAllTables doc comment. Callers must hold b.mu.
func (b *InMemoryBackend) endpointSchemasStore(region string) map[string][]string {
	if b.endpointSchemas[region] == nil {
		b.endpointSchemas[region] = make(map[string][]string)
	}

	return b.endpointSchemas[region]
}

// DescribeSchemas returns the schema names available on an endpoint.
func (b *InMemoryBackend) DescribeSchemas(ctx context.Context, endpointARN string) ([]string, error) {
	b.mu.RLock("DescribeSchemas")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	schemas := b.endpointSchemasStore(region)[endpointARN]

	if schemas == nil {
		return []string{}, nil
	}

	result := make([]string, len(schemas))
	copy(result, schemas)

	return result, nil
}

// RefreshSchemas seeds schema discovery for an endpoint (emulates async refresh).
func (b *InMemoryBackend) RefreshSchemas(ctx context.Context, endpointARN string) error {
	b.mu.Lock("RefreshSchemas")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	ep, ok := lookupUnique(b.endpointsByARN, regionKey(region, endpointARN))

	if !ok {
		return fmt.Errorf("%w: endpoint %s not found", ErrNotFound, endpointARN)
	}

	b.endpointSchemasStore(region)[endpointARN] = defaultSchemasForEngine(ep.EngineName)

	return nil
}

// defaultSchemasForEngine returns a realistic default schema list for a given engine.
func defaultSchemasForEngine(engine string) []string {
	switch engine {
	case "postgres", "aurora-postgresql":
		return []string{"public", "information_schema", "pg_catalog"}
	case "oracle":
		return []string{"SYS", "SYSTEM", "HR", "OE"}
	case "sqlserver":
		return []string{"dbo", "sys", "INFORMATION_SCHEMA"}
	default:
		return []string{"main", "information_schema"}
	}
}

// StartMetadataModelRequest persists a metadata model operation request and returns its ID.
func (b *InMemoryBackend) StartMetadataModelRequest(
	ctx context.Context,
	projectIdentifier, reqType, selectionRules string,
) (string, error) {
	b.mu.Lock("StartMetadataModelRequest")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	projectARN := b.resolveProjectARN(region, projectIdentifier)
	reqID := uuid.NewString()
	b.metadataModelRequests.Put(&MetadataModelRequest{
		RequestIdentifier:          reqID,
		MigrationProjectIdentifier: projectARN,
		Status:                     "running",
		RequestType:                reqType,
		SelectionRules:             selectionRules,
		Region:                     region,
	})

	return reqID, nil
}

// ListMetadataModelRequests returns all requests of a given type for a migration project.
func (b *InMemoryBackend) ListMetadataModelRequests(
	ctx context.Context,
	projectIdentifier, reqType string,
) ([]*MetadataModelRequest, error) {
	b.mu.RLock("ListMetadataModelRequests")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	projectARN := b.resolveProjectARN(region, projectIdentifier)
	items := b.metadataModelRequestsByProject.Get(metadataModelRequestProjectKey(region, projectARN))
	result := make([]*MetadataModelRequest, 0)

	for _, req := range items {
		if req.RequestType == reqType {
			cp := *req
			result = append(result, &cp)
		}
	}

	return result, nil
}

// AddReplicationInstanceInternal seeds a replication instance directly without HTTP.
func (b *InMemoryBackend) AddReplicationInstanceInternal(identifier, class string) {
	b.mu.Lock("AddReplicationInstanceInternal")
	defer b.mu.Unlock()
	instanceARN := arn.Build("dms", b.region, b.accountID, "rep:"+identifier)
	t := tags.New("dms.replication-instance." + identifier + ".tags")
	ri := &ReplicationInstance{
		ReplicationInstanceIdentifier: identifier,
		ReplicationInstanceArn:        instanceARN,
		ReplicationInstanceClass:      class,
		EngineVersion:                 defaultEngineVersion,
		ReplicationInstanceStatus:     statusAvailable,
		PrivateIPAddress:              "10.0.0.1",
		AllocatedStorage:              defaultAllocatedStorage,
		AccountID:                     b.accountID,
		Region:                        b.region,
		CreationTime:                  time.Now().UTC(),
		Tags:                          t,
	}
	b.replicationInstances.Put(ri)
}

// AddEndpointInternal seeds an endpoint directly without HTTP.
func (b *InMemoryBackend) AddEndpointInternal(identifier, endpointType, engineName string) {
	b.mu.Lock("AddEndpointInternal")
	defer b.mu.Unlock()
	epID := uuid.NewString()
	epARN := arn.Build("dms", b.region, b.accountID, "endpoint:"+epID)
	t := tags.New("dms.endpoint." + identifier + ".tags")
	ep := &Endpoint{
		EndpointIdentifier: identifier,
		EndpointArn:        epARN,
		EndpointType:       endpointType,
		EngineName:         engineName,
		Status:             statusActive,
		AccountID:          b.accountID,
		Region:             b.region,
		CreationTime:       time.Now().UTC(),
		Tags:               t,
	}
	b.endpoints.Put(ep)
}

// AddReplicationTaskInternal seeds a replication task directly without HTTP.
func (b *InMemoryBackend) AddReplicationTaskInternal(
	identifier, srcARN, tgtARN, instARN, migrationType string,
) {
	b.mu.Lock("AddReplicationTaskInternal")
	defer b.mu.Unlock()
	taskARN := arn.Build("dms", b.region, b.accountID, "task:"+uuid.NewString())
	t := tags.New("dms.task." + identifier + ".tags")
	rt := &ReplicationTask{
		ReplicationTaskIdentifier: identifier,
		ReplicationTaskArn:        taskARN,
		SourceEndpointArn:         srcARN,
		TargetEndpointArn:         tgtARN,
		ReplicationInstanceArn:    instARN,
		MigrationType:             migrationType,
		Status:                    statusReady,
		AccountID:                 b.accountID,
		Region:                    b.region,
		CreationTime:              time.Now().UTC(),
		Tags:                      t,
	}
	b.replicationTasks.Put(rt)
	if b.tasksByInstanceARN[instARN] == nil {
		b.tasksByInstanceARN[instARN] = make(map[string]struct{})
	}
	b.tasksByInstanceARN[instARN][taskARN] = struct{}{}
	if b.tasksByEndpointARN[srcARN] == nil {
		b.tasksByEndpointARN[srcARN] = make(map[string]struct{})
	}
	b.tasksByEndpointARN[srcARN][taskARN] = struct{}{}
	if b.tasksByEndpointARN[tgtARN] == nil {
		b.tasksByEndpointARN[tgtARN] = make(map[string]struct{})
	}
	b.tasksByEndpointARN[tgtARN][taskARN] = struct{}{}
}

// AddDataMigrationInternal seeds a data migration directly without HTTP.
func (b *InMemoryBackend) AddDataMigrationInternal(name, migrationType string) {
	b.mu.Lock("AddDataMigrationInternal")
	defer b.mu.Unlock()
	migrationARN := arn.Build("dms", b.region, b.accountID, "data-migration:"+uuid.NewString())
	t := tags.New("dms.data-migration." + name + ".tags")
	dm := &DataMigration{
		DataMigrationName:   name,
		DataMigrationArn:    migrationARN,
		DataMigrationType:   migrationType,
		DataMigrationStatus: statusReady,
		NumberOfJobs:        1,
		AccountID:           b.accountID,
		Region:              b.region,
		CreationTime:        time.Now().UTC(),
		Tags:                t,
	}
	b.dataMigrations.Put(dm)
}

// AddDataProviderInternal seeds a data provider directly without HTTP.
func (b *InMemoryBackend) AddDataProviderInternal(name, engine string) {
	b.mu.Lock("AddDataProviderInternal")
	defer b.mu.Unlock()
	providerARN := arn.Build("dms", b.region, b.accountID, "data-provider:"+uuid.NewString())
	t := tags.New("dms.data-provider." + name + ".tags")
	now := time.Now().UTC()
	dp := &DataProvider{
		DataProviderName: name,
		DataProviderArn:  providerARN,
		Engine:           engine,
		AccountID:        b.accountID,
		Region:           b.region,
		CreationTime:     now,
		Tags:             t,
	}
	b.dataProviders.Put(dp)
}

// AddEventSubscriptionInternal seeds an event subscription directly without HTTP.
func (b *InMemoryBackend) AddEventSubscriptionInternal(name, snsTopicArn string) {
	b.mu.Lock("AddEventSubscriptionInternal")
	defer b.mu.Unlock()
	t := tags.New("dms.event-subscription." + name + ".tags")
	es := &EventSubscription{
		SubscriptionName: name,
		SnsTopicArn:      snsTopicArn,
		Status:           statusActive,
		Enabled:          true,
		SourceIDsList:    []string{},
		EventCategories:  []string{},
		AccountID:        b.accountID,
		Region:           b.region,
		CreationTime:     time.Now().UTC(),
		Tags:             t,
	}
	b.eventSubscriptions.Put(es)
}

// AddFleetAdvisorCollectorInternal seeds a Fleet Advisor collector directly without HTTP.
func (b *InMemoryBackend) AddFleetAdvisorCollectorInternal(name string) {
	b.mu.Lock("AddFleetAdvisorCollectorInternal")
	defer b.mu.Unlock()
	collectorID := uuid.NewString()
	t := tags.New("dms.fleet-advisor-collector." + name + ".tags")
	col := &FleetAdvisorCollector{
		CollectorName:         name,
		CollectorReferencedID: collectorID,
		CollectorVersion:      "1.0.0",
		CollectorHealthCheck:  "HEALTHY",
		AccountID:             b.accountID,
		Region:                b.region,
		CreatedDate:           time.Now().UTC(),
		Tags:                  t,
	}
	b.fleetAdvisorCollectors.Put(col)
}

// AddInstanceProfileInternal seeds an instance profile directly without HTTP.
func (b *InMemoryBackend) AddInstanceProfileInternal(name string) {
	b.mu.Lock("AddInstanceProfileInternal")
	defer b.mu.Unlock()
	if name == "" {
		name = uuid.NewString()
	}
	profileARN := arn.Build("dms", b.region, b.accountID, "instance-profile:"+uuid.NewString())
	t := tags.New("dms.instance-profile." + name + ".tags")
	ip := &InstanceProfile{
		InstanceProfileName: name,
		InstanceProfileArn:  profileARN,
		AccountID:           b.accountID,
		Region:              b.region,
		CreationTime:        time.Now().UTC(),
		Tags:                t,
	}
	b.instanceProfiles.Put(ip)
}

// PaginationSecret returns the HMAC secret for pagination tokens.
func (b *InMemoryBackend) PaginationSecret() string { return b.paginationSecret }

// ModifyEndpoint updates endpoint settings.
func (b *InMemoryBackend) ModifyEndpoint(
	ctx context.Context,
	arnOrID, serverName, databaseName, username string,
	port int32,
) (*Endpoint, error) {
	b.mu.Lock("ModifyEndpoint")
	defer b.mu.Unlock()

	ep := b.findEndpoint(ctx, arnOrID)
	if ep == nil {
		return nil, fmt.Errorf("%w: endpoint %s not found", ErrNotFound, arnOrID)
	}

	if serverName != "" {
		ep.ServerName = serverName
	}

	if databaseName != "" {
		ep.DatabaseName = databaseName
	}

	if username != "" {
		ep.Username = username
	}

	if port != 0 {
		ep.Port = port
	}

	cp := *ep

	return &cp, nil
}

// findEndpoint locates an endpoint by identifier or ARN within the request
// region (must hold a lock).
func (b *InMemoryBackend) findEndpoint(ctx context.Context, arnOrID string) *Endpoint {
	region := getRegion(ctx, b.region)
	if ep, ok := b.endpoints.Get(regionKey(region, arnOrID)); ok {
		return ep
	}

	if ep, ok := lookupUnique(b.endpointsByARN, regionKey(region, arnOrID)); ok {
		return ep
	}

	return nil
}

// ModifyReplicationInstance updates a replication instance's class and engineVersion.
func (b *InMemoryBackend) ModifyReplicationInstance(
	ctx context.Context,
	arnOrID, class, engineVersion string,
	multiAZ, autoMinorVersionUpgrade *bool,
	allocatedStorage *int32,
) (*ReplicationInstance, error) {
	b.mu.Lock("ModifyReplicationInstance")
	defer b.mu.Unlock()

	ri := b.findReplicationInstance(ctx, arnOrID)
	if ri == nil {
		return nil, fmt.Errorf("%w: replication instance %s not found", ErrNotFound, arnOrID)
	}

	if class != "" {
		ri.ReplicationInstanceClass = class
	}

	if engineVersion != "" {
		ri.EngineVersion = engineVersion
	}

	if multiAZ != nil {
		ri.MultiAZ = *multiAZ
	}

	if autoMinorVersionUpgrade != nil {
		ri.AutoMinorVersionUpgrade = *autoMinorVersionUpgrade
	}

	if allocatedStorage != nil {
		ri.AllocatedStorage = *allocatedStorage
	}

	cp := *ri

	return &cp, nil
}

// findReplicationInstance locates a replication instance by identifier or ARN
// within the request region (must hold a lock).
func (b *InMemoryBackend) findReplicationInstance(ctx context.Context, arnOrID string) *ReplicationInstance {
	region := getRegion(ctx, b.region)
	if ri, ok := b.replicationInstances.Get(regionKey(region, arnOrID)); ok {
		return ri
	}

	if ri, ok := lookupUnique(b.replicationInstancesByARN, regionKey(region, arnOrID)); ok {
		return ri
	}

	return nil
}

// ModifyReplicationTask updates task settings.
// AWS does not allow modifying a running task.
func (b *InMemoryBackend) ModifyReplicationTask(
	ctx context.Context,
	arnOrID, migrationType, tableMappings, replicationTaskSettings string,
) (*ReplicationTask, error) {
	b.mu.Lock("ModifyReplicationTask")
	defer b.mu.Unlock()

	rt := b.findTask(ctx, arnOrID)
	if rt == nil {
		return nil, fmt.Errorf("%w: replication task %s not found", ErrNotFound, arnOrID)
	}

	if rt.Status == statusRunning {
		return nil, fmt.Errorf(
			"%w: replication task %s cannot be modified while running; stop it first",
			ErrInvalidState,
			arnOrID,
		)
	}

	if migrationType != "" {
		rt.MigrationType = migrationType
	}

	if tableMappings != "" {
		rt.TableMappings = tableMappings
	}

	if replicationTaskSettings != "" {
		rt.ReplicationTaskSettings = replicationTaskSettings
	}

	cp := *rt

	return &cp, nil
}

// DeleteDataMigration deletes a data migration by name or ARN.
func (b *InMemoryBackend) DeleteDataMigration(ctx context.Context, nameOrArn string) (*DataMigration, error) {
	b.mu.Lock("DeleteDataMigration")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if dm, ok := b.dataMigrations.Get(regionKey(region, nameOrArn)); ok {
		cp := *dm
		dm.Tags.Close()
		b.dataMigrations.Delete(regionKey(region, nameOrArn))

		return &cp, nil
	}

	if dm, ok := lookupUnique(b.dataMigrationsByARN, regionKey(region, nameOrArn)); ok {
		cp := *dm
		dm.Tags.Close()
		b.dataMigrations.Delete(regionKey(region, dm.DataMigrationName))

		return &cp, nil
	}

	return nil, fmt.Errorf("%w: data migration %s not found", ErrNotFound, nameOrArn)
}

// DeleteDataProvider deletes a data provider by name or ARN.
func (b *InMemoryBackend) DeleteDataProvider(ctx context.Context, nameOrArn string) (*DataProvider, error) {
	b.mu.Lock("DeleteDataProvider")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if dp, ok := b.dataProviders.Get(regionKey(region, nameOrArn)); ok {
		cp := *dp
		dp.Tags.Close()
		b.dataProviders.Delete(regionKey(region, nameOrArn))

		return &cp, nil
	}

	if dp, ok := lookupUnique(b.dataProvidersByARN, regionKey(region, nameOrArn)); ok {
		cp := *dp
		dp.Tags.Close()
		b.dataProviders.Delete(regionKey(region, dp.DataProviderName))

		return &cp, nil
	}

	return nil, fmt.Errorf("%w: data provider %s not found", ErrNotFound, nameOrArn)
}

// DeleteEventSubscription deletes an event subscription by name.
func (b *InMemoryBackend) DeleteEventSubscription(ctx context.Context, name string) (*EventSubscription, error) {
	b.mu.Lock("DeleteEventSubscription")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	es, ok := b.eventSubscriptions.Get(regionKey(region, name))
	if !ok {
		return nil, fmt.Errorf("%w: event subscription %s not found", ErrNotFound, name)
	}

	cp := *es
	cp.SourceIDsList = copyStringsOrEmpty(es.SourceIDsList)
	cp.EventCategories = copyStringsOrEmpty(es.EventCategories)
	es.Tags.Close()
	b.eventSubscriptions.Delete(regionKey(region, name))

	return &cp, nil
}

// DeleteFleetAdvisorCollector deletes a fleet advisor collector by name or ID.
func (b *InMemoryBackend) DeleteFleetAdvisorCollector(ctx context.Context, nameOrID string) error {
	b.mu.Lock("DeleteFleetAdvisorCollector")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if col, ok := b.fleetAdvisorCollectors.Get(regionKey(region, nameOrID)); ok {
		col.Tags.Close()
		b.fleetAdvisorCollectors.Delete(regionKey(region, nameOrID))

		return nil
	}

	if col, ok := lookupUnique(b.fleetAdvisorCollectorsByID, regionKey(region, nameOrID)); ok {
		col.Tags.Close()
		b.fleetAdvisorCollectors.Delete(regionKey(region, col.CollectorName))

		return nil
	}

	return fmt.Errorf("%w: fleet advisor collector %s not found", ErrNotFound, nameOrID)
}

// DeleteInstanceProfile deletes an instance profile by name or ARN.
func (b *InMemoryBackend) DeleteInstanceProfile(ctx context.Context, nameOrArn string) error {
	b.mu.Lock("DeleteInstanceProfile")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if ip, ok := b.instanceProfiles.Get(regionKey(region, nameOrArn)); ok {
		ip.Tags.Close()
		b.instanceProfiles.Delete(regionKey(region, nameOrArn))

		return nil
	}

	if ip, ok := lookupUnique(b.instanceProfilesByARN, regionKey(region, nameOrArn)); ok {
		ip.Tags.Close()
		b.instanceProfiles.Delete(regionKey(region, ip.InstanceProfileName))

		return nil
	}

	return fmt.Errorf("%w: instance profile %s not found", ErrNotFound, nameOrArn)
}

// findResourceTags returns the Tags for a resource ARN within the given region
// (must hold a lock). Returns nil if not found.
func (b *InMemoryBackend) findResourceTags(region, resourceArn string) *tags.Tags {
	if ri, ok := lookupUnique(b.replicationInstancesByARN, regionKey(region, resourceArn)); ok {
		return ri.Tags
	}

	if ep, ok := lookupUnique(b.endpointsByARN, regionKey(region, resourceArn)); ok {
		return ep.Tags
	}

	if rt, ok := lookupUnique(b.replicationTasksByARN, regionKey(region, resourceArn)); ok {
		return rt.Tags
	}

	if dm, ok := lookupUnique(b.dataMigrationsByARN, regionKey(region, resourceArn)); ok {
		return dm.Tags
	}

	if dp, ok := lookupUnique(b.dataProvidersByARN, regionKey(region, resourceArn)); ok {
		return dp.Tags
	}

	if ip, ok := lookupUnique(b.instanceProfilesByARN, regionKey(region, resourceArn)); ok {
		return ip.Tags
	}

	if mp, ok := lookupUnique(b.migrationProjectsByARN, regionKey(region, resourceArn)); ok {
		return mp.Tags
	}

	if sg, ok := lookupUnique(b.replicationSubnetGroupsByARN, regionKey(region, resourceArn)); ok {
		return sg.Tags
	}

	if rc, ok := lookupUnique(b.replicationConfigsByARN, regionKey(region, resourceArn)); ok {
		return rc.Tags
	}

	return nil
}

// RemoveTagsFromResource removes tags from a DMS resource by ARN.
func (b *InMemoryBackend) RemoveTagsFromResource(ctx context.Context, resourceArn string, tagKeys []string) error {
	b.mu.Lock("RemoveTagsFromResource")
	defer b.mu.Unlock()

	t := b.findResourceTags(getRegion(ctx, b.region), resourceArn)
	if t == nil {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceArn)
	}

	t.DeleteKeys(tagKeys)

	return nil
}

// ModifyDataMigration updates a data migration.
func (b *InMemoryBackend) ModifyDataMigration(
	ctx context.Context,
	nameOrArn, migrationType, serviceAccessRoleArn string,
	numberOfJobs *int32,
) (*DataMigration, error) {
	b.mu.Lock("ModifyDataMigration")
	defer b.mu.Unlock()

	dm := b.findDataMigration(ctx, nameOrArn)
	if dm == nil {
		return nil, fmt.Errorf("%w: data migration %s not found", ErrNotFound, nameOrArn)
	}

	if migrationType != "" {
		dm.DataMigrationType = migrationType
	}

	if serviceAccessRoleArn != "" {
		dm.ServiceAccessRoleArn = serviceAccessRoleArn
	}

	if numberOfJobs != nil {
		dm.NumberOfJobs = *numberOfJobs
	}

	cp := *dm

	return &cp, nil
}

// findDataMigration locates a data migration by name or ARN within the request
// region (must hold a lock).
func (b *InMemoryBackend) findDataMigration(ctx context.Context, nameOrArn string) *DataMigration {
	region := getRegion(ctx, b.region)
	if dm, ok := b.dataMigrations.Get(regionKey(region, nameOrArn)); ok {
		return dm
	}

	if dm, ok := lookupUnique(b.dataMigrationsByARN, regionKey(region, nameOrArn)); ok {
		return dm
	}

	return nil
}

// ModifyDataProvider updates a data provider.
func (b *InMemoryBackend) ModifyDataProvider(
	ctx context.Context,
	nameOrArn, engine, description string,
) (*DataProvider, error) {
	b.mu.Lock("ModifyDataProvider")
	defer b.mu.Unlock()

	dp := b.findDataProvider(ctx, nameOrArn)
	if dp == nil {
		return nil, fmt.Errorf("%w: data provider %s not found", ErrNotFound, nameOrArn)
	}

	if engine != "" {
		dp.Engine = engine
	}

	if description != "" {
		dp.Description = description
	}

	cp := *dp

	return &cp, nil
}

// findDataProvider locates a data provider by name or ARN within the request
// region (must hold a lock).
func (b *InMemoryBackend) findDataProvider(ctx context.Context, nameOrArn string) *DataProvider {
	region := getRegion(ctx, b.region)
	if dp, ok := b.dataProviders.Get(regionKey(region, nameOrArn)); ok {
		return dp
	}

	if dp, ok := lookupUnique(b.dataProvidersByARN, regionKey(region, nameOrArn)); ok {
		return dp
	}

	return nil
}

// ModifyEventSubscription updates an event subscription.
func (b *InMemoryBackend) ModifyEventSubscription(
	ctx context.Context,
	name string,
	enabled *bool,
) (*EventSubscription, error) {
	b.mu.Lock("ModifyEventSubscription")
	defer b.mu.Unlock()

	es, ok := b.eventSubscriptions.Get(regionKey(getRegion(ctx, b.region), name))
	if !ok {
		return nil, fmt.Errorf("%w: event subscription %s not found", ErrNotFound, name)
	}

	if enabled != nil {
		es.Enabled = *enabled
	}

	cp := *es
	cp.SourceIDsList = copyStringsOrEmpty(es.SourceIDsList)
	cp.EventCategories = copyStringsOrEmpty(es.EventCategories)

	return &cp, nil
}

// ModifyInstanceProfile updates an instance profile.
func (b *InMemoryBackend) ModifyInstanceProfile(
	ctx context.Context,
	nameOrArn, availabilityZone, description, networkType string,
) (*InstanceProfile, error) {
	b.mu.Lock("ModifyInstanceProfile")
	defer b.mu.Unlock()

	ip := b.findInstanceProfile(ctx, nameOrArn)
	if ip == nil {
		return nil, fmt.Errorf("%w: instance profile %s not found", ErrNotFound, nameOrArn)
	}

	if availabilityZone != "" {
		ip.AvailabilityZone = availabilityZone
	}

	if description != "" {
		ip.Description = description
	}

	if networkType != "" {
		ip.NetworkType = networkType
	}

	cp := *ip

	return &cp, nil
}

// findInstanceProfile locates an instance profile by name or ARN within the
// request region (must hold a lock).
func (b *InMemoryBackend) findInstanceProfile(ctx context.Context, nameOrArn string) *InstanceProfile {
	region := getRegion(ctx, b.region)
	if ip, ok := b.instanceProfiles.Get(regionKey(region, nameOrArn)); ok {
		return ip
	}

	if ip, ok := lookupUnique(b.instanceProfilesByARN, regionKey(region, nameOrArn)); ok {
		return ip
	}

	return nil
}

// StartDataMigration transitions a data migration to running status.
func (b *InMemoryBackend) StartDataMigration(ctx context.Context, nameOrArn string) (*DataMigration, error) {
	b.mu.Lock("StartDataMigration")
	defer b.mu.Unlock()

	dm := b.findDataMigration(ctx, nameOrArn)
	if dm == nil {
		return nil, fmt.Errorf("%w: data migration %s not found", ErrNotFound, nameOrArn)
	}

	dm.DataMigrationStatus = statusRunning
	cp := *dm

	return &cp, nil
}

// StopDataMigration transitions a data migration to stopped status.
func (b *InMemoryBackend) StopDataMigration(ctx context.Context, nameOrArn string) (*DataMigration, error) {
	b.mu.Lock("StopDataMigration")
	defer b.mu.Unlock()

	dm := b.findDataMigration(ctx, nameOrArn)
	if dm == nil {
		return nil, fmt.Errorf("%w: data migration %s not found", ErrNotFound, nameOrArn)
	}

	dm.DataMigrationStatus = statusStopped
	cp := *dm

	return &cp, nil
}

// RebootReplicationInstance reboots a replication instance (no-op in memory).
func (b *InMemoryBackend) RebootReplicationInstance(ctx context.Context, arnOrID string) (*ReplicationInstance, error) {
	b.mu.RLock("RebootReplicationInstance")
	defer b.mu.RUnlock()

	ri := b.findReplicationInstance(ctx, arnOrID)
	if ri == nil {
		return nil, fmt.Errorf("%w: replication instance %s not found", ErrNotFound, arnOrID)
	}

	cp := *ri

	return &cp, nil
}

// MoveReplicationTask moves a replication task to a different instance.
func (b *InMemoryBackend) MoveReplicationTask(
	ctx context.Context,
	taskArnOrID, targetInstanceArn string,
) (*ReplicationTask, error) {
	b.mu.Lock("MoveReplicationTask")
	defer b.mu.Unlock()

	rt := b.findTask(ctx, taskArnOrID)
	if rt == nil {
		return nil, fmt.Errorf("%w: replication task %s not found", ErrNotFound, taskArnOrID)
	}

	rt.ReplicationInstanceArn = targetInstanceArn
	cp := *rt

	return &cp, nil
}

// TestConnection tests a DMS connection and records the result.
func (b *InMemoryBackend) TestConnection(
	ctx context.Context,
	replicationInstanceArn, endpointArn string,
) (*Connection, error) {
	b.mu.Lock("TestConnection")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	ri, ok := lookupUnique(b.replicationInstancesByARN, regionKey(region, replicationInstanceArn))
	if !ok {
		return nil, fmt.Errorf(
			"%w: replication instance %s not found",
			ErrNotFound,
			replicationInstanceArn,
		)
	}

	ep, ok := lookupUnique(b.endpointsByARN, regionKey(region, endpointArn))
	if !ok {
		return nil, fmt.Errorf("%w: endpoint %s not found", ErrNotFound, endpointArn)
	}

	conn := &Connection{
		ReplicationInstanceArn:        replicationInstanceArn,
		ReplicationInstanceIdentifier: ri.ReplicationInstanceIdentifier,
		EndpointArn:                   endpointArn,
		EndpointIdentifier:            ep.EndpointIdentifier,
		Status:                        statusSuccessful,
		Region:                        region,
	}
	b.connections.Put(conn)
	cp := *conn

	return &cp, nil
}

// DescribeConnections returns stored connections, optionally filtered by replication instance ARN or endpoint ARN.
func (b *InMemoryBackend) DescribeConnections(
	ctx context.Context,
	replicationInstanceArn, endpointArn string,
) ([]*Connection, error) {
	b.mu.RLock("DescribeConnections")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	items := b.connectionsByRegion.Get(region)
	list := make([]*Connection, 0, len(items))
	for _, conn := range items {
		if replicationInstanceArn != "" && conn.ReplicationInstanceArn != replicationInstanceArn {
			continue
		}
		if endpointArn != "" && conn.EndpointArn != endpointArn {
			continue
		}
		cp := *conn
		list = append(list, &cp)
	}

	return list, nil
}

// ImportCertificate creates a certificate record.
func (b *InMemoryBackend) ImportCertificate(ctx context.Context, identifier, certPem string) (*Certificate, error) {
	b.mu.Lock("ImportCertificate")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if b.certificates.Has(regionKey(region, identifier)) {
		return nil, fmt.Errorf("%w: certificate %s already exists", ErrAlreadyExists, identifier)
	}

	certARN := arn.Build("dms", region, b.accountID, "certificate:"+identifier)
	cert := &Certificate{
		CertificateIdentifier: identifier,
		CertificateArn:        certARN,
		CertificatePem:        certPem,
		AccountID:             b.accountID,
		Region:                region,
	}
	b.certificates.Put(cert)
	cp := *cert

	return &cp, nil
}

// DeleteCertificate deletes a certificate by identifier or ARN.
func (b *InMemoryBackend) DeleteCertificate(ctx context.Context, identifierOrArn string) (*Certificate, error) {
	b.mu.Lock("DeleteCertificate")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if cert, ok := b.certificates.Get(regionKey(region, identifierOrArn)); ok {
		cp := *cert
		b.certificates.Delete(regionKey(region, identifierOrArn))

		return &cp, nil
	}

	if cert, ok := lookupUnique(b.certificatesByARN, regionKey(region, identifierOrArn)); ok {
		cp := *cert
		b.certificates.Delete(regionKey(region, cert.CertificateIdentifier))

		return &cp, nil
	}

	return nil, fmt.Errorf("%w: certificate %s not found", ErrNotFound, identifierOrArn)
}

// CreateMigrationProject creates a migration project.
func (b *InMemoryBackend) CreateMigrationProject(
	ctx context.Context,
	name, description string,
	kv map[string]string,
) (*MigrationProject, error) {
	b.mu.Lock("CreateMigrationProject")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if b.migrationProjects.Has(regionKey(region, name)) {
		return nil, fmt.Errorf("%w: migration project %s already exists", ErrAlreadyExists, name)
	}

	projectARN := arn.Build("dms", region, b.accountID, "migration-project:"+uuid.NewString())
	t := tags.New("dms.migration-project." + name + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}
	mp := &MigrationProject{
		MigrationProjectName:       name,
		MigrationProjectArn:        projectARN,
		MigrationProjectIdentifier: name,
		Description:                description,
		AccountID:                  b.accountID,
		Region:                     region,
		Tags:                       t,
	}
	b.migrationProjects.Put(mp)
	cp := *mp

	return &cp, nil
}

// DeleteMigrationProject deletes a migration project by name or ARN.
func (b *InMemoryBackend) DeleteMigrationProject(ctx context.Context, nameOrArn string) error {
	b.mu.Lock("DeleteMigrationProject")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if mp, ok := b.migrationProjects.Get(regionKey(region, nameOrArn)); ok {
		mp.Tags.Close()
		b.migrationProjects.Delete(regionKey(region, nameOrArn))

		return nil
	}

	if mp, ok := lookupUnique(b.migrationProjectsByARN, regionKey(region, nameOrArn)); ok {
		mp.Tags.Close()
		b.migrationProjects.Delete(regionKey(region, mp.MigrationProjectName))

		return nil
	}

	return fmt.Errorf("%w: migration project %s not found", ErrNotFound, nameOrArn)
}

// CreateReplicationSubnetGroup creates a subnet group.
func (b *InMemoryBackend) CreateReplicationSubnetGroup(
	ctx context.Context,
	identifier, description, vpcID string,
	kv map[string]string,
) (*ReplicationSubnetGroup, error) {
	b.mu.Lock("CreateReplicationSubnetGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if b.replicationSubnetGroups.Has(regionKey(region, identifier)) {
		return nil, fmt.Errorf(
			"%w: replication subnet group %s already exists",
			ErrAlreadyExists,
			identifier,
		)
	}

	sgARN := arn.Build("dms", region, b.accountID, "subgrp:"+identifier)
	t := tags.New("dms.replication-subnet-group." + identifier + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}
	sg := &ReplicationSubnetGroup{
		ReplicationSubnetGroupIdentifier:  identifier,
		ReplicationSubnetGroupArn:         sgARN,
		ReplicationSubnetGroupDescription: description,
		VpcID:                             vpcID,
		AccountID:                         b.accountID,
		Region:                            region,
		Tags:                              t,
	}
	b.replicationSubnetGroups.Put(sg)
	cp := *sg

	return &cp, nil
}

// DeleteReplicationSubnetGroup deletes a subnet group by identifier or ARN.
func (b *InMemoryBackend) DeleteReplicationSubnetGroup(ctx context.Context, identifierOrArn string) error {
	b.mu.Lock("DeleteReplicationSubnetGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if sg, ok := b.replicationSubnetGroups.Get(regionKey(region, identifierOrArn)); ok {
		sg.Tags.Close()
		b.replicationSubnetGroups.Delete(regionKey(region, identifierOrArn))

		return nil
	}

	if sg, ok := lookupUnique(b.replicationSubnetGroupsByARN, regionKey(region, identifierOrArn)); ok {
		sg.Tags.Close()
		b.replicationSubnetGroups.Delete(regionKey(region, sg.ReplicationSubnetGroupIdentifier))

		return nil
	}

	return fmt.Errorf("%w: replication subnet group %s not found", ErrNotFound, identifierOrArn)
}

// CreateReplicationConfig creates a replication config.
func (b *InMemoryBackend) CreateReplicationConfig(
	ctx context.Context,
	identifier, replicationType, sourceEndpointArn, targetEndpointArn string,
	kv map[string]string,
) (*ReplicationConfig, error) {
	b.mu.Lock("CreateReplicationConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if b.replicationConfigs.Has(regionKey(region, identifier)) {
		return nil, fmt.Errorf(
			"%w: replication config %s already exists",
			ErrAlreadyExists,
			identifier,
		)
	}

	configARN := arn.Build("dms", region, b.accountID, "replication-config:"+uuid.NewString())
	t := tags.New("dms.replication-config." + identifier + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}
	rc := &ReplicationConfig{
		ReplicationConfigIdentifier: identifier,
		ReplicationConfigArn:        configARN,
		ReplicationType:             replicationType,
		SourceEndpointArn:           sourceEndpointArn,
		TargetEndpointArn:           targetEndpointArn,
		AccountID:                   b.accountID,
		Region:                      region,
		Tags:                        t,
	}
	b.replicationConfigs.Put(rc)
	cp := *rc

	return &cp, nil
}

// DeleteReplicationConfig deletes a replication config by identifier or ARN.
func (b *InMemoryBackend) DeleteReplicationConfig(ctx context.Context, identifierOrArn string) error {
	b.mu.Lock("DeleteReplicationConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if rc, ok := b.replicationConfigs.Get(regionKey(region, identifierOrArn)); ok {
		rc.Tags.Close()
		b.replicationConfigs.Delete(regionKey(region, identifierOrArn))

		return nil
	}

	if rc, ok := lookupUnique(b.replicationConfigsByARN, regionKey(region, identifierOrArn)); ok {
		rc.Tags.Close()
		b.replicationConfigs.Delete(regionKey(region, rc.ReplicationConfigIdentifier))

		return nil
	}

	return fmt.Errorf("%w: replication config %s not found", ErrNotFound, identifierOrArn)
}

// DescribeDataMigrations returns all data migrations (optionally filtered by name/arn).
func (b *InMemoryBackend) DescribeDataMigrations(ctx context.Context, nameOrArn string) ([]*DataMigration, error) {
	b.mu.RLock("DescribeDataMigrations")
	defer b.mu.RUnlock()

	if nameOrArn != "" {
		dm := b.findDataMigration(ctx, nameOrArn)
		if dm == nil {
			return []*DataMigration{}, nil
		}

		cp := *dm

		return []*DataMigration{&cp}, nil
	}

	items := b.dataMigrationsByRegion.Get(getRegion(ctx, b.region))
	list := make([]*DataMigration, 0, len(items))
	for _, dm := range items {
		cp := *dm
		list = append(list, &cp)
	}

	return list, nil
}

// DescribeDataProviders returns all data providers (optionally filtered by name/arn).
func (b *InMemoryBackend) DescribeDataProviders(ctx context.Context, nameOrArn string) ([]*DataProvider, error) {
	b.mu.RLock("DescribeDataProviders")
	defer b.mu.RUnlock()

	if nameOrArn != "" {
		dp := b.findDataProvider(ctx, nameOrArn)
		if dp == nil {
			return []*DataProvider{}, nil
		}

		cp := *dp

		return []*DataProvider{&cp}, nil
	}

	items := b.dataProvidersByRegion.Get(getRegion(ctx, b.region))
	list := make([]*DataProvider, 0, len(items))
	for _, dp := range items {
		cp := *dp
		list = append(list, &cp)
	}

	return list, nil
}

// DescribeEventSubscriptions returns all event subscriptions (optionally filtered by name).
func (b *InMemoryBackend) DescribeEventSubscriptions(ctx context.Context, name string) ([]*EventSubscription, error) {
	b.mu.RLock("DescribeEventSubscriptions")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	if name != "" {
		es, ok := b.eventSubscriptions.Get(regionKey(region, name))
		if !ok {
			return []*EventSubscription{}, nil
		}

		cp := *es
		cp.SourceIDsList = copyStringsOrEmpty(es.SourceIDsList)
		cp.EventCategories = copyStringsOrEmpty(es.EventCategories)

		return []*EventSubscription{&cp}, nil
	}

	items := b.eventSubscriptionsByRegion.Get(region)
	list := make([]*EventSubscription, 0, len(items))
	for _, es := range items {
		cp := *es
		cp.SourceIDsList = copyStringsOrEmpty(es.SourceIDsList)
		cp.EventCategories = copyStringsOrEmpty(es.EventCategories)
		list = append(list, &cp)
	}

	return list, nil
}

// DescribeFleetAdvisorCollectors returns all fleet advisor collectors.
func (b *InMemoryBackend) DescribeFleetAdvisorCollectors(ctx context.Context) ([]*FleetAdvisorCollector, error) {
	b.mu.RLock("DescribeFleetAdvisorCollectors")
	defer b.mu.RUnlock()

	items := b.fleetAdvisorCollectorsByRegion.Get(getRegion(ctx, b.region))
	list := make([]*FleetAdvisorCollector, 0, len(items))
	for _, col := range items {
		cp := *col
		list = append(list, &cp)
	}

	return list, nil
}

// DescribeInstanceProfiles returns all instance profiles.
func (b *InMemoryBackend) DescribeInstanceProfiles(ctx context.Context) ([]*InstanceProfile, error) {
	b.mu.RLock("DescribeInstanceProfiles")
	defer b.mu.RUnlock()

	items := b.instanceProfilesByRegion.Get(getRegion(ctx, b.region))
	list := make([]*InstanceProfile, 0, len(items))
	for _, ip := range items {
		cp := *ip
		list = append(list, &cp)
	}

	return list, nil
}

// DescribeMigrationProjects returns all migration projects.
func (b *InMemoryBackend) DescribeMigrationProjects(ctx context.Context) ([]*MigrationProject, error) {
	b.mu.RLock("DescribeMigrationProjects")
	defer b.mu.RUnlock()

	items := b.migrationProjectsByRegion.Get(getRegion(ctx, b.region))
	list := make([]*MigrationProject, 0, len(items))
	for _, mp := range items {
		cp := *mp
		list = append(list, &cp)
	}

	return list, nil
}

// DescribeReplicationSubnetGroups returns all subnet groups.
func (b *InMemoryBackend) DescribeReplicationSubnetGroups(ctx context.Context) ([]*ReplicationSubnetGroup, error) {
	b.mu.RLock("DescribeReplicationSubnetGroups")
	defer b.mu.RUnlock()

	items := b.replicationSubnetGroupsByRegion.Get(getRegion(ctx, b.region))
	list := make([]*ReplicationSubnetGroup, 0, len(items))
	for _, sg := range items {
		cp := *sg
		list = append(list, &cp)
	}

	return list, nil
}

// DescribeReplicationConfigs returns all replication configs.
func (b *InMemoryBackend) DescribeReplicationConfigs(ctx context.Context) ([]*ReplicationConfig, error) {
	b.mu.RLock("DescribeReplicationConfigs")
	defer b.mu.RUnlock()

	items := b.replicationConfigsByRegion.Get(getRegion(ctx, b.region))
	list := make([]*ReplicationConfig, 0, len(items))
	for _, rc := range items {
		cp := *rc
		list = append(list, &cp)
	}

	return list, nil
}

// DeleteConnection removes a connection record created by TestConnection.
func (b *InMemoryBackend) DeleteConnection(
	ctx context.Context,
	replicationInstanceArn, endpointArn string,
) (*Connection, error) {
	b.mu.Lock("DeleteConnection")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	key := regionKey(region, replicationInstanceArn+":"+endpointArn)

	conn, ok := b.connections.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: connection not found", ErrNotFound)
	}

	cp := *conn
	b.connections.Delete(key)

	return &cp, nil
}

// ModifyMigrationProject updates the description of an existing migration project.
func (b *InMemoryBackend) ModifyMigrationProject(
	ctx context.Context,
	nameOrArn, description string,
) (*MigrationProject, error) {
	b.mu.Lock("ModifyMigrationProject")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if mp, ok := b.migrationProjects.Get(regionKey(region, nameOrArn)); ok {
		mp.Description = description
		cp := *mp

		return &cp, nil
	}

	if mp, ok := lookupUnique(b.migrationProjectsByARN, regionKey(region, nameOrArn)); ok {
		mp.Description = description
		cp := *mp

		return &cp, nil
	}

	return nil, fmt.Errorf("%w: migration project %s not found", ErrNotFound, nameOrArn)
}

// ModifyReplicationConfig updates the replication type of an existing replication config.
func (b *InMemoryBackend) ModifyReplicationConfig(
	ctx context.Context,
	identifierOrArn, replicationType string,
) (*ReplicationConfig, error) {
	b.mu.Lock("ModifyReplicationConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if rc, ok := b.replicationConfigs.Get(regionKey(region, identifierOrArn)); ok {
		if replicationType != "" {
			rc.ReplicationType = replicationType
		}
		cp := *rc

		return &cp, nil
	}

	if rc, ok := lookupUnique(b.replicationConfigsByARN, regionKey(region, identifierOrArn)); ok {
		if replicationType != "" {
			rc.ReplicationType = replicationType
		}
		cp := *rc

		return &cp, nil
	}

	return nil, fmt.Errorf("%w: replication config %s not found", ErrNotFound, identifierOrArn)
}

// DescribeCertificates returns all certificates.
func (b *InMemoryBackend) DescribeCertificates(ctx context.Context) ([]*Certificate, error) {
	b.mu.RLock("DescribeCertificates")
	defer b.mu.RUnlock()

	items := b.certificatesByRegion.Get(getRegion(ctx, b.region))
	list := make([]*Certificate, 0, len(items))
	for _, cert := range items {
		cp := *cert
		list = append(list, &cp)
	}

	return list, nil
}
