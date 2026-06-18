// Package dms provides an in-memory implementation of the AWS Database Migration Service.
package dms

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
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
	defaultEngineVersion = "3.5.3"
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

// Connection represents a DMS connection between a replication instance and an endpoint.
type Connection struct {
	ReplicationInstanceArn        string
	ReplicationInstanceIdentifier string
	EndpointArn                   string
	EndpointIdentifier            string
	Status                        string
	LastFailureMessage            string
}

// InMemoryBackend is the in-memory store for AWS DMS resources.
//
// All resource maps are nested by region (outer key = region) so that
// same-named resources are isolated across regions. The per-region inner maps
// are created lazily via the *Store helpers. Callers must hold b.mu while
// accessing the inner maps.
type InMemoryBackend struct {
	replicationInstances      map[string]map[string]*ReplicationInstance
	endpoints                 map[string]map[string]*Endpoint
	replicationTasks          map[string]map[string]*ReplicationTask
	dataMigrations            map[string]map[string]*DataMigration
	dataProviders             map[string]map[string]*DataProvider
	eventSubscriptions        map[string]map[string]*EventSubscription
	fleetAdvisorCollectors    map[string]map[string]*FleetAdvisorCollector
	instanceProfiles          map[string]map[string]*InstanceProfile
	replicationInstancesByARN map[string]map[string]*ReplicationInstance
	endpointsByARN            map[string]map[string]*Endpoint
	replicationTasksByARN     map[string]map[string]*ReplicationTask
	// tasksByInstanceARN indexes task ARNs by the instance ARN they are attached to,
	// enabling O(1) checks in DeleteReplicationInstance instead of scanning all tasks.
	tasksByInstanceARN           map[string]map[string]struct{}
	dataMigrationsByARN          map[string]map[string]*DataMigration
	dataProvidersByARN           map[string]map[string]*DataProvider
	instanceProfilesByARN        map[string]map[string]*InstanceProfile
	certificates                 map[string]map[string]*Certificate
	replicationSubnetGroups      map[string]map[string]*ReplicationSubnetGroup
	replicationSubnetGroupsByARN map[string]map[string]*ReplicationSubnetGroup
	migrationProjects            map[string]map[string]*MigrationProject
	migrationProjectsByARN       map[string]map[string]*MigrationProject
	replicationConfigs           map[string]map[string]*ReplicationConfig
	replicationConfigsByARN      map[string]map[string]*ReplicationConfig
	connections                  map[string]map[string]*Connection // inner key: "riArn:epArn"
	mu                           *lockmetrics.RWMutex
	accountID                    string
	region                       string
	paginationSecret             string
}

// NewInMemoryBackend creates a new in-memory DMS backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		replicationInstances:         make(map[string]map[string]*ReplicationInstance),
		endpoints:                    make(map[string]map[string]*Endpoint),
		replicationTasks:             make(map[string]map[string]*ReplicationTask),
		dataMigrations:               make(map[string]map[string]*DataMigration),
		dataProviders:                make(map[string]map[string]*DataProvider),
		eventSubscriptions:           make(map[string]map[string]*EventSubscription),
		fleetAdvisorCollectors:       make(map[string]map[string]*FleetAdvisorCollector),
		instanceProfiles:             make(map[string]map[string]*InstanceProfile),
		replicationInstancesByARN:    make(map[string]map[string]*ReplicationInstance),
		endpointsByARN:               make(map[string]map[string]*Endpoint),
		replicationTasksByARN:        make(map[string]map[string]*ReplicationTask),
		tasksByInstanceARN:           make(map[string]map[string]struct{}),
		dataMigrationsByARN:          make(map[string]map[string]*DataMigration),
		dataProvidersByARN:           make(map[string]map[string]*DataProvider),
		instanceProfilesByARN:        make(map[string]map[string]*InstanceProfile),
		certificates:                 make(map[string]map[string]*Certificate),
		replicationSubnetGroups:      make(map[string]map[string]*ReplicationSubnetGroup),
		replicationSubnetGroupsByARN: make(map[string]map[string]*ReplicationSubnetGroup),
		migrationProjects:            make(map[string]map[string]*MigrationProject),
		migrationProjectsByARN:       make(map[string]map[string]*MigrationProject),
		replicationConfigs:           make(map[string]map[string]*ReplicationConfig),
		replicationConfigsByARN:      make(map[string]map[string]*ReplicationConfig),
		connections:                  make(map[string]map[string]*Connection),
		accountID:                    accountID,
		region:                       region,
		paginationSecret:             uuid.NewString(),
		mu:                           lockmetrics.New("dms"),
	}
}

// The *Store helpers return the per-region inner map, lazily creating it.
// Callers must hold b.mu.

func (b *InMemoryBackend) replicationInstancesStore(region string) map[string]*ReplicationInstance {
	if b.replicationInstances[region] == nil {
		b.replicationInstances[region] = make(map[string]*ReplicationInstance)
	}

	return b.replicationInstances[region]
}

func (b *InMemoryBackend) replicationInstancesByARNStore(region string) map[string]*ReplicationInstance {
	if b.replicationInstancesByARN[region] == nil {
		b.replicationInstancesByARN[region] = make(map[string]*ReplicationInstance)
	}

	return b.replicationInstancesByARN[region]
}

func (b *InMemoryBackend) endpointsStore(region string) map[string]*Endpoint {
	if b.endpoints[region] == nil {
		b.endpoints[region] = make(map[string]*Endpoint)
	}

	return b.endpoints[region]
}

func (b *InMemoryBackend) endpointsByARNStore(region string) map[string]*Endpoint {
	if b.endpointsByARN[region] == nil {
		b.endpointsByARN[region] = make(map[string]*Endpoint)
	}

	return b.endpointsByARN[region]
}

func (b *InMemoryBackend) replicationTasksStore(region string) map[string]*ReplicationTask {
	if b.replicationTasks[region] == nil {
		b.replicationTasks[region] = make(map[string]*ReplicationTask)
	}

	return b.replicationTasks[region]
}

func (b *InMemoryBackend) replicationTasksByARNStore(region string) map[string]*ReplicationTask {
	if b.replicationTasksByARN[region] == nil {
		b.replicationTasksByARN[region] = make(map[string]*ReplicationTask)
	}

	return b.replicationTasksByARN[region]
}

func (b *InMemoryBackend) dataMigrationsStore(region string) map[string]*DataMigration {
	if b.dataMigrations[region] == nil {
		b.dataMigrations[region] = make(map[string]*DataMigration)
	}

	return b.dataMigrations[region]
}

func (b *InMemoryBackend) dataMigrationsByARNStore(region string) map[string]*DataMigration {
	if b.dataMigrationsByARN[region] == nil {
		b.dataMigrationsByARN[region] = make(map[string]*DataMigration)
	}

	return b.dataMigrationsByARN[region]
}

func (b *InMemoryBackend) dataProvidersStore(region string) map[string]*DataProvider {
	if b.dataProviders[region] == nil {
		b.dataProviders[region] = make(map[string]*DataProvider)
	}

	return b.dataProviders[region]
}

func (b *InMemoryBackend) dataProvidersByARNStore(region string) map[string]*DataProvider {
	if b.dataProvidersByARN[region] == nil {
		b.dataProvidersByARN[region] = make(map[string]*DataProvider)
	}

	return b.dataProvidersByARN[region]
}

func (b *InMemoryBackend) eventSubscriptionsStore(region string) map[string]*EventSubscription {
	if b.eventSubscriptions[region] == nil {
		b.eventSubscriptions[region] = make(map[string]*EventSubscription)
	}

	return b.eventSubscriptions[region]
}

func (b *InMemoryBackend) fleetAdvisorCollectorsStore(region string) map[string]*FleetAdvisorCollector {
	if b.fleetAdvisorCollectors[region] == nil {
		b.fleetAdvisorCollectors[region] = make(map[string]*FleetAdvisorCollector)
	}

	return b.fleetAdvisorCollectors[region]
}

func (b *InMemoryBackend) instanceProfilesStore(region string) map[string]*InstanceProfile {
	if b.instanceProfiles[region] == nil {
		b.instanceProfiles[region] = make(map[string]*InstanceProfile)
	}

	return b.instanceProfiles[region]
}

func (b *InMemoryBackend) instanceProfilesByARNStore(region string) map[string]*InstanceProfile {
	if b.instanceProfilesByARN[region] == nil {
		b.instanceProfilesByARN[region] = make(map[string]*InstanceProfile)
	}

	return b.instanceProfilesByARN[region]
}

func (b *InMemoryBackend) certificatesStore(region string) map[string]*Certificate {
	if b.certificates[region] == nil {
		b.certificates[region] = make(map[string]*Certificate)
	}

	return b.certificates[region]
}

func (b *InMemoryBackend) replicationSubnetGroupsStore(region string) map[string]*ReplicationSubnetGroup {
	if b.replicationSubnetGroups[region] == nil {
		b.replicationSubnetGroups[region] = make(map[string]*ReplicationSubnetGroup)
	}

	return b.replicationSubnetGroups[region]
}

func (b *InMemoryBackend) replicationSubnetGroupsByARNStore(region string) map[string]*ReplicationSubnetGroup {
	if b.replicationSubnetGroupsByARN[region] == nil {
		b.replicationSubnetGroupsByARN[region] = make(map[string]*ReplicationSubnetGroup)
	}

	return b.replicationSubnetGroupsByARN[region]
}

func (b *InMemoryBackend) migrationProjectsStore(region string) map[string]*MigrationProject {
	if b.migrationProjects[region] == nil {
		b.migrationProjects[region] = make(map[string]*MigrationProject)
	}

	return b.migrationProjects[region]
}

func (b *InMemoryBackend) migrationProjectsByARNStore(region string) map[string]*MigrationProject {
	if b.migrationProjectsByARN[region] == nil {
		b.migrationProjectsByARN[region] = make(map[string]*MigrationProject)
	}

	return b.migrationProjectsByARN[region]
}

func (b *InMemoryBackend) replicationConfigsStore(region string) map[string]*ReplicationConfig {
	if b.replicationConfigs[region] == nil {
		b.replicationConfigs[region] = make(map[string]*ReplicationConfig)
	}

	return b.replicationConfigs[region]
}

func (b *InMemoryBackend) replicationConfigsByARNStore(region string) map[string]*ReplicationConfig {
	if b.replicationConfigsByARN[region] == nil {
		b.replicationConfigsByARN[region] = make(map[string]*ReplicationConfig)
	}

	return b.replicationConfigsByARN[region]
}

func (b *InMemoryBackend) connectionsStore(region string) map[string]*Connection {
	if b.connections[region] == nil {
		b.connections[region] = make(map[string]*Connection)
	}

	return b.connections[region]
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
	store := b.replicationInstancesStore(region)
	byARN := b.replicationInstancesByARNStore(region)

	if _, ok := store[identifier]; ok {
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
	store[identifier] = ri
	byARN[instanceARN] = ri
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

	store := b.replicationInstancesStore(getRegion(ctx, b.region))

	if identifierOrArn != "" {
		// Try by identifier first, then by ARN index.
		if ri, ok := store[identifierOrArn]; ok {
			cp := *ri

			return []*ReplicationInstance{&cp}, nil
		}

		byARN := b.replicationInstancesByARNStore(getRegion(ctx, b.region))
		if ri, ok := byARN[identifierOrArn]; ok {
			cp := *ri

			return []*ReplicationInstance{&cp}, nil
		}

		return []*ReplicationInstance{}, nil
	}

	list := make([]*ReplicationInstance, 0, len(store))
	for _, ri := range store {
		cp := *ri
		list = append(list, &cp)
	}

	return list, nil
}

// DeleteReplicationInstance deletes a replication instance by ARN or identifier.
// AWS requires all replication tasks on the instance to be deleted first.
func (b *InMemoryBackend) DeleteReplicationInstance(ctx context.Context, arnOrID string) error {
	b.mu.Lock("DeleteReplicationInstance")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.replicationInstancesStore(region)
	byARN := b.replicationInstancesByARNStore(region)

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
		delete(byARN, ri.ReplicationInstanceArn)
		delete(b.tasksByInstanceARN, ri.ReplicationInstanceArn)
		delete(store, id)

		return nil
	}

	// Try by identifier first, then by ARN index.
	if ri, ok := store[arnOrID]; ok {
		return deleteInstance(ri, arnOrID)
	}
	if ri, ok := byARN[arnOrID]; ok {
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
	store := b.endpointsStore(region)
	byARN := b.endpointsByARNStore(region)

	if _, ok := store[identifier]; ok {
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
	store[identifier] = ep
	byARN[endpointARN] = ep
	cp := *ep

	return &cp, nil
}

// DescribeEndpoints returns endpoints, optionally filtered by identifier or ARN.
func (b *InMemoryBackend) DescribeEndpoints(ctx context.Context, identifierOrArn string) ([]*Endpoint, error) {
	b.mu.RLock("DescribeEndpoints")
	defer b.mu.RUnlock()

	store := b.endpointsStore(getRegion(ctx, b.region))

	if identifierOrArn != "" {
		// Try by identifier first, then by ARN index.
		if ep, ok := store[identifierOrArn]; ok {
			cp := *ep

			return []*Endpoint{&cp}, nil
		}

		byARN := b.endpointsByARNStore(getRegion(ctx, b.region))
		if ep, ok := byARN[identifierOrArn]; ok {
			cp := *ep

			return []*Endpoint{&cp}, nil
		}

		return []*Endpoint{}, nil
	}

	list := make([]*Endpoint, 0, len(store))
	for _, ep := range store {
		cp := *ep
		list = append(list, &cp)
	}

	return list, nil
}

// DeleteEndpoint deletes an endpoint by ARN or identifier.
func (b *InMemoryBackend) DeleteEndpoint(ctx context.Context, arnOrID string) (*Endpoint, error) {
	b.mu.Lock("DeleteEndpoint")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.endpointsStore(region)
	byARN := b.endpointsByARNStore(region)

	// Try by identifier first.
	if ep, ok := store[arnOrID]; ok {
		cp := *ep
		ep.Tags.Close()
		delete(byARN, ep.EndpointArn)
		delete(store, arnOrID)

		return &cp, nil
	}
	// Try by ARN index.
	if ep, ok := byARN[arnOrID]; ok {
		cp := *ep
		ep.Tags.Close()
		delete(byARN, arnOrID)
		delete(store, ep.EndpointIdentifier)

		return &cp, nil
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
	store := b.replicationTasksStore(region)
	byARN := b.replicationTasksByARNStore(region)

	if _, ok := store[identifier]; ok {
		return nil, fmt.Errorf(
			"%w: replication task %s already exists",
			ErrAlreadyExists,
			identifier,
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
	store[identifier] = rt
	byARN[taskARN] = rt
	if b.tasksByInstanceARN[replicationInstanceArn] == nil {
		b.tasksByInstanceARN[replicationInstanceArn] = make(map[string]struct{})
	}
	b.tasksByInstanceARN[replicationInstanceArn][taskARN] = struct{}{}
	cp := *rt

	return &cp, nil
}

// DescribeReplicationTasks returns replication tasks, optionally filtered by ARN or identifier.
func (b *InMemoryBackend) DescribeReplicationTasks(ctx context.Context, arnOrID string) ([]*ReplicationTask, error) {
	b.mu.RLock("DescribeReplicationTasks")
	defer b.mu.RUnlock()

	store := b.replicationTasksStore(getRegion(ctx, b.region))

	if arnOrID != "" {
		// Try by identifier first, then by ARN index.
		if rt, ok := store[arnOrID]; ok {
			cp := *rt

			return []*ReplicationTask{&cp}, nil
		}

		byARN := b.replicationTasksByARNStore(getRegion(ctx, b.region))
		if rt, ok := byARN[arnOrID]; ok {
			cp := *rt

			return []*ReplicationTask{&cp}, nil
		}

		return []*ReplicationTask{}, nil
	}

	list := make([]*ReplicationTask, 0, len(store))
	for _, rt := range store {
		cp := *rt
		list = append(list, &cp)
	}

	return list, nil
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
	cp := *rt

	return &cp, nil
}

// StopReplicationTask transitions a replication task to stopped status.
func (b *InMemoryBackend) StopReplicationTask(ctx context.Context, arnOrID string) (*ReplicationTask, error) {
	b.mu.Lock("StopReplicationTask")
	defer b.mu.Unlock()

	rt := b.findTask(ctx, arnOrID)
	if rt == nil {
		return nil, fmt.Errorf("%w: replication task %s not found", ErrNotFound, arnOrID)
	}

	rt.Status = statusStopped
	cp := *rt

	return &cp, nil
}

// DeleteReplicationTask deletes a replication task by ARN or identifier.
// AWS does not allow deleting a task while it is running.
func (b *InMemoryBackend) DeleteReplicationTask(ctx context.Context, arnOrID string) (*ReplicationTask, error) {
	b.mu.Lock("DeleteReplicationTask")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.replicationTasksStore(region)
	byARN := b.replicationTasksByARNStore(region)

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
		delete(byARN, rt.ReplicationTaskArn)
		delete(store, id)
		// Remove from reverse instance→tasks index.
		if instTasks := b.tasksByInstanceARN[rt.ReplicationInstanceArn]; instTasks != nil {
			delete(instTasks, rt.ReplicationTaskArn)
		}

		return &cp, nil
	}

	// Try by identifier first, then by ARN index.
	if rt, ok := store[arnOrID]; ok {
		return deleteTask(rt, arnOrID)
	}
	if rt, ok := byARN[arnOrID]; ok {
		return deleteTask(rt, rt.ReplicationTaskIdentifier)
	}

	return nil, fmt.Errorf("%w: replication task %s not found", ErrNotFound, arnOrID)
}

// findTask locates a replication task by identifier or ARN within the request
// region (must hold a lock).
func (b *InMemoryBackend) findTask(ctx context.Context, arnOrID string) *ReplicationTask {
	store := b.replicationTasksStore(getRegion(ctx, b.region))
	if rt, ok := store[arnOrID]; ok {
		return rt
	}

	byARN := b.replicationTasksByARNStore(getRegion(ctx, b.region))
	if rt, ok := byARN[arnOrID]; ok {
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
	byARN := b.replicationInstancesByARNStore(region)
	ri, ok := byARN[replicationInstanceArn]
	if !ok {
		store := b.replicationInstancesStore(region)
		ri, ok = store[replicationInstanceArn]
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
func (b *InMemoryBackend) BatchStartRecommendations(_ context.Context) error {
	return nil
}

// CancelMetadataModelConversion cancels a pending metadata model conversion task.
func (b *InMemoryBackend) CancelMetadataModelConversion(
	_ context.Context,
	migrationProjectIdentifier, requestIdentifier string,
) (string, error) {
	if migrationProjectIdentifier == "" {
		return "", fmt.Errorf("%w: MigrationProjectIdentifier is required", ErrValidation)
	}

	if requestIdentifier == "" {
		return "", fmt.Errorf("%w: RequestIdentifier is required", ErrValidation)
	}

	return requestIdentifier, nil
}

// CancelMetadataModelCreation cancels a pending metadata model creation task.
func (b *InMemoryBackend) CancelMetadataModelCreation(
	_ context.Context,
	migrationProjectIdentifier, requestIdentifier string,
) (string, error) {
	if migrationProjectIdentifier == "" {
		return "", fmt.Errorf("%w: MigrationProjectIdentifier is required", ErrValidation)
	}

	if requestIdentifier == "" {
		return "", fmt.Errorf("%w: RequestIdentifier is required", ErrValidation)
	}

	return requestIdentifier, nil
}

// CancelReplicationTaskAssessmentRun cancels a single premigration assessment run.
func (b *InMemoryBackend) CancelReplicationTaskAssessmentRun(
	_ context.Context,
	replicationTaskAssessmentRunArn string,
) error {
	if replicationTaskAssessmentRunArn == "" {
		return fmt.Errorf("%w: ReplicationTaskAssessmentRunArn is required", ErrValidation)
	}

	// In-memory: there are no real assessment runs to cancel; return not-found.
	return fmt.Errorf(
		"%w: assessment run %s not found",
		ErrNotFound,
		replicationTaskAssessmentRunArn,
	)
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
	store := b.dataMigrationsStore(region)
	byARN := b.dataMigrationsByARNStore(region)

	if _, ok := store[name]; ok {
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
	store[name] = dm
	byARN[migrationARN] = dm
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
	store := b.dataProvidersStore(region)
	byARN := b.dataProvidersByARNStore(region)

	if _, ok := store[name]; ok {
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
	store[name] = dp
	byARN[providerARN] = dp
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
	store := b.eventSubscriptionsStore(region)

	if _, ok := store[subscriptionName]; ok {
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
	store[subscriptionName] = es
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
	store := b.fleetAdvisorCollectorsStore(region)

	if _, ok := store[collectorName]; ok {
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
	store[collectorName] = col
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
	store := b.instanceProfilesStore(region)
	byARN := b.instanceProfilesByARNStore(region)

	key := instanceProfileName
	if key == "" {
		key = uuid.NewString()
	}

	if _, ok := store[key]; ok {
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
	store[key] = ip
	byARN[profileARN] = ip
	cp := *ip

	return &cp, nil
}

// closeTagged closes the Tags registry on every value across all per-region
// inner maps. The Tagged constraint matches resource structs that embed a
// *tags.Tags accessible via a Close-able registry; closeTagged uses a closer
// callback so it stays generic over the concrete resource type.
func closeAllTags[T any](m map[string]map[string]*T, closer func(*T)) {
	for _, regionMap := range m {
		for _, v := range regionMap {
			closer(v)
		}
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

	b.replicationInstances = make(map[string]map[string]*ReplicationInstance)
	b.replicationInstancesByARN = make(map[string]map[string]*ReplicationInstance)
	b.endpoints = make(map[string]map[string]*Endpoint)
	b.endpointsByARN = make(map[string]map[string]*Endpoint)
	b.replicationTasks = make(map[string]map[string]*ReplicationTask)
	b.replicationTasksByARN = make(map[string]map[string]*ReplicationTask)
	b.tasksByInstanceARN = make(map[string]map[string]struct{})
	b.dataMigrations = make(map[string]map[string]*DataMigration)
	b.dataMigrationsByARN = make(map[string]map[string]*DataMigration)
	b.dataProviders = make(map[string]map[string]*DataProvider)
	b.dataProvidersByARN = make(map[string]map[string]*DataProvider)
	b.eventSubscriptions = make(map[string]map[string]*EventSubscription)
	b.fleetAdvisorCollectors = make(map[string]map[string]*FleetAdvisorCollector)
	b.instanceProfiles = make(map[string]map[string]*InstanceProfile)
	b.instanceProfilesByARN = make(map[string]map[string]*InstanceProfile)
	b.certificates = make(map[string]map[string]*Certificate)
	b.replicationSubnetGroups = make(map[string]map[string]*ReplicationSubnetGroup)
	b.replicationSubnetGroupsByARN = make(map[string]map[string]*ReplicationSubnetGroup)
	b.migrationProjects = make(map[string]map[string]*MigrationProject)
	b.migrationProjectsByARN = make(map[string]map[string]*MigrationProject)
	b.replicationConfigs = make(map[string]map[string]*ReplicationConfig)
	b.replicationConfigsByARN = make(map[string]map[string]*ReplicationConfig)
	b.connections = make(map[string]map[string]*Connection)
}

// AddReplicationInstanceInternal seeds a replication instance directly without HTTP.
func (b *InMemoryBackend) AddReplicationInstanceInternal(identifier, class string) {
	b.mu.Lock("AddReplicationInstanceInternal")
	defer b.mu.Unlock()
	store := b.replicationInstancesStore(b.region)
	byARN := b.replicationInstancesByARNStore(b.region)
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
	store[identifier] = ri
	byARN[instanceARN] = ri
}

// AddEndpointInternal seeds an endpoint directly without HTTP.
func (b *InMemoryBackend) AddEndpointInternal(identifier, endpointType, engineName string) {
	b.mu.Lock("AddEndpointInternal")
	defer b.mu.Unlock()
	store := b.endpointsStore(b.region)
	byARN := b.endpointsByARNStore(b.region)
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
	store[identifier] = ep
	byARN[epARN] = ep
}

// AddReplicationTaskInternal seeds a replication task directly without HTTP.
func (b *InMemoryBackend) AddReplicationTaskInternal(
	identifier, srcARN, tgtARN, instARN, migrationType string,
) {
	b.mu.Lock("AddReplicationTaskInternal")
	defer b.mu.Unlock()
	store := b.replicationTasksStore(b.region)
	byARN := b.replicationTasksByARNStore(b.region)
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
	store[identifier] = rt
	byARN[taskARN] = rt
	if b.tasksByInstanceARN[instARN] == nil {
		b.tasksByInstanceARN[instARN] = make(map[string]struct{})
	}
	b.tasksByInstanceARN[instARN][taskARN] = struct{}{}
}

// AddDataMigrationInternal seeds a data migration directly without HTTP.
func (b *InMemoryBackend) AddDataMigrationInternal(name, migrationType string) {
	b.mu.Lock("AddDataMigrationInternal")
	defer b.mu.Unlock()
	store := b.dataMigrationsStore(b.region)
	byARN := b.dataMigrationsByARNStore(b.region)
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
	store[name] = dm
	byARN[migrationARN] = dm
}

// AddDataProviderInternal seeds a data provider directly without HTTP.
func (b *InMemoryBackend) AddDataProviderInternal(name, engine string) {
	b.mu.Lock("AddDataProviderInternal")
	defer b.mu.Unlock()
	store := b.dataProvidersStore(b.region)
	byARN := b.dataProvidersByARNStore(b.region)
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
	store[name] = dp
	byARN[providerARN] = dp
}

// AddEventSubscriptionInternal seeds an event subscription directly without HTTP.
func (b *InMemoryBackend) AddEventSubscriptionInternal(name, snsTopicArn string) {
	b.mu.Lock("AddEventSubscriptionInternal")
	defer b.mu.Unlock()
	store := b.eventSubscriptionsStore(b.region)
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
	store[name] = es
}

// AddFleetAdvisorCollectorInternal seeds a Fleet Advisor collector directly without HTTP.
func (b *InMemoryBackend) AddFleetAdvisorCollectorInternal(name string) {
	b.mu.Lock("AddFleetAdvisorCollectorInternal")
	defer b.mu.Unlock()
	store := b.fleetAdvisorCollectorsStore(b.region)
	t := tags.New("dms.fleet-advisor-collector." + name + ".tags")
	col := &FleetAdvisorCollector{
		CollectorName:         name,
		CollectorReferencedID: uuid.NewString(),
		CollectorVersion:      "1.0.0",
		CollectorHealthCheck:  "HEALTHY",
		AccountID:             b.accountID,
		Region:                b.region,
		CreatedDate:           time.Now().UTC(),
		Tags:                  t,
	}
	store[name] = col
}

// AddInstanceProfileInternal seeds an instance profile directly without HTTP.
func (b *InMemoryBackend) AddInstanceProfileInternal(name string) {
	b.mu.Lock("AddInstanceProfileInternal")
	defer b.mu.Unlock()
	if name == "" {
		name = uuid.NewString()
	}
	store := b.instanceProfilesStore(b.region)
	byARN := b.instanceProfilesByARNStore(b.region)
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
	store[name] = ip
	byARN[profileARN] = ip
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
	store := b.endpointsStore(getRegion(ctx, b.region))
	if ep, ok := store[arnOrID]; ok {
		return ep
	}

	for _, ep := range store {
		if ep.EndpointArn == arnOrID {
			return ep
		}
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
	store := b.replicationInstancesStore(getRegion(ctx, b.region))
	if ri, ok := store[arnOrID]; ok {
		return ri
	}

	for _, ri := range store {
		if ri.ReplicationInstanceArn == arnOrID {
			return ri
		}
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
	store := b.dataMigrationsStore(region)
	byARN := b.dataMigrationsByARNStore(region)

	if dm, ok := store[nameOrArn]; ok {
		cp := *dm
		dm.Tags.Close()
		delete(byARN, dm.DataMigrationArn)
		delete(store, nameOrArn)

		return &cp, nil
	}

	for id, dm := range store {
		if dm.DataMigrationArn == nameOrArn {
			cp := *dm
			dm.Tags.Close()
			delete(byARN, nameOrArn)
			delete(store, id)

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: data migration %s not found", ErrNotFound, nameOrArn)
}

// DeleteDataProvider deletes a data provider by name or ARN.
func (b *InMemoryBackend) DeleteDataProvider(ctx context.Context, nameOrArn string) (*DataProvider, error) {
	b.mu.Lock("DeleteDataProvider")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.dataProvidersStore(region)
	byARN := b.dataProvidersByARNStore(region)

	if dp, ok := store[nameOrArn]; ok {
		cp := *dp
		dp.Tags.Close()
		delete(byARN, dp.DataProviderArn)
		delete(store, nameOrArn)

		return &cp, nil
	}

	for id, dp := range store {
		if dp.DataProviderArn == nameOrArn {
			cp := *dp
			dp.Tags.Close()
			delete(byARN, nameOrArn)
			delete(store, id)

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: data provider %s not found", ErrNotFound, nameOrArn)
}

// DeleteEventSubscription deletes an event subscription by name.
func (b *InMemoryBackend) DeleteEventSubscription(ctx context.Context, name string) (*EventSubscription, error) {
	b.mu.Lock("DeleteEventSubscription")
	defer b.mu.Unlock()

	store := b.eventSubscriptionsStore(getRegion(ctx, b.region))

	es, ok := store[name]
	if !ok {
		return nil, fmt.Errorf("%w: event subscription %s not found", ErrNotFound, name)
	}

	cp := *es
	cp.SourceIDsList = copyStringsOrEmpty(es.SourceIDsList)
	cp.EventCategories = copyStringsOrEmpty(es.EventCategories)
	es.Tags.Close()
	delete(store, name)

	return &cp, nil
}

// DeleteFleetAdvisorCollector deletes a fleet advisor collector by name or ID.
func (b *InMemoryBackend) DeleteFleetAdvisorCollector(ctx context.Context, nameOrID string) error {
	b.mu.Lock("DeleteFleetAdvisorCollector")
	defer b.mu.Unlock()

	store := b.fleetAdvisorCollectorsStore(getRegion(ctx, b.region))

	if col, ok := store[nameOrID]; ok {
		col.Tags.Close()
		delete(store, nameOrID)

		return nil
	}

	for name, col := range store {
		if col.CollectorReferencedID == nameOrID {
			col.Tags.Close()
			delete(store, name)

			return nil
		}
	}

	return fmt.Errorf("%w: fleet advisor collector %s not found", ErrNotFound, nameOrID)
}

// DeleteInstanceProfile deletes an instance profile by name or ARN.
func (b *InMemoryBackend) DeleteInstanceProfile(ctx context.Context, nameOrArn string) error {
	b.mu.Lock("DeleteInstanceProfile")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.instanceProfilesStore(region)
	byARN := b.instanceProfilesByARNStore(region)

	if ip, ok := store[nameOrArn]; ok {
		ip.Tags.Close()
		delete(byARN, ip.InstanceProfileArn)
		delete(store, nameOrArn)

		return nil
	}

	for name, ip := range store {
		if ip.InstanceProfileArn == nameOrArn {
			ip.Tags.Close()
			delete(byARN, nameOrArn)
			delete(store, name)

			return nil
		}
	}

	return fmt.Errorf("%w: instance profile %s not found", ErrNotFound, nameOrArn)
}

// findResourceTags returns the Tags for a resource ARN within the given region
// (must hold a lock). Returns nil if not found.
func (b *InMemoryBackend) findResourceTags(region, resourceArn string) *tags.Tags {
	if ri, ok := b.replicationInstancesByARNStore(region)[resourceArn]; ok {
		return ri.Tags
	}

	if ep, ok := b.endpointsByARNStore(region)[resourceArn]; ok {
		return ep.Tags
	}

	if rt, ok := b.replicationTasksByARNStore(region)[resourceArn]; ok {
		return rt.Tags
	}

	if dm, ok := b.dataMigrationsByARNStore(region)[resourceArn]; ok {
		return dm.Tags
	}

	if dp, ok := b.dataProvidersByARNStore(region)[resourceArn]; ok {
		return dp.Tags
	}

	if ip, ok := b.instanceProfilesByARNStore(region)[resourceArn]; ok {
		return ip.Tags
	}

	if mp, ok := b.migrationProjectsByARNStore(region)[resourceArn]; ok {
		return mp.Tags
	}

	if sg, ok := b.replicationSubnetGroupsByARNStore(region)[resourceArn]; ok {
		return sg.Tags
	}

	if rc, ok := b.replicationConfigsByARNStore(region)[resourceArn]; ok {
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
	store := b.dataMigrationsStore(getRegion(ctx, b.region))
	if dm, ok := store[nameOrArn]; ok {
		return dm
	}

	for _, dm := range store {
		if dm.DataMigrationArn == nameOrArn {
			return dm
		}
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
	store := b.dataProvidersStore(getRegion(ctx, b.region))
	if dp, ok := store[nameOrArn]; ok {
		return dp
	}

	for _, dp := range store {
		if dp.DataProviderArn == nameOrArn {
			return dp
		}
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

	es, ok := b.eventSubscriptionsStore(getRegion(ctx, b.region))[name]
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
	store := b.instanceProfilesStore(getRegion(ctx, b.region))
	if ip, ok := store[nameOrArn]; ok {
		return ip
	}

	for _, ip := range store {
		if ip.InstanceProfileArn == nameOrArn {
			return ip
		}
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

	ri, ok := b.replicationInstancesByARNStore(region)[replicationInstanceArn]
	if !ok {
		return nil, fmt.Errorf(
			"%w: replication instance %s not found",
			ErrNotFound,
			replicationInstanceArn,
		)
	}

	ep, ok := b.endpointsByARNStore(region)[endpointArn]
	if !ok {
		return nil, fmt.Errorf("%w: endpoint %s not found", ErrNotFound, endpointArn)
	}

	key := replicationInstanceArn + ":" + endpointArn
	conn := &Connection{
		ReplicationInstanceArn:        replicationInstanceArn,
		ReplicationInstanceIdentifier: ri.ReplicationInstanceIdentifier,
		EndpointArn:                   endpointArn,
		EndpointIdentifier:            ep.EndpointIdentifier,
		Status:                        "successful",
	}
	b.connectionsStore(region)[key] = conn
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

	store := b.connectionsStore(getRegion(ctx, b.region))
	list := make([]*Connection, 0, len(store))
	for _, conn := range store {
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
	store := b.certificatesStore(region)

	if _, ok := store[identifier]; ok {
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
	store[identifier] = cert
	cp := *cert

	return &cp, nil
}

// DeleteCertificate deletes a certificate by identifier or ARN.
func (b *InMemoryBackend) DeleteCertificate(ctx context.Context, identifierOrArn string) (*Certificate, error) {
	b.mu.Lock("DeleteCertificate")
	defer b.mu.Unlock()

	store := b.certificatesStore(getRegion(ctx, b.region))

	if cert, ok := store[identifierOrArn]; ok {
		cp := *cert
		delete(store, identifierOrArn)

		return &cp, nil
	}

	for id, cert := range store {
		if cert.CertificateArn == identifierOrArn {
			cp := *cert
			delete(store, id)

			return &cp, nil
		}
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
	store := b.migrationProjectsStore(region)
	byARN := b.migrationProjectsByARNStore(region)

	if _, ok := store[name]; ok {
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
	store[name] = mp
	byARN[projectARN] = mp
	cp := *mp

	return &cp, nil
}

// DeleteMigrationProject deletes a migration project by name or ARN.
func (b *InMemoryBackend) DeleteMigrationProject(ctx context.Context, nameOrArn string) error {
	b.mu.Lock("DeleteMigrationProject")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.migrationProjectsStore(region)
	byARN := b.migrationProjectsByARNStore(region)

	if mp, ok := store[nameOrArn]; ok {
		mp.Tags.Close()
		delete(byARN, mp.MigrationProjectArn)
		delete(store, nameOrArn)

		return nil
	}

	for name, mp := range store {
		if mp.MigrationProjectArn == nameOrArn {
			mp.Tags.Close()
			delete(byARN, nameOrArn)
			delete(store, name)

			return nil
		}
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
	store := b.replicationSubnetGroupsStore(region)
	byARN := b.replicationSubnetGroupsByARNStore(region)

	if _, ok := store[identifier]; ok {
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
	store[identifier] = sg
	byARN[sgARN] = sg
	cp := *sg

	return &cp, nil
}

// DeleteReplicationSubnetGroup deletes a subnet group by identifier or ARN.
func (b *InMemoryBackend) DeleteReplicationSubnetGroup(ctx context.Context, identifierOrArn string) error {
	b.mu.Lock("DeleteReplicationSubnetGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.replicationSubnetGroupsStore(region)
	byARN := b.replicationSubnetGroupsByARNStore(region)

	if sg, ok := store[identifierOrArn]; ok {
		sg.Tags.Close()
		delete(byARN, sg.ReplicationSubnetGroupArn)
		delete(store, identifierOrArn)

		return nil
	}

	for id, sg := range store {
		if sg.ReplicationSubnetGroupArn == identifierOrArn {
			sg.Tags.Close()
			delete(byARN, identifierOrArn)
			delete(store, id)

			return nil
		}
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
	store := b.replicationConfigsStore(region)
	byARN := b.replicationConfigsByARNStore(region)

	if _, ok := store[identifier]; ok {
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
	store[identifier] = rc
	byARN[configARN] = rc
	cp := *rc

	return &cp, nil
}

// DeleteReplicationConfig deletes a replication config by identifier or ARN.
func (b *InMemoryBackend) DeleteReplicationConfig(ctx context.Context, identifierOrArn string) error {
	b.mu.Lock("DeleteReplicationConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.replicationConfigsStore(region)
	byARN := b.replicationConfigsByARNStore(region)

	if rc, ok := store[identifierOrArn]; ok {
		rc.Tags.Close()
		delete(byARN, rc.ReplicationConfigArn)
		delete(store, identifierOrArn)

		return nil
	}

	for id, rc := range store {
		if rc.ReplicationConfigArn == identifierOrArn {
			rc.Tags.Close()
			delete(byARN, identifierOrArn)
			delete(store, id)

			return nil
		}
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

	store := b.dataMigrationsStore(getRegion(ctx, b.region))
	list := make([]*DataMigration, 0, len(store))
	for _, dm := range store {
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

	store := b.dataProvidersStore(getRegion(ctx, b.region))
	list := make([]*DataProvider, 0, len(store))
	for _, dp := range store {
		cp := *dp
		list = append(list, &cp)
	}

	return list, nil
}

// DescribeEventSubscriptions returns all event subscriptions (optionally filtered by name).
func (b *InMemoryBackend) DescribeEventSubscriptions(ctx context.Context, name string) ([]*EventSubscription, error) {
	b.mu.RLock("DescribeEventSubscriptions")
	defer b.mu.RUnlock()

	store := b.eventSubscriptionsStore(getRegion(ctx, b.region))

	if name != "" {
		es, ok := store[name]
		if !ok {
			return []*EventSubscription{}, nil
		}

		cp := *es
		cp.SourceIDsList = copyStringsOrEmpty(es.SourceIDsList)
		cp.EventCategories = copyStringsOrEmpty(es.EventCategories)

		return []*EventSubscription{&cp}, nil
	}

	list := make([]*EventSubscription, 0, len(store))
	for _, es := range store {
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

	store := b.fleetAdvisorCollectorsStore(getRegion(ctx, b.region))
	list := make([]*FleetAdvisorCollector, 0, len(store))
	for _, col := range store {
		cp := *col
		list = append(list, &cp)
	}

	return list, nil
}

// DescribeInstanceProfiles returns all instance profiles.
func (b *InMemoryBackend) DescribeInstanceProfiles(ctx context.Context) ([]*InstanceProfile, error) {
	b.mu.RLock("DescribeInstanceProfiles")
	defer b.mu.RUnlock()

	store := b.instanceProfilesStore(getRegion(ctx, b.region))
	list := make([]*InstanceProfile, 0, len(store))
	for _, ip := range store {
		cp := *ip
		list = append(list, &cp)
	}

	return list, nil
}

// DescribeMigrationProjects returns all migration projects.
func (b *InMemoryBackend) DescribeMigrationProjects(ctx context.Context) ([]*MigrationProject, error) {
	b.mu.RLock("DescribeMigrationProjects")
	defer b.mu.RUnlock()

	store := b.migrationProjectsStore(getRegion(ctx, b.region))
	list := make([]*MigrationProject, 0, len(store))
	for _, mp := range store {
		cp := *mp
		list = append(list, &cp)
	}

	return list, nil
}

// DescribeReplicationSubnetGroups returns all subnet groups.
func (b *InMemoryBackend) DescribeReplicationSubnetGroups(ctx context.Context) ([]*ReplicationSubnetGroup, error) {
	b.mu.RLock("DescribeReplicationSubnetGroups")
	defer b.mu.RUnlock()

	store := b.replicationSubnetGroupsStore(getRegion(ctx, b.region))
	list := make([]*ReplicationSubnetGroup, 0, len(store))
	for _, sg := range store {
		cp := *sg
		list = append(list, &cp)
	}

	return list, nil
}

// DescribeReplicationConfigs returns all replication configs.
func (b *InMemoryBackend) DescribeReplicationConfigs(ctx context.Context) ([]*ReplicationConfig, error) {
	b.mu.RLock("DescribeReplicationConfigs")
	defer b.mu.RUnlock()

	store := b.replicationConfigsStore(getRegion(ctx, b.region))
	list := make([]*ReplicationConfig, 0, len(store))
	for _, rc := range store {
		cp := *rc
		list = append(list, &cp)
	}

	return list, nil
}

// DescribeCertificates returns all certificates.
func (b *InMemoryBackend) DescribeCertificates(ctx context.Context) ([]*Certificate, error) {
	b.mu.RLock("DescribeCertificates")
	defer b.mu.RUnlock()

	store := b.certificatesStore(getRegion(ctx, b.region))
	list := make([]*Certificate, 0, len(store))
	for _, cert := range store {
		cp := *cert
		list = append(list, &cp)
	}

	return list, nil
}
