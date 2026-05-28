// Package dms provides an in-memory implementation of the AWS Database Migration Service.
package dms

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

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
type InMemoryBackend struct {
	replicationInstances         map[string]*ReplicationInstance
	endpoints                    map[string]*Endpoint
	replicationTasks             map[string]*ReplicationTask
	dataMigrations               map[string]*DataMigration
	dataProviders                map[string]*DataProvider
	eventSubscriptions           map[string]*EventSubscription
	fleetAdvisorCollectors       map[string]*FleetAdvisorCollector
	instanceProfiles             map[string]*InstanceProfile
	replicationInstancesByARN    map[string]*ReplicationInstance
	endpointsByARN               map[string]*Endpoint
	replicationTasksByARN        map[string]*ReplicationTask
	dataMigrationsByARN          map[string]*DataMigration
	dataProvidersByARN           map[string]*DataProvider
	instanceProfilesByARN        map[string]*InstanceProfile
	certificates                 map[string]*Certificate
	replicationSubnetGroups      map[string]*ReplicationSubnetGroup
	replicationSubnetGroupsByARN map[string]*ReplicationSubnetGroup
	migrationProjects            map[string]*MigrationProject
	migrationProjectsByARN       map[string]*MigrationProject
	replicationConfigs           map[string]*ReplicationConfig
	replicationConfigsByARN      map[string]*ReplicationConfig
	connections                  map[string]*Connection // key: "riArn:epArn"
	mu                           *lockmetrics.RWMutex
	accountID                    string
	region                       string
	paginationSecret             string
}

// NewInMemoryBackend creates a new in-memory DMS backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		replicationInstances:         make(map[string]*ReplicationInstance),
		endpoints:                    make(map[string]*Endpoint),
		replicationTasks:             make(map[string]*ReplicationTask),
		dataMigrations:               make(map[string]*DataMigration),
		dataProviders:                make(map[string]*DataProvider),
		eventSubscriptions:           make(map[string]*EventSubscription),
		fleetAdvisorCollectors:       make(map[string]*FleetAdvisorCollector),
		instanceProfiles:             make(map[string]*InstanceProfile),
		replicationInstancesByARN:    make(map[string]*ReplicationInstance),
		endpointsByARN:               make(map[string]*Endpoint),
		replicationTasksByARN:        make(map[string]*ReplicationTask),
		dataMigrationsByARN:          make(map[string]*DataMigration),
		dataProvidersByARN:           make(map[string]*DataProvider),
		instanceProfilesByARN:        make(map[string]*InstanceProfile),
		certificates:                 make(map[string]*Certificate),
		replicationSubnetGroups:      make(map[string]*ReplicationSubnetGroup),
		replicationSubnetGroupsByARN: make(map[string]*ReplicationSubnetGroup),
		migrationProjects:            make(map[string]*MigrationProject),
		migrationProjectsByARN:       make(map[string]*MigrationProject),
		replicationConfigs:           make(map[string]*ReplicationConfig),
		replicationConfigsByARN:      make(map[string]*ReplicationConfig),
		connections:                  make(map[string]*Connection),
		accountID:                    accountID,
		region:                       region,
		paginationSecret:             uuid.NewString(),
		mu:                           lockmetrics.New("dms"),
	}
}

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// mustDescribeReplicationInstances returns all replication instances without error (for internal use).
func (b *InMemoryBackend) mustDescribeReplicationInstances() []*ReplicationInstance {
	list, _ := b.DescribeReplicationInstances("")

	return list
}

// mustDescribeEndpoints returns all endpoints without error (for internal use).
func (b *InMemoryBackend) mustDescribeEndpoints() []*Endpoint {
	list, _ := b.DescribeEndpoints("")

	return list
}

// mustDescribeReplicationTasks returns all replication tasks without error (for internal use).
func (b *InMemoryBackend) mustDescribeReplicationTasks() []*ReplicationTask {
	list, _ := b.DescribeReplicationTasks("")

	return list
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// CreateReplicationInstance creates a new DMS replication instance.
func (b *InMemoryBackend) CreateReplicationInstance(
	identifier, class, engineVersion, availabilityZone string,
	allocatedStorage int32,
	multiAZ, autoMinorVersionUpgrade, publiclyAccessible bool,
	kv map[string]string,
) (*ReplicationInstance, error) {
	b.mu.Lock("CreateReplicationInstance")
	defer b.mu.Unlock()

	if _, ok := b.replicationInstances[identifier]; ok {
		return nil, fmt.Errorf(
			"%w: replication instance %s already exists",
			ErrAlreadyExists,
			identifier,
		)
	}

	instanceARN := arn.Build("dms", b.region, b.accountID, "rep:"+identifier)
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
		AccountID:                     b.accountID,
		Region:                        b.region,
		CreationTime:                  time.Now().UTC(),
		Tags:                          t,
	}
	b.replicationInstances[identifier] = ri
	b.replicationInstancesByARN[instanceARN] = ri
	cp := *ri

	return &cp, nil
}

// DescribeReplicationInstances returns replication instances, optionally filtered by identifier or ARN.
func (b *InMemoryBackend) DescribeReplicationInstances(
	identifierOrArn string,
) ([]*ReplicationInstance, error) {
	b.mu.RLock("DescribeReplicationInstances")
	defer b.mu.RUnlock()

	if identifierOrArn != "" {
		// Try by identifier first.
		if ri, ok := b.replicationInstances[identifierOrArn]; ok {
			cp := *ri

			return []*ReplicationInstance{&cp}, nil
		}
		// Try by ARN.
		for _, ri := range b.replicationInstances {
			if ri.ReplicationInstanceArn == identifierOrArn {
				cp := *ri

				return []*ReplicationInstance{&cp}, nil
			}
		}

		return []*ReplicationInstance{}, nil
	}

	list := make([]*ReplicationInstance, 0, len(b.replicationInstances))
	for _, ri := range b.replicationInstances {
		cp := *ri
		list = append(list, &cp)
	}

	return list, nil
}

// DeleteReplicationInstance deletes a replication instance by ARN or identifier.
// AWS requires all replication tasks on the instance to be deleted first.
func (b *InMemoryBackend) DeleteReplicationInstance(arnOrID string) error {
	b.mu.Lock("DeleteReplicationInstance")
	defer b.mu.Unlock()

	deleteInstance := func(ri *ReplicationInstance, id string) error {
		// Check for tasks attached to this instance.
		for _, rt := range b.replicationTasks {
			if rt.ReplicationInstanceArn == ri.ReplicationInstanceArn {
				return fmt.Errorf(
					"%w: replication instance %s has tasks attached; delete all tasks first",
					ErrInvalidState,
					arnOrID,
				)
			}
		}
		ri.Tags.Close()
		delete(b.replicationInstancesByARN, ri.ReplicationInstanceArn)
		delete(b.replicationInstances, id)

		return nil
	}

	// Try by identifier first.
	if ri, ok := b.replicationInstances[arnOrID]; ok {
		return deleteInstance(ri, arnOrID)
	}
	// Try by ARN.
	for id, ri := range b.replicationInstances {
		if ri.ReplicationInstanceArn == arnOrID {
			return deleteInstance(ri, id)
		}
	}

	return fmt.Errorf("%w: replication instance %s not found", ErrNotFound, arnOrID)
}

// CreateEndpoint creates a new DMS endpoint.
func (b *InMemoryBackend) CreateEndpoint(
	identifier, endpointType, engineName, serverName, databaseName, username string,
	port int32,
	kv map[string]string,
) (*Endpoint, error) {
	b.mu.Lock("CreateEndpoint")
	defer b.mu.Unlock()

	if _, ok := b.endpoints[identifier]; ok {
		return nil, fmt.Errorf("%w: endpoint %s already exists", ErrAlreadyExists, identifier)
	}

	endpointID := uuid.NewString()
	endpointARN := arn.Build("dms", b.region, b.accountID, "endpoint:"+endpointID)
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
		Region:             b.region,
		CreationTime:       time.Now().UTC(),
		Tags:               t,
	}
	b.endpoints[identifier] = ep
	b.endpointsByARN[endpointARN] = ep
	cp := *ep

	return &cp, nil
}

// DescribeEndpoints returns endpoints, optionally filtered by identifier or ARN.
func (b *InMemoryBackend) DescribeEndpoints(identifierOrArn string) ([]*Endpoint, error) {
	b.mu.RLock("DescribeEndpoints")
	defer b.mu.RUnlock()

	if identifierOrArn != "" {
		// Try by identifier first.
		if ep, ok := b.endpoints[identifierOrArn]; ok {
			cp := *ep

			return []*Endpoint{&cp}, nil
		}
		// Try by ARN.
		for _, ep := range b.endpoints {
			if ep.EndpointArn == identifierOrArn {
				cp := *ep

				return []*Endpoint{&cp}, nil
			}
		}

		return []*Endpoint{}, nil
	}

	list := make([]*Endpoint, 0, len(b.endpoints))
	for _, ep := range b.endpoints {
		cp := *ep
		list = append(list, &cp)
	}

	return list, nil
}

// DeleteEndpoint deletes an endpoint by ARN or identifier.
func (b *InMemoryBackend) DeleteEndpoint(arnOrID string) (*Endpoint, error) {
	b.mu.Lock("DeleteEndpoint")
	defer b.mu.Unlock()

	// Try by identifier first.
	if ep, ok := b.endpoints[arnOrID]; ok {
		cp := *ep
		ep.Tags.Close()
		delete(b.endpointsByARN, ep.EndpointArn)
		delete(b.endpoints, arnOrID)

		return &cp, nil
	}
	// Try by ARN.
	for id, ep := range b.endpoints {
		if ep.EndpointArn == arnOrID {
			cp := *ep
			ep.Tags.Close()
			delete(b.endpointsByARN, arnOrID)
			delete(b.endpoints, id)

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: endpoint %s not found", ErrNotFound, arnOrID)
}

// CreateReplicationTask creates a new DMS replication task.
func (b *InMemoryBackend) CreateReplicationTask(
	identifier, sourceEndpointArn, targetEndpointArn, replicationInstanceArn,
	migrationType, tableMappings, settings string,
	kv map[string]string,
) (*ReplicationTask, error) {
	b.mu.Lock("CreateReplicationTask")
	defer b.mu.Unlock()

	if _, ok := b.replicationTasks[identifier]; ok {
		return nil, fmt.Errorf(
			"%w: replication task %s already exists",
			ErrAlreadyExists,
			identifier,
		)
	}

	taskARN := arn.Build("dms", b.region, b.accountID, "task:"+uuid.NewString())
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
		Region:                    b.region,
		CreationTime:              time.Now().UTC(),
		Tags:                      t,
	}
	b.replicationTasks[identifier] = rt
	b.replicationTasksByARN[taskARN] = rt
	cp := *rt

	return &cp, nil
}

// DescribeReplicationTasks returns replication tasks, optionally filtered by ARN or identifier.
func (b *InMemoryBackend) DescribeReplicationTasks(arnOrID string) ([]*ReplicationTask, error) {
	b.mu.RLock("DescribeReplicationTasks")
	defer b.mu.RUnlock()

	if arnOrID != "" {
		// Try by identifier first.
		if rt, ok := b.replicationTasks[arnOrID]; ok {
			cp := *rt

			return []*ReplicationTask{&cp}, nil
		}
		// Try by ARN.
		for _, rt := range b.replicationTasks {
			if rt.ReplicationTaskArn == arnOrID {
				cp := *rt

				return []*ReplicationTask{&cp}, nil
			}
		}

		return []*ReplicationTask{}, nil
	}

	list := make([]*ReplicationTask, 0, len(b.replicationTasks))
	for _, rt := range b.replicationTasks {
		cp := *rt
		list = append(list, &cp)
	}

	return list, nil
}

// StartReplicationTask transitions a replication task to running status.
func (b *InMemoryBackend) StartReplicationTask(arnOrID string) (*ReplicationTask, error) {
	b.mu.Lock("StartReplicationTask")
	defer b.mu.Unlock()

	rt := b.findTask(arnOrID)
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
func (b *InMemoryBackend) StopReplicationTask(arnOrID string) (*ReplicationTask, error) {
	b.mu.Lock("StopReplicationTask")
	defer b.mu.Unlock()

	rt := b.findTask(arnOrID)
	if rt == nil {
		return nil, fmt.Errorf("%w: replication task %s not found", ErrNotFound, arnOrID)
	}

	rt.Status = statusStopped
	cp := *rt

	return &cp, nil
}

// DeleteReplicationTask deletes a replication task by ARN or identifier.
// AWS does not allow deleting a task while it is running.
func (b *InMemoryBackend) DeleteReplicationTask(arnOrID string) (*ReplicationTask, error) {
	b.mu.Lock("DeleteReplicationTask")
	defer b.mu.Unlock()

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
		delete(b.replicationTasksByARN, rt.ReplicationTaskArn)
		delete(b.replicationTasks, id)
		return &cp, nil
	}

	// Try by identifier first.
	if rt, ok := b.replicationTasks[arnOrID]; ok {
		return deleteTask(rt, arnOrID)
	}
	// Try by ARN.
	for id, rt := range b.replicationTasks {
		if rt.ReplicationTaskArn == arnOrID {
			return deleteTask(rt, id)
		}
	}

	return nil, fmt.Errorf("%w: replication task %s not found", ErrNotFound, arnOrID)
}

// findTask locates a replication task by identifier or ARN (must hold a lock).
func (b *InMemoryBackend) findTask(arnOrID string) *ReplicationTask {
	if rt, ok := b.replicationTasks[arnOrID]; ok {
		return rt
	}
	for _, rt := range b.replicationTasks {
		if rt.ReplicationTaskArn == arnOrID {
			return rt
		}
	}

	return nil
}

// AddTagsToResource adds tags to a DMS resource by ARN.
func (b *InMemoryBackend) AddTagsToResource(resourceArn string, kv map[string]string) error {
	b.mu.Lock("AddTagsToResource")
	defer b.mu.Unlock()

	t := b.findResourceTags(resourceArn)
	if t == nil {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceArn)
	}

	t.Merge(kv)

	return nil
}

// ListTagsForResource returns tags for a DMS resource by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	t := b.findResourceTags(resourceArn)
	if t == nil {
		return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceArn)
	}

	return t.Clone(), nil
}

// ApplyPendingMaintenanceAction applies a pending maintenance action to a replication instance.
func (b *InMemoryBackend) ApplyPendingMaintenanceAction(
	replicationInstanceArn, applyAction, optInType string,
) (*ReplicationInstance, error) {
	b.mu.Lock("ApplyPendingMaintenanceAction")
	defer b.mu.Unlock()

	for _, ri := range b.replicationInstances {
		if ri.ReplicationInstanceArn == replicationInstanceArn {
			// In-memory: mark the action as applied by updating the engine version
			// for "os-upgrade" / "db-upgrade" or just acknowledge for others.
			_ = applyAction
			_ = optInType
			cp := *ri

			return &cp, nil
		}
	}

	return nil, fmt.Errorf(
		"%w: replication instance %s not found",
		ErrNotFound,
		replicationInstanceArn,
	)
}

// BatchStartRecommendations starts the analysis to generate recommendations.
// In-memory: always returns an empty error list (all successful).
func (b *InMemoryBackend) BatchStartRecommendations() error {
	return nil
}

// CancelMetadataModelConversion cancels a pending metadata model conversion task.
func (b *InMemoryBackend) CancelMetadataModelConversion(
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
	name, migrationProjectArn, migrationType, serviceAccessRoleArn, selectionRules string,
	numberOfJobs int32,
	enableCloudwatchLogs bool,
	kv map[string]string,
) (*DataMigration, error) {
	b.mu.Lock("CreateDataMigration")
	defer b.mu.Unlock()

	if _, ok := b.dataMigrations[name]; ok {
		return nil, fmt.Errorf("%w: data migration %s already exists", ErrAlreadyExists, name)
	}

	if !isValidMigrationType(migrationType) {
		return nil, fmt.Errorf(
			"%w: invalid DataMigrationType %q; valid: full-load, cdc, full-load-and-cdc",
			ErrValidation,
			migrationType,
		)
	}

	migrationARN := arn.Build("dms", b.region, b.accountID, "data-migration:"+uuid.NewString())
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
		Region:               b.region,
		CreationTime:         time.Now().UTC(),
		Tags:                 t,
	}
	b.dataMigrations[name] = dm
	b.dataMigrationsByARN[migrationARN] = dm
	cp := *dm

	return &cp, nil
}

// CreateDataProvider creates a new data provider.
func (b *InMemoryBackend) CreateDataProvider(
	name, engine, description string,
	kv map[string]string,
) (*DataProvider, error) {
	b.mu.Lock("CreateDataProvider")
	defer b.mu.Unlock()

	if _, ok := b.dataProviders[name]; ok {
		return nil, fmt.Errorf("%w: data provider %s already exists", ErrAlreadyExists, name)
	}

	providerARN := arn.Build("dms", b.region, b.accountID, "data-provider:"+uuid.NewString())
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
		Region:           b.region,
		CreationTime:     now,
		Tags:             t,
	}
	b.dataProviders[name] = dp
	b.dataProvidersByARN[providerARN] = dp
	cp := *dp

	return &cp, nil
}

// CreateEventSubscription creates a new event subscription.
func (b *InMemoryBackend) CreateEventSubscription(
	subscriptionName, snsTopicArn, sourceType string,
	sourceIDs, eventCategories []string,
	enabled bool,
	kv map[string]string,
) (*EventSubscription, error) {
	b.mu.Lock("CreateEventSubscription")
	defer b.mu.Unlock()

	if _, ok := b.eventSubscriptions[subscriptionName]; ok {
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
		Region:           b.region,
		CreationTime:     time.Now().UTC(),
		Tags:             t,
	}
	b.eventSubscriptions[subscriptionName] = es
	cp := *es
	cp.SourceIDsList = copyStringsOrEmpty(es.SourceIDsList)
	cp.EventCategories = copyStringsOrEmpty(es.EventCategories)

	return &cp, nil
}

// CreateFleetAdvisorCollector creates a new Fleet Advisor collector.
func (b *InMemoryBackend) CreateFleetAdvisorCollector(
	collectorName, description, serviceAccessRoleArn, s3BucketName string,
) (*FleetAdvisorCollector, error) {
	b.mu.Lock("CreateFleetAdvisorCollector")
	defer b.mu.Unlock()

	if _, ok := b.fleetAdvisorCollectors[collectorName]; ok {
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
		Region:                b.region,
		CreatedDate:           time.Now().UTC(),
		Tags:                  t,
	}
	b.fleetAdvisorCollectors[collectorName] = col
	cp := *col

	return &cp, nil
}

func isValidNetworkType(s string) bool {
	return s == "" || s == "IPV4" || s == "IPV6" || s == "DUAL"
}

// CreateInstanceProfile creates a new instance profile.
func (b *InMemoryBackend) CreateInstanceProfile(
	instanceProfileName, availabilityZone, kmsKeyArn, networkType, description, subnetGroupIdentifier string,
	publiclyAccessible bool,
	kv map[string]string,
) (*InstanceProfile, error) {
	b.mu.Lock("CreateInstanceProfile")
	defer b.mu.Unlock()

	key := instanceProfileName
	if key == "" {
		key = uuid.NewString()
	}

	if _, ok := b.instanceProfiles[key]; ok {
		return nil, fmt.Errorf("%w: instance profile %s already exists", ErrAlreadyExists, key)
	}

	if !isValidNetworkType(networkType) {
		return nil, fmt.Errorf(
			"%w: invalid NetworkType %q; valid: IPV4, IPV6, DUAL",
			ErrValidation,
			networkType,
		)
	}

	profileARN := arn.Build("dms", b.region, b.accountID, "instance-profile:"+uuid.NewString())
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
		Region:                b.region,
		CreationTime:          time.Now().UTC(),
		Tags:                  t,
	}
	b.instanceProfiles[key] = ip
	b.instanceProfilesByARN[profileARN] = ip
	cp := *ip

	return &cp, nil
}

// Reset clears all backend state and closes all tag registries.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, ri := range b.replicationInstances {
		ri.Tags.Close()
	}
	for _, ep := range b.endpoints {
		ep.Tags.Close()
	}
	for _, rt := range b.replicationTasks {
		rt.Tags.Close()
	}
	for _, dm := range b.dataMigrations {
		dm.Tags.Close()
	}
	for _, dp := range b.dataProviders {
		dp.Tags.Close()
	}
	for _, es := range b.eventSubscriptions {
		es.Tags.Close()
	}
	for _, col := range b.fleetAdvisorCollectors {
		col.Tags.Close()
	}
	for _, ip := range b.instanceProfiles {
		ip.Tags.Close()
	}

	b.replicationInstances = make(map[string]*ReplicationInstance)
	b.replicationInstancesByARN = make(map[string]*ReplicationInstance)
	b.endpoints = make(map[string]*Endpoint)
	b.endpointsByARN = make(map[string]*Endpoint)
	for _, mp := range b.migrationProjects {
		mp.Tags.Close()
	}
	for _, sg := range b.replicationSubnetGroups {
		sg.Tags.Close()
	}
	for _, rc := range b.replicationConfigs {
		rc.Tags.Close()
	}

	b.replicationTasks = make(map[string]*ReplicationTask)
	b.replicationTasksByARN = make(map[string]*ReplicationTask)
	b.dataMigrations = make(map[string]*DataMigration)
	b.dataMigrationsByARN = make(map[string]*DataMigration)
	b.dataProviders = make(map[string]*DataProvider)
	b.dataProvidersByARN = make(map[string]*DataProvider)
	b.eventSubscriptions = make(map[string]*EventSubscription)
	b.fleetAdvisorCollectors = make(map[string]*FleetAdvisorCollector)
	b.instanceProfiles = make(map[string]*InstanceProfile)
	b.instanceProfilesByARN = make(map[string]*InstanceProfile)
	b.certificates = make(map[string]*Certificate)
	b.replicationSubnetGroups = make(map[string]*ReplicationSubnetGroup)
	b.replicationSubnetGroupsByARN = make(map[string]*ReplicationSubnetGroup)
	b.migrationProjects = make(map[string]*MigrationProject)
	b.migrationProjectsByARN = make(map[string]*MigrationProject)
	b.replicationConfigs = make(map[string]*ReplicationConfig)
	b.replicationConfigsByARN = make(map[string]*ReplicationConfig)
	b.connections = make(map[string]*Connection)
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
		AllocatedStorage:              defaultAllocatedStorage,
		AccountID:                     b.accountID,
		Region:                        b.region,
		CreationTime:                  time.Now().UTC(),
		Tags:                          t,
	}
	b.replicationInstances[identifier] = ri
	b.replicationInstancesByARN[instanceARN] = ri
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
	b.endpoints[identifier] = ep
	b.endpointsByARN[epARN] = ep
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
	b.replicationTasks[identifier] = rt
	b.replicationTasksByARN[taskARN] = rt
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
	b.dataMigrations[name] = dm
	b.dataMigrationsByARN[migrationARN] = dm
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
	b.dataProviders[name] = dp
	b.dataProvidersByARN[providerARN] = dp
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
	b.eventSubscriptions[name] = es
}

// AddFleetAdvisorCollectorInternal seeds a Fleet Advisor collector directly without HTTP.
func (b *InMemoryBackend) AddFleetAdvisorCollectorInternal(name string) {
	b.mu.Lock("AddFleetAdvisorCollectorInternal")
	defer b.mu.Unlock()
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
	b.fleetAdvisorCollectors[name] = col
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
	b.instanceProfiles[name] = ip
	b.instanceProfilesByARN[profileARN] = ip
}

// PaginationSecret returns the HMAC secret for pagination tokens.
func (b *InMemoryBackend) PaginationSecret() string { return b.paginationSecret }

// ModifyEndpoint updates endpoint settings.
func (b *InMemoryBackend) ModifyEndpoint(
	arnOrID, serverName, databaseName, username string,
	port int32,
) (*Endpoint, error) {
	b.mu.Lock("ModifyEndpoint")
	defer b.mu.Unlock()

	ep := b.findEndpoint(arnOrID)
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

// findEndpoint locates an endpoint by identifier or ARN (must hold a lock).
func (b *InMemoryBackend) findEndpoint(arnOrID string) *Endpoint {
	if ep, ok := b.endpoints[arnOrID]; ok {
		return ep
	}

	for _, ep := range b.endpoints {
		if ep.EndpointArn == arnOrID {
			return ep
		}
	}

	return nil
}

// ModifyReplicationInstance updates a replication instance's class and engineVersion.
func (b *InMemoryBackend) ModifyReplicationInstance(
	arnOrID, class, engineVersion string,
	multiAZ, autoMinorVersionUpgrade *bool,
	allocatedStorage *int32,
) (*ReplicationInstance, error) {
	b.mu.Lock("ModifyReplicationInstance")
	defer b.mu.Unlock()

	ri := b.findReplicationInstance(arnOrID)
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

// findReplicationInstance locates a replication instance by identifier or ARN (must hold a lock).
func (b *InMemoryBackend) findReplicationInstance(arnOrID string) *ReplicationInstance {
	if ri, ok := b.replicationInstances[arnOrID]; ok {
		return ri
	}

	for _, ri := range b.replicationInstances {
		if ri.ReplicationInstanceArn == arnOrID {
			return ri
		}
	}

	return nil
}

// ModifyReplicationTask updates task settings.
// AWS does not allow modifying a running task.
func (b *InMemoryBackend) ModifyReplicationTask(
	arnOrID, migrationType, tableMappings, replicationTaskSettings string,
) (*ReplicationTask, error) {
	b.mu.Lock("ModifyReplicationTask")
	defer b.mu.Unlock()

	rt := b.findTask(arnOrID)
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
func (b *InMemoryBackend) DeleteDataMigration(nameOrArn string) (*DataMigration, error) {
	b.mu.Lock("DeleteDataMigration")
	defer b.mu.Unlock()

	if dm, ok := b.dataMigrations[nameOrArn]; ok {
		cp := *dm
		dm.Tags.Close()
		delete(b.dataMigrationsByARN, dm.DataMigrationArn)
		delete(b.dataMigrations, nameOrArn)

		return &cp, nil
	}

	for id, dm := range b.dataMigrations {
		if dm.DataMigrationArn == nameOrArn {
			cp := *dm
			dm.Tags.Close()
			delete(b.dataMigrationsByARN, nameOrArn)
			delete(b.dataMigrations, id)

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: data migration %s not found", ErrNotFound, nameOrArn)
}

// DeleteDataProvider deletes a data provider by name or ARN.
func (b *InMemoryBackend) DeleteDataProvider(nameOrArn string) (*DataProvider, error) {
	b.mu.Lock("DeleteDataProvider")
	defer b.mu.Unlock()

	if dp, ok := b.dataProviders[nameOrArn]; ok {
		cp := *dp
		dp.Tags.Close()
		delete(b.dataProvidersByARN, dp.DataProviderArn)
		delete(b.dataProviders, nameOrArn)

		return &cp, nil
	}

	for id, dp := range b.dataProviders {
		if dp.DataProviderArn == nameOrArn {
			cp := *dp
			dp.Tags.Close()
			delete(b.dataProvidersByARN, nameOrArn)
			delete(b.dataProviders, id)

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: data provider %s not found", ErrNotFound, nameOrArn)
}

// DeleteEventSubscription deletes an event subscription by name.
func (b *InMemoryBackend) DeleteEventSubscription(name string) (*EventSubscription, error) {
	b.mu.Lock("DeleteEventSubscription")
	defer b.mu.Unlock()

	es, ok := b.eventSubscriptions[name]
	if !ok {
		return nil, fmt.Errorf("%w: event subscription %s not found", ErrNotFound, name)
	}

	cp := *es
	cp.SourceIDsList = copyStringsOrEmpty(es.SourceIDsList)
	cp.EventCategories = copyStringsOrEmpty(es.EventCategories)
	es.Tags.Close()
	delete(b.eventSubscriptions, name)

	return &cp, nil
}

// DeleteFleetAdvisorCollector deletes a fleet advisor collector by name or ID.
func (b *InMemoryBackend) DeleteFleetAdvisorCollector(nameOrID string) error {
	b.mu.Lock("DeleteFleetAdvisorCollector")
	defer b.mu.Unlock()

	if col, ok := b.fleetAdvisorCollectors[nameOrID]; ok {
		col.Tags.Close()
		delete(b.fleetAdvisorCollectors, nameOrID)

		return nil
	}

	for name, col := range b.fleetAdvisorCollectors {
		if col.CollectorReferencedID == nameOrID {
			col.Tags.Close()
			delete(b.fleetAdvisorCollectors, name)

			return nil
		}
	}

	return fmt.Errorf("%w: fleet advisor collector %s not found", ErrNotFound, nameOrID)
}

// DeleteInstanceProfile deletes an instance profile by name or ARN.
func (b *InMemoryBackend) DeleteInstanceProfile(nameOrArn string) error {
	b.mu.Lock("DeleteInstanceProfile")
	defer b.mu.Unlock()

	if ip, ok := b.instanceProfiles[nameOrArn]; ok {
		ip.Tags.Close()
		delete(b.instanceProfilesByARN, ip.InstanceProfileArn)
		delete(b.instanceProfiles, nameOrArn)

		return nil
	}

	for name, ip := range b.instanceProfiles {
		if ip.InstanceProfileArn == nameOrArn {
			ip.Tags.Close()
			delete(b.instanceProfilesByARN, nameOrArn)
			delete(b.instanceProfiles, name)

			return nil
		}
	}

	return fmt.Errorf("%w: instance profile %s not found", ErrNotFound, nameOrArn)
}

// findResourceTags returns the Tags for a resource ARN (must hold a lock).
// Returns nil if not found.
func (b *InMemoryBackend) findResourceTags(resourceArn string) *tags.Tags {
	if ri, ok := b.replicationInstancesByARN[resourceArn]; ok {
		return ri.Tags
	}

	if ep, ok := b.endpointsByARN[resourceArn]; ok {
		return ep.Tags
	}

	if rt, ok := b.replicationTasksByARN[resourceArn]; ok {
		return rt.Tags
	}

	if dm, ok := b.dataMigrationsByARN[resourceArn]; ok {
		return dm.Tags
	}

	if dp, ok := b.dataProvidersByARN[resourceArn]; ok {
		return dp.Tags
	}

	if ip, ok := b.instanceProfilesByARN[resourceArn]; ok {
		return ip.Tags
	}

	if mp, ok := b.migrationProjectsByARN[resourceArn]; ok {
		return mp.Tags
	}

	if sg, ok := b.replicationSubnetGroupsByARN[resourceArn]; ok {
		return sg.Tags
	}

	if rc, ok := b.replicationConfigsByARN[resourceArn]; ok {
		return rc.Tags
	}

	return nil
}

// RemoveTagsFromResource removes tags from a DMS resource by ARN.
func (b *InMemoryBackend) RemoveTagsFromResource(resourceArn string, tagKeys []string) error {
	b.mu.Lock("RemoveTagsFromResource")
	defer b.mu.Unlock()

	t := b.findResourceTags(resourceArn)
	if t == nil {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceArn)
	}

	t.DeleteKeys(tagKeys)

	return nil
}

// ModifyDataMigration updates a data migration.
func (b *InMemoryBackend) ModifyDataMigration(
	nameOrArn, migrationType, serviceAccessRoleArn string,
	numberOfJobs *int32,
) (*DataMigration, error) {
	b.mu.Lock("ModifyDataMigration")
	defer b.mu.Unlock()

	dm := b.findDataMigration(nameOrArn)
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

// findDataMigration locates a data migration by name or ARN (must hold a lock).
func (b *InMemoryBackend) findDataMigration(nameOrArn string) *DataMigration {
	if dm, ok := b.dataMigrations[nameOrArn]; ok {
		return dm
	}

	for _, dm := range b.dataMigrations {
		if dm.DataMigrationArn == nameOrArn {
			return dm
		}
	}

	return nil
}

// ModifyDataProvider updates a data provider.
func (b *InMemoryBackend) ModifyDataProvider(
	nameOrArn, engine, description string,
) (*DataProvider, error) {
	b.mu.Lock("ModifyDataProvider")
	defer b.mu.Unlock()

	dp := b.findDataProvider(nameOrArn)
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

// findDataProvider locates a data provider by name or ARN (must hold a lock).
func (b *InMemoryBackend) findDataProvider(nameOrArn string) *DataProvider {
	if dp, ok := b.dataProviders[nameOrArn]; ok {
		return dp
	}

	for _, dp := range b.dataProviders {
		if dp.DataProviderArn == nameOrArn {
			return dp
		}
	}

	return nil
}

// ModifyEventSubscription updates an event subscription.
func (b *InMemoryBackend) ModifyEventSubscription(
	name string,
	enabled *bool,
) (*EventSubscription, error) {
	b.mu.Lock("ModifyEventSubscription")
	defer b.mu.Unlock()

	es, ok := b.eventSubscriptions[name]
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
	nameOrArn, availabilityZone, description, networkType string,
) (*InstanceProfile, error) {
	b.mu.Lock("ModifyInstanceProfile")
	defer b.mu.Unlock()

	ip := b.findInstanceProfile(nameOrArn)
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

// findInstanceProfile locates an instance profile by name or ARN (must hold a lock).
func (b *InMemoryBackend) findInstanceProfile(nameOrArn string) *InstanceProfile {
	if ip, ok := b.instanceProfiles[nameOrArn]; ok {
		return ip
	}

	for _, ip := range b.instanceProfiles {
		if ip.InstanceProfileArn == nameOrArn {
			return ip
		}
	}

	return nil
}

// StartDataMigration transitions a data migration to running status.
func (b *InMemoryBackend) StartDataMigration(nameOrArn string) (*DataMigration, error) {
	b.mu.Lock("StartDataMigration")
	defer b.mu.Unlock()

	dm := b.findDataMigration(nameOrArn)
	if dm == nil {
		return nil, fmt.Errorf("%w: data migration %s not found", ErrNotFound, nameOrArn)
	}

	dm.DataMigrationStatus = statusRunning
	cp := *dm

	return &cp, nil
}

// StopDataMigration transitions a data migration to stopped status.
func (b *InMemoryBackend) StopDataMigration(nameOrArn string) (*DataMigration, error) {
	b.mu.Lock("StopDataMigration")
	defer b.mu.Unlock()

	dm := b.findDataMigration(nameOrArn)
	if dm == nil {
		return nil, fmt.Errorf("%w: data migration %s not found", ErrNotFound, nameOrArn)
	}

	dm.DataMigrationStatus = statusStopped
	cp := *dm

	return &cp, nil
}

// RebootReplicationInstance reboots a replication instance (no-op in memory).
func (b *InMemoryBackend) RebootReplicationInstance(arnOrID string) (*ReplicationInstance, error) {
	b.mu.RLock("RebootReplicationInstance")
	defer b.mu.RUnlock()

	ri := b.findReplicationInstance(arnOrID)
	if ri == nil {
		return nil, fmt.Errorf("%w: replication instance %s not found", ErrNotFound, arnOrID)
	}

	cp := *ri

	return &cp, nil
}

// MoveReplicationTask moves a replication task to a different instance.
func (b *InMemoryBackend) MoveReplicationTask(
	taskArnOrID, targetInstanceArn string,
) (*ReplicationTask, error) {
	b.mu.Lock("MoveReplicationTask")
	defer b.mu.Unlock()

	rt := b.findTask(taskArnOrID)
	if rt == nil {
		return nil, fmt.Errorf("%w: replication task %s not found", ErrNotFound, taskArnOrID)
	}

	rt.ReplicationInstanceArn = targetInstanceArn
	cp := *rt

	return &cp, nil
}

// TestConnection tests a DMS connection and records the result.
func (b *InMemoryBackend) TestConnection(replicationInstanceArn, endpointArn string) (*Connection, error) {
	b.mu.Lock("TestConnection")
	defer b.mu.Unlock()

	ri, ok := b.replicationInstancesByARN[replicationInstanceArn]
	if !ok {
		return nil, fmt.Errorf(
			"%w: replication instance %s not found",
			ErrNotFound,
			replicationInstanceArn,
		)
	}

	ep, ok := b.endpointsByARN[endpointArn]
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
	b.connections[key] = conn
	cp := *conn

	return &cp, nil
}

// DescribeConnections returns stored connections, optionally filtered by replication instance ARN or endpoint ARN.
func (b *InMemoryBackend) DescribeConnections(replicationInstanceArn, endpointArn string) ([]*Connection, error) {
	b.mu.RLock("DescribeConnections")
	defer b.mu.RUnlock()

	list := make([]*Connection, 0, len(b.connections))
	for _, conn := range b.connections {
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
func (b *InMemoryBackend) ImportCertificate(identifier, certPem string) (*Certificate, error) {
	b.mu.Lock("ImportCertificate")
	defer b.mu.Unlock()

	if _, ok := b.certificates[identifier]; ok {
		return nil, fmt.Errorf("%w: certificate %s already exists", ErrAlreadyExists, identifier)
	}

	certARN := arn.Build("dms", b.region, b.accountID, "certificate:"+identifier)
	cert := &Certificate{
		CertificateIdentifier: identifier,
		CertificateArn:        certARN,
		CertificatePem:        certPem,
		AccountID:             b.accountID,
		Region:                b.region,
	}
	b.certificates[identifier] = cert
	cp := *cert

	return &cp, nil
}

// DeleteCertificate deletes a certificate by identifier or ARN.
func (b *InMemoryBackend) DeleteCertificate(identifierOrArn string) (*Certificate, error) {
	b.mu.Lock("DeleteCertificate")
	defer b.mu.Unlock()

	if cert, ok := b.certificates[identifierOrArn]; ok {
		cp := *cert
		delete(b.certificates, identifierOrArn)

		return &cp, nil
	}

	for id, cert := range b.certificates {
		if cert.CertificateArn == identifierOrArn {
			cp := *cert
			delete(b.certificates, id)

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: certificate %s not found", ErrNotFound, identifierOrArn)
}

// CreateMigrationProject creates a migration project.
func (b *InMemoryBackend) CreateMigrationProject(
	name, description string,
	kv map[string]string,
) (*MigrationProject, error) {
	b.mu.Lock("CreateMigrationProject")
	defer b.mu.Unlock()

	if _, ok := b.migrationProjects[name]; ok {
		return nil, fmt.Errorf("%w: migration project %s already exists", ErrAlreadyExists, name)
	}

	projectARN := arn.Build("dms", b.region, b.accountID, "migration-project:"+uuid.NewString())
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
		Region:                     b.region,
		Tags:                       t,
	}
	b.migrationProjects[name] = mp
	b.migrationProjectsByARN[projectARN] = mp
	cp := *mp

	return &cp, nil
}

// DeleteMigrationProject deletes a migration project by name or ARN.
func (b *InMemoryBackend) DeleteMigrationProject(nameOrArn string) error {
	b.mu.Lock("DeleteMigrationProject")
	defer b.mu.Unlock()

	if mp, ok := b.migrationProjects[nameOrArn]; ok {
		mp.Tags.Close()
		delete(b.migrationProjectsByARN, mp.MigrationProjectArn)
		delete(b.migrationProjects, nameOrArn)

		return nil
	}

	for name, mp := range b.migrationProjects {
		if mp.MigrationProjectArn == nameOrArn {
			mp.Tags.Close()
			delete(b.migrationProjectsByARN, nameOrArn)
			delete(b.migrationProjects, name)

			return nil
		}
	}

	return fmt.Errorf("%w: migration project %s not found", ErrNotFound, nameOrArn)
}

// CreateReplicationSubnetGroup creates a subnet group.
func (b *InMemoryBackend) CreateReplicationSubnetGroup(
	identifier, description, vpcID string,
	kv map[string]string,
) (*ReplicationSubnetGroup, error) {
	b.mu.Lock("CreateReplicationSubnetGroup")
	defer b.mu.Unlock()

	if _, ok := b.replicationSubnetGroups[identifier]; ok {
		return nil, fmt.Errorf(
			"%w: replication subnet group %s already exists",
			ErrAlreadyExists,
			identifier,
		)
	}

	sgARN := arn.Build("dms", b.region, b.accountID, "subgrp:"+identifier)
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
		Region:                            b.region,
		Tags:                              t,
	}
	b.replicationSubnetGroups[identifier] = sg
	b.replicationSubnetGroupsByARN[sgARN] = sg
	cp := *sg

	return &cp, nil
}

// DeleteReplicationSubnetGroup deletes a subnet group by identifier or ARN.
func (b *InMemoryBackend) DeleteReplicationSubnetGroup(identifierOrArn string) error {
	b.mu.Lock("DeleteReplicationSubnetGroup")
	defer b.mu.Unlock()

	if sg, ok := b.replicationSubnetGroups[identifierOrArn]; ok {
		sg.Tags.Close()
		delete(b.replicationSubnetGroupsByARN, sg.ReplicationSubnetGroupArn)
		delete(b.replicationSubnetGroups, identifierOrArn)

		return nil
	}

	for id, sg := range b.replicationSubnetGroups {
		if sg.ReplicationSubnetGroupArn == identifierOrArn {
			sg.Tags.Close()
			delete(b.replicationSubnetGroupsByARN, identifierOrArn)
			delete(b.replicationSubnetGroups, id)

			return nil
		}
	}

	return fmt.Errorf("%w: replication subnet group %s not found", ErrNotFound, identifierOrArn)
}

// CreateReplicationConfig creates a replication config.
func (b *InMemoryBackend) CreateReplicationConfig(
	identifier, replicationType, sourceEndpointArn, targetEndpointArn string,
	kv map[string]string,
) (*ReplicationConfig, error) {
	b.mu.Lock("CreateReplicationConfig")
	defer b.mu.Unlock()

	if _, ok := b.replicationConfigs[identifier]; ok {
		return nil, fmt.Errorf(
			"%w: replication config %s already exists",
			ErrAlreadyExists,
			identifier,
		)
	}

	configARN := arn.Build("dms", b.region, b.accountID, "replication-config:"+uuid.NewString())
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
		Region:                      b.region,
		Tags:                        t,
	}
	b.replicationConfigs[identifier] = rc
	b.replicationConfigsByARN[configARN] = rc
	cp := *rc

	return &cp, nil
}

// DeleteReplicationConfig deletes a replication config by identifier or ARN.
func (b *InMemoryBackend) DeleteReplicationConfig(identifierOrArn string) error {
	b.mu.Lock("DeleteReplicationConfig")
	defer b.mu.Unlock()

	if rc, ok := b.replicationConfigs[identifierOrArn]; ok {
		rc.Tags.Close()
		delete(b.replicationConfigsByARN, rc.ReplicationConfigArn)
		delete(b.replicationConfigs, identifierOrArn)

		return nil
	}

	for id, rc := range b.replicationConfigs {
		if rc.ReplicationConfigArn == identifierOrArn {
			rc.Tags.Close()
			delete(b.replicationConfigsByARN, identifierOrArn)
			delete(b.replicationConfigs, id)

			return nil
		}
	}

	return fmt.Errorf("%w: replication config %s not found", ErrNotFound, identifierOrArn)
}

// DescribeDataMigrations returns all data migrations (optionally filtered by name/arn).
func (b *InMemoryBackend) DescribeDataMigrations(nameOrArn string) ([]*DataMigration, error) {
	b.mu.RLock("DescribeDataMigrations")
	defer b.mu.RUnlock()

	if nameOrArn != "" {
		dm := b.findDataMigration(nameOrArn)
		if dm == nil {
			return []*DataMigration{}, nil
		}

		cp := *dm

		return []*DataMigration{&cp}, nil
	}

	list := make([]*DataMigration, 0, len(b.dataMigrations))
	for _, dm := range b.dataMigrations {
		cp := *dm
		list = append(list, &cp)
	}

	return list, nil
}

// DescribeDataProviders returns all data providers (optionally filtered by name/arn).
func (b *InMemoryBackend) DescribeDataProviders(nameOrArn string) ([]*DataProvider, error) {
	b.mu.RLock("DescribeDataProviders")
	defer b.mu.RUnlock()

	if nameOrArn != "" {
		dp := b.findDataProvider(nameOrArn)
		if dp == nil {
			return []*DataProvider{}, nil
		}

		cp := *dp

		return []*DataProvider{&cp}, nil
	}

	list := make([]*DataProvider, 0, len(b.dataProviders))
	for _, dp := range b.dataProviders {
		cp := *dp
		list = append(list, &cp)
	}

	return list, nil
}

// DescribeEventSubscriptions returns all event subscriptions (optionally filtered by name).
func (b *InMemoryBackend) DescribeEventSubscriptions(name string) ([]*EventSubscription, error) {
	b.mu.RLock("DescribeEventSubscriptions")
	defer b.mu.RUnlock()

	if name != "" {
		es, ok := b.eventSubscriptions[name]
		if !ok {
			return []*EventSubscription{}, nil
		}

		cp := *es
		cp.SourceIDsList = copyStringsOrEmpty(es.SourceIDsList)
		cp.EventCategories = copyStringsOrEmpty(es.EventCategories)

		return []*EventSubscription{&cp}, nil
	}

	list := make([]*EventSubscription, 0, len(b.eventSubscriptions))
	for _, es := range b.eventSubscriptions {
		cp := *es
		cp.SourceIDsList = copyStringsOrEmpty(es.SourceIDsList)
		cp.EventCategories = copyStringsOrEmpty(es.EventCategories)
		list = append(list, &cp)
	}

	return list, nil
}

// DescribeFleetAdvisorCollectors returns all fleet advisor collectors.
func (b *InMemoryBackend) DescribeFleetAdvisorCollectors() ([]*FleetAdvisorCollector, error) {
	b.mu.RLock("DescribeFleetAdvisorCollectors")
	defer b.mu.RUnlock()

	list := make([]*FleetAdvisorCollector, 0, len(b.fleetAdvisorCollectors))
	for _, col := range b.fleetAdvisorCollectors {
		cp := *col
		list = append(list, &cp)
	}

	return list, nil
}

// DescribeInstanceProfiles returns all instance profiles.
func (b *InMemoryBackend) DescribeInstanceProfiles() ([]*InstanceProfile, error) {
	b.mu.RLock("DescribeInstanceProfiles")
	defer b.mu.RUnlock()

	list := make([]*InstanceProfile, 0, len(b.instanceProfiles))
	for _, ip := range b.instanceProfiles {
		cp := *ip
		list = append(list, &cp)
	}

	return list, nil
}

// DescribeMigrationProjects returns all migration projects.
func (b *InMemoryBackend) DescribeMigrationProjects() ([]*MigrationProject, error) {
	b.mu.RLock("DescribeMigrationProjects")
	defer b.mu.RUnlock()

	list := make([]*MigrationProject, 0, len(b.migrationProjects))
	for _, mp := range b.migrationProjects {
		cp := *mp
		list = append(list, &cp)
	}

	return list, nil
}

// DescribeReplicationSubnetGroups returns all subnet groups.
func (b *InMemoryBackend) DescribeReplicationSubnetGroups() ([]*ReplicationSubnetGroup, error) {
	b.mu.RLock("DescribeReplicationSubnetGroups")
	defer b.mu.RUnlock()

	list := make([]*ReplicationSubnetGroup, 0, len(b.replicationSubnetGroups))
	for _, sg := range b.replicationSubnetGroups {
		cp := *sg
		list = append(list, &cp)
	}

	return list, nil
}

// DescribeReplicationConfigs returns all replication configs.
func (b *InMemoryBackend) DescribeReplicationConfigs() ([]*ReplicationConfig, error) {
	b.mu.RLock("DescribeReplicationConfigs")
	defer b.mu.RUnlock()

	list := make([]*ReplicationConfig, 0, len(b.replicationConfigs))
	for _, rc := range b.replicationConfigs {
		cp := *rc
		list = append(list, &cp)
	}

	return list, nil
}

// DescribeCertificates returns all certificates.
func (b *InMemoryBackend) DescribeCertificates() ([]*Certificate, error) {
	b.mu.RLock("DescribeCertificates")
	defer b.mu.RUnlock()

	list := make([]*Certificate, 0, len(b.certificates))
	for _, cert := range b.certificates {
		cp := *cert
		list = append(list, &cp)
	}

	return list, nil
}
