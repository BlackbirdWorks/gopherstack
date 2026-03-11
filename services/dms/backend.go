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

var (
	// ErrNotFound is returned when a requested DMS resource cannot be found.
	ErrNotFound = awserr.New("ResourceNotFoundFault", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a DMS resource already exists.
	ErrAlreadyExists = awserr.New("ResourceAlreadyExistsFault", awserr.ErrConflict)
	// ErrInvalidState is returned when a DMS resource is in an invalid state for the requested operation.
	ErrInvalidState = awserr.New("InvalidResourceStateFault", awserr.ErrInvalidParameter)
)

// ReplicationInstance represents an AWS DMS replication instance.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via AddTagsToResource or CreateReplicationInstance.
type ReplicationInstance struct {
	CreationTime                  time.Time  `json:"creationTime"`
	Tags                          *tags.Tags `json:"tags,omitempty"`
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
	Tags               *tags.Tags `json:"tags,omitempty"`
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
	Tags                      *tags.Tags `json:"tags,omitempty"`
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

// InMemoryBackend is the in-memory store for AWS DMS resources.
type InMemoryBackend struct {
	replicationInstances map[string]*ReplicationInstance
	endpoints            map[string]*Endpoint
	replicationTasks     map[string]*ReplicationTask
	mu                   *lockmetrics.RWMutex
	accountID            string
	region               string
}

// NewInMemoryBackend creates a new in-memory DMS backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		replicationInstances: make(map[string]*ReplicationInstance),
		endpoints:            make(map[string]*Endpoint),
		replicationTasks:     make(map[string]*ReplicationTask),
		accountID:            accountID,
		region:               region,
		mu:                   lockmetrics.New("dms"),
	}
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
		return nil, fmt.Errorf("%w: replication instance %s already exists", ErrAlreadyExists, identifier)
	}

	instanceARN := arn.Build("dms", b.region, b.accountID, "rep:"+identifier)
	t := tags.New("dms.replication-instance." + identifier + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}

	if engineVersion == "" {
		engineVersion = "3.5.3"
	}

	if allocatedStorage == 0 {
		allocatedStorage = 50
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
		ReplicationInstanceStatus:     "available",
		AccountID:                     b.accountID,
		Region:                        b.region,
		CreationTime:                  time.Now().UTC(),
		Tags:                          t,
	}
	b.replicationInstances[identifier] = ri
	cp := *ri

	return &cp, nil
}

// DescribeReplicationInstances returns replication instances, optionally filtered by identifier or ARN.
func (b *InMemoryBackend) DescribeReplicationInstances(identifierOrArn string) ([]*ReplicationInstance, error) {
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

		return nil, fmt.Errorf("%w: replication instance %s not found", ErrNotFound, identifierOrArn)
	}

	list := make([]*ReplicationInstance, 0, len(b.replicationInstances))
	for _, ri := range b.replicationInstances {
		cp := *ri
		list = append(list, &cp)
	}

	return list, nil
}

// DeleteReplicationInstance deletes a replication instance by ARN or identifier.
func (b *InMemoryBackend) DeleteReplicationInstance(arnOrID string) error {
	b.mu.Lock("DeleteReplicationInstance")
	defer b.mu.Unlock()

	// Try by identifier first.
	if _, ok := b.replicationInstances[arnOrID]; ok {
		delete(b.replicationInstances, arnOrID)

		return nil
	}
	// Try by ARN.
	for id, ri := range b.replicationInstances {
		if ri.ReplicationInstanceArn == arnOrID {
			delete(b.replicationInstances, id)

			return nil
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
		Status:             "active",
		AccountID:          b.accountID,
		Region:             b.region,
		CreationTime:       time.Now().UTC(),
		Tags:               t,
	}
	b.endpoints[identifier] = ep
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

		return nil, fmt.Errorf("%w: endpoint %s not found", ErrNotFound, identifierOrArn)
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
		delete(b.endpoints, arnOrID)

		return &cp, nil
	}
	// Try by ARN.
	for id, ep := range b.endpoints {
		if ep.EndpointArn == arnOrID {
			cp := *ep
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
		return nil, fmt.Errorf("%w: replication task %s already exists", ErrAlreadyExists, identifier)
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
		Status:                    "ready",
		AccountID:                 b.accountID,
		Region:                    b.region,
		CreationTime:              time.Now().UTC(),
		Tags:                      t,
	}
	b.replicationTasks[identifier] = rt
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

		return nil, fmt.Errorf("%w: replication task %s not found", ErrNotFound, arnOrID)
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

	if rt.Status == "running" {
		return nil, fmt.Errorf("%w: replication task %s is already running", ErrInvalidState, arnOrID)
	}

	rt.Status = "running"
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

	rt.Status = "stopped"
	cp := *rt

	return &cp, nil
}

// DeleteReplicationTask deletes a replication task by ARN or identifier.
func (b *InMemoryBackend) DeleteReplicationTask(arnOrID string) (*ReplicationTask, error) {
	b.mu.Lock("DeleteReplicationTask")
	defer b.mu.Unlock()

	// Try by identifier first.
	if rt, ok := b.replicationTasks[arnOrID]; ok {
		cp := *rt
		delete(b.replicationTasks, arnOrID)

		return &cp, nil
	}
	// Try by ARN.
	for id, rt := range b.replicationTasks {
		if rt.ReplicationTaskArn == arnOrID {
			cp := *rt
			delete(b.replicationTasks, id)

			return &cp, nil
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

	for _, ri := range b.replicationInstances {
		if ri.ReplicationInstanceArn == resourceArn {
			ri.Tags.Merge(kv)

			return nil
		}
	}
	for _, ep := range b.endpoints {
		if ep.EndpointArn == resourceArn {
			ep.Tags.Merge(kv)

			return nil
		}
	}
	for _, rt := range b.replicationTasks {
		if rt.ReplicationTaskArn == resourceArn {
			rt.Tags.Merge(kv)

			return nil
		}
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceArn)
}

// ListTagsForResource returns tags for a DMS resource by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	for _, ri := range b.replicationInstances {
		if ri.ReplicationInstanceArn == resourceArn {
			return ri.Tags.Clone(), nil
		}
	}
	for _, ep := range b.endpoints {
		if ep.EndpointArn == resourceArn {
			return ep.Tags.Clone(), nil
		}
	}
	for _, rt := range b.replicationTasks {
		if rt.ReplicationTaskArn == resourceArn {
			return rt.Tags.Clone(), nil
		}
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceArn)
}
