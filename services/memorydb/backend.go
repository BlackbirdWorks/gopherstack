package memorydb

import (
	"fmt"
	"maps"
	"sort"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

const (
	// openAccessACL is the default ACL name that allows all connections.
	openAccessACL = "open-access"
	// defaultEngineVersion is the default Redis version for new clusters.
	defaultEngineVersion = "7.0"
	// defaultNodeType is the default node type for new clusters.
	defaultNodeType = "db.r6g.large"
	// defaultPort is the default MemoryDB port.
	defaultPort = int32(6379)
	// clusterStatusAvailable is the status for a running cluster.
	clusterStatusAvailable = "available"
	// aclStatusActive is the status for an active ACL.
	aclStatusActive = "active"
	// userStatusActive is the status for an active user.
	userStatusActive = "active"

	// Resource kind constants for tag routing.
	resourceKindCluster        = "cluster"
	resourceKindACL            = "acl"
	resourceKindSubnetGroup    = "subnetgroup"
	resourceKindUser           = "user"
	resourceKindParameterGroup = "parametergroup"
)

// Errors used by the backend.
var (
	// ErrClusterNotFound is returned when a cluster does not exist.
	ErrClusterNotFound = awserr.New("ClusterNotFoundFault: cluster not found", awserr.ErrNotFound)
	// ErrClusterAlreadyExists is returned when a cluster already exists.
	ErrClusterAlreadyExists = awserr.New("ClusterAlreadyExistsFault: cluster already exists", awserr.ErrAlreadyExists)
	// ErrACLNotFound is returned when an ACL does not exist.
	ErrACLNotFound = awserr.New("ACLNotFoundFault: ACL not found", awserr.ErrNotFound)
	// ErrACLAlreadyExists is returned when an ACL already exists.
	ErrACLAlreadyExists = awserr.New("ACLAlreadyExistsFault: ACL already exists", awserr.ErrAlreadyExists)
	// ErrSubnetGroupNotFound is returned when a subnet group does not exist.
	ErrSubnetGroupNotFound = awserr.New("SubnetGroupNotFoundFault: subnet group not found", awserr.ErrNotFound)
	// ErrSubnetGroupAlreadyExists is returned when a subnet group already exists.
	ErrSubnetGroupAlreadyExists = awserr.New(
		"SubnetGroupAlreadyExistsFault: subnet group already exists",
		awserr.ErrAlreadyExists,
	)
	// ErrUserNotFound is returned when a user does not exist.
	ErrUserNotFound = awserr.New("UserNotFoundFault: user not found", awserr.ErrNotFound)
	// ErrUserAlreadyExists is returned when a user already exists.
	ErrUserAlreadyExists = awserr.New("UserAlreadyExistsFault: user already exists", awserr.ErrAlreadyExists)
	// ErrParameterGroupNotFound is returned when a parameter group does not exist.
	ErrParameterGroupNotFound = awserr.New("ParameterGroupNotFoundFault: parameter group not found", awserr.ErrNotFound)
	// ErrParameterGroupAlreadyExists is returned when a parameter group already exists.
	ErrParameterGroupAlreadyExists = awserr.New(
		"ParameterGroupAlreadyExistsFault: parameter group already exists",
		awserr.ErrAlreadyExists,
	)
)

// StorageBackend is the interface for the MemoryDB in-memory backend.
type StorageBackend interface {
	// Cluster operations
	CreateCluster(region, accountID string, req *createClusterRequest) (*Cluster, error)
	DescribeClusters(name string) ([]*Cluster, error)
	DeleteCluster(name string) (*Cluster, error)
	UpdateCluster(req *updateClusterRequest) (*Cluster, error)

	// ACL operations
	CreateACL(region, accountID string, req *createACLRequest) (*ACL, error)
	DescribeACLs(name string) ([]*ACL, error)
	DeleteACL(name string) (*ACL, error)
	UpdateACL(req *updateACLRequest) (*ACL, error)

	// SubnetGroup operations
	CreateSubnetGroup(region, accountID string, req *createSubnetGroupRequest) (*SubnetGroup, error)
	DescribeSubnetGroups(name string) ([]*SubnetGroup, error)
	DeleteSubnetGroup(name string) (*SubnetGroup, error)
	UpdateSubnetGroup(req *updateSubnetGroupRequest) (*SubnetGroup, error)

	// User operations
	CreateUser(region, accountID string, req *createUserRequest) (*User, error)
	DescribeUsers(name string) ([]*User, error)
	DeleteUser(name string) (*User, error)
	UpdateUser(req *updateUserRequest) (*User, error)

	// ParameterGroup operations
	CreateParameterGroup(region, accountID string, req *createParameterGroupRequest) (*ParameterGroup, error)
	DescribeParameterGroups(name string) ([]*ParameterGroup, error)
	DeleteParameterGroup(name string) (*ParameterGroup, error)
	UpdateParameterGroup(req *updateParameterGroupRequest) (*ParameterGroup, error)

	// Tag operations
	ListTags(resourceArn string) (map[string]string, error)
	TagResource(resourceArn string, tags map[string]string) error
	UntagResource(resourceArn string, tagKeys []string) error
}

// InMemoryBackend is the in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	clusters        map[string]*Cluster
	acls            map[string]*ACL
	subnetGroups    map[string]*SubnetGroup
	users           map[string]*User
	parameterGroups map[string]*ParameterGroup
	// arnToResource maps ARN strings to their resource type+name for tag lookups.
	arnToResource map[string]resourceRef
	mu            sync.RWMutex
}

type resourceRef struct {
	kind string
	name string
}

// NewInMemoryBackend creates a new MemoryDB in-memory backend.
// It pre-seeds the "open-access" ACL which is required by most clusters.
func NewInMemoryBackend() *InMemoryBackend {
	return newInMemoryBackendWithDefaults("us-east-1", "000000000000")
}

// newInMemoryBackendWithDefaults creates a backend pre-seeded with the given region and account.
func newInMemoryBackendWithDefaults(region, accountID string) *InMemoryBackend {
	b := &InMemoryBackend{
		clusters:        make(map[string]*Cluster),
		acls:            make(map[string]*ACL),
		subnetGroups:    make(map[string]*SubnetGroup),
		users:           make(map[string]*User),
		parameterGroups: make(map[string]*ParameterGroup),
		arnToResource:   make(map[string]resourceRef),
	}

	// Pre-seed the open-access ACL so Terraform resources that omit an explicit
	// ACL name can reference it without first creating it.
	openAccessARN := arn.Build("memorydb", region, accountID, fmt.Sprintf("acl/%s", openAccessACL))
	b.acls[openAccessACL] = &ACL{
		Name:      openAccessACL,
		ARN:       openAccessARN,
		Status:    aclStatusActive,
		UserNames: []string{},
		CreatedAt: time.Now(),
		Tags:      make(map[string]string),
	}
	b.arnToResource[openAccessARN] = resourceRef{kind: resourceKindACL, name: openAccessACL}

	return b
}

// -- Cluster operations ----------------------------------------------------------

// CreateCluster creates a new MemoryDB cluster.
func (b *InMemoryBackend) CreateCluster(region, accountID string, req *createClusterRequest) (*Cluster, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.clusters[req.ClusterName]; exists {
		return nil, ErrClusterAlreadyExists
	}

	engineVersion := req.EngineVersion
	if engineVersion == "" {
		engineVersion = defaultEngineVersion
	}

	nodeType := req.NodeType
	if nodeType == "" {
		nodeType = defaultNodeType
	}

	port := defaultPort
	if req.Port != nil {
		port = *req.Port
	}

	numShards := int32(1)
	if req.NumShards != nil {
		numShards = *req.NumShards
	}

	numReplicas := int32(1)
	if req.NumReplicasPerShard != nil {
		numReplicas = *req.NumReplicasPerShard
	}

	tlsEnabled := true
	if req.TLSEnabled != nil {
		tlsEnabled = *req.TLSEnabled
	}

	clusterARN := arn.Build("memorydb", region, accountID, fmt.Sprintf("cluster/%s", req.ClusterName))

	aclName := req.ACLName
	if aclName == "" {
		aclName = openAccessACL
	}

	c := &Cluster{
		Name:                req.ClusterName,
		ARN:                 clusterARN,
		Description:         req.Description,
		NodeType:            nodeType,
		EngineVersion:       engineVersion,
		ACLName:             aclName,
		SubnetGroupName:     req.SubnetGroupName,
		ParameterGroupName:  req.ParameterGroupName,
		KmsKeyID:            req.KmsKeyID,
		SnsTopicArn:         req.SnsTopicArn,
		MaintenanceWindow:   req.MaintenanceWindow,
		SnapshotWindow:      req.SnapshotWindow,
		NumShards:           numShards,
		NumReplicasPerShard: numReplicas,
		Port:                port,
		TLSEnabled:          tlsEnabled,
		Status:              clusterStatusAvailable,
		Tags:                tagsFromSlice(req.Tags),
		CreatedAt:           time.Now(),
		Region:              region,
	}

	if req.SnapshotRetentionLimit != nil {
		c.SnapshotRetentionLimit = *req.SnapshotRetentionLimit
	}

	b.clusters[req.ClusterName] = c
	b.arnToResource[clusterARN] = resourceRef{kind: resourceKindCluster, name: req.ClusterName}

	return c, nil
}

// DescribeClusters returns clusters, optionally filtered by name.
func (b *InMemoryBackend) DescribeClusters(name string) ([]*Cluster, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if name != "" {
		c, ok := b.clusters[name]
		if !ok {
			return nil, ErrClusterNotFound
		}

		return []*Cluster{c}, nil
	}

	result := make([]*Cluster, 0, len(b.clusters))

	for _, c := range b.clusters {
		result = append(result, c)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
}

// DeleteCluster removes a cluster.
func (b *InMemoryBackend) DeleteCluster(name string) (*Cluster, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	c, ok := b.clusters[name]
	if !ok {
		return nil, ErrClusterNotFound
	}

	delete(b.clusters, name)
	delete(b.arnToResource, c.ARN)

	return c, nil
}

// UpdateCluster modifies an existing cluster.
func (b *InMemoryBackend) UpdateCluster(req *updateClusterRequest) (*Cluster, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	c, ok := b.clusters[req.ClusterName]
	if !ok {
		return nil, ErrClusterNotFound
	}

	if req.Description != "" {
		c.Description = req.Description
	}

	if req.ACLName != "" {
		c.ACLName = req.ACLName
	}

	if req.NodeType != "" {
		c.NodeType = req.NodeType
	}

	if req.EngineVersion != "" {
		c.EngineVersion = req.EngineVersion
	}

	if req.MaintenanceWindow != "" {
		c.MaintenanceWindow = req.MaintenanceWindow
	}

	if req.SnapshotWindow != "" {
		c.SnapshotWindow = req.SnapshotWindow
	}

	if req.SnsTopicArn != "" {
		c.SnsTopicArn = req.SnsTopicArn
	}

	if req.SnapshotRetentionLimit != nil {
		c.SnapshotRetentionLimit = *req.SnapshotRetentionLimit
	}

	if req.ReplicaConfiguration != nil && req.ReplicaConfiguration.ReplicaCount != nil {
		c.NumReplicasPerShard = *req.ReplicaConfiguration.ReplicaCount
	}

	if req.ShardConfiguration != nil && req.ShardConfiguration.ShardCount != nil {
		c.NumShards = *req.ShardConfiguration.ShardCount
	}

	return c, nil
}

// -- ACL operations --------------------------------------------------------------

// CreateACL creates a new ACL.
func (b *InMemoryBackend) CreateACL(region, accountID string, req *createACLRequest) (*ACL, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.acls[req.ACLName]; exists {
		return nil, ErrACLAlreadyExists
	}

	aclARN := arn.Build("memorydb", region, accountID, fmt.Sprintf("acl/%s", req.ACLName))

	userNames := req.UserNames
	if userNames == nil {
		userNames = []string{}
	}

	a := &ACL{
		Name:      req.ACLName,
		ARN:       aclARN,
		Status:    aclStatusActive,
		UserNames: userNames,
		Tags:      tagsFromSlice(req.Tags),
		CreatedAt: time.Now(),
	}

	b.acls[req.ACLName] = a
	b.arnToResource[aclARN] = resourceRef{kind: resourceKindACL, name: req.ACLName}

	return a, nil
}

// DescribeACLs returns ACLs, optionally filtered by name.
func (b *InMemoryBackend) DescribeACLs(name string) ([]*ACL, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if name != "" {
		a, ok := b.acls[name]
		if !ok {
			return nil, ErrACLNotFound
		}

		return []*ACL{a}, nil
	}

	result := make([]*ACL, 0, len(b.acls))

	for _, a := range b.acls {
		result = append(result, a)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
}

// DeleteACL removes an ACL.
func (b *InMemoryBackend) DeleteACL(name string) (*ACL, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	a, ok := b.acls[name]
	if !ok {
		return nil, ErrACLNotFound
	}

	delete(b.acls, name)
	delete(b.arnToResource, a.ARN)

	return a, nil
}

// UpdateACL modifies an existing ACL.
func (b *InMemoryBackend) UpdateACL(req *updateACLRequest) (*ACL, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	a, ok := b.acls[req.ACLName]
	if !ok {
		return nil, ErrACLNotFound
	}

	// Add users.
	existing := make(map[string]bool, len(a.UserNames))

	for _, u := range a.UserNames {
		existing[u] = true
	}

	for _, u := range req.UserNamesToAdd {
		if !existing[u] {
			a.UserNames = append(a.UserNames, u)
			existing[u] = true
		}
	}

	// Remove users.
	toRemove := make(map[string]bool, len(req.UserNamesToRemove))

	for _, u := range req.UserNamesToRemove {
		toRemove[u] = true
	}

	if len(toRemove) > 0 {
		filtered := a.UserNames[:0]

		for _, u := range a.UserNames {
			if !toRemove[u] {
				filtered = append(filtered, u)
			}
		}

		a.UserNames = filtered
	}

	return a, nil
}

// -- SubnetGroup operations -------------------------------------------------------

// CreateSubnetGroup creates a new subnet group.
func (b *InMemoryBackend) CreateSubnetGroup(
	region, accountID string,
	req *createSubnetGroupRequest,
) (*SubnetGroup, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.subnetGroups[req.SubnetGroupName]; exists {
		return nil, ErrSubnetGroupAlreadyExists
	}

	sgARN := arn.Build("memorydb", region, accountID, fmt.Sprintf("subnetgroup/%s", req.SubnetGroupName))

	sg := &SubnetGroup{
		Name:        req.SubnetGroupName,
		ARN:         sgARN,
		Description: req.Description,
		SubnetIDs:   req.SubnetIDs,
		Tags:        tagsFromSlice(req.Tags),
		CreatedAt:   time.Now(),
	}

	b.subnetGroups[req.SubnetGroupName] = sg
	b.arnToResource[sgARN] = resourceRef{kind: resourceKindSubnetGroup, name: req.SubnetGroupName}

	return sg, nil
}

// DescribeSubnetGroups returns subnet groups, optionally filtered by name.
func (b *InMemoryBackend) DescribeSubnetGroups(name string) ([]*SubnetGroup, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if name != "" {
		sg, ok := b.subnetGroups[name]
		if !ok {
			return nil, ErrSubnetGroupNotFound
		}

		return []*SubnetGroup{sg}, nil
	}

	result := make([]*SubnetGroup, 0, len(b.subnetGroups))

	for _, sg := range b.subnetGroups {
		result = append(result, sg)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
}

// DeleteSubnetGroup removes a subnet group.
func (b *InMemoryBackend) DeleteSubnetGroup(name string) (*SubnetGroup, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	sg, ok := b.subnetGroups[name]
	if !ok {
		return nil, ErrSubnetGroupNotFound
	}

	delete(b.subnetGroups, name)
	delete(b.arnToResource, sg.ARN)

	return sg, nil
}

// UpdateSubnetGroup modifies an existing subnet group.
func (b *InMemoryBackend) UpdateSubnetGroup(req *updateSubnetGroupRequest) (*SubnetGroup, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	sg, ok := b.subnetGroups[req.SubnetGroupName]
	if !ok {
		return nil, ErrSubnetGroupNotFound
	}

	if req.Description != "" {
		sg.Description = req.Description
	}

	if len(req.SubnetIDs) > 0 {
		sg.SubnetIDs = req.SubnetIDs
	}

	return sg, nil
}

// -- User operations -------------------------------------------------------------

// CreateUser creates a new MemoryDB user.
func (b *InMemoryBackend) CreateUser(region, accountID string, req *createUserRequest) (*User, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.users[req.UserName]; exists {
		return nil, ErrUserAlreadyExists
	}

	userARN := arn.Build("memorydb", region, accountID, fmt.Sprintf("user/%s", req.UserName))

	u := &User{
		Name:         req.UserName,
		ARN:          userARN,
		AccessString: req.AccessString,
		Status:       userStatusActive,
		AuthType:     req.AuthenticationMode.Type,
		Passwords:    req.AuthenticationMode.Passwords,
		Tags:         tagsFromSlice(req.Tags),
		CreatedAt:    time.Now(),
	}

	b.users[req.UserName] = u
	b.arnToResource[userARN] = resourceRef{kind: resourceKindUser, name: req.UserName}

	return u, nil
}

// DescribeUsers returns users, optionally filtered by name.
func (b *InMemoryBackend) DescribeUsers(name string) ([]*User, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if name != "" {
		u, ok := b.users[name]
		if !ok {
			return nil, ErrUserNotFound
		}

		return []*User{u}, nil
	}

	result := make([]*User, 0, len(b.users))

	for _, u := range b.users {
		result = append(result, u)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
}

// DeleteUser removes a user.
func (b *InMemoryBackend) DeleteUser(name string) (*User, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	u, ok := b.users[name]
	if !ok {
		return nil, ErrUserNotFound
	}

	delete(b.users, name)
	delete(b.arnToResource, u.ARN)

	return u, nil
}

// UpdateUser modifies an existing user.
func (b *InMemoryBackend) UpdateUser(req *updateUserRequest) (*User, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	u, ok := b.users[req.UserName]
	if !ok {
		return nil, ErrUserNotFound
	}

	if req.AccessString != "" {
		u.AccessString = req.AccessString
	}

	if req.AuthenticationMode != nil {
		if req.AuthenticationMode.Type != "" {
			u.AuthType = req.AuthenticationMode.Type
		}

		if len(req.AuthenticationMode.Passwords) > 0 {
			u.Passwords = req.AuthenticationMode.Passwords
		}
	}

	return u, nil
}

// -- ParameterGroup operations ---------------------------------------------------

// CreateParameterGroup creates a new parameter group.
func (b *InMemoryBackend) CreateParameterGroup(
	region, accountID string,
	req *createParameterGroupRequest,
) (*ParameterGroup, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.parameterGroups[req.ParameterGroupName]; exists {
		return nil, ErrParameterGroupAlreadyExists
	}

	pgARN := arn.Build("memorydb", region, accountID, fmt.Sprintf("parametergroup/%s", req.ParameterGroupName))

	pg := &ParameterGroup{
		Name:        req.ParameterGroupName,
		ARN:         pgARN,
		Description: req.Description,
		Family:      req.Family,
		Parameters:  make(map[string]string),
		Tags:        tagsFromSlice(req.Tags),
		CreatedAt:   time.Now(),
	}

	b.parameterGroups[req.ParameterGroupName] = pg
	b.arnToResource[pgARN] = resourceRef{kind: resourceKindParameterGroup, name: req.ParameterGroupName}

	return pg, nil
}

// DescribeParameterGroups returns parameter groups, optionally filtered by name.
func (b *InMemoryBackend) DescribeParameterGroups(name string) ([]*ParameterGroup, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if name != "" {
		pg, ok := b.parameterGroups[name]
		if !ok {
			return nil, ErrParameterGroupNotFound
		}

		return []*ParameterGroup{pg}, nil
	}

	result := make([]*ParameterGroup, 0, len(b.parameterGroups))

	for _, pg := range b.parameterGroups {
		result = append(result, pg)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
}

// DeleteParameterGroup removes a parameter group.
func (b *InMemoryBackend) DeleteParameterGroup(name string) (*ParameterGroup, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	pg, ok := b.parameterGroups[name]
	if !ok {
		return nil, ErrParameterGroupNotFound
	}

	delete(b.parameterGroups, name)
	delete(b.arnToResource, pg.ARN)

	return pg, nil
}

// UpdateParameterGroup modifies parameter values in a parameter group.
func (b *InMemoryBackend) UpdateParameterGroup(req *updateParameterGroupRequest) (*ParameterGroup, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	pg, ok := b.parameterGroups[req.ParameterGroupName]
	if !ok {
		return nil, ErrParameterGroupNotFound
	}

	for _, pnv := range req.ParameterNameValues {
		pg.Parameters[pnv.ParameterName] = pnv.ParameterValue
	}

	return pg, nil
}

// -- Tag operations --------------------------------------------------------------

// ListTags returns the tags for a resource identified by ARN.
func (b *InMemoryBackend) ListTags(resourceArn string) (map[string]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	ref, ok := b.arnToResource[resourceArn]
	if !ok {
		return nil, awserr.New("ResourceNotFoundFault: resource not found", awserr.ErrNotFound)
	}

	tags := b.tagsForRef(ref)

	return tags, nil
}

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceArn string, tags map[string]string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	ref, ok := b.arnToResource[resourceArn]
	if !ok {
		return awserr.New("ResourceNotFoundFault: resource not found", awserr.ErrNotFound)
	}

	b.applyTags(ref, tags)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceArn string, tagKeys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	ref, ok := b.arnToResource[resourceArn]
	if !ok {
		return awserr.New("ResourceNotFoundFault: resource not found", awserr.ErrNotFound)
	}

	b.removeTags(ref, tagKeys)

	return nil
}

// tagsForRef returns a copy of the tags for the referenced resource (must hold at least RLock).
func (b *InMemoryBackend) tagsForRef(ref resourceRef) map[string]string {
	var src map[string]string

	switch ref.kind {
	case resourceKindCluster:
		if c, ok := b.clusters[ref.name]; ok {
			src = c.Tags
		}
	case resourceKindACL:
		if a, ok := b.acls[ref.name]; ok {
			src = a.Tags
		}
	case resourceKindSubnetGroup:
		if sg, ok := b.subnetGroups[ref.name]; ok {
			src = sg.Tags
		}
	case resourceKindUser:
		if u, ok := b.users[ref.name]; ok {
			src = u.Tags
		}
	case resourceKindParameterGroup:
		if pg, ok := b.parameterGroups[ref.name]; ok {
			src = pg.Tags
		}
	}

	return maps.Clone(src)
}

// applyTags merges tags into the referenced resource (must hold Lock).
// mergeTags ensures dst is initialized then copies all src entries into it.
func mergeTags(dst *map[string]string, src map[string]string) {
	if *dst == nil {
		*dst = make(map[string]string, len(src))
	}

	maps.Copy(*dst, src)
}

func (b *InMemoryBackend) applyTags(ref resourceRef, tags map[string]string) {
	switch ref.kind {
	case resourceKindCluster:
		if c, ok := b.clusters[ref.name]; ok {
			mergeTags(&c.Tags, tags)
		}
	case resourceKindACL:
		if a, ok := b.acls[ref.name]; ok {
			mergeTags(&a.Tags, tags)
		}
	case resourceKindSubnetGroup:
		if sg, ok := b.subnetGroups[ref.name]; ok {
			mergeTags(&sg.Tags, tags)
		}
	case resourceKindUser:
		if u, ok := b.users[ref.name]; ok {
			mergeTags(&u.Tags, tags)
		}
	case resourceKindParameterGroup:
		if pg, ok := b.parameterGroups[ref.name]; ok {
			mergeTags(&pg.Tags, tags)
		}
	}
}

// removeTags deletes the given tag keys from the referenced resource (must hold Lock).
func (b *InMemoryBackend) removeTags(ref resourceRef, tagKeys []string) {
	keysSet := make(map[string]bool, len(tagKeys))

	for _, k := range tagKeys {
		keysSet[k] = true
	}

	doDelete := func(m map[string]string) {
		for _, k := range tagKeys {
			if keysSet[k] {
				delete(m, k)
			}
		}
	}

	switch ref.kind {
	case resourceKindCluster:
		if c, ok := b.clusters[ref.name]; ok {
			doDelete(c.Tags)
		}
	case resourceKindACL:
		if a, ok := b.acls[ref.name]; ok {
			doDelete(a.Tags)
		}
	case resourceKindSubnetGroup:
		if sg, ok := b.subnetGroups[ref.name]; ok {
			doDelete(sg.Tags)
		}
	case resourceKindUser:
		if u, ok := b.users[ref.name]; ok {
			doDelete(u.Tags)
		}
	case resourceKindParameterGroup:
		if pg, ok := b.parameterGroups[ref.name]; ok {
			doDelete(pg.Tags)
		}
	}
}

// -- helpers ---------------------------------------------------------------------

// tagsFromSlice converts []tagEntry to map[string]string.
func tagsFromSlice(tags []tagEntry) map[string]string {
	result := make(map[string]string, len(tags))

	for _, t := range tags {
		result[t.Key] = t.Value
	}

	return result
}

// tagsToSlice converts map[string]string to []tagEntry sorted by key.
func tagsToSlice(tags map[string]string) []tagEntry {
	result := make([]tagEntry, 0, len(tags))

	for k, v := range tags {
		result = append(result, tagEntry{Key: k, Value: v})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Key < result[j].Key
	})

	return result
}

// ListClusters returns all clusters for use by the dashboard.
func (b *InMemoryBackend) ListClusters() []*Cluster {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]*Cluster, 0, len(b.clusters))

	for _, c := range b.clusters {
		result = append(result, c)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result
}
