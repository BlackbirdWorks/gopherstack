package dax

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// Sentinel errors for the DAX backend.
var (
	// ErrClusterNotFound is returned when a cluster does not exist.
	ErrClusterNotFound = awserr.New("ClusterNotFoundFault", awserr.ErrNotFound)
	// ErrClusterAlreadyExists is returned when a cluster with the same name already exists.
	ErrClusterAlreadyExists = awserr.New("ClusterAlreadyExistsFault", awserr.ErrConflict)
	// ErrParameterGroupNotFound is returned when a parameter group does not exist.
	ErrParameterGroupNotFound = awserr.New("ParameterGroupNotFoundFault", awserr.ErrNotFound)
	// ErrParameterGroupAlreadyExists is returned when a parameter group already exists.
	ErrParameterGroupAlreadyExists = awserr.New(
		"ParameterGroupAlreadyExistsFault",
		awserr.ErrConflict,
	)
	// ErrSubnetGroupNotFound is returned when a subnet group does not exist.
	ErrSubnetGroupNotFound = awserr.New("SubnetGroupNotFoundFault", awserr.ErrNotFound)
	// ErrSubnetGroupAlreadyExists is returned when a subnet group already exists.
	ErrSubnetGroupAlreadyExists = awserr.New("SubnetGroupAlreadyExistsFault", awserr.ErrConflict)
	// ErrInvalidClusterState is returned when an operation is not valid for the cluster state.
	ErrInvalidClusterState = awserr.New("InvalidClusterStateFault", awserr.ErrConflict)
	// ErrTagNotFound is returned when a tag or resource is not found.
	ErrTagNotFound = awserr.New("TagNotFoundFault", awserr.ErrNotFound)
	// ErrInvalidARN is returned for invalid ARNs.
	ErrInvalidARN = awserr.New("InvalidARNFault", awserr.ErrInvalidParameter)
)

const (
	// daxPort is the standard DAX cluster port.
	daxPort = 8111

	// daxClusterURLPort is the URL port for DAX.
	daxClusterURLPort = 8111

	// maxClustersDefault is the default maximum number of clusters per describe call.
	maxClustersDefault = 100

	// paramApplyStatusInSync is the value reported for parameter group status when in sync.
	paramApplyStatusInSync = "in-sync"

	// defaultMaintenanceWindow is the default preferred maintenance window.
	defaultMaintenanceWindow = "sun:05:00-sun:09:00"

	// sseStatusDisabled is the SSE status when not enabled.
	sseStatusDisabled = "DISABLED"

	// sseStatusEnabled is the SSE status when enabled.
	sseStatusEnabled = "ENABLED"
)

// InMemoryBackend is the in-memory DAX backend.
type InMemoryBackend struct {
	clusters     map[string]*Cluster
	paramGroups  map[string]*ParameterGroup
	subnetGroups map[string]*SubnetGroup
	tags         map[string]map[string]string // resourceArn -> tags
	mu           *lockmetrics.RWMutex
	AccountID    string
	Region       string
}

// NewInMemoryBackend creates a new DAX backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		clusters:     make(map[string]*Cluster),
		paramGroups:  make(map[string]*ParameterGroup),
		subnetGroups: make(map[string]*SubnetGroup),
		tags:         make(map[string]map[string]string),
		mu:           lockmetrics.New("dax"),
		AccountID:    accountID,
		Region:       region,
	}

	// Pre-populate the default parameter group.
	b.paramGroups[DefaultParameterGroupName] = &ParameterGroup{
		ParameterGroupName: DefaultParameterGroupName,
		Description:        "Default parameter group for DAX 1.0",
		Parameters:         make(map[string]string),
	}

	// Pre-populate the default subnet group.
	b.subnetGroups[DefaultSubnetGroupName] = &SubnetGroup{
		SubnetGroupName: DefaultSubnetGroupName,
		Description:     "Default subnet group",
		VpcID:           "vpc-default",
		SubnetIDs:       []string{"subnet-default"},
	}

	return b
}

// clusterARN builds a DAX cluster ARN.
func (b *InMemoryBackend) clusterARN(name string) string {
	return fmt.Sprintf("arn:aws:dax:%s:%s:cache/%s", b.Region, b.AccountID, name)
}

// daxURL builds a dax:// URL from a host address and port number.
func daxURL(addr string, port int) string {
	return "dax://" + net.JoinHostPort(addr, strconv.Itoa(port))
}

// clusterEndpointAddress generates a realistic DAX endpoint address.
func clusterEndpointAddress(name, region string) string {
	// Generate a pseudo-random hex suffix for the DNS name.
	const maxSuffix uint32 = 0xFFFFFF
	suffix := fmt.Sprintf("%06x", rand.Uint32N(maxSuffix)) //nolint:gosec // not security sensitive

	return fmt.Sprintf("%s.%s.dax-clusters.%s.amazonaws.com", name, suffix, region)
}

// nodeEndpointAddress generates a node-level endpoint address.
func nodeEndpointAddress(clusterName, nodeID, region string) string {
	return fmt.Sprintf("%s-%s.nodes.dax-clusters.%s.amazonaws.com", clusterName, nodeID, region)
}

// CreateCluster creates a new DAX cluster.
// validateCreateCluster validates the CreateCluster input before acquiring the lock.
func validateCreateCluster(input *CreateClusterInput) error {
	if input.ClusterName == "" {
		return fmt.Errorf("%w: ClusterName is required", ErrInvalidARN)
	}

	if input.NodeType == "" {
		return fmt.Errorf("%w: NodeType is required", ErrInvalidARN)
	}

	if input.IamRoleArn == "" {
		return fmt.Errorf("%w: IamRoleArn is required", ErrInvalidARN)
	}

	return nil
}

// applyCreateClusterDefaults fills in default values for optional fields.
func applyCreateClusterDefaults(input *CreateClusterInput) {
	if input.ReplicationFactor < 1 {
		input.ReplicationFactor = 1
	}

	if input.SubnetGroupName == "" {
		input.SubnetGroupName = DefaultSubnetGroupName
	}

	if input.ParameterGroupName == "" {
		input.ParameterGroupName = DefaultParameterGroupName
	}
}

// buildClusterNodes builds the node list for a new cluster.
func (b *InMemoryBackend) buildClusterNodes(input CreateClusterInput, now time.Time) []Node {
	nodes := make([]Node, 0, input.ReplicationFactor)

	for i := range input.ReplicationFactor {
		nodeID := fmt.Sprintf("%s-%04d", input.ClusterName, i)
		az := b.Region + "a"

		if i < len(input.AvailabilityZones) {
			az = input.AvailabilityZones[i]
		}

		addr := nodeEndpointAddress(input.ClusterName, fmt.Sprintf("%04d", i), b.Region)
		nodes = append(nodes, Node{
			NodeID:               nodeID,
			NodeStatus:           StatusAvailable,
			AvailabilityZone:     az,
			CreateTime:           now,
			ParameterGroupStatus: paramApplyStatusInSync,
			Endpoint: &Endpoint{
				Address: addr,
				Port:    daxPort,
				URL:     daxURL(addr, daxClusterURLPort),
			},
		})
	}

	return nodes
}

// CreateCluster creates a new DAX cluster.
func (b *InMemoryBackend) CreateCluster(input CreateClusterInput) (*Cluster, error) {
	if err := validateCreateCluster(&input); err != nil {
		return nil, err
	}

	applyCreateClusterDefaults(&input)

	b.mu.Lock("CreateCluster")
	defer b.mu.Unlock()

	if _, exists := b.clusters[input.ClusterName]; exists {
		return nil, fmt.Errorf("%w: %s", ErrClusterAlreadyExists, input.ClusterName)
	}

	if _, exists := b.subnetGroups[input.SubnetGroupName]; !exists {
		return nil, fmt.Errorf("%w: %s", ErrSubnetGroupNotFound, input.SubnetGroupName)
	}

	if _, exists := b.paramGroups[input.ParameterGroupName]; !exists {
		return nil, fmt.Errorf("%w: %s", ErrParameterGroupNotFound, input.ParameterGroupName)
	}

	now := time.Now().UTC()
	clusterARN := b.clusterARN(input.ClusterName)
	nodes := b.buildClusterNodes(input, now)

	sseStatus := sseStatusDisabled
	if input.SSESpecificationEnabled {
		sseStatus = sseStatusEnabled
	}

	maintenanceWindow := input.PreferredMaintenanceWindow
	if maintenanceWindow == "" {
		maintenanceWindow = defaultMaintenanceWindow
	}

	clusterEndpoint := clusterEndpointAddress(input.ClusterName, b.Region)

	cluster := &Cluster{
		ClusterName:                input.ClusterName,
		ClusterArn:                 clusterARN,
		Description:                input.Description,
		NodeType:                   input.NodeType,
		Status:                     StatusAvailable,
		IamRoleArn:                 input.IamRoleArn,
		SubnetGroupName:            input.SubnetGroupName,
		SecurityGroupIDs:           input.SecurityGroupIDs,
		PreferredMaintenanceWindow: maintenanceWindow,
		CreateTime:                 now,
		TotalNodes:                 input.ReplicationFactor,
		ActiveNodes:                input.ReplicationFactor,
		Nodes:                      nodes,
		Endpoint: &Endpoint{
			Address: clusterEndpoint,
			Port:    daxPort,
			URL:     daxURL(clusterEndpoint, daxClusterURLPort),
		},
		ParameterGroup: ParameterGroupStatus{
			ParameterGroupName:   input.ParameterGroupName,
			ParameterApplyStatus: paramApplyStatusInSync,
		},
		SSEDescription: SSEDescription{
			Status: sseStatus,
		},
		Tags: make(map[string]string),
	}

	// Copy tags.
	maps.Copy(cluster.Tags, input.Tags)

	b.clusters[input.ClusterName] = cluster

	// Store tags in the tag index too.
	if len(input.Tags) > 0 {
		b.tags[clusterARN] = make(map[string]string)
		maps.Copy(b.tags[clusterARN], input.Tags)
	}

	cp := b.clusterCopy(cluster)

	return cp, nil
}

// collectClustersLocked collects clusters, filtering by name if provided.
// Must be called with b.mu held for reading.
func (b *InMemoryBackend) collectClustersLocked(clusterNames []string) ([]*Cluster, error) {
	if len(clusterNames) == 0 {
		all := make([]*Cluster, 0, len(b.clusters))
		for _, c := range b.clusters {
			all = append(all, c)
		}

		return all, nil
	}

	// Check for missing clusters before collecting.
	for _, name := range clusterNames {
		if _, ok := b.clusters[name]; !ok {
			return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, name)
		}
	}

	all := make([]*Cluster, 0, len(clusterNames))
	for _, name := range clusterNames {
		if c, ok := b.clusters[name]; ok {
			all = append(all, c)
		}
	}

	return all, nil
}

// paginateClusters applies pagination to a sorted cluster slice and returns copies.
func (b *InMemoryBackend) paginateClusters(
	all []*Cluster,
	maxResults int,
	nextToken string,
) ([]*Cluster, string) {
	start := 0

	if nextToken != "" {
		for i, c := range all {
			if c.ClusterName == nextToken {
				start = i

				break
			}
		}
	}

	if start >= len(all) {
		return []*Cluster{}, ""
	}

	end := start + maxResults
	newNextToken := ""

	if end < len(all) {
		newNextToken = all[end].ClusterName
	} else {
		end = len(all)
	}

	page := all[start:end]
	result := make([]*Cluster, 0, len(page))

	for _, c := range page {
		result = append(result, b.clusterCopy(c))
	}

	return result, newNextToken
}

// DescribeClusters returns DAX clusters, optionally filtered by name.
func (b *InMemoryBackend) DescribeClusters(
	clusterNames []string,
	maxResults int,
	nextToken string,
) ([]*Cluster, string, error) {
	b.mu.RLock("DescribeClusters")
	defer b.mu.RUnlock()

	if maxResults <= 0 {
		maxResults = maxClustersDefault
	}

	all, err := b.collectClustersLocked(clusterNames)
	if err != nil {
		return nil, "", err
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].ClusterName < all[j].ClusterName
	})

	result, token := b.paginateClusters(all, maxResults, nextToken)

	return result, token, nil
}

// UpdateCluster updates a DAX cluster's configuration.
func (b *InMemoryBackend) UpdateCluster(input UpdateClusterInput) (*Cluster, error) {
	if input.ClusterName == "" {
		return nil, fmt.Errorf("%w: ClusterName is required", ErrInvalidARN)
	}

	b.mu.Lock("UpdateCluster")
	defer b.mu.Unlock()

	cluster, ok := b.clusters[input.ClusterName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, input.ClusterName)
	}

	if cluster.Status == StatusDeleting {
		return nil, fmt.Errorf(
			"%w: cluster %s is being deleted",
			ErrInvalidClusterState,
			input.ClusterName,
		)
	}

	if input.Description != "" {
		cluster.Description = input.Description
	}

	if input.PreferredMaintenanceWindow != "" {
		cluster.PreferredMaintenanceWindow = input.PreferredMaintenanceWindow
	}

	if len(input.SecurityGroupIDs) > 0 {
		cluster.SecurityGroupIDs = append([]string(nil), input.SecurityGroupIDs...)
	}

	if input.ParameterGroupName != "" {
		if _, exists := b.paramGroups[input.ParameterGroupName]; !exists {
			return nil, fmt.Errorf("%w: %s", ErrParameterGroupNotFound, input.ParameterGroupName)
		}

		cluster.ParameterGroup.ParameterGroupName = input.ParameterGroupName
	}

	cp := b.clusterCopy(cluster)

	return cp, nil
}

// DeleteCluster deletes a DAX cluster.
func (b *InMemoryBackend) DeleteCluster(clusterName string) (*Cluster, error) {
	if clusterName == "" {
		return nil, fmt.Errorf("%w: ClusterName is required", ErrInvalidARN)
	}

	b.mu.Lock("DeleteCluster")
	defer b.mu.Unlock()

	cluster, ok := b.clusters[clusterName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, clusterName)
	}

	cp := b.clusterCopy(cluster)
	cp.Status = StatusDeleting

	delete(b.clusters, clusterName)
	delete(b.tags, cluster.ClusterArn)

	return cp, nil
}

// TagResource adds tags to a DAX resource.
func (b *InMemoryBackend) TagResource(resourceArn string, tags map[string]string) error {
	if resourceArn == "" {
		return fmt.Errorf("%w: ResourceName is required", ErrInvalidARN)
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if !b.arnExists(resourceArn) {
		return fmt.Errorf("%w: %s", ErrTagNotFound, resourceArn)
	}

	if b.tags[resourceArn] == nil {
		b.tags[resourceArn] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceArn], tags)

	// Update cluster tags if applicable.
	for _, cluster := range b.clusters {
		if cluster.ClusterArn == resourceArn {
			maps.Copy(cluster.Tags, tags)

			break
		}
	}

	return nil
}

// UntagResource removes tags from a DAX resource.
func (b *InMemoryBackend) UntagResource(resourceArn string, tagKeys []string) error {
	if resourceArn == "" {
		return fmt.Errorf("%w: ResourceName is required", ErrInvalidARN)
	}

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if !b.arnExists(resourceArn) {
		return fmt.Errorf("%w: %s", ErrTagNotFound, resourceArn)
	}

	if b.tags[resourceArn] != nil {
		for _, k := range tagKeys {
			delete(b.tags[resourceArn], k)
		}
	}

	// Update cluster tags if applicable.
	for _, cluster := range b.clusters {
		if cluster.ClusterArn == resourceArn {
			for _, k := range tagKeys {
				delete(cluster.Tags, k)
			}

			break
		}
	}

	return nil
}

// ListTags returns tags for a DAX resource with optional pagination.
func (b *InMemoryBackend) ListTags(
	resourceArn string,
	_ string,
) (map[string]string, string, error) {
	if resourceArn == "" {
		return nil, "", fmt.Errorf("%w: ResourceName is required", ErrInvalidARN)
	}

	b.mu.RLock("ListTags")
	defer b.mu.RUnlock()

	if !b.arnExists(resourceArn) {
		return nil, "", fmt.Errorf("%w: %s", ErrTagNotFound, resourceArn)
	}

	tags := make(map[string]string)

	if t, ok := b.tags[resourceArn]; ok {
		maps.Copy(tags, t)
	}

	return tags, "", nil
}

// CreateParameterGroup creates a DAX parameter group.
func (b *InMemoryBackend) CreateParameterGroup(name, description string) (*ParameterGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: ParameterGroupName is required", ErrParameterGroupNotFound)
	}

	b.mu.Lock("CreateParameterGroup")
	defer b.mu.Unlock()

	if _, exists := b.paramGroups[name]; exists {
		return nil, fmt.Errorf("%w: %s", ErrParameterGroupAlreadyExists, name)
	}

	pg := &ParameterGroup{
		ParameterGroupName: name,
		Description:        description,
		Parameters:         make(map[string]string),
	}

	b.paramGroups[name] = pg

	cp := *pg

	return &cp, nil
}

// DescribeParameterGroups returns DAX parameter groups.
func (b *InMemoryBackend) DescribeParameterGroups(
	names []string,
	_ int,
	_ string,
) ([]*ParameterGroup, string, error) {
	b.mu.RLock("DescribeParameterGroups")
	defer b.mu.RUnlock()

	var all []*ParameterGroup

	if len(names) > 0 {
		for _, name := range names {
			pg, ok := b.paramGroups[name]
			if !ok {
				return nil, "", fmt.Errorf("%w: %s", ErrParameterGroupNotFound, name)
			}

			cp := *pg
			all = append(all, &cp)
		}
	} else {
		for _, pg := range b.paramGroups {
			cp := *pg
			all = append(all, &cp)
		}

		sort.Slice(all, func(i, j int) bool {
			return all[i].ParameterGroupName < all[j].ParameterGroupName
		})
	}

	return all, "", nil
}

// DeleteParameterGroup deletes a DAX parameter group.
func (b *InMemoryBackend) DeleteParameterGroup(name string) error {
	if name == "" {
		return fmt.Errorf("%w: ParameterGroupName is required", ErrParameterGroupNotFound)
	}

	b.mu.Lock("DeleteParameterGroup")
	defer b.mu.Unlock()

	if _, ok := b.paramGroups[name]; !ok {
		return fmt.Errorf("%w: %s", ErrParameterGroupNotFound, name)
	}

	// Check if any cluster uses this parameter group.
	for _, cluster := range b.clusters {
		if cluster.ParameterGroup.ParameterGroupName == name {
			return fmt.Errorf("%w: parameter group %s is in use by cluster %s",
				ErrInvalidClusterState, name, cluster.ClusterName)
		}
	}

	delete(b.paramGroups, name)

	return nil
}

// CreateSubnetGroup creates a DAX subnet group.
func (b *InMemoryBackend) CreateSubnetGroup(
	name, description string,
	subnetIDs []string,
) (*SubnetGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: SubnetGroupName is required", ErrSubnetGroupNotFound)
	}

	b.mu.Lock("CreateSubnetGroup")
	defer b.mu.Unlock()

	if _, exists := b.subnetGroups[name]; exists {
		return nil, fmt.Errorf("%w: %s", ErrSubnetGroupAlreadyExists, name)
	}

	sg := &SubnetGroup{
		SubnetGroupName: name,
		Description:     description,
		SubnetIDs:       append([]string(nil), subnetIDs...),
	}

	b.subnetGroups[name] = sg

	cp := *sg
	cp.SubnetIDs = append([]string(nil), sg.SubnetIDs...)

	return &cp, nil
}

// DescribeSubnetGroups returns DAX subnet groups.
func (b *InMemoryBackend) DescribeSubnetGroups(
	names []string,
	_ int,
	_ string,
) ([]*SubnetGroup, string, error) {
	b.mu.RLock("DescribeSubnetGroups")
	defer b.mu.RUnlock()

	var all []*SubnetGroup

	if len(names) > 0 {
		for _, name := range names {
			sg, ok := b.subnetGroups[name]
			if !ok {
				return nil, "", fmt.Errorf("%w: %s", ErrSubnetGroupNotFound, name)
			}

			cp := *sg
			cp.SubnetIDs = append([]string(nil), sg.SubnetIDs...)
			all = append(all, &cp)
		}
	} else {
		for _, sg := range b.subnetGroups {
			cp := *sg
			cp.SubnetIDs = append([]string(nil), sg.SubnetIDs...)
			all = append(all, &cp)
		}

		sort.Slice(all, func(i, j int) bool {
			return all[i].SubnetGroupName < all[j].SubnetGroupName
		})
	}

	return all, "", nil
}

// DeleteSubnetGroup deletes a DAX subnet group.
func (b *InMemoryBackend) DeleteSubnetGroup(name string) error {
	if name == "" {
		return fmt.Errorf("%w: SubnetGroupName is required", ErrSubnetGroupNotFound)
	}

	b.mu.Lock("DeleteSubnetGroup")
	defer b.mu.Unlock()

	if _, ok := b.subnetGroups[name]; !ok {
		return fmt.Errorf("%w: %s", ErrSubnetGroupNotFound, name)
	}

	// Check if any cluster uses this subnet group.
	for _, cluster := range b.clusters {
		if cluster.SubnetGroupName == name {
			return fmt.Errorf("%w: subnet group %s is in use by cluster %s",
				ErrInvalidClusterState, name, cluster.ClusterName)
		}
	}

	delete(b.subnetGroups, name)

	return nil
}

// Reset clears all DAX state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.clusters = make(map[string]*Cluster)
	b.tags = make(map[string]map[string]string)
	b.paramGroups = make(map[string]*ParameterGroup)
	b.subnetGroups = make(map[string]*SubnetGroup)

	// Re-populate defaults.
	b.paramGroups[DefaultParameterGroupName] = &ParameterGroup{
		ParameterGroupName: DefaultParameterGroupName,
		Description:        "Default parameter group for DAX 1.0",
		Parameters:         make(map[string]string),
	}

	b.subnetGroups[DefaultSubnetGroupName] = &SubnetGroup{
		SubnetGroupName: DefaultSubnetGroupName,
		Description:     "Default subnet group",
		VpcID:           "vpc-default",
		SubnetIDs:       []string{"subnet-default"},
	}
}

// arnExists returns true if the ARN corresponds to an existing DAX resource.
func (b *InMemoryBackend) arnExists(arn string) bool {
	for _, cluster := range b.clusters {
		if cluster.ClusterArn == arn {
			return true
		}
	}

	// Also check if it's a direct cluster name lookup (for convenience).
	prefix := fmt.Sprintf("arn:aws:dax:%s:%s:cache/", b.Region, b.AccountID)
	if name, ok := strings.CutPrefix(arn, prefix); ok {
		if _, exists := b.clusters[name]; exists {
			return true
		}
	}

	return false
}

// clusterCopy returns a deep copy of a Cluster.
func (b *InMemoryBackend) clusterCopy(c *Cluster) *Cluster {
	cp := *c
	cp.Tags = make(map[string]string)

	maps.Copy(cp.Tags, c.Tags)

	cp.SecurityGroupIDs = append([]string(nil), c.SecurityGroupIDs...)

	// Copy nodes.
	cp.Nodes = make([]Node, len(c.Nodes))
	for i, n := range c.Nodes {
		nodeCp := n

		if n.Endpoint != nil {
			endpointCp := *n.Endpoint
			nodeCp.Endpoint = &endpointCp
		}

		cp.Nodes[i] = nodeCp
	}

	if c.Endpoint != nil {
		endpointCp := *c.Endpoint
		cp.Endpoint = &endpointCp
	}

	return &cp
}
