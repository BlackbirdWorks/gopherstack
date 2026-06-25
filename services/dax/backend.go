package dax

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
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
	// ErrInvalidParameterValue is returned when a parameter value is invalid.
	ErrInvalidParameterValue = awserr.New("InvalidParameterValueException", awserr.ErrInvalidParameter)
	// ErrInvalidParameterCombination is returned for invalid parameter combinations.
	ErrInvalidParameterCombination = awserr.New("InvalidParameterCombinationException", awserr.ErrInvalidParameter)
	// ErrNodeNotFound is returned when a node does not exist.
	ErrNodeNotFound = awserr.New("NodeNotFoundFault", awserr.ErrNotFound)
	// ErrTagQuotaExceeded is returned when adding tags would exceed the per-resource limit.
	ErrTagQuotaExceeded = awserr.New("TagQuotaPerResourceExceeded", awserr.ErrInvalidParameter)
	// ErrSubnetGroupInUse is returned when attempting to delete a subnet group used by a cluster.
	ErrSubnetGroupInUse = awserr.New("SubnetGroupInUseFault", awserr.ErrConflict)
	// ErrParameterGroupInUse is returned when attempting to delete a parameter group used by a cluster.
	ErrParameterGroupInUse = awserr.New("ParameterGroupInUseFault", awserr.ErrConflict)
)

const (
	// daxPort is the standard DAX cluster port.
	daxPort = 8111

	// daxClusterURLPort is the URL port for DAX.
	daxClusterURLPort = 8111

	// maxClustersDefault is the default maximum number of clusters per describe call.
	maxClustersDefault = 100

	// maxPageSizeDefault is the default page size for paginated describe calls.
	maxPageSizeDefault = 100

	// paramApplyStatusInSync is the value reported for parameter group status when in sync.
	paramApplyStatusInSync = "in-sync"

	// sseStatusDisabled is the SSE status when not enabled.
	sseStatusDisabled = "DISABLED"

	// sseStatusEnabled is the SSE status when enabled.
	sseStatusEnabled = "ENABLED"

	// notificationTopicStatusActive is the active SNS topic status.
	notificationTopicStatusActive = "active"

	// maintenanceWindowMinutes is the fixed width of the simulated maintenance window.
	maintenanceWindowMinutes = 60

	// hoursPerDay is the number of hours in a day.
	hoursPerDay = 24

	// minutesPerHour is the number of minutes in an hour.
	minutesPerHour = 60

	// maxClusterNameLength is the maximum allowed length for a DAX cluster name.
	maxClusterNameLength = 20

	// maxResourceNameLength is the maximum allowed length for parameter/subnet group names.
	maxResourceNameLength = 255

	// listTagsPageSize is the number of tags returned per ListTags page.
	listTagsPageSize = 10
)

// nameRegexp validates DAX resource names: must start with a letter, contain only
// letters/digits/hyphens, and not end with a hyphen. Used for clusters, parameter groups,
// and subnet groups.
var nameRegexp = regexp.MustCompile(`^[a-zA-Z]([a-zA-Z0-9-]*[a-zA-Z0-9])?$`)

// vpcSuffixMaxLen is the maximum length of the VPC ID suffix derived from a subnet ID.
const vpcSuffixMaxLen = 8

// maintenanceWindowDays maps random seeds to day abbreviations for the maintenance window.
//
//nolint:gochecknoglobals // package-level lookup table
var maintenanceWindowDays = []string{
	"sun",
	"mon",
	"tue",
	"wed",
	"thu",
	"fri",
	"sat",
}

// InMemoryBackend is the in-memory DAX backend.
type InMemoryBackend struct {
	clusters     map[string]*Cluster
	paramGroups  map[string]*ParameterGroup
	subnetGroups map[string]*SubnetGroup
	tags         map[string]map[string]string
	mu           *lockmetrics.RWMutex
	AccountID    string
	Region       string
	events       []*Event
}

// NewInMemoryBackend creates a new DAX backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		clusters:     make(map[string]*Cluster),
		paramGroups:  make(map[string]*ParameterGroup),
		subnetGroups: make(map[string]*SubnetGroup),
		tags:         make(map[string]map[string]string),
		events:       make([]*Event, 0),
		mu:           lockmetrics.New("dax"),
		AccountID:    accountID,
		Region:       region,
	}

	b.seedDefaults()

	return b
}

// seedDefaults populates factory-default parameter and subnet groups.
func (b *InMemoryBackend) seedDefaults() {
	params := make(map[string]string, len(defaultParameterValues))
	maps.Copy(params, defaultParameterValues)

	b.paramGroups[DefaultParameterGroupName] = &ParameterGroup{
		ParameterGroupName: DefaultParameterGroupName,
		Description:        "Default parameter group for DAX 1.0",
		Parameters:         params,
	}

	b.subnetGroups[DefaultSubnetGroupName] = &SubnetGroup{
		SubnetGroupName: DefaultSubnetGroupName,
		Description:     "Default subnet group",
		VpcID:           "vpc-default",
		Subnets:         []SubnetEntry{{SubnetID: "subnet-default", AvailabilityZone: b.Region + "a"}},
	}
}

// clusterARN builds a DAX cluster ARN.
func (b *InMemoryBackend) clusterARN(name string) string {
	return arn.Build("dax", b.Region, b.AccountID, fmt.Sprintf("cache/%s", name))
}

// daxURL builds a dax:// URL from a host address and port number.
func daxURL(addr string, port int) string {
	return "dax://" + net.JoinHostPort(addr, strconv.Itoa(port))
}

// clusterEndpointAddress generates a realistic DAX endpoint address.
func clusterEndpointAddress(name, region string) string {
	const maxSuffix uint32 = 0xFFFFFF
	suffix := fmt.Sprintf("%06x", rand.Uint32N(maxSuffix)) //nolint:gosec // not security sensitive

	return fmt.Sprintf("%s.%s.dax-clusters.%s.amazonaws.com", name, suffix, region)
}

// nodeEndpointAddress generates a node-level endpoint address.
func nodeEndpointAddress(clusterName, nodeID, region string) string {
	return fmt.Sprintf("%s-%s.nodes.dax-clusters.%s.amazonaws.com", clusterName, nodeID, region)
}

// randomMaintenanceWindow returns a random 60-minute maintenance window slot.
func randomMaintenanceWindow() string {
	//nolint:gosec // not security sensitive
	day := maintenanceWindowDays[rand.Uint32N(uint32(len(maintenanceWindowDays)))]
	hour := rand.Uint32N(hoursPerDay)      //nolint:gosec // not security sensitive
	minute := rand.Uint32N(minutesPerHour) //nolint:gosec // not security sensitive

	totalMinutes := hour*minutesPerHour + minute + uint32(maintenanceWindowMinutes)
	endHour := totalMinutes / minutesPerHour % hoursPerDay
	endMinute := totalMinutes % minutesPerHour

	return fmt.Sprintf("%s:%02d:%02d-%s:%02d:%02d", day, hour, minute, day, endHour, endMinute)
}

// validateClusterName validates the DAX cluster name format per AWS constraints.
func validateClusterName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: ClusterName is required", ErrInvalidParameterValue)
	}

	if len(name) > maxClusterNameLength {
		return fmt.Errorf(
			"%w: ClusterName %q exceeds maximum length of %d characters",
			ErrInvalidParameterValue, name, maxClusterNameLength,
		)
	}

	if !nameRegexp.MatchString(name) {
		return fmt.Errorf(
			"%w: ClusterName %q is invalid: must start with a letter, "+
				"contain only letters, numbers, and hyphens, and not end with a hyphen",
			ErrInvalidParameterValue, name,
		)
	}

	if strings.Contains(name, "--") {
		return fmt.Errorf(
			"%w: ClusterName %q is invalid: must not contain consecutive hyphens",
			ErrInvalidParameterValue, name,
		)
	}

	return nil
}

// validateResourceName validates a parameter group or subnet group name.
func validateResourceName(name, kind string) error {
	if name == "" {
		return fmt.Errorf("%w: %s is required", ErrInvalidParameterValue, kind)
	}

	if len(name) > maxResourceNameLength {
		return fmt.Errorf(
			"%w: %s %q exceeds maximum length of %d characters",
			ErrInvalidParameterValue, kind, name, maxResourceNameLength,
		)
	}

	if !nameRegexp.MatchString(name) {
		return fmt.Errorf(
			"%w: %s %q is invalid: must start with a letter, "+
				"contain only letters, numbers, and hyphens, and not end with a hyphen",
			ErrInvalidParameterValue, kind, name,
		)
	}

	if strings.Contains(name, "--") {
		return fmt.Errorf(
			"%w: %s %q is invalid: must not contain consecutive hyphens",
			ErrInvalidParameterValue, kind, name,
		)
	}

	return nil
}

// validateCreateCluster validates the CreateCluster input before acquiring the lock.
func validateCreateCluster(input *CreateClusterInput) error {
	if err := validateClusterName(input.ClusterName); err != nil {
		return err
	}

	if input.NodeType == "" {
		return fmt.Errorf("%w: NodeType is required", ErrInvalidParameterValue)
	}

	if !validNodeTypes[input.NodeType] {
		return fmt.Errorf("%w: unsupported node type %q", ErrInvalidParameterValue, input.NodeType)
	}

	if input.IamRoleArn == "" {
		return fmt.Errorf("%w: IamRoleArn is required", ErrInvalidARN)
	}

	if input.ReplicationFactor < minReplicationFactor {
		return fmt.Errorf(
			"%w: ReplicationFactor %d is below minimum of %d",
			ErrInvalidParameterCombination,
			input.ReplicationFactor,
			minReplicationFactor,
		)
	}

	if input.ReplicationFactor > maxReplicationFactor {
		return fmt.Errorf(
			"%w: ReplicationFactor %d exceeds maximum of %d",
			ErrInvalidParameterCombination,
			input.ReplicationFactor,
			maxReplicationFactor,
		)
	}

	if input.ClusterEndpointEncryptionType != "" &&
		input.ClusterEndpointEncryptionType != EncryptionTypeNone &&
		input.ClusterEndpointEncryptionType != EncryptionTypeTLS {
		return fmt.Errorf(
			"%w: ClusterEndpointEncryptionType must be %q or %q",
			ErrInvalidParameterValue,
			EncryptionTypeNone,
			EncryptionTypeTLS,
		)
	}

	return nil
}

// applyCreateClusterDefaults fills in default values for optional fields.
func applyCreateClusterDefaults(input *CreateClusterInput) {
	if input.SubnetGroupName == "" {
		input.SubnetGroupName = DefaultSubnetGroupName
	}

	if input.ParameterGroupName == "" {
		input.ParameterGroupName = DefaultParameterGroupName
	}

	if input.ClusterEndpointEncryptionType == "" {
		input.ClusterEndpointEncryptionType = EncryptionTypeNone
	}
}

// buildClusterNodes builds the node list for a new cluster.
func (b *InMemoryBackend) buildClusterNodes(input CreateClusterInput, now time.Time) []Node {
	capacity := input.ReplicationFactor
	const maxCapacity = 100
	if capacity > maxCapacity {
		capacity = maxCapacity
	} else if capacity < 0 {
		capacity = 0
	}
	nodes := make([]Node, 0, capacity)

	for i := range capacity {
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
		maintenanceWindow = randomMaintenanceWindow()
	}

	clusterEndpoint := clusterEndpointAddress(input.ClusterName, b.Region)

	cluster := &Cluster{
		ClusterName:                   input.ClusterName,
		ClusterArn:                    clusterARN,
		Description:                   input.Description,
		NodeType:                      input.NodeType,
		Status:                        StatusAvailable,
		IamRoleArn:                    input.IamRoleArn,
		SubnetGroupName:               input.SubnetGroupName,
		SecurityGroupIDs:              input.SecurityGroupIDs,
		PreferredMaintenanceWindow:    maintenanceWindow,
		ClusterEndpointEncryptionType: input.ClusterEndpointEncryptionType,
		CreateTime:                    now,
		TotalNodes:                    input.ReplicationFactor,
		ActiveNodes:                   input.ReplicationFactor,
		Nodes:                         nodes,
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

	if input.NotificationTopicArn != "" {
		cluster.NotificationConfiguration = &NotificationConfiguration{
			TopicArn:    input.NotificationTopicArn,
			TopicStatus: notificationTopicStatusActive,
		}
	}

	maps.Copy(cluster.Tags, input.Tags)

	b.clusters[input.ClusterName] = cluster

	if len(input.Tags) > 0 {
		b.tags[clusterARN] = make(map[string]string)
		maps.Copy(b.tags[clusterARN], input.Tags)
	}

	b.emitEventLocked(input.ClusterName, EventSourceTypeCluster,
		fmt.Sprintf("Cluster %s has been created.", input.ClusterName))

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

	if input.Description != nil {
		cluster.Description = *input.Description
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

	if input.NotificationTopicArn != "" {
		status := notificationTopicStatusActive
		if input.NotificationTopicStatus != "" {
			status = input.NotificationTopicStatus
		}

		cluster.NotificationConfiguration = &NotificationConfiguration{
			TopicArn:    input.NotificationTopicArn,
			TopicStatus: status,
		}
	} else if input.NotificationTopicStatus != "" && cluster.NotificationConfiguration != nil {
		cluster.NotificationConfiguration.TopicStatus = input.NotificationTopicStatus
	}

	cp := b.clusterCopy(cluster)

	return cp, nil
}

// DeleteCluster marks a DAX cluster as deleting and removes it from the store.
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

	if cluster.Status == StatusDeleting {
		return nil, fmt.Errorf("%w: cluster %s is already being deleted", ErrInvalidClusterState, clusterName)
	}

	cp := b.clusterCopy(cluster)
	cp.Status = StatusDeleting

	b.emitEventLocked(clusterName, EventSourceTypeCluster,
		fmt.Sprintf("Cluster %s has been deleted.", clusterName))

	delete(b.clusters, clusterName)
	delete(b.tags, cluster.ClusterArn)

	return cp, nil
}

// IncreaseReplicationFactor adds nodes to a cluster.
func (b *InMemoryBackend) IncreaseReplicationFactor(input IncreaseReplicationFactorInput) (*Cluster, error) {
	if input.ClusterName == "" {
		return nil, fmt.Errorf("%w: ClusterName is required", ErrInvalidARN)
	}

	if input.NewReplicationFactor < minReplicationFactor || input.NewReplicationFactor > maxReplicationFactor {
		return nil, fmt.Errorf(
			"%w: NewReplicationFactor must be between %d and %d",
			ErrInvalidParameterCombination,
			minReplicationFactor,
			maxReplicationFactor,
		)
	}

	b.mu.Lock("IncreaseReplicationFactor")
	defer b.mu.Unlock()

	cluster, ok := b.clusters[input.ClusterName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, input.ClusterName)
	}

	if cluster.Status != StatusAvailable {
		return nil, fmt.Errorf(
			"%w: cluster %s must be %s to resize (currently %s)",
			ErrInvalidClusterState,
			input.ClusterName,
			StatusAvailable,
			cluster.Status,
		)
	}

	if input.NewReplicationFactor <= len(cluster.Nodes) {
		return nil, fmt.Errorf(
			"%w: NewReplicationFactor %d must be greater than current %d",
			ErrInvalidParameterCombination,
			input.NewReplicationFactor,
			len(cluster.Nodes),
		)
	}

	now := time.Now().UTC()
	existingCount := len(cluster.Nodes)

	for i := existingCount; i < input.NewReplicationFactor; i++ {
		az := b.Region + "a"
		if j := i - existingCount; j < len(input.AvailabilityZones) {
			az = input.AvailabilityZones[j]
		}

		nodeID := fmt.Sprintf("%s-%04d", input.ClusterName, i)
		addr := nodeEndpointAddress(input.ClusterName, fmt.Sprintf("%04d", i), b.Region)

		cluster.Nodes = append(cluster.Nodes, Node{
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

	cluster.TotalNodes = input.NewReplicationFactor
	cluster.ActiveNodes = input.NewReplicationFactor

	b.emitEventLocked(input.ClusterName, EventSourceTypeCluster,
		fmt.Sprintf("Replication factor increased to %d.", input.NewReplicationFactor))

	return b.clusterCopy(cluster), nil
}

// DecreaseReplicationFactor removes nodes from a cluster.
func (b *InMemoryBackend) DecreaseReplicationFactor(input DecreaseReplicationFactorInput) (*Cluster, error) {
	if input.ClusterName == "" {
		return nil, fmt.Errorf("%w: ClusterName is required", ErrInvalidARN)
	}

	if input.NewReplicationFactor < minReplicationFactor || input.NewReplicationFactor > maxReplicationFactor {
		return nil, fmt.Errorf(
			"%w: NewReplicationFactor must be between %d and %d",
			ErrInvalidParameterCombination,
			minReplicationFactor,
			maxReplicationFactor,
		)
	}

	b.mu.Lock("DecreaseReplicationFactor")
	defer b.mu.Unlock()

	cluster, ok := b.clusters[input.ClusterName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, input.ClusterName)
	}

	if cluster.Status != StatusAvailable {
		return nil, fmt.Errorf(
			"%w: cluster %s must be %s to resize (currently %s)",
			ErrInvalidClusterState,
			input.ClusterName,
			StatusAvailable,
			cluster.Status,
		)
	}

	if input.NewReplicationFactor >= len(cluster.Nodes) {
		return nil, fmt.Errorf(
			"%w: NewReplicationFactor %d must be less than current %d",
			ErrInvalidParameterCombination,
			input.NewReplicationFactor,
			len(cluster.Nodes),
		)
	}

	if len(input.NodeIDsToRemove) > 0 {
		kept, err := removeSpecificNodes(
			cluster.Nodes, input.NodeIDsToRemove, input.ClusterName, input.NewReplicationFactor,
		)
		if err != nil {
			return nil, err
		}

		cluster.Nodes = kept
	} else {
		cluster.Nodes = cluster.Nodes[:input.NewReplicationFactor]
	}

	cluster.TotalNodes = input.NewReplicationFactor
	cluster.ActiveNodes = input.NewReplicationFactor

	b.emitEventLocked(input.ClusterName, EventSourceTypeCluster,
		fmt.Sprintf("Replication factor decreased to %d.", input.NewReplicationFactor))

	return b.clusterCopy(cluster), nil
}

// RebootNode initiates a reboot of a specific node in a cluster.
func (b *InMemoryBackend) RebootNode(clusterName, nodeID string) (*Cluster, error) {
	if clusterName == "" {
		return nil, fmt.Errorf("%w: ClusterName is required", ErrInvalidARN)
	}

	if nodeID == "" {
		return nil, fmt.Errorf("%w: NodeId is required", ErrInvalidParameterValue)
	}

	b.mu.Lock("RebootNode")
	defer b.mu.Unlock()

	cluster, ok := b.clusters[clusterName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, clusterName)
	}

	if cluster.Status != StatusAvailable {
		return nil, fmt.Errorf(
			"%w: cluster %s must be %s to reboot a node",
			ErrInvalidClusterState,
			clusterName,
			StatusAvailable,
		)
	}

	found := false

	for i := range cluster.Nodes {
		if cluster.Nodes[i].NodeID == nodeID {
			cluster.Nodes[i].NodeStatus = StatusRebooting
			found = true

			break
		}
	}

	if !found {
		return nil, fmt.Errorf("%w: node %s not found in cluster %s", ErrNodeNotFound, nodeID, clusterName)
	}

	b.emitEventLocked(clusterName, EventSourceTypeNode,
		fmt.Sprintf("Node %s reboot initiated.", nodeID))

	go func() {
		time.Sleep(time.Second)
		b.mu.Lock("RebootNode:recovery")
		defer b.mu.Unlock()
		c, exists := b.clusters[clusterName]
		if !exists {
			return
		}
		for i := range c.Nodes {
			if c.Nodes[i].NodeID == nodeID {
				c.Nodes[i].NodeStatus = StatusAvailable

				break
			}
		}
		b.emitEventLocked(clusterName, EventSourceTypeNode,
			fmt.Sprintf("Node %s reboot complete.", nodeID))
	}()

	return b.clusterCopy(cluster), nil
}

// TagResource adds tags to a DAX resource and returns the complete tag set.
func (b *InMemoryBackend) TagResource(resourceArn string, tags map[string]string) (map[string]string, error) {
	if resourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceName is required", ErrInvalidARN)
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if !b.arnExists(resourceArn) {
		return nil, fmt.Errorf("%w: %s", ErrTagNotFound, resourceArn)
	}

	existing := b.tags[resourceArn]
	merged := len(existing)

	for k := range tags {
		if _, alreadyExists := existing[k]; !alreadyExists {
			merged++
		}
	}

	if merged > maxTagsPerResource {
		return nil, fmt.Errorf("%w: resource would have %d tags (max %d)",
			ErrTagQuotaExceeded, merged, maxTagsPerResource)
	}

	if b.tags[resourceArn] == nil {
		b.tags[resourceArn] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceArn], tags)

	// Propagate to cluster.Tags if this is a cluster ARN.
	if cluster := b.clusterByARN(resourceArn); cluster != nil {
		maps.Copy(cluster.Tags, tags)
	}

	result := make(map[string]string, len(b.tags[resourceArn]))
	maps.Copy(result, b.tags[resourceArn])

	return result, nil
}

// UntagResource removes tags from a DAX resource and returns the remaining tags.
func (b *InMemoryBackend) UntagResource(resourceArn string, tagKeys []string) (map[string]string, error) {
	if resourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceName is required", ErrInvalidARN)
	}

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if !b.arnExists(resourceArn) {
		return nil, fmt.Errorf("%w: %s", ErrTagNotFound, resourceArn)
	}

	if b.tags[resourceArn] != nil {
		for _, k := range tagKeys {
			delete(b.tags[resourceArn], k)
		}
	}

	if cluster := b.clusterByARN(resourceArn); cluster != nil {
		for _, k := range tagKeys {
			delete(cluster.Tags, k)
		}
	}

	result := make(map[string]string)
	if b.tags[resourceArn] != nil {
		maps.Copy(result, b.tags[resourceArn])
	}

	return result, nil
}

// ListTags returns tags for a DAX resource with optional pagination.
func (b *InMemoryBackend) ListTags(
	resourceArn string,
	nextToken string,
) (map[string]string, string, error) {
	if resourceArn == "" {
		return nil, "", fmt.Errorf("%w: ResourceName is required", ErrInvalidARN)
	}

	b.mu.RLock("ListTags")
	defer b.mu.RUnlock()

	if !b.arnExists(resourceArn) {
		return nil, "", fmt.Errorf("%w: %s", ErrTagNotFound, resourceArn)
	}

	allTags := b.tags[resourceArn]

	keys := collections.SortedKeys(allTags)

	startIdx := 0

	if nextToken != "" {
		for i, k := range keys {
			if k == nextToken {
				startIdx = i

				break
			}
		}
	}

	end := min(startIdx+listTagsPageSize, len(keys))

	page := keys[startIdx:end]
	result := make(map[string]string, len(page))

	for _, k := range page {
		result[k] = allTags[k]
	}

	var outToken string
	if end < len(keys) {
		outToken = keys[end]
	}

	return result, outToken, nil
}

// CreateParameterGroup creates a DAX parameter group.
func (b *InMemoryBackend) CreateParameterGroup(name, description string) (*ParameterGroup, error) {
	if err := validateResourceName(name, "ParameterGroupName"); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateParameterGroup")
	defer b.mu.Unlock()

	if _, exists := b.paramGroups[name]; exists {
		return nil, fmt.Errorf("%w: %s", ErrParameterGroupAlreadyExists, name)
	}

	params := make(map[string]string, len(defaultParameterValues))
	maps.Copy(params, defaultParameterValues)

	pg := &ParameterGroup{
		ParameterGroupName: name,
		Description:        description,
		Parameters:         params,
	}

	b.paramGroups[name] = pg

	b.emitEventLocked(name, EventSourceTypeParameterGroup,
		fmt.Sprintf("Parameter group %s created.", name))

	cp := *pg
	cp.Parameters = make(map[string]string, len(pg.Parameters))
	maps.Copy(cp.Parameters, pg.Parameters)

	return &cp, nil
}

// DescribeParameterGroups returns DAX parameter groups with pagination.
func (b *InMemoryBackend) DescribeParameterGroups(
	names []string,
	maxResults int,
	nextToken string,
) ([]*ParameterGroup, string, error) {
	b.mu.RLock("DescribeParameterGroups")
	defer b.mu.RUnlock()

	if maxResults <= 0 {
		maxResults = maxPageSizeDefault
	}

	var all []*ParameterGroup

	if len(names) > 0 {
		for _, name := range names {
			pg, ok := b.paramGroups[name]
			if !ok {
				return nil, "", fmt.Errorf("%w: %s", ErrParameterGroupNotFound, name)
			}

			cp := paramGroupCopy(pg)
			all = append(all, cp)
		}
		// Named lookup: return all matches without pagination.
		return all, "", nil
	}

	for _, pg := range b.paramGroups {
		cp := paramGroupCopy(pg)
		all = append(all, cp)
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].ParameterGroupName < all[j].ParameterGroupName
	})

	start := 0
	if nextToken != "" {
		for i, pg := range all {
			if pg.ParameterGroupName == nextToken {
				start = i

				break
			}
		}
	}

	if start >= len(all) {
		return []*ParameterGroup{}, "", nil
	}

	end := start + maxResults
	newNextToken := ""
	if end < len(all) {
		newNextToken = all[end].ParameterGroupName
	} else {
		end = len(all)
	}

	return all[start:end], newNextToken, nil
}

// UpdateParameterGroup updates parameter values in a parameter group.
func (b *InMemoryBackend) UpdateParameterGroup(input UpdateParameterGroupInput) (*ParameterGroup, error) {
	if input.ParameterGroupName == "" {
		return nil, fmt.Errorf("%w: ParameterGroupName is required", ErrParameterGroupNotFound)
	}

	b.mu.Lock("UpdateParameterGroup")
	defer b.mu.Unlock()

	pg, ok := b.paramGroups[input.ParameterGroupName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrParameterGroupNotFound, input.ParameterGroupName)
	}

	for _, pv := range input.ParameterNameValues {
		if _, known := defaultParameterValues[pv.ParameterName]; !known {
			return nil, fmt.Errorf(
				"%w: unknown parameter %q",
				ErrInvalidParameterValue,
				pv.ParameterName,
			)
		}

		if pv.ParameterValue == "" {
			return nil, fmt.Errorf(
				"%w: value for %q must be a non-negative integer",
				ErrInvalidParameterValue, pv.ParameterName,
			)
		}

		val, err := strconv.ParseInt(pv.ParameterValue, 10, 64)
		if err != nil || val < 0 {
			return nil, fmt.Errorf(
				"%w: value for %q must be a non-negative integer, got %q",
				ErrInvalidParameterValue,
				pv.ParameterName,
				pv.ParameterValue,
			)
		}

		pg.Parameters[pv.ParameterName] = pv.ParameterValue
	}

	for _, cluster := range b.clusters {
		if cluster.ParameterGroup.ParameterGroupName != input.ParameterGroupName {
			continue
		}
		nodeIDs := make([]string, 0, len(cluster.Nodes))
		for _, n := range cluster.Nodes {
			nodeIDs = append(nodeIDs, n.NodeID)
		}
		cluster.ParameterGroup.ParameterApplyStatus = "pending-reboot"
		cluster.ParameterGroup.NodeIDsToReboot = nodeIDs
	}

	b.emitEventLocked(input.ParameterGroupName, EventSourceTypeParameterGroup,
		fmt.Sprintf("Parameter group %s updated.", input.ParameterGroupName))

	return paramGroupCopy(pg), nil
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

	for _, cluster := range b.clusters {
		if cluster.ParameterGroup.ParameterGroupName == name {
			return fmt.Errorf("%w: parameter group %s is in use by cluster %s",
				ErrParameterGroupInUse, name, cluster.ClusterName)
		}
	}

	delete(b.paramGroups, name)

	return nil
}

// buildParameter constructs a Parameter from a name, value, and source.
func buildParameter(name, value, source string) *Parameter {
	return &Parameter{
		ParameterName:  name,
		ParameterValue: value,
		Description:    defaultParameterDescriptions[name],
		Source:         source,
		DataType:       "integer",
		IsModifiable:   "TRUE",
		ChangeType:     "requires-reboot",
		AllowedValues:  defaultParameterAllowedValues[name],
		ParameterType:  ParameterTypeDefault,
	}
}

// paginateParameters applies pagination to a sorted parameter slice.
func paginateParameters(all []*Parameter, maxResults int, nextToken string) ([]*Parameter, string) {
	start := 0
	if nextToken != "" {
		idx, err := strconv.Atoi(nextToken)
		if err == nil && idx >= 0 && idx < len(all) {
			start = idx
		}
	}

	if start >= len(all) {
		return []*Parameter{}, ""
	}

	end := start + maxResults
	newNextToken := ""
	if end < len(all) {
		newNextToken = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return all[start:end], newNextToken
}

// DescribeParameters returns the parameters for a specific parameter group with pagination.
func (b *InMemoryBackend) DescribeParameters(
	paramGroupName string,
	maxResults int,
	nextToken string,
) ([]*Parameter, string, error) {
	if paramGroupName == "" {
		return nil, "", fmt.Errorf("%w: ParameterGroupName is required", ErrParameterGroupNotFound)
	}

	if maxResults <= 0 {
		maxResults = maxPageSizeDefault
	}

	b.mu.RLock("DescribeParameters")
	defer b.mu.RUnlock()

	pg, ok := b.paramGroups[paramGroupName]
	if !ok {
		return nil, "", fmt.Errorf("%w: %s", ErrParameterGroupNotFound, paramGroupName)
	}

	params := make([]*Parameter, 0, len(pg.Parameters))

	for name, value := range pg.Parameters {
		source := "user"
		if def, isDefault := defaultParameterValues[name]; isDefault && value == def {
			source = "system"
		}

		params = append(params, buildParameter(name, value, source))
	}

	sort.Slice(params, func(i, j int) bool {
		return params[i].ParameterName < params[j].ParameterName
	})

	page, token := paginateParameters(params, maxResults, nextToken)

	return page, token, nil
}

// DescribeDefaultParameters returns the default DAX 1.0 parameter definitions with pagination.
func (b *InMemoryBackend) DescribeDefaultParameters(maxResults int, nextToken string) ([]*Parameter, string, error) {
	if maxResults <= 0 {
		maxResults = maxPageSizeDefault
	}

	params := make([]*Parameter, 0, len(defaultParameterValues))

	for name, value := range defaultParameterValues {
		params = append(params, buildParameter(name, value, "system"))
	}

	sort.Slice(params, func(i, j int) bool {
		return params[i].ParameterName < params[j].ParameterName
	})

	page, token := paginateParameters(params, maxResults, nextToken)

	return page, token, nil
}

// ResetParameterGroup resets parameter group parameters to defaults.
func (b *InMemoryBackend) ResetParameterGroup(name string, parameterNames []string) (*ParameterGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: ParameterGroupName is required", ErrParameterGroupNotFound)
	}

	b.mu.Lock("ResetParameterGroup")
	defer b.mu.Unlock()

	pg, ok := b.paramGroups[name]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrParameterGroupNotFound, name)
	}

	if len(parameterNames) == 0 {
		// Reset all to defaults.
		maps.Copy(pg.Parameters, defaultParameterValues)
	} else {
		for _, pname := range parameterNames {
			if def, found := defaultParameterValues[pname]; found {
				pg.Parameters[pname] = def
			}
		}
	}

	b.emitEventLocked(name, EventSourceTypeParameterGroup,
		fmt.Sprintf("Parameter group %s reset to defaults.", name))

	return paramGroupCopy(pg), nil
}

// CreateSubnetGroup creates a DAX subnet group.
func (b *InMemoryBackend) CreateSubnetGroup(
	name, description string,
	subnetIDs []string,
) (*SubnetGroup, error) {
	if err := validateResourceName(name, "SubnetGroupName"); err != nil {
		return nil, err
	}

	if len(subnetIDs) == 0 {
		return nil, fmt.Errorf("%w: at least one SubnetId is required", ErrInvalidParameterValue)
	}

	b.mu.Lock("CreateSubnetGroup")
	defer b.mu.Unlock()

	if _, exists := b.subnetGroups[name]; exists {
		return nil, fmt.Errorf("%w: %s", ErrSubnetGroupAlreadyExists, name)
	}

	subnets := subnetEntriesFromIDs(subnetIDs, b.Region)
	vpcID := vpcIDFromSubnets(subnetIDs)

	sg := &SubnetGroup{
		SubnetGroupName: name,
		Description:     description,
		VpcID:           vpcID,
		Subnets:         subnets,
	}

	b.subnetGroups[name] = sg

	b.emitEventLocked(name, EventSourceTypeSubnetGroup,
		fmt.Sprintf("Subnet group %s created.", name))

	return subnetGroupCopy(sg), nil
}

// DescribeSubnetGroups returns DAX subnet groups with pagination.
func (b *InMemoryBackend) DescribeSubnetGroups(
	names []string,
	maxResults int,
	nextToken string,
) ([]*SubnetGroup, string, error) {
	b.mu.RLock("DescribeSubnetGroups")
	defer b.mu.RUnlock()

	if maxResults <= 0 {
		maxResults = maxPageSizeDefault
	}

	var all []*SubnetGroup

	if len(names) > 0 {
		for _, name := range names {
			sg, ok := b.subnetGroups[name]
			if !ok {
				return nil, "", fmt.Errorf("%w: %s", ErrSubnetGroupNotFound, name)
			}

			all = append(all, subnetGroupCopy(sg))
		}

		return all, "", nil
	}

	for _, sg := range b.subnetGroups {
		all = append(all, subnetGroupCopy(sg))
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].SubnetGroupName < all[j].SubnetGroupName
	})

	start := 0
	if nextToken != "" {
		for i, sg := range all {
			if sg.SubnetGroupName == nextToken {
				start = i

				break
			}
		}
	}

	if start >= len(all) {
		return []*SubnetGroup{}, "", nil
	}

	end := start + maxResults
	newNextToken := ""
	if end < len(all) {
		newNextToken = all[end].SubnetGroupName
	} else {
		end = len(all)
	}

	return all[start:end], newNextToken, nil
}

// UpdateSubnetGroup updates a subnet group's description and/or subnet list.
func (b *InMemoryBackend) UpdateSubnetGroup(input UpdateSubnetGroupInput) (*SubnetGroup, error) {
	if input.SubnetGroupName == "" {
		return nil, fmt.Errorf("%w: SubnetGroupName is required", ErrSubnetGroupNotFound)
	}

	b.mu.Lock("UpdateSubnetGroup")
	defer b.mu.Unlock()

	sg, ok := b.subnetGroups[input.SubnetGroupName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrSubnetGroupNotFound, input.SubnetGroupName)
	}

	if input.Description != "" {
		sg.Description = input.Description
	}

	if len(input.SubnetIDs) > 0 {
		sg.Subnets = subnetEntriesFromIDs(input.SubnetIDs, b.Region)
		sg.VpcID = vpcIDFromSubnets(input.SubnetIDs)
	}

	b.emitEventLocked(input.SubnetGroupName, EventSourceTypeSubnetGroup,
		fmt.Sprintf("Subnet group %s updated.", input.SubnetGroupName))

	return subnetGroupCopy(sg), nil
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

	for _, cluster := range b.clusters {
		if cluster.SubnetGroupName == name {
			return fmt.Errorf("%w: subnet group %s is in use by cluster %s",
				ErrSubnetGroupInUse, name, cluster.ClusterName)
		}
	}

	delete(b.subnetGroups, name)

	return nil
}

// eventMatches returns true if the event matches the given filters.
func eventMatches(ev *Event, sourceName, sourceType string, startTime, endTime *time.Time) bool {
	if sourceName != "" && ev.SourceName != sourceName {
		return false
	}

	if sourceType != "" && ev.SourceType != sourceType {
		return false
	}

	if startTime != nil && ev.Date.Before(*startTime) {
		return false
	}

	if endTime != nil && ev.Date.After(*endTime) {
		return false
	}

	return true
}

// DescribeEvents returns events filtered by source and time range.
func (b *InMemoryBackend) DescribeEvents(
	sourceName string,
	sourceType string,
	startTime *time.Time,
	endTime *time.Time,
	maxResults int,
	nextToken string,
) ([]*Event, string, error) {
	b.mu.RLock("DescribeEvents")
	defer b.mu.RUnlock()

	if maxResults <= 0 {
		maxResults = maxClustersDefault
	}

	var filtered []*Event

	for _, ev := range b.events {
		if !eventMatches(ev, sourceName, sourceType, startTime, endTime) {
			continue
		}

		cp := *ev
		filtered = append(filtered, &cp)
	}

	// Apply pagination via token = index as string.
	start := 0

	if nextToken != "" {
		idx, err := strconv.Atoi(nextToken)
		if err == nil && idx >= 0 && idx < len(filtered) {
			start = idx
		}
	}

	if start >= len(filtered) {
		return []*Event{}, "", nil
	}

	end := start + maxResults
	newNextToken := ""

	if end < len(filtered) {
		newNextToken = strconv.Itoa(end)
	} else {
		end = len(filtered)
	}

	return filtered[start:end], newNextToken, nil
}

// Reset clears all DAX state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.clusters = make(map[string]*Cluster)
	b.tags = make(map[string]map[string]string)
	b.paramGroups = make(map[string]*ParameterGroup)
	b.subnetGroups = make(map[string]*SubnetGroup)
	b.events = make([]*Event, 0)

	b.seedDefaults()
}

// emitEventLocked appends an event to the ring buffer. Must be called with b.mu held for write.
func (b *InMemoryBackend) emitEventLocked(sourceName, sourceType, message string) {
	ev := &Event{
		Date:       time.Now().UTC(),
		SourceName: sourceName,
		SourceType: sourceType,
		Message:    message,
	}

	b.events = append(b.events, ev)

	if len(b.events) > maxEventsPerBuffer {
		b.events = b.events[len(b.events)-maxEventsPerBuffer:]
	}
}

// arnExists returns true if the ARN corresponds to an existing DAX resource.
// Must be called with b.mu held.
func (b *InMemoryBackend) arnExists(arnStr string) bool {
	clusterPrefix := arn.Build("dax", b.Region, b.AccountID, "cache/")
	if name, ok := strings.CutPrefix(arnStr, clusterPrefix); ok {
		_, exists := b.clusters[name]

		return exists
	}

	paramPrefix := arn.Build("dax", b.Region, b.AccountID, "parametergroup/")
	if name, ok := strings.CutPrefix(arnStr, paramPrefix); ok {
		_, exists := b.paramGroups[name]

		return exists
	}

	subnetPrefix := arn.Build("dax", b.Region, b.AccountID, "subnetgroup/")
	if name, ok := strings.CutPrefix(arnStr, subnetPrefix); ok {
		_, exists := b.subnetGroups[name]

		return exists
	}

	return false
}

// clusterByARN returns the cluster matching the given ARN, or nil.
// Must be called with b.mu held.
func (b *InMemoryBackend) clusterByARN(arnStr string) *Cluster {
	prefix := arn.Build("dax", b.Region, b.AccountID, "cache/")
	if name, ok := strings.CutPrefix(arnStr, prefix); ok {
		return b.clusters[name]
	}

	return nil
}

// clusterCopy returns a deep copy of a Cluster.
func (b *InMemoryBackend) clusterCopy(c *Cluster) *Cluster {
	cp := *c
	cp.Tags = make(map[string]string)
	maps.Copy(cp.Tags, c.Tags)

	cp.SecurityGroupIDs = append([]string(nil), c.SecurityGroupIDs...)

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

	if c.NotificationConfiguration != nil {
		ncCp := *c.NotificationConfiguration
		cp.NotificationConfiguration = &ncCp
	}

	return &cp
}

// paramGroupCopy returns a deep copy of a ParameterGroup.
func paramGroupCopy(pg *ParameterGroup) *ParameterGroup {
	cp := *pg
	cp.Parameters = make(map[string]string, len(pg.Parameters))
	maps.Copy(cp.Parameters, pg.Parameters)

	return &cp
}

// subnetGroupCopy returns a deep copy of a SubnetGroup.
func subnetGroupCopy(sg *SubnetGroup) *SubnetGroup {
	cp := *sg
	cp.Subnets = append([]SubnetEntry(nil), sg.Subnets...)

	return &cp
}

// subnetEntriesFromIDs converts string subnet IDs to SubnetEntry slices using default AZ.
func subnetEntriesFromIDs(ids []string, region string) []SubnetEntry {
	entries := make([]SubnetEntry, 0, len(ids))

	for _, id := range ids {
		entries = append(entries, SubnetEntry{
			SubnetID:         id,
			AvailabilityZone: region + "a",
		})
	}

	return entries
}

// removeSpecificNodes validates NodeIDsToRemove count and existence, then returns the kept nodes.
func removeSpecificNodes(nodes []Node, nodeIDsToRemove []string, clusterName string, newFactor int) ([]Node, error) {
	expectedRemoveCount := len(nodes) - newFactor
	if len(nodeIDsToRemove) != expectedRemoveCount {
		return nil, fmt.Errorf(
			"%w: NodeIDsToRemove has %d entries but %d nodes must be removed to reach factor %d",
			ErrInvalidParameterCombination,
			len(nodeIDsToRemove),
			expectedRemoveCount,
			newFactor,
		)
	}

	existingIDs := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		existingIDs[n.NodeID] = true
	}

	for _, id := range nodeIDsToRemove {
		if !existingIDs[id] {
			return nil, fmt.Errorf(
				"%w: node %s does not exist in cluster %s",
				ErrNodeNotFound, id, clusterName,
			)
		}
	}

	removeSet := make(map[string]bool, len(nodeIDsToRemove))
	for _, id := range nodeIDsToRemove {
		removeSet[id] = true
	}

	kept := make([]Node, 0, newFactor)
	for _, n := range nodes {
		if !removeSet[n.NodeID] {
			kept = append(kept, n)
		}
	}

	return kept, nil
}

// vpcIDFromSubnets returns a deterministic placeholder VPC ID derived from the first subnet ID.
// Real AWS would look up the actual VPC; in emulation we derive a plausible ID from the subnet.
func vpcIDFromSubnets(subnetIDs []string) string {
	if len(subnetIDs) == 0 {
		return "vpc-00000000"
	}

	first := subnetIDs[0]
	if idx := strings.LastIndexByte(first, '-'); idx >= 0 && idx < len(first)-1 {
		suffix := first[idx+1:]
		if len(suffix) > vpcSuffixMaxLen {
			suffix = suffix[:vpcSuffixMaxLen]
		}

		return "vpc-" + suffix
	}

	return "vpc-00000000"
}
