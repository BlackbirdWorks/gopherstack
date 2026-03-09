package ec2

import (
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// Errors returned by the EC2 backend.
var (
	ErrInstanceNotFound      = errors.New("InvalidInstanceID.NotFound")
	ErrSecurityGroupNotFound = errors.New("InvalidGroup.NotFound")
	ErrVPCNotFound           = errors.New("InvalidVpcID.NotFound")
	ErrSubnetNotFound        = errors.New("InvalidSubnetID.NotFound")
	ErrInvalidParameter      = errors.New("InvalidParameterValue")
	ErrDuplicateSGName       = errors.New("InvalidGroup.Duplicate")
	ErrInvalidInstanceState  = errors.New("IncorrectInstanceState")
)

// EC2 instance state codes as defined by the AWS EC2 API.
const (
	stateCodeRunning      = 16
	stateCodeTerminated   = 48
	stateCodeStopped      = 80
	stateCodePending      = 0
	stateCodeShuttingDown = 32
	stateCodeStopping     = 64
)

// InstanceState represents the state of an EC2 instance.
type InstanceState struct {
	Name string `json:"name"`
	Code int    `json:"code"`
}

// Well-known instance states.
//
//nolint:gochecknoglobals // package-level sentinel values, analogous to exported errors
var (
	StateRunning      = InstanceState{Code: stateCodeRunning, Name: "running"}
	StateTerminated   = InstanceState{Code: stateCodeTerminated, Name: "terminated"}
	StateStopped      = InstanceState{Code: stateCodeStopped, Name: "stopped"}
	StatePending      = InstanceState{Code: stateCodePending, Name: "pending"}
	StateShuttingDown = InstanceState{Code: stateCodeShuttingDown, Name: "shutting-down"}
	StateStopping     = InstanceState{Code: stateCodeStopping, Name: "stopping"}
)

// Instance represents an EC2 instance (metadata only, no actual compute).
type Instance struct {
	LaunchTime     time.Time     `json:"launchTime"`
	State          InstanceState `json:"state"`
	ID             string        `json:"id"`
	InstanceType   string        `json:"instanceType"`
	ImageID        string        `json:"imageID"`
	VPCID          string        `json:"vpcID"`
	SubnetID       string        `json:"subnetID"`
	PrivateIP      string        `json:"privateIP"`
	KeyName        string        `json:"keyName"`
	SecurityGroups []string      `json:"securityGroups"`
}

// InstanceStateChange records the state transition for a single instance.
// It is returned by StartInstances, StopInstances, and TerminateInstances so
// callers have accurate before/after information without hard-coding states.
type InstanceStateChange struct {
	InstanceID    string
	PreviousState InstanceState
	CurrentState  InstanceState
}

// SecurityGroupRule represents an inbound or outbound rule.
type SecurityGroupRule struct {
	Protocol string `json:"protocol"`
	IPRange  string `json:"ipRange"`
	FromPort int    `json:"fromPort"`
	ToPort   int    `json:"toPort"`
}

// SecurityGroup represents an EC2 security group.
type SecurityGroup struct {
	ID           string              `json:"id"`
	Name         string              `json:"name"`
	Description  string              `json:"description"`
	VPCID        string              `json:"vpcID"`
	IngressRules []SecurityGroupRule `json:"ingressRules"`
	EgressRules  []SecurityGroupRule `json:"egressRules"`
}

// VPC represents an EC2 VPC.
type VPC struct {
	ID        string `json:"id"`
	CIDRBlock string `json:"cidrBlock"`
	IsDefault bool   `json:"isDefault"`
}

// Subnet represents an EC2 Subnet.
type Subnet struct {
	ID               string `json:"id"`
	VPCID            string `json:"vpcID"`
	CIDRBlock        string `json:"cidrBlock"`
	AvailabilityZone string `json:"availabilityZone"`
	IsDefault        bool   `json:"isDefault"`
}

// InMemoryBackend is the in-memory store for EC2 resources.
type InMemoryBackend struct {
	instances          map[string]*Instance
	securityGroups     map[string]*SecurityGroup
	vpcs               map[string]*VPC
	subnets            map[string]*Subnet
	keyPairs           map[string]*KeyPair
	volumes            map[string]*Volume
	addresses          map[string]*Address
	internetGateways   map[string]*InternetGateway
	routeTables        map[string]*RouteTable
	natGateways        map[string]*NatGateway
	networkInterfaces  map[string]*NetworkInterface
	tags               map[string]map[string]string // resourceID → key → value
	mu                 *lockmetrics.RWMutex
	AccountID          string
	Region             string
	nextPrivateIPIndex int
	nextElasticIPIndex int
}

// NewInMemoryBackend creates a new InMemoryBackend with a default VPC and subnet.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		instances:         make(map[string]*Instance),
		securityGroups:    make(map[string]*SecurityGroup),
		vpcs:              make(map[string]*VPC),
		subnets:           make(map[string]*Subnet),
		keyPairs:          make(map[string]*KeyPair),
		volumes:           make(map[string]*Volume),
		addresses:         make(map[string]*Address),
		internetGateways:  make(map[string]*InternetGateway),
		routeTables:       make(map[string]*RouteTable),
		natGateways:       make(map[string]*NatGateway),
		networkInterfaces: make(map[string]*NetworkInterface),
		tags:              make(map[string]map[string]string),
		AccountID:         accountID,
		Region:            region,
		mu:                lockmetrics.New("ec2"),
	}

	b.initDefaults()

	return b
}

// initDefaults pre-populates a default VPC, subnet, and security group.
func (b *InMemoryBackend) initDefaults() {
	defaultVPCID := "vpc-default"
	b.vpcs[defaultVPCID] = &VPC{
		ID:        defaultVPCID,
		CIDRBlock: "172.31.0.0/16",
		IsDefault: true,
	}

	defaultSubnetID := "subnet-default"
	b.subnets[defaultSubnetID] = &Subnet{
		ID:               defaultSubnetID,
		VPCID:            defaultVPCID,
		CIDRBlock:        "172.31.0.0/20",
		AvailabilityZone: b.Region + "a",
		IsDefault:        true,
	}

	defaultSGID := "sg-default"
	b.securityGroups[defaultSGID] = &SecurityGroup{
		ID:          defaultSGID,
		Name:        "default",
		Description: "default VPC security group",
		VPCID:       defaultVPCID,
	}
}

// RunInstances creates one or more EC2 instance stubs.
func (b *InMemoryBackend) RunInstances(imageID, instanceType, subnetID string, count int) ([]*Instance, error) {
	if imageID == "" {
		return nil, fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	if count < 1 {
		count = 1
	}

	b.mu.Lock("RunInstances")
	defer b.mu.Unlock()

	if subnetID == "" {
		subnetID = b.findDefaultSubnetID()
	} else if _, ok := b.subnets[subnetID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrSubnetNotFound, subnetID)
	}

	vpcID := ""

	if sub, ok := b.subnets[subnetID]; ok {
		vpcID = sub.VPCID
	}

	instances := make([]*Instance, 0, count)

	for range count {
		id := "i-" + uuid.New().String()[:17]
		inst := &Instance{
			ID:           id,
			ImageID:      imageID,
			InstanceType: instanceType,
			// AWS state machine: pending → running.
			// The mock completes this transition immediately so instances are
			// always observable as running after RunInstances returns.
			State:      StateRunning,
			VPCID:      vpcID,
			SubnetID:   subnetID,
			LaunchTime: time.Now(),
		}
		inst.PrivateIP = b.allocPrivateIP()
		eniID := "eni-" + uuid.New().String()[:17]
		b.networkInterfaces[eniID] = &NetworkInterface{
			ID:         eniID,
			SubnetID:   subnetID,
			VPCID:      vpcID,
			PrivateIP:  inst.PrivateIP,
			InstanceID: id,
			Status:     "in-use",
		}
		b.instances[id] = inst
		instances = append(instances, inst)
	}

	return instances, nil
}

// findDefaultSubnetID returns the ID of the default subnet, or empty string if none.
// Must be called with b.mu held.
func (b *InMemoryBackend) findDefaultSubnetID() string {
	for id, s := range b.subnets {
		if s.IsDefault {
			return id
		}
	}

	return ""
}

// DescribeInstances returns instances, optionally filtered by IDs or state.
func (b *InMemoryBackend) DescribeInstances(ids []string, state string) []*Instance {
	b.mu.RLock("DescribeInstances")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	var out []*Instance

	for _, inst := range b.instances {
		if len(idSet) > 0 && !idSet[inst.ID] {
			continue
		}

		if state != "" && inst.State.Name != state {
			continue
		}

		cp := *inst
		out = append(out, &cp)
	}

	return out
}

// TerminateInstances transitions instances to shutting-down then terminated.
// Returns the previous and current state for each instance.
func (b *InMemoryBackend) TerminateInstances(ids []string) ([]*InstanceStateChange, error) {
	b.mu.Lock("TerminateInstances")
	defer b.mu.Unlock()

	var result []*InstanceStateChange

	for _, id := range ids {
		inst, ok := b.instances[id]
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, id)
		}

		prev := inst.State
		// AWS state machine: any state → shutting-down → terminated.
		// The mock completes this transition immediately.
		inst.State = StateTerminated
		result = append(result, &InstanceStateChange{
			InstanceID:    id,
			PreviousState: prev,
			CurrentState:  inst.State,
		})
	}

	return result, nil
}

// DescribeSecurityGroups returns security groups, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeSecurityGroups(ids []string) []*SecurityGroup {
	b.mu.RLock("DescribeSecurityGroups")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	var out []*SecurityGroup

	for _, sg := range b.securityGroups {
		if len(idSet) > 0 && !idSet[sg.ID] {
			continue
		}

		cp := *sg
		out = append(out, &cp)
	}

	return out
}

// CreateSecurityGroup creates a new security group and returns its ID.
func (b *InMemoryBackend) CreateSecurityGroup(name, description, vpcID string) (*SecurityGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: GroupName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateSecurityGroup")
	defer b.mu.Unlock()

	if vpcID != "" {
		if _, ok := b.vpcs[vpcID]; !ok {
			return nil, fmt.Errorf("%w: %s", ErrVPCNotFound, vpcID)
		}
	}

	for _, sg := range b.securityGroups {
		if sg.Name == name && sg.VPCID == vpcID {
			return nil, fmt.Errorf("%w: group named %s already exists in VPC %s", ErrDuplicateSGName, name, vpcID)
		}
	}

	id := "sg-" + uuid.New().String()[:17]
	sg := &SecurityGroup{
		ID:          id,
		Name:        name,
		Description: description,
		VPCID:       vpcID,
	}
	b.securityGroups[id] = sg

	return sg, nil
}

// DeleteSecurityGroup removes a security group by ID.
func (b *InMemoryBackend) DeleteSecurityGroup(id string) error {
	b.mu.Lock("DeleteSecurityGroup")
	defer b.mu.Unlock()

	if _, ok := b.securityGroups[id]; !ok {
		return fmt.Errorf("%w: %s", ErrSecurityGroupNotFound, id)
	}

	delete(b.securityGroups, id)

	return nil
}

// DescribeVpcs returns VPCs, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeVpcs(ids []string) []*VPC {
	b.mu.RLock("DescribeVpcs")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	var out []*VPC

	for _, v := range b.vpcs {
		if len(idSet) > 0 && !idSet[v.ID] {
			continue
		}

		cp := *v
		out = append(out, &cp)
	}

	return out
}

// CreateVpc creates a new VPC with the given CIDR block.
func (b *InMemoryBackend) CreateVpc(cidr string) (*VPC, error) {
	if cidr == "" {
		return nil, fmt.Errorf("%w: CidrBlock is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateVpc")
	defer b.mu.Unlock()

	id := "vpc-" + uuid.New().String()[:17]
	v := &VPC{
		ID:        id,
		CIDRBlock: cidr,
	}
	b.vpcs[id] = v

	return v, nil
}

// DeleteVpc removes a VPC by ID.
func (b *InMemoryBackend) DeleteVpc(id string) error {
	b.mu.Lock("DeleteVpc")
	defer b.mu.Unlock()

	if _, ok := b.vpcs[id]; !ok {
		return fmt.Errorf("%w: %s", ErrVPCNotFound, id)
	}

	delete(b.vpcs, id)

	return nil
}

// DescribeSubnets returns subnets, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeSubnets(ids []string) []*Subnet {
	b.mu.RLock("DescribeSubnets")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	var out []*Subnet

	for _, s := range b.subnets {
		if len(idSet) > 0 && !idSet[s.ID] {
			continue
		}

		cp := *s
		out = append(out, &cp)
	}

	return out
}

// CreateSubnet creates a new subnet in the given VPC.
func (b *InMemoryBackend) CreateSubnet(vpcID, cidr, az string) (*Subnet, error) {
	if vpcID == "" {
		return nil, fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	if cidr == "" {
		return nil, fmt.Errorf("%w: CidrBlock is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateSubnet")
	defer b.mu.Unlock()

	if _, ok := b.vpcs[vpcID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrVPCNotFound, vpcID)
	}

	if az == "" {
		az = b.Region + "a"
	}

	id := "subnet-" + uuid.New().String()[:17]
	s := &Subnet{
		ID:               id,
		VPCID:            vpcID,
		CIDRBlock:        cidr,
		AvailabilityZone: az,
	}
	b.subnets[id] = s

	return s, nil
}

// DeleteSubnet removes a subnet by ID.
func (b *InMemoryBackend) DeleteSubnet(id string) error {
	b.mu.Lock("DeleteSubnet")
	defer b.mu.Unlock()

	if _, ok := b.subnets[id]; !ok {
		return fmt.Errorf("%w: %s", ErrSubnetNotFound, id)
	}

	delete(b.subnets, id)

	return nil
}

// TagEntry holds a single resource-tag association returned by DescribeTags.
type TagEntry struct {
	ResourceID   string
	ResourceType string
	Key          string
	Value        string
}

// resourceTypeByID infers the EC2 resource type from the ID prefix.
func resourceTypeByID(id string) string {
	prefixes := []struct {
		prefix string
		rtype  string
	}{
		{"i-", "instance"},
		{"sg-", "security-group"},
		{"vpc-", "vpc"},
		{"subnet-", "subnet"},
		{"vol-", "volume"},
		{"igw-", "internet-gateway"},
		{"rtb-", "route-table"},
		{"nat-", "natgateway"},
		{"eipalloc-", "elastic-ip"},
	}

	for _, e := range prefixes {
		if strings.HasPrefix(id, e.prefix) {
			return e.rtype
		}
	}

	return "resource"
}

// CreateTags adds or updates tags on one or more resources.
func (b *InMemoryBackend) CreateTags(resourceIDs []string, tags map[string]string) error {
	b.mu.Lock("CreateTags")
	defer b.mu.Unlock()

	for _, id := range resourceIDs {
		if b.tags[id] == nil {
			b.tags[id] = make(map[string]string)
		}

		maps.Copy(b.tags[id], tags)
	}

	return nil
}

// DeleteTags removes the specified tag keys from one or more resources.
// If keys is empty, the operation is a no-op (EC2 requires at least one tag key).
// Empty per-resource tag maps are removed after deletions.
func (b *InMemoryBackend) DeleteTags(resourceIDs []string, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	b.mu.Lock("DeleteTags")
	defer b.mu.Unlock()

	for _, id := range resourceIDs {
		if b.tags[id] == nil {
			continue
		}

		for _, k := range keys {
			delete(b.tags[id], k)
		}

		if len(b.tags[id]) == 0 {
			delete(b.tags, id)
		}
	}

	return nil
}

// DescribeTags returns all tag entries, optionally filtered by resource IDs.
func (b *InMemoryBackend) DescribeTags(resourceIDs []string) []TagEntry {
	b.mu.RLock("DescribeTags")
	defer b.mu.RUnlock()

	filterSet := make(map[string]bool, len(resourceIDs))
	for _, id := range resourceIDs {
		filterSet[id] = true
	}

	var entries []TagEntry

	for resourceID, tagMap := range b.tags {
		if len(filterSet) > 0 && !filterSet[resourceID] {
			continue
		}

		resType := resourceTypeByID(resourceID)

		for k, v := range tagMap {
			entries = append(entries, TagEntry{
				ResourceID:   resourceID,
				ResourceType: resType,
				Key:          k,
				Value:        v,
			})
		}
	}

	return entries
}
