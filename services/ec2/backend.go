package ec2

import (
	"errors"
	"fmt"
	"maps"
	"net"
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
	ErrSpotFleetNotFound     = errors.New("InvalidSpotFleetRequestId.NotFound")
	ErrCIDRConflict          = errors.New("InvalidVpc.Conflict")
	ErrDryRunOperation       = errors.New("request would have succeeded, but DryRun flag is set")
)

// EC2 instance state codes as defined by the AWS EC2 API.
const (
	stateCodeRunning      = 16
	stateCodeTerminated   = 48
	stateCodeStopped      = 80
	stateCodePending      = 0
	stateCodeShuttingDown = 32
	stateCodeStopping     = 64

	// stateAvailable is the "available" state string shared by volumes, ENIs,
	// and other resources that are not currently in use.
	stateAvailable = "available"

	stateInUse              = "in-use"
	stateCancelled          = "cancelled"
	resourceTypeVPC         = "vpc"
	vpcDefaultName          = "vpc-default"
	archX8664               = "x86_64"
	resourceTypeFISInstance = "aws:ec2:instance"
	ec2BooleanFalse         = "false"

	// stateActive is the "active" state string used by peering connections,
	// capacity reservations, and spot instance requests.
	stateActive = "active"
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
	LaunchTime            time.Time     `json:"launchTime"`
	TerminatedAt          time.Time     `json:"terminatedAt"`
	PublicDNSName         string        `json:"publicDNSName,omitempty"`
	KeyName               string        `json:"keyName"`
	InstanceType          string        `json:"instanceType"`
	ImageID               string        `json:"imageID"`
	VPCID                 string        `json:"vpcID"`
	SubnetID              string        `json:"subnetID"`
	MetadataOptionsTokens string        `json:"metadataOptionsTokens,omitempty"`
	ID                    string        `json:"id"`
	PrivateIP             string        `json:"privateIP"`
	PublicIPAddress       string        `json:"publicIPAddress,omitempty"`
	MetadataOptionsState  string        `json:"metadataOptionsState,omitempty"`
	UserData              string        `json:"userData,omitempty"`
	SriovNetSupport       string        `json:"sriovNetSupport,omitempty"`
	ProviderID            string        `json:"providerID,omitempty"`
	SecurityGroups        []string      `json:"securityGroups"`
	State                 InstanceState `json:"state"`
	SSHPort               int           `json:"sshPort,omitempty"`
	EnaSupport            bool          `json:"enaSupport"`
}

// LaunchTemplate represents an EC2 launch template.
type LaunchTemplate struct {
	CreateTime           time.Time `json:"createTime"`
	ID                   string    `json:"id"`
	Name                 string    `json:"name"`
	ImageID              string    `json:"imageID"`
	InstanceType         string    `json:"instanceType"`
	CreatedBy            string    `json:"createdBy"`
	DefaultVersionNumber int64     `json:"defaultVersionNumber"`
	LatestVersionNumber  int64     `json:"latestVersionNumber"`
}

// ImageUsageReport represents a synthetic AMI usage report entry.
type ImageUsageReport struct {
	GenerationDate string `json:"generationDate"`
	ImageID        string `json:"imageID"`
	State          string `json:"state"`
}

// VpcEndpoint represents an EC2 VPC endpoint.
type VpcEndpoint struct {
	CreateTime      time.Time `json:"createTime"`
	ID              string    `json:"id"`
	VPCID           string    `json:"vpcID"`
	ServiceName     string    `json:"serviceName"`
	State           string    `json:"state"`
	VpcEndpointType string    `json:"vpcEndpointType"`
	SubnetIDs       []string  `json:"subnetIDs"`
}

// NetworkACL represents an EC2 network ACL.
type NetworkACL struct {
	ID             string   `json:"id"`
	VPCID          string   `json:"vpcID"`
	AssociationIDs []string `json:"associationIDs"`
	IsDefault      bool     `json:"isDefault"`
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
// Either IPRange or SourceGroupID is set; both can be empty for protocol-only rules.
type SecurityGroupRule struct {
	Protocol           string `json:"protocol"`
	IPRange            string `json:"ipRange"`
	SourceGroupID      string `json:"sourceGroupId,omitempty"`
	SourceGroupOwnerID string `json:"sourceGroupOwnerId,omitempty"`
	FromPort           int    `json:"fromPort"`
	ToPort             int    `json:"toPort"`
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
	ID                  string `json:"id"`
	VPCID               string `json:"vpcID"`
	CIDRBlock           string `json:"cidrBlock"`
	AvailabilityZone    string `json:"availabilityZone"`
	IsDefault           bool   `json:"isDefault"`
	MapPublicIPOnLaunch bool   `json:"mapPublicIpOnLaunch"`
}

// InMemoryBackend is the in-memory store for EC2 resources.
type InMemoryBackend struct {
	addressTransfers               map[string]*AddressTransfer
	capacityReservations           map[string]*CapacityReservation
	vpcs                           map[string]*VPC
	subnets                        map[string]*Subnet
	keyPairs                       map[string]*KeyPair
	reservedInstancesExchanges     map[string]*ReservedInstancesExchange
	addresses                      map[string]*Address
	internetGateways               map[string]*InternetGateway
	natGateways                    map[string]*NatGateway
	routeTables                    map[string]*RouteTable
	placementGroups                map[string]*PlacementGroup
	spotRequests                   map[string]*SpotInstanceRequest
	instances                      map[string]*Instance
	images                         map[string]*AMIStub
	imageUsageReports              map[string]*ImageUsageReport
	launchTemplates                map[string]*LaunchTemplate
	vpcEndpoints                   map[string]*VpcEndpoint
	tags                           map[string]map[string]string
	securityGroups                 map[string]*SecurityGroup
	networkInterfaces              map[string]*NetworkInterface
	volumes                        map[string]*Volume
	tgwMulticastDomainAssociations map[string]*TransitGatewayMulticastDomainAssociation
	tgwPeeringAttachments          map[string]*TransitGatewayPeeringAttachment
	tgwVpcAttachments              map[string]*TransitGatewayVpcAttachment
	vpcEndpointConnections         map[string]*VpcEndpointConnection
	vpcPeeringConnections          map[string]*VpcPeeringConnection
	byoipCidrs                     map[string]*ByoipCidr
	dedicatedHosts                 map[string]*Host
	snapshots                      map[string]*Snapshot
	networkACLs                    map[string]*StoredNetworkACL
	transitGateways                map[string]*TransitGateway
	flowLogs                       map[string]*FlowLog
	dhcpOptionSets                 map[string]*DhcpOptions
	egressOnlyIGWs                 map[string]*EgressOnlyInternetGateway
	iamAssociations                map[string]*IamInstanceProfileAssociation
	tgwRouteTables                 map[string]*TransitGatewayRouteTable
	tgwRoutes                      map[string]*TransitGatewayRoute
	tgwRTAssociations              map[string]*TransitGatewayRouteTableAssociation
	vpcCidrAssociations            map[string]*VpcCidrBlockAssociation
	vpnGateways                    map[string]*VpnGateway
	customerGateways               map[string]*CustomerGateway
	vpnConnections                 map[string]*VpnConnection
	vpcEndpointServiceConfigs      map[string]*VpcEndpointServiceConfig
	ipams                          map[string]*Ipam
	ipamPools                      map[string]*IpamPool
	ipamPoolAllocations            map[string]*IpamPoolAllocation
	spotFleets                     map[string]*SpotFleetRequest
	spotFleetHistory               map[string][]SpotFleetHistoryRecord
	mu                             *lockmetrics.RWMutex
	eniIDByAttachment              map[string]string
	eniIDsByInstance               map[string]map[string]struct{}
	instanceIDsByVPC               map[string]map[string]struct{}
	compute                        Compute
	dnsRegistrar                   DNSRegistrar
	Region                         string
	AccountID                      string
	freePrivateIPs                 []string
	nextPrivateIPIndex             int
	nextElasticIPIndex             int
}

// NewInMemoryBackend creates a new InMemoryBackend with a default VPC and subnet.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		instances:                      make(map[string]*Instance),
		securityGroups:                 make(map[string]*SecurityGroup),
		vpcs:                           make(map[string]*VPC),
		subnets:                        make(map[string]*Subnet),
		keyPairs:                       make(map[string]*KeyPair),
		volumes:                        make(map[string]*Volume),
		addresses:                      make(map[string]*Address),
		internetGateways:               make(map[string]*InternetGateway),
		routeTables:                    make(map[string]*RouteTable),
		natGateways:                    make(map[string]*NatGateway),
		networkInterfaces:              make(map[string]*NetworkInterface),
		spotRequests:                   make(map[string]*SpotInstanceRequest),
		placementGroups:                make(map[string]*PlacementGroup),
		images:                         make(map[string]*AMIStub),
		imageUsageReports:              make(map[string]*ImageUsageReport),
		launchTemplates:                make(map[string]*LaunchTemplate),
		vpcEndpoints:                   make(map[string]*VpcEndpoint),
		tags:                           make(map[string]map[string]string),
		addressTransfers:               make(map[string]*AddressTransfer),
		capacityReservations:           make(map[string]*CapacityReservation),
		reservedInstancesExchanges:     make(map[string]*ReservedInstancesExchange),
		tgwMulticastDomainAssociations: make(map[string]*TransitGatewayMulticastDomainAssociation),
		tgwPeeringAttachments:          make(map[string]*TransitGatewayPeeringAttachment),
		tgwVpcAttachments:              make(map[string]*TransitGatewayVpcAttachment),
		vpcEndpointConnections:         make(map[string]*VpcEndpointConnection),
		vpcPeeringConnections:          make(map[string]*VpcPeeringConnection),
		byoipCidrs:                     make(map[string]*ByoipCidr),
		dedicatedHosts:                 make(map[string]*Host),
		snapshots:                      make(map[string]*Snapshot),
		networkACLs:                    make(map[string]*StoredNetworkACL),
		transitGateways:                make(map[string]*TransitGateway),
		flowLogs:                       make(map[string]*FlowLog),
		dhcpOptionSets:                 make(map[string]*DhcpOptions),
		egressOnlyIGWs:                 make(map[string]*EgressOnlyInternetGateway),
		iamAssociations:                make(map[string]*IamInstanceProfileAssociation),
		tgwRouteTables:                 make(map[string]*TransitGatewayRouteTable),
		tgwRoutes:                      make(map[string]*TransitGatewayRoute),
		tgwRTAssociations:              make(map[string]*TransitGatewayRouteTableAssociation),
		vpcCidrAssociations:            make(map[string]*VpcCidrBlockAssociation),
		vpnGateways:                    make(map[string]*VpnGateway),
		customerGateways:               make(map[string]*CustomerGateway),
		vpnConnections:                 make(map[string]*VpnConnection),
		vpcEndpointServiceConfigs:      make(map[string]*VpcEndpointServiceConfig),
		ipams:                          make(map[string]*Ipam),
		ipamPools:                      make(map[string]*IpamPool),
		ipamPoolAllocations:            make(map[string]*IpamPoolAllocation),
		spotFleets:                     make(map[string]*SpotFleetRequest),
		spotFleetHistory:               make(map[string][]SpotFleetHistoryRecord),
		instanceIDsByVPC:               make(map[string]map[string]struct{}),
		eniIDsByInstance:               make(map[string]map[string]struct{}),
		eniIDByAttachment:              make(map[string]string),
		AccountID:                      accountID,
		Region:                         region,
		mu:                             lockmetrics.New("ec2"),
	}

	b.initDefaults()

	return b
}

// initDefaults pre-populates a default VPC, subnet, and security group.
func (b *InMemoryBackend) initDefaults() {
	defaultVPCID := vpcDefaultName
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
func (b *InMemoryBackend) RunInstances(
	imageID, instanceType, subnetID string,
	count int,
) ([]*Instance, error) {
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
	mapPublicIP := false

	if sub, ok := b.subnets[subnetID]; ok {
		vpcID = sub.VPCID
		mapPublicIP = sub.MapPublicIPOnLaunch
	}

	// No capacity hint — user-derived values in the make capacity position
	// trigger CodeQL go/slice-memory-allocation-excessive-size even after
	// clamping. count is only used for the loop count below (safe).
	// nolint:prealloc,nolintlint // satisfies CodeQL by removing tainted capacity hint
	instances := make([]*Instance, 0)

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
			EnaSupport: true,
		}
		inst.PrivateIP = b.allocPrivateIP()
		if mapPublicIP {
			inst.PublicIPAddress = b.allocElasticIP()
			inst.PublicDNSName = fmt.Sprintf("ec2-%s.compute-1.amazonaws.com",
				strings.ReplaceAll(inst.PublicIPAddress, ".", "-"))
		}
		eniID := "eni-" + uuid.New().String()[:17]
		attachID := "eni-attach-" + uuid.New().String()[:8]
		b.networkInterfaces[eniID] = &NetworkInterface{
			ID:              eniID,
			SubnetID:        subnetID,
			VPCID:           vpcID,
			PrivateIP:       inst.PrivateIP,
			InstanceID:      id,
			AttachmentID:    attachID,
			DeviceIndex:     0,
			Status:          stateInUse,
			SourceDestCheck: true,
		}
		b.instances[id] = inst
		b.indexInstanceLocked(inst)
		b.indexENILocked(eniID, b.networkInterfaces[eniID])
		instances = append(instances, inst)
	}

	return instances, nil
}

// DNSRegistrar can register and deregister hostnames with an embedded DNS
// server. It mirrors the optional registrar interfaces used by RDS, OpenSearch
// and friends so the EC2 docker-compute provider can publish synthetic
// AWS-style instance hostnames (e.g. ec2-1-2-3-4.compute-1.amazonaws.com) that
// resolve to the host the container is reachable on.
type DNSRegistrar interface {
	Register(hostname string)
	Deregister(hostname string)
}

// WithCompute installs an optional Compute provider. When non-nil, RunInstances,
// TerminateInstances, StartInstances and StopInstances will call into the
// provider after updating in-memory state. Passing nil disables the hook.
func (b *InMemoryBackend) WithCompute(c Compute) {
	b.mu.Lock("WithCompute")
	defer b.mu.Unlock()
	b.compute = c
}

// Compute returns the currently installed Compute provider (may be nil).
//
//nolint:ireturn // returning the configured interface is the intent
func (b *InMemoryBackend) Compute() Compute {
	b.mu.RLock("Compute")
	defer b.mu.RUnlock()

	return b.compute
}

// SetDNSRegistrar wires an embedded DNS server so synthetic instance
// hostnames produced by the Compute provider can be resolved by callers
// outside the gopherstack process.
func (b *InMemoryBackend) SetDNSRegistrar(r DNSRegistrar) {
	b.mu.Lock("SetDNSRegistrar")
	defer b.mu.Unlock()
	b.dnsRegistrar = r
}

// DNSRegistrar returns the configured DNS registrar (may be nil).
//
//nolint:ireturn // returning the configured interface is the intent
func (b *InMemoryBackend) DNSRegistrar() DNSRegistrar {
	b.mu.RLock("DNSRegistrar")
	defer b.mu.RUnlock()

	return b.dnsRegistrar
}

// LookupKeyPairAuthorizedKey returns the OpenSSH authorized_keys-format public
// key for the named key pair, or empty string when the key pair is unknown or
// has no derivable public key. Used by Compute providers to seed the launched
// container's authorized_keys.
func (b *InMemoryBackend) LookupKeyPairAuthorizedKey(name string) string {
	b.mu.RLock("LookupKeyPairAuthorizedKey")
	defer b.mu.RUnlock()

	kp, ok := b.keyPairs[name]
	if !ok {
		return ""
	}

	return kp.PublicKey
}

// SetComputeResult merges a LaunchResult onto an existing instance and its
// primary ENI. Empty string fields in the result are ignored. Returns
// ErrInstanceNotFound when the instance has been removed before the compute
// provider returned.
func (b *InMemoryBackend) SetComputeResult(instanceID string, r LaunchResult) error {
	b.mu.Lock("SetComputeResult")
	defer b.mu.Unlock()

	inst, ok := b.instances[instanceID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	if r.ProviderID != "" {
		inst.ProviderID = r.ProviderID
	}

	if r.PrivateIP != "" {
		oldPrivate := inst.PrivateIP
		inst.PrivateIP = r.PrivateIP
		// keep primary ENI in sync so DescribeNetworkInterfaces shows the
		// container's actual address
		for eniID := range b.eniIDsByInstance[instanceID] {
			if eni, exists := b.networkInterfaces[eniID]; exists && eni.PrivateIP == oldPrivate {
				eni.PrivateIP = r.PrivateIP
			}
		}
	}

	if r.PublicIPAddress != "" {
		inst.PublicIPAddress = r.PublicIPAddress
	}

	if r.PublicDNSName != "" {
		inst.PublicDNSName = r.PublicDNSName
	}

	if r.SSHPort != 0 {
		inst.SSHPort = r.SSHPort
	}

	return nil
}

// LookupInstanceProviderID returns the ProviderID stored on an instance, or
// empty string when the instance is unknown.
func (b *InMemoryBackend) LookupInstanceProviderID(instanceID string) string {
	b.mu.RLock("LookupInstanceProviderID")
	defer b.mu.RUnlock()

	inst, ok := b.instances[instanceID]
	if !ok {
		return ""
	}

	return inst.ProviderID
}

// LookupInstancePublicDNSName returns the synthetic public DNS name assigned
// to an instance, or empty string when the instance is unknown or has none.
func (b *InMemoryBackend) LookupInstancePublicDNSName(instanceID string) string {
	b.mu.RLock("LookupInstancePublicDNSName")
	defer b.mu.RUnlock()

	inst, ok := b.instances[instanceID]
	if !ok {
		return ""
	}

	return inst.PublicDNSName
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
// When ids are provided, lookups are O(len(ids)) via the instance map rather
// than scanning every instance in the backend.
func (b *InMemoryBackend) DescribeInstances(ids []string, state string) []*Instance {
	b.mu.RLock("DescribeInstances")
	defer b.mu.RUnlock()

	if len(ids) > 0 {
		out := make([]*Instance, 0, len(ids))

		for _, id := range ids {
			inst, ok := b.instances[id]
			if !ok {
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

	out := make([]*Instance, 0, len(b.instances))

	for _, inst := range b.instances {
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
// Terminated instances remain visible (matching AWS ~1 hour grace period)
// until the janitor sweeps them.
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
		inst.TerminatedAt = time.Now()
		result = append(result, &InstanceStateChange{
			InstanceID:    id,
			PreviousState: prev,
			CurrentState:  inst.State,
		})

		// Mirror AWS behaviour: when the backing instance of a spot request is
		// terminated, the request transitions to "closed" (not stateCancelled).
		for _, req := range b.spotRequests {
			if req.InstanceID == id && req.State == stateActive {
				req.State = "closed"
				req.CancelledAt = time.Now()
			}
		}

		// Delete all ENIs attached to the terminated instance and recycle their
		// private IPs. This mirrors the AWS behaviour where all network interfaces
		// are deleted when an instance is terminated (deleteOnTermination=true is
		// the default for all network interfaces attached at launch).
		eniIDs := b.eniIDsByInstance[id]
		for eniID := range eniIDs {
			eni, exists := b.networkInterfaces[eniID]
			if !exists {
				continue
			}

			b.deindexENILocked(eniID, eni)
			b.recycleENIIPsLocked(eni)
			delete(b.networkInterfaces, eniID)
			delete(b.tags, eniID)
		}
		b.deindexInstanceLocked(inst)

		// Detach any EBS volumes whose attachment refers to this instance so they
		// return to "available" state. AWS deletes the root volume by default
		// (deleteOnTermination=true) but detaches additional volumes; since we do
		// not track the deleteOnTermination flag per attachment, detaching all of
		// them is the safe, non-destructive equivalent.
		// Also disassociate any Elastic IPs associated with the terminated instance.
		b.detachVolumesAndEIPsLocked(id)
	}

	return result, nil
}

// DescribeSecurityGroups returns security groups, optionally filtered by IDs.
// When ids are provided, lookups are O(len(ids)) via the security-group map
// rather than scanning every group in the backend.
func (b *InMemoryBackend) DescribeSecurityGroups(ids []string) []*SecurityGroup {
	b.mu.RLock("DescribeSecurityGroups")
	defer b.mu.RUnlock()

	if len(ids) > 0 {
		out := make([]*SecurityGroup, 0, len(ids))

		for _, id := range ids {
			sg, ok := b.securityGroups[id]
			if !ok {
				continue
			}

			cp := *sg
			out = append(out, &cp)
		}

		return out
	}

	out := make([]*SecurityGroup, 0, len(b.securityGroups))

	for _, sg := range b.securityGroups {
		cp := *sg
		out = append(out, &cp)
	}

	return out
}

// CreateSecurityGroup creates a new security group and returns its ID.
func (b *InMemoryBackend) CreateSecurityGroup(
	name, description, vpcID string,
) (*SecurityGroup, error) {
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
			return nil, fmt.Errorf(
				"%w: group named %s already exists in VPC %s",
				ErrDuplicateSGName,
				name,
				vpcID,
			)
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
	delete(b.tags, id)

	return nil
}

// DescribeVpcs returns VPCs, optionally filtered by IDs.
// When ids are provided, lookups are O(len(ids)) via the VPC map rather than
// scanning every VPC in the backend.
func (b *InMemoryBackend) DescribeVpcs(ids []string) []*VPC {
	b.mu.RLock("DescribeVpcs")
	defer b.mu.RUnlock()

	if len(ids) > 0 {
		out := make([]*VPC, 0, len(ids))

		for _, id := range ids {
			v, ok := b.vpcs[id]
			if !ok {
				continue
			}

			cp := *v
			out = append(out, &cp)
		}

		return out
	}

	out := make([]*VPC, 0, len(b.vpcs))

	for _, v := range b.vpcs {
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

	for _, existing := range b.vpcs {
		if cidrsOverlap(cidr, existing.CIDRBlock) {
			return nil, fmt.Errorf("%w: CIDR %s overlaps with existing VPC %s (%s)",
				ErrCIDRConflict, cidr, existing.ID, existing.CIDRBlock)
		}
	}

	id := "vpc-" + uuid.New().String()[:17]
	v := &VPC{
		ID:        id,
		CIDRBlock: cidr,
	}
	b.vpcs[id] = v

	return v, nil
}

// cascadeDeleteVpcIGWsLocked removes all internet gateways that have an
// attachment to the given VPC. Must be called with b.mu held.
func (b *InMemoryBackend) cascadeDeleteVpcIGWsLocked(vpcID string) {
	for igwID, igw := range b.internetGateways {
		for _, att := range igw.Attachments {
			if att.VPCID == vpcID {
				delete(b.internetGateways, igwID)
				delete(b.tags, igwID)

				break
			}
		}
	}
}

// DeleteVpc removes a VPC by ID, cascade-deleting all dependent resources
// (instances, internet gateways, NAT gateways, route tables, security groups,
// network interfaces, and subnets) along with their tags.
func (b *InMemoryBackend) DeleteVpc(id string) error {
	b.mu.Lock("DeleteVpc")
	defer b.mu.Unlock()

	if _, ok := b.vpcs[id]; !ok {
		return fmt.Errorf("%w: %s", ErrVPCNotFound, id)
	}

	// Cascade: terminate instances belonging to this VPC.
	for instID, inst := range b.instances {
		if inst.VPCID == id {
			inst.State = StateTerminated
			inst.TerminatedAt = time.Now()
			delete(b.tags, instID)
			b.detachVolumesAndEIPsLocked(instID)
		}
	}

	// Cascade: detach and delete internet gateways attached to this VPC.
	b.cascadeDeleteVpcIGWsLocked(id)

	// Cascade: delete NAT gateways in subnets belonging to this VPC.
	for ngwID, ngw := range b.natGateways {
		if sub, ok := b.subnets[ngw.SubnetID]; ok && sub.VPCID == id {
			b.recycleIPLocked(ngw.PrivateIP)
			delete(b.natGateways, ngwID)
			delete(b.tags, ngwID)
		}
	}

	// Cascade: remove route tables belonging to this VPC.
	for rtID, rt := range b.routeTables {
		if rt.VPCID == id {
			delete(b.routeTables, rtID)
			delete(b.tags, rtID)
		}
	}

	// Cascade: remove security groups belonging to this VPC.
	for sgID, sg := range b.securityGroups {
		if sg.VPCID == id {
			delete(b.securityGroups, sgID)
			delete(b.tags, sgID)
		}
	}

	// Cascade: remove network interfaces belonging to this VPC.
	for eniID, eni := range b.networkInterfaces {
		if eni.VPCID == id {
			b.recycleENIIPsLocked(eni)
			delete(b.networkInterfaces, eniID)
			delete(b.tags, eniID)
		}
	}

	// Cascade: remove subnets belonging to this VPC.
	for subnetID, subnet := range b.subnets {
		if subnet.VPCID == id {
			delete(b.subnets, subnetID)
			delete(b.tags, subnetID)
		}
	}

	delete(b.vpcs, id)
	delete(b.tags, id)

	return nil
}

// DescribeSubnets returns subnets, optionally filtered by IDs.
// When ids are provided, lookups are O(len(ids)) via the subnet map rather
// than scanning every subnet in the backend.
func (b *InMemoryBackend) DescribeSubnets(ids []string) []*Subnet {
	b.mu.RLock("DescribeSubnets")
	defer b.mu.RUnlock()

	if len(ids) > 0 {
		out := make([]*Subnet, 0, len(ids))

		for _, id := range ids {
			s, ok := b.subnets[id]
			if !ok {
				continue
			}

			cp := *s
			out = append(out, &cp)
		}

		return out
	}

	out := make([]*Subnet, 0, len(b.subnets))

	for _, s := range b.subnets {
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

	vpc := b.vpcs[vpcID]
	if !cidrContains(vpc.CIDRBlock, cidr) {
		return nil, fmt.Errorf("%w: subnet CIDR %s is not within VPC CIDR %s",
			ErrInvalidParameter, cidr, vpc.CIDRBlock)
	}

	for _, existing := range b.subnets {
		if existing.VPCID == vpcID && cidrsOverlap(cidr, existing.CIDRBlock) {
			return nil, fmt.Errorf("%w: CIDR %s overlaps with existing subnet %s (%s)",
				ErrCIDRConflict, cidr, existing.ID, existing.CIDRBlock)
		}
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

// DeleteSubnet removes a subnet by ID, cascade-deleting any instances, NAT
// gateways, and network interfaces in that subnet along with their tags.
func (b *InMemoryBackend) DeleteSubnet(id string) error {
	b.mu.Lock("DeleteSubnet")
	defer b.mu.Unlock()

	if _, ok := b.subnets[id]; !ok {
		return fmt.Errorf("%w: %s", ErrSubnetNotFound, id)
	}

	// Cascade: terminate instances in this subnet.
	for instID, inst := range b.instances {
		if inst.SubnetID == id {
			inst.State = StateTerminated
			inst.TerminatedAt = time.Now()
			delete(b.tags, instID)
			b.detachVolumesAndEIPsLocked(instID)
		}
	}

	// Cascade: delete NAT gateways in this subnet.
	for ngwID, ngw := range b.natGateways {
		if ngw.SubnetID == id {
			b.recycleIPLocked(ngw.PrivateIP)
			delete(b.natGateways, ngwID)
			delete(b.tags, ngwID)
		}
	}

	// Cascade: remove network interfaces in this subnet.
	for eniID, eni := range b.networkInterfaces {
		if eni.SubnetID == id {
			b.recycleENIIPsLocked(eni)
			delete(b.networkInterfaces, eniID)
			delete(b.tags, eniID)
		}
	}

	delete(b.subnets, id)
	delete(b.tags, id)

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
		{"vpc-", resourceTypeVPC},
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

// recycleIPLocked adds ip to the free list if it is an auto-allocated
// 172.31.x.y address and the free list has capacity remaining.
// Must be called with b.mu held.
func (b *InMemoryBackend) recycleIPLocked(ip string) {
	if strings.HasPrefix(ip, "172.31.") && len(b.freePrivateIPs) < maxFreePrivateIPs {
		b.freePrivateIPs = append(b.freePrivateIPs, ip)
	}
}

// recycleENIIPsLocked returns the primary and secondary auto-allocated private
// IPs from an ENI back to the free list so they can be reused.
// Must be called with b.mu held.
func (b *InMemoryBackend) recycleENIIPsLocked(eni *NetworkInterface) {
	b.recycleIPLocked(eni.PrivateIP)

	for _, ip := range eni.SecondaryPrivateIPs {
		b.recycleIPLocked(ip)
	}
}

// detachVolumesAndEIPsLocked clears volume attachments and Elastic IP
// associations that reference instanceID so that no dangling references remain
// after the instance transitions to the terminated state.
// Must be called with b.mu held.
func (b *InMemoryBackend) detachVolumesAndEIPsLocked(instanceID string) {
	for _, vol := range b.volumes {
		if vol.Attachment != nil && vol.Attachment.InstanceID == instanceID {
			vol.Attachment = nil
			vol.State = stateAvailable
		}
	}

	for _, addr := range b.addresses {
		if addr.InstanceID == instanceID {
			addr.AssociationID = ""
			addr.InstanceID = ""
		}
	}
}

// resourceExistsLocked reports whether id refers to any known EC2 resource.
// Must be called with b.mu held.
func (b *InMemoryBackend) resourceExistsLocked(id string) bool {
	if _, ok := b.instances[id]; ok {
		return true
	}

	if _, ok := b.securityGroups[id]; ok {
		return true
	}

	if _, ok := b.vpcs[id]; ok {
		return true
	}

	if _, ok := b.subnets[id]; ok {
		return true
	}

	if _, ok := b.keyPairs[id]; ok {
		return true
	}

	if _, ok := b.volumes[id]; ok {
		return true
	}

	if _, ok := b.addresses[id]; ok {
		return true
	}

	if _, ok := b.internetGateways[id]; ok {
		return true
	}

	if _, ok := b.routeTables[id]; ok {
		return true
	}

	if _, ok := b.natGateways[id]; ok {
		return true
	}

	if _, ok := b.networkInterfaces[id]; ok {
		return true
	}

	if _, ok := b.spotRequests[id]; ok {
		return true
	}

	if _, ok := b.placementGroups[id]; ok {
		return true
	}

	return false
}

// CreateTags adds or updates tags on one or more resources.
// Returns ErrInvalidParameter if any resource ID does not exist.
// All IDs are validated before any tags are written, making the operation atomic
// with respect to failures: either all resources are tagged or none are.
func (b *InMemoryBackend) CreateTags(resourceIDs []string, tags map[string]string) error {
	b.mu.Lock("CreateTags")
	defer b.mu.Unlock()

	// Pre-validate: all resource IDs must exist before any tags are written.
	for _, id := range resourceIDs {
		if !b.resourceExistsLocked(id) {
			return fmt.Errorf("%w: resource %s does not exist", ErrInvalidParameter, id)
		}
	}

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

// TagsForResource returns a copy of the tags currently set on the given
// resource, or an empty map when nothing is tagged. Safe for concurrent use.
func (b *InMemoryBackend) TagsForResource(resourceID string) map[string]string {
	b.mu.RLock("TagsForResource")
	defer b.mu.RUnlock()

	src, ok := b.tags[resourceID]
	if !ok || len(src) == 0 {
		return map[string]string{}
	}

	out := make(map[string]string, len(src))
	maps.Copy(out, src)

	return out
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

// cidrsOverlap reports whether two CIDR blocks overlap.
// Malformed CIDRs are treated as non-overlapping to avoid panics on bad input.
func cidrsOverlap(a, b string) bool {
	_, netA, err1 := net.ParseCIDR(a)
	_, netB, err2 := net.ParseCIDR(b)

	if err1 != nil || err2 != nil {
		return false
	}

	return netA.Contains(netB.IP) || netB.Contains(netA.IP)
}

// cidrContains reports whether outer fully contains inner.
func cidrContains(outer, inner string) bool {
	_, outerNet, err1 := net.ParseCIDR(outer)
	_, innerNet, err2 := net.ParseCIDR(inner)

	if err1 != nil || err2 != nil {
		return false
	}

	// outer must contain inner's base address and inner's broadcast address
	ones1, _ := outerNet.Mask.Size()
	ones2, _ := innerNet.Mask.Size()

	return outerNet.Contains(innerNet.IP) && ones1 <= ones2
}
