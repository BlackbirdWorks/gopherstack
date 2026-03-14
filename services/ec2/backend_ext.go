package ec2

import (
	"crypto/md5" //nolint:gosec // MD5 used for fingerprint display only, not security
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Additional errors for extended EC2 operations.
var (
	ErrKeyPairNotFound             = errors.New("InvalidKeyPair.NotFound")
	ErrDuplicateKeyPairName        = errors.New("InvalidKeyPair.Duplicate")
	ErrVolumeNotFound              = errors.New("InvalidVolume.NotFound")
	ErrVolumeInUse                 = errors.New("VolumeInUse")
	ErrAddressNotFound             = errors.New("InvalidAllocationID.NotFound")
	ErrInternetGatewayNotFound     = errors.New("InvalidInternetGatewayID.NotFound")
	ErrRouteTableNotFound          = errors.New("InvalidRouteTableID.NotFound")
	ErrNatGatewayNotFound          = errors.New("InvalidNatGatewayID.NotFound")
	ErrNetworkInterfaceNotFound    = errors.New("InvalidNetworkInterfaceID.NotFound")
	ErrNetworkInterfaceInUse       = errors.New("InvalidParameterValue.NetworkInterfaceInUse")
	ErrAttachmentNotFound          = errors.New("InvalidAttachmentID.NotFound")
	ErrSpotRequestNotFound         = errors.New("InvalidSpotInstanceRequestID.NotFound")
	ErrPlacementGroupNotFound      = errors.New("InvalidPlacementGroup.NotFound")
	ErrDuplicatePlacementGroupName = errors.New("InvalidPlacementGroup.Duplicate")
	ErrRouteNotFound               = errors.New("InvalidRoute.NotFound")
	ErrAssociationNotFound         = errors.New("InvalidAssociationID.NotFound")
)

// Attribute name and boolean-string constants shared across backend and handler.
const (
	attrSourceDest = "sourceDestCheck"
	ec2BooleanTrue = "true"
)

// KeyPair represents an EC2 key pair.
type KeyPair struct {
	Name        string `json:"name"`
	Fingerprint string `json:"fingerprint"`
	Material    string `json:"material,omitempty"` // only on create
}

// Volume represents an EBS volume.
type Volume struct {
	CreateTime time.Time         `json:"createTime"`
	Attachment *VolumeAttachment `json:"attachment,omitempty"`
	ID         string            `json:"id"`
	AZ         string            `json:"az"`
	VolumeType string            `json:"volumeType"`
	State      string            `json:"state"`
	Size       int               `json:"size"`
}

// VolumeAttachment represents the attachment state of a volume.
type VolumeAttachment struct {
	AttachTime time.Time `json:"attachTime"`
	VolumeID   string    `json:"volumeID"`
	InstanceID string    `json:"instanceID"`
	Device     string    `json:"device"`
	State      string    `json:"state"`
}

// Address represents an Elastic IP address.
type Address struct {
	AllocationID  string `json:"allocationID"`
	AssociationID string `json:"associationID,omitempty"`
	PublicIP      string `json:"publicIP"`
	InstanceID    string `json:"instanceID,omitempty"`
}

// IGWAttachment represents the attachment of an Internet Gateway to a VPC.
type IGWAttachment struct {
	VPCID string `json:"vpcID"`
	State string `json:"state"`
}

// InternetGateway represents an EC2 Internet Gateway.
type InternetGateway struct {
	ID          string          `json:"id"`
	Attachments []IGWAttachment `json:"attachments"`
}

// Route represents a route table entry.
type Route struct {
	DestinationCIDR string `json:"destinationCIDR"`
	GatewayID       string `json:"gatewayID,omitempty"`
	NatGatewayID    string `json:"natGatewayID,omitempty"`
	State           string `json:"state"`
}

// RouteAssociation represents an association between a route table and a subnet.
type RouteAssociation struct {
	ID           string `json:"id"`
	RouteTableID string `json:"routeTableID"`
	SubnetID     string `json:"subnetID"`
}

// RouteTable represents an EC2 Route Table.
type RouteTable struct {
	ID           string             `json:"id"`
	VPCID        string             `json:"vpcID"`
	Routes       []Route            `json:"routes"`
	Associations []RouteAssociation `json:"associations"`
}

// NatGateway represents an EC2 NAT Gateway.
type NatGateway struct {
	CreateTime   time.Time `json:"createTime"`
	ID           string    `json:"id"`
	SubnetID     string    `json:"subnetID"`
	AllocationID string    `json:"allocationID"`
	PublicIP     string    `json:"publicIP"`
	PrivateIP    string    `json:"privateIP"`
	State        string    `json:"state"`
}

// NetworkInterface represents an EC2 Network Interface (ENI).
type NetworkInterface struct {
	ID                  string   `json:"id"`
	SubnetID            string   `json:"subnetID"`
	VPCID               string   `json:"vpcID"`
	PrivateIP           string   `json:"privateIP"`
	Description         string   `json:"description"`
	InstanceID          string   `json:"instanceID,omitempty"`
	AttachmentID        string   `json:"attachmentID,omitempty"`
	Status              string   `json:"status"`
	SecondaryPrivateIPs []string `json:"secondaryPrivateIPs,omitempty"`
	DeviceIndex         int      `json:"deviceIndex,omitempty"`
	SourceDestCheck     bool     `json:"sourceDestCheck"`
}

// SpotLaunchSpecification holds launch parameters for a spot instance request.
type SpotLaunchSpecification struct {
	ImageID      string `json:"imageID"`
	InstanceType string `json:"instanceType"`
	SubnetID     string `json:"subnetID"`
}

// SpotInstanceRequest represents an EC2 spot instance request.
type SpotInstanceRequest struct {
	CreateTime time.Time               `json:"createTime"`
	LaunchSpec SpotLaunchSpecification `json:"launchSpec"`
	ID         string                  `json:"id"`
	InstanceID string                  `json:"instanceID,omitempty"`
	State      string                  `json:"state"`
	SpotPrice  string                  `json:"spotPrice"`
	Type       string                  `json:"type"`
}

// PlacementGroup represents an EC2 placement group.
type PlacementGroup struct {
	Name     string `json:"name"`
	Strategy string `json:"strategy"`
	State    string `json:"state"`
}

// AMIStub is a static image entry.
type AMIStub struct {
	ImageID        string
	Name           string
	Description    string
	Architecture   string
	Platform       string
	RootDeviceName string
}

//nolint:gochecknoglobals // package-level stub data for describe operations
var stubAMIs = []AMIStub{
	{
		ImageID:        "ami-0c55b159cbfafe1f0",
		Name:           "amzn2-ami-hvm",
		Description:    "Amazon Linux 2 (x86_64)",
		Architecture:   "x86_64",
		RootDeviceName: "/dev/xvda",
	},
	{
		ImageID:        "ami-0eb260c4d5475b901",
		Name:           "ubuntu-22.04-lts",
		Description:    "Ubuntu 22.04 LTS (x86_64)",
		Architecture:   "x86_64",
		RootDeviceName: "/dev/sda1",
	},
	{
		ImageID:        "ami-09d3b3274b6c5d4aa",
		Name:           "windows-server-2022",
		Description:    "Windows Server 2022",
		Architecture:   "x86_64",
		Platform:       "windows",
		RootDeviceName: "/dev/sda1",
	},
}

//nolint:gochecknoglobals // package-level stub data for describe operations
var stubRegions = []string{
	"us-east-1", "us-east-2", "us-west-1", "us-west-2",
	"eu-west-1", "eu-west-2", "eu-central-1",
	"ap-southeast-1", "ap-southeast-2", "ap-northeast-1",
}

const (
	privateIPOctetRange = 253
	elasticIPOctetRange = 254
	rsaKeyBits          = 2048
	// stubFingerprintUUIDLen is the number of UUID hex characters used to build
	// a stub fingerprint for ImportKeyPair (no actual public key is parsed).
	stubFingerprintUUIDLen = 11
)

// allocPrivateIP returns the next 172.31.x.y private IP. Must be called with mu held.
func (b *InMemoryBackend) allocPrivateIP() string {
	idx := b.nextPrivateIPIndex
	b.nextPrivateIPIndex++
	third := idx / privateIPOctetRange
	fourth := idx%privateIPOctetRange + 1

	return fmt.Sprintf("172.31.%d.%d", third, fourth)
}

// allocElasticIP returns the next 54.230.x.y elastic IP. Must be called with mu held.
func (b *InMemoryBackend) allocElasticIP() string {
	idx := b.nextElasticIPIndex
	b.nextElasticIPIndex++
	third := idx / elasticIPOctetRange
	fourth := idx%elasticIPOctetRange + 1

	return fmt.Sprintf("54.230.%d.%d", third, fourth)
}

// keyFingerprint computes the MD5 fingerprint of an RSA public key in DER form.
func keyFingerprint(pubKey *rsa.PublicKey) (string, error) {
	der, err := x509.MarshalPKIXPublicKey(pubKey)
	if err != nil {
		return "", err
	}

	sum := md5.Sum(der) //nolint:gosec // MD5 used for fingerprint display only, not security
	parts := make([]string, len(sum))

	for i, by := range sum {
		parts[i] = fmt.Sprintf("%02x", by)
	}

	return strings.Join(parts, ":"), nil
}

// StartInstances transitions stopped instances to running.
// Returns ErrInvalidInstanceState if any instance is not in the stopped state.
func (b *InMemoryBackend) StartInstances(ids []string) ([]*InstanceStateChange, error) {
	b.mu.Lock("StartInstances")
	defer b.mu.Unlock()

	var result []*InstanceStateChange

	for _, id := range ids {
		inst, ok := b.instances[id]
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, id)
		}

		if inst.State != StateStopped {
			return nil, fmt.Errorf(
				"%w: instance %s is in state %s, expected stopped",
				ErrInvalidInstanceState,
				id,
				inst.State.Name,
			)
		}

		prev := inst.State
		// AWS state machine: stopped → pending → running.
		// The mock completes this transition immediately.
		inst.State = StateRunning
		result = append(result, &InstanceStateChange{
			InstanceID:    id,
			PreviousState: prev,
			CurrentState:  inst.State,
		})
	}

	return result, nil
}

// StopInstances transitions running instances to stopped.
// Returns ErrInvalidInstanceState if any instance is not in the running state.
func (b *InMemoryBackend) StopInstances(ids []string) ([]*InstanceStateChange, error) {
	b.mu.Lock("StopInstances")
	defer b.mu.Unlock()

	var result []*InstanceStateChange

	for _, id := range ids {
		inst, ok := b.instances[id]
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, id)
		}

		if inst.State != StateRunning {
			return nil, fmt.Errorf(
				"%w: instance %s is in state %s, expected running",
				ErrInvalidInstanceState,
				id,
				inst.State.Name,
			)
		}

		prev := inst.State
		// AWS state machine: running → stopping → stopped.
		// The mock completes this transition immediately.
		inst.State = StateStopped
		result = append(result, &InstanceStateChange{
			InstanceID:    id,
			PreviousState: prev,
			CurrentState:  inst.State,
		})
	}

	return result, nil
}

// RebootInstances validates that all given instances exist (no state change).
func (b *InMemoryBackend) RebootInstances(ids []string) error {
	b.mu.RLock("RebootInstances")
	defer b.mu.RUnlock()

	for _, id := range ids {
		if _, ok := b.instances[id]; !ok {
			return fmt.Errorf("%w: %s", ErrInstanceNotFound, id)
		}
	}

	return nil
}

// DescribeInstanceStatus returns instances for status reporting.
func (b *InMemoryBackend) DescribeInstanceStatus(ids []string) []*Instance {
	b.mu.RLock("DescribeInstanceStatus")
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

		cp := *inst
		out = append(out, &cp)
	}

	return out
}

// DescribeImages returns stub AMIs.
func (b *InMemoryBackend) DescribeImages() []AMIStub {
	return stubAMIs
}

// DescribeRegions returns stub region names.
func (b *InMemoryBackend) DescribeRegions() []string {
	return stubRegions
}

// DescribeAvailabilityZones returns AZs for a region.
func (b *InMemoryBackend) DescribeAvailabilityZones(region string) []string {
	if region == "" {
		region = b.Region
	}

	return []string{region + "a", region + "b", region + "c"}
}

// CreateKeyPair generates a new RSA key pair.
func (b *InMemoryBackend) CreateKeyPair(name string) (*KeyPair, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: KeyName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateKeyPair")
	defer b.mu.Unlock()

	if _, exists := b.keyPairs[name]; exists {
		return nil, fmt.Errorf("%w: %s", ErrDuplicateKeyPairName, name)
	}

	privKey, err := rsa.GenerateKey(rand.Reader, rsaKeyBits)
	if err != nil {
		return nil, fmt.Errorf("failed to generate key: %w", err)
	}

	fp, err := keyFingerprint(&privKey.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("failed to compute fingerprint: %w", err)
	}

	privDER := x509.MarshalPKCS1PrivateKey(privKey)
	privPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: privDER})

	kp := &KeyPair{
		Name:        name,
		Fingerprint: fp,
		Material:    string(privPEM),
	}
	b.keyPairs[name] = kp

	return kp, nil
}

// ImportKeyPair stores a pre-existing key pair by name without key material.
// This matches the AWS ImportKeyPair API which accepts only the public key material
// from the caller. In the mock we simply register the name with a stub fingerprint.
func (b *InMemoryBackend) ImportKeyPair(name string) (*KeyPair, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: KeyName is required", ErrInvalidParameter)
	}

	b.mu.Lock("ImportKeyPair")
	defer b.mu.Unlock()

	if _, exists := b.keyPairs[name]; exists {
		return nil, fmt.Errorf("%w: %s", ErrDuplicateKeyPairName, name)
	}

	kp := &KeyPair{
		Name:        name,
		Fingerprint: fmt.Sprintf("aa:bb:cc:dd:%s", uuid.New().String()[:stubFingerprintUUIDLen]),
	}
	b.keyPairs[name] = kp

	return kp, nil
}

// DescribeKeyPairs returns key pairs, optionally filtered by name.
func (b *InMemoryBackend) DescribeKeyPairs(names []string) []*KeyPair {
	b.mu.RLock("DescribeKeyPairs")
	defer b.mu.RUnlock()

	nameSet := make(map[string]bool, len(names))
	for _, n := range names {
		nameSet[n] = true
	}

	var out []*KeyPair

	for _, kp := range b.keyPairs {
		if len(nameSet) > 0 && !nameSet[kp.Name] {
			continue
		}

		cp := *kp
		cp.Material = "" // don't return private key material on describe
		out = append(out, &cp)
	}

	return out
}

// DeleteKeyPair removes a key pair by name.
func (b *InMemoryBackend) DeleteKeyPair(name string) error {
	b.mu.Lock("DeleteKeyPair")
	defer b.mu.Unlock()

	if _, ok := b.keyPairs[name]; !ok {
		return fmt.Errorf("%w: %s", ErrKeyPairNotFound, name)
	}

	delete(b.keyPairs, name)

	return nil
}

// CreateVolume creates a new EBS volume stub.
func (b *InMemoryBackend) CreateVolume(az, volType string, size int) (*Volume, error) {
	if az == "" {
		az = b.Region + "a"
	}

	if volType == "" {
		volType = "gp2"
	}

	if size <= 0 {
		size = 8
	}

	b.mu.Lock("CreateVolume")
	defer b.mu.Unlock()

	id := "vol-" + uuid.New().String()[:17]
	vol := &Volume{
		ID:         id,
		AZ:         az,
		VolumeType: volType,
		Size:       size,
		State:      "available",
		CreateTime: time.Now(),
	}
	b.volumes[id] = vol

	return vol, nil
}

// DescribeVolumes returns volumes, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeVolumes(ids []string) []*Volume {
	b.mu.RLock("DescribeVolumes")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	var out []*Volume

	for _, vol := range b.volumes {
		if len(idSet) > 0 && !idSet[vol.ID] {
			continue
		}

		cp := *vol
		out = append(out, &cp)
	}

	return out
}

// DeleteVolume removes a volume by ID.
func (b *InMemoryBackend) DeleteVolume(id string) error {
	b.mu.Lock("DeleteVolume")
	defer b.mu.Unlock()

	vol, ok := b.volumes[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrVolumeNotFound, id)
	}

	if vol.Attachment != nil {
		return fmt.Errorf("%w: volume %s is attached to instance %s", ErrVolumeInUse, id, vol.Attachment.InstanceID)
	}

	delete(b.volumes, id)

	return nil
}

// AttachVolume attaches a volume to an instance.
func (b *InMemoryBackend) AttachVolume(volumeID, instanceID, device string) (*VolumeAttachment, error) {
	b.mu.Lock("AttachVolume")
	defer b.mu.Unlock()

	vol, ok := b.volumes[volumeID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrVolumeNotFound, volumeID)
	}

	if vol.Attachment != nil {
		return nil, fmt.Errorf("%w: volume %s is already attached", ErrVolumeInUse, volumeID)
	}

	if _, instOK := b.instances[instanceID]; !instOK {
		return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	att := &VolumeAttachment{
		VolumeID:   volumeID,
		InstanceID: instanceID,
		Device:     device,
		State:      "attached",
		AttachTime: time.Now(),
	}
	vol.Attachment = att
	vol.State = "in-use"

	return att, nil
}

// DetachVolume detaches a volume from its instance.
func (b *InMemoryBackend) DetachVolume(volumeID string, _ bool) (*VolumeAttachment, error) {
	b.mu.Lock("DetachVolume")
	defer b.mu.Unlock()

	vol, ok := b.volumes[volumeID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrVolumeNotFound, volumeID)
	}

	if vol.Attachment == nil {
		return nil, fmt.Errorf("%w: volume %s is not attached", ErrInvalidParameter, volumeID)
	}

	att := *vol.Attachment
	att.State = "detached"
	vol.Attachment = nil
	vol.State = "available"

	return &att, nil
}

// AllocateAddress allocates a new Elastic IP address.
func (b *InMemoryBackend) AllocateAddress() (*Address, error) {
	b.mu.Lock("AllocateAddress")
	defer b.mu.Unlock()

	id := "eipalloc-" + uuid.New().String()[:17]
	addr := &Address{
		AllocationID: id,
		PublicIP:     b.allocElasticIP(),
	}
	b.addresses[id] = addr

	return addr, nil
}

// AssociateAddress associates an Elastic IP with an instance.
func (b *InMemoryBackend) AssociateAddress(allocationID, instanceID string) (string, error) {
	b.mu.Lock("AssociateAddress")
	defer b.mu.Unlock()

	addr, ok := b.addresses[allocationID]
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrAddressNotFound, allocationID)
	}

	if _, instOK := b.instances[instanceID]; !instOK {
		return "", fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	assocID := "eipassoc-" + uuid.New().String()[:17]
	addr.AssociationID = assocID
	addr.InstanceID = instanceID

	return assocID, nil
}

// DisassociateAddress removes an Elastic IP association.
func (b *InMemoryBackend) DisassociateAddress(associationID string) error {
	b.mu.Lock("DisassociateAddress")
	defer b.mu.Unlock()

	for _, addr := range b.addresses {
		if addr.AssociationID == associationID {
			addr.AssociationID = ""
			addr.InstanceID = ""

			return nil
		}
	}

	return fmt.Errorf("%w: %s", ErrAssociationNotFound, associationID)
}

// ReleaseAddress releases an Elastic IP allocation.
func (b *InMemoryBackend) ReleaseAddress(allocationID string) error {
	b.mu.Lock("ReleaseAddress")
	defer b.mu.Unlock()

	if _, ok := b.addresses[allocationID]; !ok {
		return fmt.Errorf("%w: %s", ErrAddressNotFound, allocationID)
	}

	delete(b.addresses, allocationID)

	return nil
}

// DescribeAddresses returns Elastic IPs, optionally filtered by allocation IDs.
func (b *InMemoryBackend) DescribeAddresses(allocationIDs []string) []*Address {
	b.mu.RLock("DescribeAddresses")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(allocationIDs))
	for _, id := range allocationIDs {
		idSet[id] = true
	}

	var out []*Address

	for _, addr := range b.addresses {
		if len(idSet) > 0 && !idSet[addr.AllocationID] {
			continue
		}

		cp := *addr
		out = append(out, &cp)
	}

	return out
}

// CreateInternetGateway creates a new Internet Gateway.
func (b *InMemoryBackend) CreateInternetGateway() (*InternetGateway, error) {
	b.mu.Lock("CreateInternetGateway")
	defer b.mu.Unlock()

	id := "igw-" + uuid.New().String()[:17]
	igw := &InternetGateway{
		ID:          id,
		Attachments: []IGWAttachment{},
	}
	b.internetGateways[id] = igw

	return igw, nil
}

// DeleteInternetGateway removes an Internet Gateway.
func (b *InMemoryBackend) DeleteInternetGateway(id string) error {
	b.mu.Lock("DeleteInternetGateway")
	defer b.mu.Unlock()

	if _, ok := b.internetGateways[id]; !ok {
		return fmt.Errorf("%w: %s", ErrInternetGatewayNotFound, id)
	}

	delete(b.internetGateways, id)

	return nil
}

// DescribeInternetGateways returns IGWs, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeInternetGateways(ids []string) []*InternetGateway {
	b.mu.RLock("DescribeInternetGateways")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	var out []*InternetGateway

	for _, igw := range b.internetGateways {
		if len(idSet) > 0 && !idSet[igw.ID] {
			continue
		}

		cp := *igw
		out = append(out, &cp)
	}

	return out
}

// AttachInternetGateway attaches an IGW to a VPC.
func (b *InMemoryBackend) AttachInternetGateway(igwID, vpcID string) error {
	b.mu.Lock("AttachInternetGateway")
	defer b.mu.Unlock()

	igw, ok := b.internetGateways[igwID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrInternetGatewayNotFound, igwID)
	}

	if _, vpcOK := b.vpcs[vpcID]; !vpcOK {
		return fmt.Errorf("%w: %s", ErrVPCNotFound, vpcID)
	}

	igw.Attachments = append(igw.Attachments, IGWAttachment{VPCID: vpcID, State: "available"})

	return nil
}

// DetachInternetGateway detaches an IGW from a VPC.
func (b *InMemoryBackend) DetachInternetGateway(igwID, vpcID string) error {
	b.mu.Lock("DetachInternetGateway")
	defer b.mu.Unlock()

	igw, ok := b.internetGateways[igwID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrInternetGatewayNotFound, igwID)
	}

	for i, att := range igw.Attachments {
		if att.VPCID == vpcID {
			igw.Attachments = append(igw.Attachments[:i], igw.Attachments[i+1:]...)

			return nil
		}
	}

	return fmt.Errorf("%w: IGW %s is not attached to VPC %s", ErrInvalidParameter, igwID, vpcID)
}

// CreateRouteTable creates a new route table in a VPC.
func (b *InMemoryBackend) CreateRouteTable(vpcID string) (*RouteTable, error) {
	b.mu.Lock("CreateRouteTable")
	defer b.mu.Unlock()

	if _, ok := b.vpcs[vpcID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrVPCNotFound, vpcID)
	}

	id := "rtb-" + uuid.New().String()[:17]
	rt := &RouteTable{
		ID:           id,
		VPCID:        vpcID,
		Routes:       []Route{},
		Associations: []RouteAssociation{},
	}
	b.routeTables[id] = rt

	return rt, nil
}

// DeleteRouteTable removes a route table.
func (b *InMemoryBackend) DeleteRouteTable(id string) error {
	b.mu.Lock("DeleteRouteTable")
	defer b.mu.Unlock()

	if _, ok := b.routeTables[id]; !ok {
		return fmt.Errorf("%w: %s", ErrRouteTableNotFound, id)
	}

	delete(b.routeTables, id)

	return nil
}

// DescribeRouteTables returns route tables, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeRouteTables(ids []string) []*RouteTable {
	b.mu.RLock("DescribeRouteTables")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	var out []*RouteTable

	for _, rt := range b.routeTables {
		if len(idSet) > 0 && !idSet[rt.ID] {
			continue
		}

		cp := *rt
		out = append(out, &cp)
	}

	return out
}

// CreateRoute adds a route to a route table.
func (b *InMemoryBackend) CreateRoute(rtID, destCIDR, gatewayID, natGatewayID string) error {
	b.mu.Lock("CreateRoute")
	defer b.mu.Unlock()

	rt, ok := b.routeTables[rtID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrRouteTableNotFound, rtID)
	}

	rt.Routes = append(rt.Routes, Route{
		DestinationCIDR: destCIDR,
		GatewayID:       gatewayID,
		NatGatewayID:    natGatewayID,
		State:           "active",
	})

	return nil
}

// DeleteRoute removes a route from a route table by destination CIDR.
func (b *InMemoryBackend) DeleteRoute(rtID, destCIDR string) error {
	b.mu.Lock("DeleteRoute")
	defer b.mu.Unlock()

	rt, ok := b.routeTables[rtID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrRouteTableNotFound, rtID)
	}

	for i, r := range rt.Routes {
		if r.DestinationCIDR == destCIDR {
			rt.Routes = append(rt.Routes[:i], rt.Routes[i+1:]...)

			return nil
		}
	}

	return fmt.Errorf("%w: no route with destination %s in %s", ErrRouteNotFound, destCIDR, rtID)
}

// AssociateRouteTable associates a route table with a subnet.
func (b *InMemoryBackend) AssociateRouteTable(rtID, subnetID string) (string, error) {
	b.mu.Lock("AssociateRouteTable")
	defer b.mu.Unlock()

	rt, ok := b.routeTables[rtID]
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrRouteTableNotFound, rtID)
	}

	if _, subnetOK := b.subnets[subnetID]; !subnetOK {
		return "", fmt.Errorf("%w: %s", ErrSubnetNotFound, subnetID)
	}

	assocID := "rtbassoc-" + uuid.New().String()[:17]
	rt.Associations = append(rt.Associations, RouteAssociation{
		ID:           assocID,
		RouteTableID: rtID,
		SubnetID:     subnetID,
	})

	return assocID, nil
}

// DisassociateRouteTable removes a route table association.
func (b *InMemoryBackend) DisassociateRouteTable(assocID string) error {
	b.mu.Lock("DisassociateRouteTable")
	defer b.mu.Unlock()

	for _, rt := range b.routeTables {
		for i, assoc := range rt.Associations {
			if assoc.ID == assocID {
				rt.Associations = append(rt.Associations[:i], rt.Associations[i+1:]...)

				return nil
			}
		}
	}

	return fmt.Errorf("%w: %s", ErrAssociationNotFound, assocID)
}

// CreateNatGateway creates a new NAT Gateway.
func (b *InMemoryBackend) CreateNatGateway(subnetID, allocationID string) (*NatGateway, error) {
	b.mu.Lock("CreateNatGateway")
	defer b.mu.Unlock()

	if _, ok := b.subnets[subnetID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrSubnetNotFound, subnetID)
	}

	addr, ok := b.addresses[allocationID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrAddressNotFound, allocationID)
	}

	id := "nat-" + uuid.New().String()[:17]
	ngw := &NatGateway{
		ID:           id,
		SubnetID:     subnetID,
		AllocationID: allocationID,
		PublicIP:     addr.PublicIP,
		PrivateIP:    b.allocPrivateIP(),
		State:        "available",
		CreateTime:   time.Now(),
	}
	b.natGateways[id] = ngw

	return ngw, nil
}

// DeleteNatGateway removes a NAT Gateway.
func (b *InMemoryBackend) DeleteNatGateway(id string) error {
	b.mu.Lock("DeleteNatGateway")
	defer b.mu.Unlock()

	if _, ok := b.natGateways[id]; !ok {
		return fmt.Errorf("%w: %s", ErrNatGatewayNotFound, id)
	}

	delete(b.natGateways, id)

	return nil
}

// DescribeNatGateways returns NAT Gateways, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeNatGateways(ids []string) []*NatGateway {
	b.mu.RLock("DescribeNatGateways")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	var out []*NatGateway

	for _, ngw := range b.natGateways {
		if len(idSet) > 0 && !idSet[ngw.ID] {
			continue
		}

		cp := *ngw
		out = append(out, &cp)
	}

	return out
}

// DescribeNetworkInterfaces returns network interfaces, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeNetworkInterfaces(ids []string) []*NetworkInterface {
	b.mu.RLock("DescribeNetworkInterfaces")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	var out []*NetworkInterface

	for _, eni := range b.networkInterfaces {
		if len(idSet) > 0 && !idSet[eni.ID] {
			continue
		}

		cp := *eni
		out = append(out, &cp)
	}

	return out
}

// AuthorizeSecurityGroupIngress adds ingress rules to a security group.
func (b *InMemoryBackend) AuthorizeSecurityGroupIngress(groupID string, rules []SecurityGroupRule) error {
	b.mu.Lock("AuthorizeSecurityGroupIngress")
	defer b.mu.Unlock()

	sg, ok := b.securityGroups[groupID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrSecurityGroupNotFound, groupID)
	}

	sg.IngressRules = append(sg.IngressRules, rules...)

	return nil
}

// AuthorizeSecurityGroupEgress adds egress rules to a security group.
func (b *InMemoryBackend) AuthorizeSecurityGroupEgress(groupID string, rules []SecurityGroupRule) error {
	b.mu.Lock("AuthorizeSecurityGroupEgress")
	defer b.mu.Unlock()

	sg, ok := b.securityGroups[groupID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrSecurityGroupNotFound, groupID)
	}

	sg.EgressRules = append(sg.EgressRules, rules...)

	return nil
}

// RevokeSecurityGroupIngress removes matching ingress rules from a security group.
func (b *InMemoryBackend) RevokeSecurityGroupIngress(groupID string, rules []SecurityGroupRule) error {
	b.mu.Lock("RevokeSecurityGroupIngress")
	defer b.mu.Unlock()

	sg, ok := b.securityGroups[groupID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrSecurityGroupNotFound, groupID)
	}

	for _, rule := range rules {
		sg.IngressRules = removeRule(sg.IngressRules, rule)
	}

	return nil
}

// removeRule removes matching SecurityGroupRule entries from a slice.
func removeRule(rules []SecurityGroupRule, target SecurityGroupRule) []SecurityGroupRule {
	out := rules[:0]

	for _, r := range rules {
		if r != target {
			out = append(out, r)
		}
	}

	return out
}

// ---- network interface full CRUD ----

// CreateNetworkInterface creates a new ENI in the given subnet.
func (b *InMemoryBackend) CreateNetworkInterface(subnetID, description string) (*NetworkInterface, error) {
	if subnetID == "" {
		return nil, fmt.Errorf("%w: SubnetId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateNetworkInterface")
	defer b.mu.Unlock()

	sub, ok := b.subnets[subnetID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrSubnetNotFound, subnetID)
	}

	id := "eni-" + uuid.New().String()[:17]
	eni := &NetworkInterface{
		ID:              id,
		SubnetID:        subnetID,
		VPCID:           sub.VPCID,
		PrivateIP:       b.allocPrivateIP(),
		Description:     description,
		Status:          "available",
		SourceDestCheck: true,
	}
	b.networkInterfaces[id] = eni

	return eni, nil
}

// DeleteNetworkInterface removes a network interface by ID.
// Returns ErrNetworkInterfaceInUse if the ENI is currently attached.
func (b *InMemoryBackend) DeleteNetworkInterface(id string) error {
	b.mu.Lock("DeleteNetworkInterface")
	defer b.mu.Unlock()

	eni, ok := b.networkInterfaces[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNetworkInterfaceNotFound, id)
	}

	if eni.InstanceID != "" {
		return fmt.Errorf("%w: %s is currently attached to instance %s", ErrNetworkInterfaceInUse, id, eni.InstanceID)
	}

	delete(b.networkInterfaces, id)

	return nil
}

// AttachNetworkInterface attaches an ENI to an instance and returns the attachment ID.
func (b *InMemoryBackend) AttachNetworkInterface(eniID, instanceID string, deviceIndex int) (string, error) {
	b.mu.Lock("AttachNetworkInterface")
	defer b.mu.Unlock()

	eni, ok := b.networkInterfaces[eniID]
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrNetworkInterfaceNotFound, eniID)
	}

	if _, ok = b.instances[instanceID]; !ok {
		return "", fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	attachmentID := "eni-attach-" + uuid.New().String()[:17]
	eni.InstanceID = instanceID
	eni.AttachmentID = attachmentID
	eni.DeviceIndex = deviceIndex
	eni.Status = "in-use"

	return attachmentID, nil
}

// DetachNetworkInterface detaches a network interface by attachment ID.
func (b *InMemoryBackend) DetachNetworkInterface(attachmentID string, _ bool) error {
	b.mu.Lock("DetachNetworkInterface")
	defer b.mu.Unlock()

	for _, eni := range b.networkInterfaces {
		if eni.AttachmentID == attachmentID {
			eni.InstanceID = ""
			eni.AttachmentID = ""
			eni.DeviceIndex = 0
			eni.Status = "available"

			return nil
		}
	}

	return fmt.Errorf("%w: %s", ErrAttachmentNotFound, attachmentID)
}

// AssignPrivateIPAddresses adds secondary private IP addresses to an ENI.
// count is used when ips is empty; otherwise the supplied IPs are assigned.
func (b *InMemoryBackend) AssignPrivateIPAddresses(eniID string, count int, ips []string) error {
	b.mu.Lock("AssignPrivateIPAddresses")
	defer b.mu.Unlock()

	eni, ok := b.networkInterfaces[eniID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNetworkInterfaceNotFound, eniID)
	}

	if len(ips) > 0 {
		eni.SecondaryPrivateIPs = append(eni.SecondaryPrivateIPs, ips...)

		return nil
	}

	for range count {
		eni.SecondaryPrivateIPs = append(eni.SecondaryPrivateIPs, b.allocPrivateIP())
	}

	return nil
}

// UnassignPrivateIPAddresses removes secondary private IP addresses from an ENI.
func (b *InMemoryBackend) UnassignPrivateIPAddresses(eniID string, ips []string) error {
	b.mu.Lock("UnassignPrivateIPAddresses")
	defer b.mu.Unlock()

	eni, ok := b.networkInterfaces[eniID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNetworkInterfaceNotFound, eniID)
	}

	remove := make(map[string]bool, len(ips))
	for _, ip := range ips {
		remove[ip] = true
	}

	kept := eni.SecondaryPrivateIPs[:0]

	for _, ip := range eni.SecondaryPrivateIPs {
		if !remove[ip] {
			kept = append(kept, ip)
		}
	}

	eni.SecondaryPrivateIPs = kept

	return nil
}

// ModifyNetworkInterfaceAttribute updates a single attribute of an ENI.
// Supported attributes: description, sourceDestCheck.
func (b *InMemoryBackend) ModifyNetworkInterfaceAttribute(eniID, attr, value string) error {
	b.mu.Lock("ModifyNetworkInterfaceAttribute")
	defer b.mu.Unlock()

	eni, ok := b.networkInterfaces[eniID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNetworkInterfaceNotFound, eniID)
	}

	switch attr {
	case "description":
		eni.Description = value
	case attrSourceDest:
		eni.SourceDestCheck = value == ec2BooleanTrue
	}

	return nil
}

// ---- spot instances ----

// RequestSpotInstances creates a spot instance request and immediately fulfils it with a running instance.
func (b *InMemoryBackend) RequestSpotInstances(
	imageID, instanceType, subnetID, spotPrice string,
) (*SpotInstanceRequest, error) {
	if imageID == "" {
		return nil, fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	b.mu.Lock("RequestSpotInstances")
	defer b.mu.Unlock()

	if subnetID == "" {
		subnetID = b.findDefaultSubnetID()
	} else if _, ok := b.subnets[subnetID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrSubnetNotFound, subnetID)
	}

	// Allocate a backing instance immediately (mock fulfils spot requests instantly).
	vpcID := ""
	if sub, ok := b.subnets[subnetID]; ok {
		vpcID = sub.VPCID
	}

	instanceID := "i-" + uuid.New().String()[:17]
	inst := &Instance{
		ID:           instanceID,
		ImageID:      imageID,
		InstanceType: instanceType,
		State:        StateRunning,
		VPCID:        vpcID,
		SubnetID:     subnetID,
		LaunchTime:   time.Now(),
		PrivateIP:    b.allocPrivateIP(),
	}
	b.instances[instanceID] = inst

	reqID := "sir-" + uuid.New().String()[:8]
	req := &SpotInstanceRequest{
		ID:         reqID,
		InstanceID: instanceID,
		State:      "active",
		SpotPrice:  spotPrice,
		Type:       "one-time",
		CreateTime: time.Now(),
		LaunchSpec: SpotLaunchSpecification{
			ImageID:      imageID,
			InstanceType: instanceType,
			SubnetID:     subnetID,
		},
	}
	b.spotRequests[reqID] = req

	return req, nil
}

// DescribeSpotInstanceRequests returns spot requests, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeSpotInstanceRequests(ids []string) []*SpotInstanceRequest {
	b.mu.RLock("DescribeSpotInstanceRequests")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	var out []*SpotInstanceRequest

	for _, req := range b.spotRequests {
		if len(idSet) > 0 && !idSet[req.ID] {
			continue
		}

		cp := *req
		out = append(out, &cp)
	}

	return out
}

// CancelSpotInstanceRequests cancels the given spot instance requests (transitions to cancelled).
func (b *InMemoryBackend) CancelSpotInstanceRequests(ids []string) error {
	b.mu.Lock("CancelSpotInstanceRequests")
	defer b.mu.Unlock()

	for _, id := range ids {
		req, ok := b.spotRequests[id]
		if !ok {
			return fmt.Errorf("%w: %s", ErrSpotRequestNotFound, id)
		}

		req.State = "cancelled"
	}

	return nil
}

// ---- placement groups ----

// CreatePlacementGroup creates a new placement group.
func (b *InMemoryBackend) CreatePlacementGroup(name, strategy string) (*PlacementGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: GroupName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreatePlacementGroup")
	defer b.mu.Unlock()

	if _, exists := b.placementGroups[name]; exists {
		return nil, fmt.Errorf("%w: %s", ErrDuplicatePlacementGroupName, name)
	}

	if strategy == "" {
		strategy = "cluster"
	}

	pg := &PlacementGroup{
		Name:     name,
		Strategy: strategy,
		State:    "available",
	}
	b.placementGroups[name] = pg

	return pg, nil
}

// DescribePlacementGroups returns placement groups, optionally filtered by names.
func (b *InMemoryBackend) DescribePlacementGroups(names []string) []*PlacementGroup {
	b.mu.RLock("DescribePlacementGroups")
	defer b.mu.RUnlock()

	nameSet := make(map[string]bool, len(names))
	for _, n := range names {
		nameSet[n] = true
	}

	var out []*PlacementGroup

	for _, pg := range b.placementGroups {
		if len(nameSet) > 0 && !nameSet[pg.Name] {
			continue
		}

		cp := *pg
		out = append(out, &cp)
	}

	return out
}

// DeletePlacementGroup removes a placement group by name.
func (b *InMemoryBackend) DeletePlacementGroup(name string) error {
	b.mu.Lock("DeletePlacementGroup")
	defer b.mu.Unlock()

	if _, ok := b.placementGroups[name]; !ok {
		return fmt.Errorf("%w: %s", ErrPlacementGroupNotFound, name)
	}

	delete(b.placementGroups, name)

	return nil
}
