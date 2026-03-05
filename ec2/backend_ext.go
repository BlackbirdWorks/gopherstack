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
	ErrKeyPairNotFound          = errors.New("InvalidKeyPair.NotFound")
	ErrDuplicateKeyPairName     = errors.New("InvalidKeyPair.Duplicate")
	ErrVolumeNotFound           = errors.New("InvalidVolume.NotFound")
	ErrVolumeInUse              = errors.New("VolumeInUse")
	ErrAddressNotFound          = errors.New("InvalidAllocationID.NotFound")
	ErrInternetGatewayNotFound  = errors.New("InvalidInternetGatewayID.NotFound")
	ErrRouteTableNotFound       = errors.New("InvalidRouteTableID.NotFound")
	ErrNatGatewayNotFound       = errors.New("InvalidNatGatewayID.NotFound")
	ErrNetworkInterfaceNotFound = errors.New("InvalidNetworkInterfaceID.NotFound")
	ErrRouteNotFound            = errors.New("InvalidRoute.NotFound")
	ErrAssociationNotFound      = errors.New("InvalidAssociationID.NotFound")
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

// NetworkInterface represents an EC2 Network Interface.
type NetworkInterface struct {
	ID         string `json:"id"`
	SubnetID   string `json:"subnetID"`
	VPCID      string `json:"vpcID"`
	PrivateIP  string `json:"privateIP"`
	InstanceID string `json:"instanceID,omitempty"`
	Status     string `json:"status"`
}

// AMIStub is a static image entry.
type AMIStub struct {
	ImageID      string
	Name         string
	Description  string
	Architecture string
	Platform     string
}

//nolint:gochecknoglobals // package-level stub data for describe operations
var stubAMIs = []AMIStub{
	{
		ImageID:      "ami-0c55b159cbfafe1f0",
		Name:         "amzn2-ami-hvm",
		Description:  "Amazon Linux 2 (x86_64)",
		Architecture: "x86_64",
	},
	{
		ImageID:      "ami-0eb260c4d5475b901",
		Name:         "ubuntu-22.04-lts",
		Description:  "Ubuntu 22.04 LTS (x86_64)",
		Architecture: "x86_64",
	},
	{
		ImageID:      "ami-09d3b3274b6c5d4aa",
		Name:         "windows-server-2022",
		Description:  "Windows Server 2022",
		Architecture: "x86_64",
		Platform:     "windows",
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
		//nolint:gosec // G602: i < len(parts) since parts was allocated with len(sum)
		parts[i] = fmt.Sprintf("%02x", by)
	}

	return strings.Join(parts, ":"), nil
}

// StartInstances transitions stopped instances to running.
func (b *InMemoryBackend) StartInstances(ids []string) ([]*Instance, error) {
	b.mu.Lock("StartInstances")
	defer b.mu.Unlock()

	var result []*Instance

	for _, id := range ids {
		inst, ok := b.instances[id]
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, id)
		}

		inst.State = StateRunning
		cp := *inst
		result = append(result, &cp)
	}

	return result, nil
}

// StopInstances transitions running instances to stopped.
func (b *InMemoryBackend) StopInstances(ids []string) ([]*Instance, error) {
	b.mu.Lock("StopInstances")
	defer b.mu.Unlock()

	var result []*Instance

	for _, id := range ids {
		inst, ok := b.instances[id]
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, id)
		}

		inst.State = StateStopped
		cp := *inst
		result = append(result, &cp)
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

	for _, kp := range b.keyPairs {
		if kp.Name == name {
			return nil, fmt.Errorf("%w: %s", ErrDuplicateKeyPairName, name)
		}
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
