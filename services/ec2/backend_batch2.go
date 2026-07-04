package ec2

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// Errors for batch2 operations.
var (
	ErrEndpointConnectionNotificationNotFound = errors.New(
		"InvalidConnectionNotification.NotFound",
	)
	ErrSnapshotAlreadyLocked   = errors.New("InvalidSnapshot.AlreadyLocked")
	ErrSnapshotNotLocked       = errors.New("InvalidSnapshot.NotLocked")
	ErrReplaceRootTaskNotFound = errors.New("InvalidReplaceRootVolumeTaskID.NotFound")
)

const (
	stateAvailableImg  = "available"
	stateDisabledImg   = "disabled"
	stateDeprecatedImg = "deprecated"
)

const (
	defaultEBSKmsKeyAlias    = "alias/aws/ebs"
	stateImageUnblocked      = "unblocked"
	stateImageBlockNew       = "block-new-sharing"
	stateDefaultCredit       = "standard"
	addressTransferOfferDays = 3
)

// VpcEndpointConnectionNotification tracks a VPC endpoint connection notification.
type VpcEndpointConnectionNotification struct {
	ConnectionNotificationID    string   `json:"connectionNotificationID,omitempty"`
	ServiceID                   string   `json:"serviceID,omitempty"`
	VpcEndpointID               string   `json:"vpcEndpointID,omitempty"`
	ConnectionNotificationARN   string   `json:"connectionNotificationARN,omitempty"`
	ConnectionNotificationType  string   `json:"connectionNotificationType,omitempty"`
	ConnectionNotificationState string   `json:"connectionNotificationState,omitempty"`
	ConnectionEvents            []string `json:"connectionEvents,omitempty"`
}

// SnapshotLock holds lock state for an EBS snapshot.
type SnapshotLock struct {
	LockCreatedOn    time.Time `json:"lockCreatedOn"`
	LockExpiresOn    time.Time `json:"lockExpiresOn"`
	SnapshotID       string    `json:"snapshotID,omitempty"`
	LockState        string    `json:"lockState,omitempty"`
	LockDurationDays int       `json:"lockDurationDays,omitempty"`
}

// ReplaceRootVolumeTask tracks a replace-root-volume operation.
type ReplaceRootVolumeTask struct {
	StartTime               time.Time `json:"startTime"`
	CompleteTime            time.Time `json:"completeTime"`
	ReplaceRootVolumeTaskID string    `json:"replaceRootVolumeTaskID,omitempty"`
	InstanceID              string    `json:"instanceID,omitempty"`
	TaskState               string    `json:"taskState,omitempty"`
	SnapshotID              string    `json:"snapshotID,omitempty"`
}

// SubnetCIDRReservation tracks a CIDR reservation within a subnet.
type SubnetCIDRReservation struct {
	SubnetCIDRReservationID string `json:"subnetCidrReservationID,omitempty"`
	SubnetID                string `json:"subnetID,omitempty"`
	CIDR                    string `json:"cidr,omitempty"`
	ReservationType         string `json:"reservationType,omitempty"`
	Description             string `json:"description,omitempty"`
	OwnerID                 string `json:"ownerID,omitempty"`
	State                   string `json:"state,omitempty"`
}

// ---- VPC Endpoint Connection Notifications ----

// CreateVpcEndpointConnectionNotification creates a notification for endpoint connection events.
func (b *InMemoryBackend) CreateVpcEndpointConnectionNotification(
	serviceID, endpointID, notifARN string,
	events []string,
) (*VpcEndpointConnectionNotification, error) {
	if notifARN == "" {
		return nil, fmt.Errorf("%w: ConnectionNotificationArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateVpcEndpointConnectionNotification")
	defer b.mu.Unlock()

	notif := &VpcEndpointConnectionNotification{
		ConnectionNotificationID:    "vpce-nfn-" + uuid.New().String()[:8],
		ServiceID:                   serviceID,
		VpcEndpointID:               endpointID,
		ConnectionNotificationARN:   notifARN,
		ConnectionEvents:            events,
		ConnectionNotificationType:  "Topic",
		ConnectionNotificationState: "Enabled",
	}
	b.endpointConnectionNotifs[notif.ConnectionNotificationID] = notif

	return notif, nil
}

// DescribeVpcEndpointConnectionNotifications returns notifications, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeVpcEndpointConnectionNotifications(
	ids []string,
) []*VpcEndpointConnectionNotification {
	b.mu.RLock("DescribeVpcEndpointConnectionNotifications")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []*VpcEndpointConnectionNotification
	for _, n := range b.endpointConnectionNotifs {
		if len(filter) > 0 && !filter[n.ConnectionNotificationID] {
			continue
		}
		cp := *n
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ConnectionNotificationID < out[j].ConnectionNotificationID
	})

	return out
}

// DeleteVpcEndpointConnectionNotifications removes notifications by ID.
func (b *InMemoryBackend) DeleteVpcEndpointConnectionNotifications(ids []string) error {
	b.mu.Lock("DeleteVpcEndpointConnectionNotifications")
	defer b.mu.Unlock()

	for _, id := range ids {
		if _, ok := b.endpointConnectionNotifs[id]; !ok {
			return fmt.Errorf("%w: %s", ErrEndpointConnectionNotificationNotFound, id)
		}
	}
	for _, id := range ids {
		delete(b.endpointConnectionNotifs, id)
	}

	return nil
}

// ModifyVpcEndpointConnectionNotification updates events or state for a notification.
func (b *InMemoryBackend) ModifyVpcEndpointConnectionNotification(
	id, notifARN string,
	events []string,
) (*VpcEndpointConnectionNotification, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: ConnectionNotificationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyVpcEndpointConnectionNotification")
	defer b.mu.Unlock()

	notif, ok := b.endpointConnectionNotifs[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrEndpointConnectionNotificationNotFound, id)
	}
	if notifARN != "" {
		notif.ConnectionNotificationARN = notifARN
	}
	if len(events) > 0 {
		notif.ConnectionEvents = events
	}
	cp := *notif

	return &cp, nil
}

// ---- VPC Endpoint Connections ----

// DescribeVpcEndpointConnections returns endpoint connections for owned services.
func (b *InMemoryBackend) DescribeVpcEndpointConnections(
	serviceIDs []string,
) []*VpcEndpointConnection {
	b.mu.RLock("DescribeVpcEndpointConnections")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(serviceIDs))
	for _, id := range serviceIDs {
		filter[id] = true
	}

	var out []*VpcEndpointConnection
	for _, conn := range b.vpcEndpointConnections {
		if len(filter) > 0 && !filter[conn.ServiceID] {
			continue
		}
		cp := *conn
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].VpcEndpointID < out[j].VpcEndpointID
	})

	return out
}

// DescribeVpcEndpointAssociations returns VPC-endpoint associations.
func (b *InMemoryBackend) DescribeVpcEndpointAssociations(endpointIDs []string) []*VpcEndpoint {
	b.mu.RLock("DescribeVpcEndpointAssociations")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(endpointIDs))
	for _, id := range endpointIDs {
		filter[id] = true
	}

	var out []*VpcEndpoint
	for _, ep := range b.vpcEndpoints {
		if len(filter) > 0 && !filter[ep.ID] {
			continue
		}
		cp := *ep
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out
}

// ---- VPC Endpoint Service Config modifications ----

// ModifyVpcEndpointServicePayerResponsibility updates payer responsibility.
func (b *InMemoryBackend) ModifyVpcEndpointServicePayerResponsibility(
	serviceID, _ string,
) error {
	if serviceID == "" {
		return fmt.Errorf("%w: ServiceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyVpcEndpointServicePayerResponsibility")
	defer b.mu.Unlock()

	if _, ok := b.vpcEndpointServiceConfigs[serviceID]; !ok {
		return fmt.Errorf("%w: %s", ErrVpcEndpointServiceNotFound, serviceID)
	}

	return nil
}

// DescribeVpcEndpointServicePermissions returns allowed principals for a service.
func (b *InMemoryBackend) DescribeVpcEndpointServicePermissions(serviceID string) []string {
	b.mu.RLock("DescribeVpcEndpointServicePermissions")
	defer b.mu.RUnlock()

	return append([]string{}, b.vpcEndpointServicePermissions[serviceID]...)
}

// ModifyVpcEndpointServicePermissions adds/removes allowed principals for a service.
func (b *InMemoryBackend) ModifyVpcEndpointServicePermissions(
	serviceID string,
	add, remove []string,
) error {
	if serviceID == "" {
		return fmt.Errorf("%w: ServiceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyVpcEndpointServicePermissions")
	defer b.mu.Unlock()

	if _, ok := b.vpcEndpointServiceConfigs[serviceID]; !ok {
		return fmt.Errorf("%w: %s", ErrVpcEndpointServiceNotFound, serviceID)
	}

	existing := make(map[string]bool)
	for _, p := range b.vpcEndpointServicePermissions[serviceID] {
		existing[p] = true
	}
	for _, p := range add {
		existing[p] = true
	}
	for _, p := range remove {
		delete(existing, p)
	}

	result := make([]string, 0, len(existing))
	for p := range existing {
		result = append(result, p)
	}
	sort.Strings(result)
	b.vpcEndpointServicePermissions[serviceID] = result

	return nil
}

// ---- ModifyVpcEndpoint ----

// ModifyVpcEndpoint modifies a VPC endpoint (adds/removes subnets, SGs, route tables).
func (b *InMemoryBackend) ModifyVpcEndpoint(
	endpointID string,
	addSubnetIDs, removeSubnetIDs []string,
) error {
	if endpointID == "" {
		return fmt.Errorf("%w: VpcEndpointId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyVpcEndpoint")
	defer b.mu.Unlock()

	ep, ok := b.vpcEndpoints[endpointID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrVpcEndpointNotFound, endpointID)
	}

	removeSet := make(map[string]bool, len(removeSubnetIDs))
	for _, id := range removeSubnetIDs {
		removeSet[id] = true
	}

	filtered := ep.SubnetIDs[:0]
	for _, id := range ep.SubnetIDs {
		if !removeSet[id] {
			filtered = append(filtered, id)
		}
	}
	ep.SubnetIDs = filtered
	ep.SubnetIDs = append(ep.SubnetIDs, addSubnetIDs...)

	return nil
}

// ---- EBS encryption defaults ----

// EnableEbsEncryptionByDefault enables EBS encryption by default for the account.
func (b *InMemoryBackend) EnableEbsEncryptionByDefault() {
	b.mu.Lock("EnableEbsEncryptionByDefault")
	defer b.mu.Unlock()

	b.ebsEncryptionByDefault = true
}

// DisableEbsEncryptionByDefault disables EBS encryption by default for the account.
func (b *InMemoryBackend) DisableEbsEncryptionByDefault() {
	b.mu.Lock("DisableEbsEncryptionByDefault")
	defer b.mu.Unlock()

	b.ebsEncryptionByDefault = false
}

// GetEbsEncryptionByDefault returns the current EBS encryption by default setting.
func (b *InMemoryBackend) GetEbsEncryptionByDefault() bool {
	b.mu.RLock("GetEbsEncryptionByDefault")
	defer b.mu.RUnlock()

	return b.ebsEncryptionByDefault
}

// GetEbsDefaultKmsKeyID returns the default KMS key ID for EBS encryption.
func (b *InMemoryBackend) GetEbsDefaultKmsKeyID() string {
	b.mu.RLock("GetEbsDefaultKmsKeyID")
	defer b.mu.RUnlock()

	if b.ebsDefaultKmsKeyID == "" {
		return defaultEBSKmsKeyAlias
	}

	return b.ebsDefaultKmsKeyID
}

// ModifyEbsDefaultKmsKeyID sets the default KMS key ID for EBS encryption.
func (b *InMemoryBackend) ModifyEbsDefaultKmsKeyID(kmsKeyID string) error {
	if kmsKeyID == "" {
		return fmt.Errorf("%w: KmsKeyId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyEbsDefaultKmsKeyID")
	defer b.mu.Unlock()

	b.ebsDefaultKmsKeyID = kmsKeyID

	return nil
}

// ---- EnableVolumeIO ----

// EnableVolumeIO enables IO for a volume stuck in impaired state.
func (b *InMemoryBackend) EnableVolumeIO(volumeID string) error {
	if volumeID == "" {
		return fmt.Errorf("%w: VolumeId is required", ErrInvalidParameter)
	}

	b.mu.Lock("EnableVolumeIO")
	defer b.mu.Unlock()

	if _, ok := b.volumes[volumeID]; !ok {
		return fmt.Errorf("%w: %s", ErrVolumeNotFound, volumeID)
	}

	return nil
}

// ---- Snapshot locking ----

// LockSnapshot locks a snapshot to prevent deletion.
func (b *InMemoryBackend) LockSnapshot(
	snapshotID, lockMode string,
	durationDays int,
) (*SnapshotLock, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: SnapshotId is required", ErrInvalidParameter)
	}

	b.mu.Lock("LockSnapshot")
	defer b.mu.Unlock()

	if _, ok := b.snapshots[snapshotID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrSnapshotNotFound, snapshotID)
	}
	if _, locked := b.snapshotLocks[snapshotID]; locked {
		return nil, fmt.Errorf("%w: %s", ErrSnapshotAlreadyLocked, snapshotID)
	}

	now := time.Now().UTC()
	lock := &SnapshotLock{
		SnapshotID:       snapshotID,
		LockState:        lockMode,
		LockCreatedOn:    now,
		LockDurationDays: durationDays,
	}
	if durationDays > 0 {
		lock.LockExpiresOn = now.AddDate(0, 0, durationDays)
	}
	b.snapshotLocks[snapshotID] = lock

	return lock, nil
}

// UnlockSnapshot removes the lock from a snapshot.
func (b *InMemoryBackend) UnlockSnapshot(snapshotID string) error {
	if snapshotID == "" {
		return fmt.Errorf("%w: SnapshotId is required", ErrInvalidParameter)
	}

	b.mu.Lock("UnlockSnapshot")
	defer b.mu.Unlock()

	if _, ok := b.snapshots[snapshotID]; !ok {
		return fmt.Errorf("%w: %s", ErrSnapshotNotFound, snapshotID)
	}
	if _, locked := b.snapshotLocks[snapshotID]; !locked {
		return fmt.Errorf("%w: %s", ErrSnapshotNotLocked, snapshotID)
	}
	delete(b.snapshotLocks, snapshotID)

	return nil
}

// DescribeLockedSnapshots returns locked snapshots, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeLockedSnapshots(ids []string) []*SnapshotLock {
	b.mu.RLock("DescribeLockedSnapshots")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []*SnapshotLock
	for _, lock := range b.snapshotLocks {
		if len(filter) > 0 && !filter[lock.SnapshotID] {
			continue
		}
		cp := *lock
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SnapshotID < out[j].SnapshotID })

	return out
}

// ---- CopyVolumes ----

// CopyVolumesResult holds the result of copying a volume.
type CopyVolumesResult struct {
	SourceVolumeID string `json:"sourceVolumeID,omitempty"`
	DestVolumeID   string `json:"destVolumeID,omitempty"`
}

// CopyVolumes creates copies of the given volumes.
func (b *InMemoryBackend) CopyVolumes(
	volumeIDs []string,
	_ string,
) ([]CopyVolumesResult, error) {
	if len(volumeIDs) == 0 {
		return nil, fmt.Errorf("%w: at least one VolumeId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CopyVolumes")
	defer b.mu.Unlock()

	for _, id := range volumeIDs {
		if _, ok := b.volumes[id]; !ok {
			return nil, fmt.Errorf("%w: %s", ErrVolumeNotFound, id)
		}
	}

	results := make([]CopyVolumesResult, 0, len(volumeIDs))
	for _, id := range volumeIDs {
		src := b.volumes[id]
		newVol := &Volume{
			ID:         "vol-" + uuid.New().String()[:17],
			AZ:         src.AZ,
			VolumeType: src.VolumeType,
			Size:       src.Size,
			State:      stateAvailable,
			Encrypted:  src.Encrypted,
			KmsKeyID:   src.KmsKeyID,
			CreateTime: time.Now().UTC(),
		}
		b.volumes[newVol.ID] = newVol
		results = append(results, CopyVolumesResult{SourceVolumeID: id, DestVolumeID: newVol.ID})
	}

	return results, nil
}

// ---- VPC CIDR disassociation ----

// DisassociateVpcCidrBlock removes a secondary CIDR block association from a VPC.
func (b *InMemoryBackend) DisassociateVpcCidrBlock(associationID string) error {
	if associationID == "" {
		return fmt.Errorf("%w: AssociationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DisassociateVpcCidrBlock")
	defer b.mu.Unlock()

	// Keys are stored as "vpcID:assocID"
	for key, assoc := range b.vpcCidrAssociations {
		if assoc.AssociationID == associationID {
			delete(b.vpcCidrAssociations, key)

			return nil
		}
	}

	return fmt.Errorf("%w: %s", ErrInvalidParameter, associationID)
}

// ---- NAT Gateway address ops ----

// DisassociateNatGatewayAddress removes a secondary IP from a NAT gateway.
func (b *InMemoryBackend) DisassociateNatGatewayAddress(natGatewayID string) error {
	if natGatewayID == "" {
		return fmt.Errorf("%w: NatGatewayId is required", ErrInvalidParameter)
	}

	b.mu.RLock("DisassociateNatGatewayAddress")
	defer b.mu.RUnlock()

	if _, ok := b.natGateways[natGatewayID]; !ok {
		return fmt.Errorf("%w: %s", ErrInvalidParameter, natGatewayID)
	}

	return nil
}

// AssociateNatGatewayAddress adds an allocation to a NAT gateway.
func (b *InMemoryBackend) AssociateNatGatewayAddress(natGatewayID, _ string) error {
	if natGatewayID == "" {
		return fmt.Errorf("%w: NatGatewayId is required", ErrInvalidParameter)
	}

	b.mu.RLock("AssociateNatGatewayAddress")
	defer b.mu.RUnlock()

	if _, ok := b.natGateways[natGatewayID]; !ok {
		return fmt.Errorf("%w: %s", ErrInvalidParameter, natGatewayID)
	}

	return nil
}

// AssignPrivateNatGatewayAddress assigns a new secondary private IP to a NAT
// gateway, appending it to the gateway's real address state. See
// UnassignPrivateNatGatewayAddress (backend_parity_final.go) for the inverse.
func (b *InMemoryBackend) AssignPrivateNatGatewayAddress(natGatewayID string) error {
	if natGatewayID == "" {
		return fmt.Errorf("%w: NatGatewayId is required", ErrInvalidParameter)
	}

	b.mu.Lock("AssignPrivateNatGatewayAddress")
	defer b.mu.Unlock()

	ngw, ok := b.natGateways[natGatewayID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrInvalidParameter, natGatewayID)
	}

	ngw.SecondaryPrivateIPs = append(ngw.SecondaryPrivateIPs, b.allocPrivateIP())

	return nil
}

// ---- Image lifecycle ----

// DisableImage sets an AMI to disabled state.
func (b *InMemoryBackend) DisableImage(imageID string) error {
	if imageID == "" {
		return fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DisableImage")
	defer b.mu.Unlock()

	b.imageDisabled[imageID] = true

	return nil
}

// EnableImage restores an AMI from disabled state.
func (b *InMemoryBackend) EnableImage(imageID string) error {
	if imageID == "" {
		return fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	b.mu.Lock("EnableImage")
	defer b.mu.Unlock()

	delete(b.imageDisabled, imageID)

	return nil
}

// EnableImageBlockPublicAccess sets the account-level block for public AMI sharing.
func (b *InMemoryBackend) EnableImageBlockPublicAccess(state string) error {
	b.mu.Lock("EnableImageBlockPublicAccess")
	defer b.mu.Unlock()

	b.imageBlockPublicAccess = state

	return nil
}

// DisableImageBlockPublicAccess clears the account-level block.
func (b *InMemoryBackend) DisableImageBlockPublicAccess() {
	b.mu.Lock("DisableImageBlockPublicAccess")
	defer b.mu.Unlock()

	b.imageBlockPublicAccess = stateImageUnblocked
}

// GetImageBlockPublicAccessState returns the current image block public access state.
func (b *InMemoryBackend) GetImageBlockPublicAccessState() string {
	b.mu.RLock("GetImageBlockPublicAccessState")
	defer b.mu.RUnlock()

	if b.imageBlockPublicAccess == "" {
		return stateImageUnblocked
	}

	return b.imageBlockPublicAccess
}

// EnableImageDeprecation sets a deprecation time for an AMI.
func (b *InMemoryBackend) EnableImageDeprecation(imageID, deprecateAt string) error {
	if imageID == "" {
		return fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	b.mu.Lock("EnableImageDeprecation")
	defer b.mu.Unlock()

	b.imageDeprecated[imageID] = deprecateAt

	return nil
}

// DisableImageDeprecation removes deprecation from an AMI.
func (b *InMemoryBackend) DisableImageDeprecation(imageID string) error {
	if imageID == "" {
		return fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DisableImageDeprecation")
	defer b.mu.Unlock()

	delete(b.imageDeprecated, imageID)

	return nil
}

// EnableImageDeregistrationProtection protects an AMI from deregistration.
func (b *InMemoryBackend) EnableImageDeregistrationProtection(imageID string) error {
	if imageID == "" {
		return fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	b.mu.Lock("EnableImageDeregistrationProtection")
	defer b.mu.Unlock()

	b.imageDeregistrationProtection[imageID] = true

	return nil
}

// DisableImageDeregistrationProtection removes deregistration protection from an AMI.
func (b *InMemoryBackend) DisableImageDeregistrationProtection(imageID string) error {
	if imageID == "" {
		return fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DisableImageDeregistrationProtection")
	defer b.mu.Unlock()

	delete(b.imageDeregistrationProtection, imageID)

	return nil
}

// ModifyImageAttribute modifies a mutable AMI attribute.
func (b *InMemoryBackend) ModifyImageAttribute(imageID, attribute, value string) error {
	if imageID == "" {
		return fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyImageAttribute")
	defer b.mu.Unlock()

	if b.imageAttributes[imageID] == nil {
		b.imageAttributes[imageID] = make(map[string]string)
	}
	b.imageAttributes[imageID][attribute] = value

	return nil
}

// ResetImageAttribute resets an AMI attribute to its default.
func (b *InMemoryBackend) ResetImageAttribute(imageID, attribute string) error {
	if imageID == "" {
		return fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ResetImageAttribute")
	defer b.mu.Unlock()

	if m, ok := b.imageAttributes[imageID]; ok {
		delete(m, attribute)
	}

	return nil
}

// InstanceImageMetadataItem holds image-related metadata for a single instance.
type InstanceImageMetadataItem struct {
	InstanceID string `json:"instanceID,omitempty"`
	ImageID    string `json:"imageID,omitempty"`
	ImageState string `json:"imageState,omitempty"`
}

// DescribeInstanceImageMetadata returns image metadata for instances (or all).
func (b *InMemoryBackend) DescribeInstanceImageMetadata(
	instanceIDs []string,
) []InstanceImageMetadataItem {
	b.mu.RLock("DescribeInstanceImageMetadata")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(instanceIDs))
	for _, id := range instanceIDs {
		filter[id] = true
	}

	var out []InstanceImageMetadataItem
	for _, inst := range b.instances {
		if len(filter) > 0 && !filter[inst.ID] {
			continue
		}
		imageState := stateAvailableImg
		if b.imageDisabled[inst.ImageID] {
			imageState = stateDisabledImg
		}
		out = append(out, InstanceImageMetadataItem{
			InstanceID: inst.ID,
			ImageID:    inst.ImageID,
			ImageState: imageState,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].InstanceID < out[j].InstanceID })

	return out
}

// ---- Serial console ----

// EnableSerialConsoleAccess enables serial console access for the account.
func (b *InMemoryBackend) EnableSerialConsoleAccess() {
	b.mu.Lock("EnableSerialConsoleAccess")
	defer b.mu.Unlock()

	b.serialConsoleAccess = true
}

// DisableSerialConsoleAccess disables serial console access for the account.
func (b *InMemoryBackend) DisableSerialConsoleAccess() {
	b.mu.Lock("DisableSerialConsoleAccess")
	defer b.mu.Unlock()

	b.serialConsoleAccess = false
}

// GetSerialConsoleAccessStatus returns whether serial console access is enabled.
func (b *InMemoryBackend) GetSerialConsoleAccessStatus() bool {
	b.mu.RLock("GetSerialConsoleAccessStatus")
	defer b.mu.RUnlock()

	return b.serialConsoleAccess
}

// ---- VGW route propagation ----

// EnableVgwRoutePropagation enables route propagation for a VGW in a route table.
func (b *InMemoryBackend) EnableVgwRoutePropagation(routeTableID, gatewayID string) error {
	if routeTableID == "" || gatewayID == "" {
		return fmt.Errorf("%w: RouteTableId and GatewayId are required", ErrInvalidParameter)
	}

	b.mu.Lock("EnableVgwRoutePropagation")
	defer b.mu.Unlock()

	key := routeTableID + ":" + gatewayID
	b.vgwRoutePropagation[key] = true

	return nil
}

// DisableVgwRoutePropagation disables route propagation for a VGW in a route table.
func (b *InMemoryBackend) DisableVgwRoutePropagation(routeTableID, gatewayID string) error {
	if routeTableID == "" || gatewayID == "" {
		return fmt.Errorf("%w: RouteTableId and GatewayId are required", ErrInvalidParameter)
	}

	b.mu.Lock("DisableVgwRoutePropagation")
	defer b.mu.Unlock()

	key := routeTableID + ":" + gatewayID
	delete(b.vgwRoutePropagation, key)

	return nil
}

// ---- Default credit specification ----

// GetDefaultCreditSpecification returns the account-level default CPU credit spec.
func (b *InMemoryBackend) GetDefaultCreditSpecification() string {
	b.mu.RLock("GetDefaultCreditSpecification")
	defer b.mu.RUnlock()

	if b.defaultCreditSpec == "" {
		return stateDefaultCredit
	}

	return b.defaultCreditSpec
}

// ModifyDefaultCreditSpecification sets the account-level default CPU credit spec.
func (b *InMemoryBackend) ModifyDefaultCreditSpecification(cpuCredits string) error {
	if cpuCredits == "" {
		return fmt.Errorf("%w: CpuCredits is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyDefaultCreditSpecification")
	defer b.mu.Unlock()

	b.defaultCreditSpec = cpuCredits

	return nil
}

// ---- Replace root volume tasks ----

// CreateReplaceRootVolumeTask creates a task to replace an instance's root volume.
func (b *InMemoryBackend) CreateReplaceRootVolumeTask(
	instanceID, snapshotID string,
) (*ReplaceRootVolumeTask, error) {
	if instanceID == "" {
		return nil, fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateReplaceRootVolumeTask")
	defer b.mu.Unlock()

	if _, ok := b.instances[instanceID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	task := &ReplaceRootVolumeTask{
		ReplaceRootVolumeTaskID: "replacevol-" + uuid.New().String()[:8],
		InstanceID:              instanceID,
		SnapshotID:              snapshotID,
		TaskState:               stateCompleted,
		StartTime:               time.Now().UTC(),
	}
	task.CompleteTime = task.StartTime
	b.replaceRootVolumeTasks[task.ReplaceRootVolumeTaskID] = task

	return task, nil
}

// DescribeReplaceRootVolumeTasks returns replace root volume tasks.
func (b *InMemoryBackend) DescribeReplaceRootVolumeTasks(ids []string) []*ReplaceRootVolumeTask {
	b.mu.RLock("DescribeReplaceRootVolumeTasks")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []*ReplaceRootVolumeTask
	for _, task := range b.replaceRootVolumeTasks {
		if len(filter) > 0 && !filter[task.ReplaceRootVolumeTaskID] {
			continue
		}
		cp := *task
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ReplaceRootVolumeTaskID < out[j].ReplaceRootVolumeTaskID
	})

	return out
}

// ---- Address transfers ----

// EnableAddressTransfer enables EIP transfer to another account.
func (b *InMemoryBackend) EnableAddressTransfer(
	allocationID, transferAccountID string,
) (*AddressTransfer, error) {
	if allocationID == "" {
		return nil, fmt.Errorf("%w: AllocationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("EnableAddressTransfer")
	defer b.mu.Unlock()

	addr, ok := b.addresses[allocationID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrInvalidParameter, allocationID)
	}

	transfer := &AddressTransfer{
		AllocationID:        allocationID,
		PublicIP:            addr.PublicIP,
		TransferAccountID:   transferAccountID,
		TransferOfferStatus: "pending",
		TransferOfferExpiry: time.Now().UTC().AddDate(0, 0, addressTransferOfferDays),
	}
	b.addressTransfers[allocationID] = transfer

	return transfer, nil
}

// DisableAddressTransfer cancels an EIP transfer.
func (b *InMemoryBackend) DisableAddressTransfer(allocationID string) error {
	if allocationID == "" {
		return fmt.Errorf("%w: AllocationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DisableAddressTransfer")
	defer b.mu.Unlock()

	if _, ok := b.addresses[allocationID]; !ok {
		return fmt.Errorf("%w: %s", ErrInvalidParameter, allocationID)
	}
	delete(b.addressTransfers, allocationID)

	return nil
}

// DescribeAddressTransfers returns address transfers, optionally filtered by allocation ID.
func (b *InMemoryBackend) DescribeAddressTransfers(allocationIDs []string) []*AddressTransfer {
	b.mu.RLock("DescribeAddressTransfers")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(allocationIDs))
	for _, id := range allocationIDs {
		filter[id] = true
	}

	var out []*AddressTransfer
	for _, t := range b.addressTransfers {
		if len(filter) > 0 && !filter[t.AllocationID] {
			continue
		}
		cp := *t
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].AllocationID < out[j].AllocationID })

	return out
}

// ---- Subnet CIDR reservations ----

// CreateSubnetCidrReservation creates a CIDR reservation within a subnet.
func (b *InMemoryBackend) CreateSubnetCidrReservation(
	subnetID, cidr, reservationType, description string,
) (*SubnetCIDRReservation, error) {
	if subnetID == "" || cidr == "" {
		return nil, fmt.Errorf("%w: SubnetId and Cidr are required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateSubnetCidrReservation")
	defer b.mu.Unlock()

	if _, ok := b.subnets[subnetID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrSubnetNotFound, subnetID)
	}

	reservation := &SubnetCIDRReservation{
		SubnetCIDRReservationID: "scr-" + uuid.New().String()[:8],
		SubnetID:                subnetID,
		CIDR:                    cidr,
		ReservationType:         reservationType,
		Description:             description,
		OwnerID:                 b.AccountID,
		State:                   "assigned",
	}
	b.subnetCIDRReservations[subnetID] = append(b.subnetCIDRReservations[subnetID], reservation)

	return reservation, nil
}

// DeleteSubnetCidrReservation removes a subnet CIDR reservation.
func (b *InMemoryBackend) DeleteSubnetCidrReservation(reservationID string) error {
	if reservationID == "" {
		return fmt.Errorf("%w: SubnetCidrReservationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteSubnetCidrReservation")
	defer b.mu.Unlock()

	for subnetID, reservations := range b.subnetCIDRReservations {
		for i, r := range reservations {
			if r.SubnetCIDRReservationID == reservationID {
				b.subnetCIDRReservations[subnetID] = append(reservations[:i], reservations[i+1:]...)

				return nil
			}
		}
	}

	return fmt.Errorf("%w: %s", ErrInvalidParameter, reservationID)
}
