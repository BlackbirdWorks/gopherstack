package ec2

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// Errors for batch3 operations.
var (
	ErrInstanceConnectEndpointNotFound = errors.New("InvalidInstanceConnectEndpointId.NotFound")
	ErrInstanceEventWindowNotFound     = errors.New("InvalidInstanceEventWindowId.NotFound")
	ErrCapacityReservationFull         = errors.New("CapacityReservationFull")
)

const (
	stateInRecycleBin      = "InRecycleBin"
	stateRestoring         = "restoring"
	statePending           = "pending"
	stateTaskCompleted     = "completed"
	stateTaskInProgress    = "InProgress"
	stateEnabledFastLaunch = "enabled"
	magicSplitLen          = 2
)

// InstanceConnectEndpoint represents an EC2 Instance Connect Endpoint.
type InstanceConnectEndpoint struct {
	CreateTime                 time.Time `json:"createTime"`
	InstanceConnectEndpointID  string    `json:"instanceConnectEndpointId"`
	InstanceConnectEndpointARN string    `json:"instanceConnectEndpointArn"`
	SubnetID                   string    `json:"subnetId"`
	VPCID                      string    `json:"vpcId"`
	State                      string    `json:"state"`
	SecurityGroupIDs           []string  `json:"securityGroupIds"`
	PreserveClientIP           bool      `json:"preserveClientIp"`
}

// InstanceEventWindow represents a scheduled maintenance window for instances.
type InstanceEventWindow struct {
	InstanceEventWindowID string `json:"instanceEventWindowId"`
	Name                  string `json:"name"`
	CronExpression        string `json:"cronExpression,omitempty"`
	State                 string `json:"state"`
}

// SpotDatafeed holds the spot instance data feed subscription settings.
type SpotDatafeed struct {
	Bucket string `json:"bucket"`
	Prefix string `json:"prefix"`
	State  string `json:"state"`
}

// RecycleBinImage holds a soft-deleted AMI.
type RecycleBinImage struct {
	RecycleBinEnterTime time.Time `json:"recycleBinEnterTime"`
	RecycleBinExitTime  time.Time `json:"recycleBinExitTime"`
	ImageID             string    `json:"imageId"`
	Name                string    `json:"name"`
}

// RecycleBinVolume holds a soft-deleted EBS volume.
type RecycleBinVolume struct {
	RecycleBinEnterTime time.Time `json:"recycleBinEnterTime"`
	RecycleBinExitTime  time.Time `json:"recycleBinExitTime"`
	VolumeID            string    `json:"volumeId"`
}

// ImageImportTask holds status for an ImportImage task.
type ImageImportTask struct {
	ImportTaskID string `json:"importTaskId"`
	Description  string `json:"description"`
	Architecture string `json:"architecture"`
	Platform     string `json:"platform"`
	Status       string `json:"status"`
}

// SnapshotImportTask holds status for an ImportSnapshot task.
type SnapshotImportTask struct {
	ImportTaskID string `json:"importTaskId"`
	Description  string `json:"description"`
	SnapshotID   string `json:"snapshotId,omitempty"`
	Status       string `json:"status"`
}

// ---- Capacity Reservation ----

// CreateCapacityReservation creates a new capacity reservation.
func (b *InMemoryBackend) CreateCapacityReservation(
	instanceType, availabilityZone string,
	instanceCount int,
) (*CapacityReservation, error) {
	if instanceType == "" {
		return nil, fmt.Errorf("%w: InstanceType is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateCapacityReservation")
	defer b.mu.Unlock()

	cr := &CapacityReservation{
		CapacityReservationID:  "cr-" + uuid.New().String()[:8],
		InstanceType:           instanceType,
		AvailabilityZone:       availabilityZone,
		TotalInstanceCount:     instanceCount,
		AvailableInstanceCount: instanceCount,
		State:                  stateActive,
		CreateTime:             time.Now().UTC(),
		OwnedBy:                b.AccountID,
	}
	b.capacityReservations[cr.CapacityReservationID] = cr

	return cr, nil
}

// CancelCapacityReservation cancels an active capacity reservation.
func (b *InMemoryBackend) CancelCapacityReservation(reservationID string) error {
	if reservationID == "" {
		return fmt.Errorf("%w: CapacityReservationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CancelCapacityReservation")
	defer b.mu.Unlock()

	cr, ok := b.capacityReservations[reservationID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrInvalidParameter, reservationID)
	}
	cr.State = "cancelled"

	return nil
}

// ModifyCapacityReservation updates instance count for a capacity reservation.
func (b *InMemoryBackend) ModifyCapacityReservation(reservationID string, instanceCount int) error {
	if reservationID == "" {
		return fmt.Errorf("%w: CapacityReservationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyCapacityReservation")
	defer b.mu.Unlock()

	cr, ok := b.capacityReservations[reservationID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrInvalidParameter, reservationID)
	}
	cr.TotalInstanceCount = instanceCount
	cr.AvailableInstanceCount = instanceCount

	return nil
}

// GetGroupsForCapacityReservation returns placement groups associated with a reservation.
// In this implementation, returns an empty list (no group associations tracked).
func (b *InMemoryBackend) GetGroupsForCapacityReservation(reservationID string) ([]string, error) {
	if reservationID == "" {
		return nil, fmt.Errorf("%w: CapacityReservationId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetGroupsForCapacityReservation")
	defer b.mu.RUnlock()

	if _, ok := b.capacityReservations[reservationID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrInvalidParameter, reservationID)
	}

	return []string{}, nil
}

// ---- Instance Connect Endpoint ----

// CreateInstanceConnectEndpoint creates a new Instance Connect Endpoint.
func (b *InMemoryBackend) CreateInstanceConnectEndpoint(
	subnetID string,
	securityGroupIDs []string,
	preserveClientIP bool,
) (*InstanceConnectEndpoint, error) {
	if subnetID == "" {
		return nil, fmt.Errorf("%w: SubnetId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateInstanceConnectEndpoint")
	defer b.mu.Unlock()

	subnet, ok := b.subnets[subnetID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrSubnetNotFound, subnetID)
	}

	id := "eice-" + uuid.New().String()[:8]
	ep := &InstanceConnectEndpoint{
		InstanceConnectEndpointID:  id,
		InstanceConnectEndpointARN: "arn:aws:ec2:" + b.Region + ":" + b.AccountID + ":instance-connect-endpoint/" + id,
		SubnetID:                   subnetID,
		VPCID:                      subnet.VPCID,
		SecurityGroupIDs:           securityGroupIDs,
		State:                      stateActive,
		PreserveClientIP:           preserveClientIP,
		CreateTime:                 time.Now().UTC(),
	}
	b.instanceConnectEndpoints[id] = ep

	return ep, nil
}

// DeleteInstanceConnectEndpoint removes an Instance Connect Endpoint.
func (b *InMemoryBackend) DeleteInstanceConnectEndpoint(id string) error {
	if id == "" {
		return fmt.Errorf("%w: InstanceConnectEndpointId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteInstanceConnectEndpoint")
	defer b.mu.Unlock()

	if _, ok := b.instanceConnectEndpoints[id]; !ok {
		return fmt.Errorf("%w: %s", ErrInstanceConnectEndpointNotFound, id)
	}
	delete(b.instanceConnectEndpoints, id)

	return nil
}

// DescribeInstanceConnectEndpoints returns endpoints, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeInstanceConnectEndpoints(
	ids []string,
) []*InstanceConnectEndpoint {
	b.mu.RLock("DescribeInstanceConnectEndpoints")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []*InstanceConnectEndpoint
	for _, ep := range b.instanceConnectEndpoints {
		if len(filter) > 0 && !filter[ep.InstanceConnectEndpointID] {
			continue
		}
		cp := *ep
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].InstanceConnectEndpointID < out[j].InstanceConnectEndpointID
	})

	return out
}

// ModifyInstanceConnectEndpoint modifies preserveClientIP for an endpoint.
func (b *InMemoryBackend) ModifyInstanceConnectEndpoint(id string, preserveClientIP bool) error {
	if id == "" {
		return fmt.Errorf("%w: InstanceConnectEndpointId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyInstanceConnectEndpoint")
	defer b.mu.Unlock()

	ep, ok := b.instanceConnectEndpoints[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrInstanceConnectEndpointNotFound, id)
	}
	ep.PreserveClientIP = preserveClientIP

	return nil
}

// ---- Instance Event Windows ----

// CreateInstanceEventWindow creates a maintenance event window.
func (b *InMemoryBackend) CreateInstanceEventWindow(
	name, cronExpression string,
) (*InstanceEventWindow, error) {
	b.mu.Lock("CreateInstanceEventWindow")
	defer b.mu.Unlock()

	ew := &InstanceEventWindow{
		InstanceEventWindowID: "iew-" + uuid.New().String()[:8],
		Name:                  name,
		CronExpression:        cronExpression,
		State:                 stateActive,
	}
	b.instanceEventWindows[ew.InstanceEventWindowID] = ew

	return ew, nil
}

// DeleteInstanceEventWindow removes an event window.
func (b *InMemoryBackend) DeleteInstanceEventWindow(id string) error {
	if id == "" {
		return fmt.Errorf("%w: InstanceEventWindowId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteInstanceEventWindow")
	defer b.mu.Unlock()

	if _, ok := b.instanceEventWindows[id]; !ok {
		return fmt.Errorf("%w: %s", ErrInstanceEventWindowNotFound, id)
	}
	delete(b.instanceEventWindows, id)

	return nil
}

// DescribeInstanceEventWindows returns event windows, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeInstanceEventWindows(ids []string) []*InstanceEventWindow {
	b.mu.RLock("DescribeInstanceEventWindows")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []*InstanceEventWindow
	for _, ew := range b.instanceEventWindows {
		if len(filter) > 0 && !filter[ew.InstanceEventWindowID] {
			continue
		}
		cp := *ew
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].InstanceEventWindowID < out[j].InstanceEventWindowID
	})

	return out
}

// ModifyInstanceEventWindow updates the cron expression for an event window.
func (b *InMemoryBackend) ModifyInstanceEventWindow(id, name, cronExpression string) error {
	if id == "" {
		return fmt.Errorf("%w: InstanceEventWindowId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyInstanceEventWindow")
	defer b.mu.Unlock()

	ew, ok := b.instanceEventWindows[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrInstanceEventWindowNotFound, id)
	}
	if name != "" {
		ew.Name = name
	}
	if cronExpression != "" {
		ew.CronExpression = cronExpression
	}

	return nil
}

// ---- Spot Datafeed ----

// CreateSpotDatafeedSubscription creates the account-level spot data feed.
func (b *InMemoryBackend) CreateSpotDatafeedSubscription(
	bucket, prefix string,
) (*SpotDatafeed, error) {
	if bucket == "" {
		return nil, fmt.Errorf("%w: Bucket is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateSpotDatafeedSubscription")
	defer b.mu.Unlock()

	b.spotDatafeed = &SpotDatafeed{Bucket: bucket, Prefix: prefix, State: stateActive}

	return b.spotDatafeed, nil
}

// DeleteSpotDatafeedSubscription removes the spot data feed.
func (b *InMemoryBackend) DeleteSpotDatafeedSubscription() {
	b.mu.Lock("DeleteSpotDatafeedSubscription")
	defer b.mu.Unlock()

	b.spotDatafeed = nil
}

// DescribeSpotDatafeedSubscription returns the current spot data feed.
func (b *InMemoryBackend) DescribeSpotDatafeedSubscription() *SpotDatafeed {
	b.mu.RLock("DescribeSpotDatafeedSubscription")
	defer b.mu.RUnlock()

	if b.spotDatafeed == nil {
		return nil
	}
	cp := *b.spotDatafeed

	return &cp
}

// ---- Image lifecycle ----

// RegisterImage registers a new AMI from a snapshot or manifest.
func (b *InMemoryBackend) RegisterImage(name, description, architecture string) (*AMIStub, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.Lock("RegisterImage")
	defer b.mu.Unlock()

	img := &AMIStub{
		ImageID:      "ami-" + uuid.New().String()[:17],
		Name:         name,
		Description:  description,
		Architecture: architecture,
	}
	if img.Architecture == "" {
		img.Architecture = archX8664
	}
	b.images[img.ImageID] = img

	return img, nil
}

// ImportImage creates an import task for importing a VM image.
func (b *InMemoryBackend) ImportImage(
	description, architecture, platform string,
) (*ImageImportTask, error) {
	b.mu.Lock("ImportImage")
	defer b.mu.Unlock()

	task := &ImageImportTask{
		ImportTaskID: "import-ami-" + uuid.New().String()[:8],
		Description:  description,
		Architecture: architecture,
		Platform:     platform,
		Status:       stateTaskCompleted,
	}
	b.imageImportTasks[task.ImportTaskID] = task

	return task, nil
}

// DescribeImportImageTasks returns import image tasks.
func (b *InMemoryBackend) DescribeImportImageTasks(taskIDs []string) []*ImageImportTask {
	b.mu.RLock("DescribeImportImageTasks")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(taskIDs))
	for _, id := range taskIDs {
		filter[id] = true
	}

	var out []*ImageImportTask
	for _, t := range b.imageImportTasks {
		if len(filter) > 0 && !filter[t.ImportTaskID] {
			continue
		}
		cp := *t
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ImportTaskID < out[j].ImportTaskID })

	return out
}

// ExportImage creates an export task for an AMI.
func (b *InMemoryBackend) ExportImage(imageID string, _ string) (string, error) {
	if imageID == "" {
		return "", fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	b.mu.RLock("ExportImage")
	defer b.mu.RUnlock()

	return "export-ami-" + uuid.New().String()[:8], nil
}

// ListImagesInRecycleBin returns soft-deleted AMIs.
func (b *InMemoryBackend) ListImagesInRecycleBin(imageIDs []string) []*RecycleBinImage {
	b.mu.RLock("ListImagesInRecycleBin")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(imageIDs))
	for _, id := range imageIDs {
		filter[id] = true
	}

	var out []*RecycleBinImage
	for _, img := range b.recycleBinImages {
		if len(filter) > 0 && !filter[img.ImageID] {
			continue
		}
		cp := *img
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ImageID < out[j].ImageID })

	return out
}

// RestoreImageFromRecycleBin restores a soft-deleted AMI.
func (b *InMemoryBackend) RestoreImageFromRecycleBin(imageID string) error {
	if imageID == "" {
		return fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	b.mu.Lock("RestoreImageFromRecycleBin")
	defer b.mu.Unlock()

	delete(b.recycleBinImages, imageID)

	return nil
}

// ---- Snapshot recycle bin ----

// ListSnapshotsInRecycleBin returns soft-deleted snapshots.
func (b *InMemoryBackend) ListSnapshotsInRecycleBin(snapshotIDs []string) []*Snapshot {
	b.mu.RLock("ListSnapshotsInRecycleBin")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(snapshotIDs))
	for _, id := range snapshotIDs {
		filter[id] = true
	}

	var out []*Snapshot
	for _, snap := range b.recycleBinSnapshots {
		if len(filter) > 0 && !filter[snap.SnapshotID] {
			continue
		}
		cp := *snap
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SnapshotID < out[j].SnapshotID })

	return out
}

// RestoreSnapshotFromRecycleBin restores a snapshot from recycle bin.
func (b *InMemoryBackend) RestoreSnapshotFromRecycleBin(snapshotID string) error {
	if snapshotID == "" {
		return fmt.Errorf("%w: SnapshotId is required", ErrInvalidParameter)
	}

	b.mu.Lock("RestoreSnapshotFromRecycleBin")
	defer b.mu.Unlock()

	snap, ok := b.recycleBinSnapshots[snapshotID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrSnapshotNotFound, snapshotID)
	}
	b.snapshots[snapshotID] = snap
	delete(b.recycleBinSnapshots, snapshotID)

	return nil
}

// RestoreSnapshotTier restores a snapshot from archive tier to standard.
func (b *InMemoryBackend) RestoreSnapshotTier(snapshotID string) error {
	if snapshotID == "" {
		return fmt.Errorf("%w: SnapshotId is required", ErrInvalidParameter)
	}

	b.mu.Lock("RestoreSnapshotTier")
	defer b.mu.Unlock()

	if _, ok := b.snapshots[snapshotID]; !ok {
		return fmt.Errorf("%w: %s", ErrSnapshotNotFound, snapshotID)
	}
	b.snapshotTiers[snapshotID] = stateDefaultCredit

	return nil
}

// ---- Import snapshot ----

// ImportSnapshot creates an import task for a snapshot.
func (b *InMemoryBackend) ImportSnapshot(description string) (*SnapshotImportTask, error) {
	b.mu.Lock("ImportSnapshot")
	defer b.mu.Unlock()

	task := &SnapshotImportTask{
		ImportTaskID: "import-snap-" + uuid.New().String()[:8],
		Description:  description,
		Status:       stateTaskCompleted,
	}
	b.snapshotImportTasks[task.ImportTaskID] = task

	return task, nil
}

// DescribeImportSnapshotTasks returns import snapshot tasks.
func (b *InMemoryBackend) DescribeImportSnapshotTasks(taskIDs []string) []*SnapshotImportTask {
	b.mu.RLock("DescribeImportSnapshotTasks")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(taskIDs))
	for _, id := range taskIDs {
		filter[id] = true
	}

	var out []*SnapshotImportTask
	for _, t := range b.snapshotImportTasks {
		if len(filter) > 0 && !filter[t.ImportTaskID] {
			continue
		}
		cp := *t
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ImportTaskID < out[j].ImportTaskID })

	return out
}

// ---- Fast launch / fast snapshot restores ----

// EnableFastLaunch enables Windows fast launch for an AMI.
func (b *InMemoryBackend) EnableFastLaunch(imageID string) error {
	if imageID == "" {
		return fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	b.mu.Lock("EnableFastLaunch")
	defer b.mu.Unlock()

	b.fastLaunchImages[imageID] = true

	return nil
}

// DisableFastLaunch disables Windows fast launch for an AMI.
func (b *InMemoryBackend) DisableFastLaunch(imageID string) error {
	if imageID == "" {
		return fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DisableFastLaunch")
	defer b.mu.Unlock()

	delete(b.fastLaunchImages, imageID)

	return nil
}

// FastLaunchImageItem holds fast launch enabled state for a single AMI.
type FastLaunchImageItem struct {
	ImageID string
	State   string
}

// DescribeFastLaunchImages returns AMIs with fast launch enabled.
func (b *InMemoryBackend) DescribeFastLaunchImages(imageIDs []string) []FastLaunchImageItem {
	b.mu.RLock("DescribeFastLaunchImages")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(imageIDs))
	for _, id := range imageIDs {
		filter[id] = true
	}

	var out []FastLaunchImageItem
	for imageID := range b.fastLaunchImages {
		if len(filter) > 0 && !filter[imageID] {
			continue
		}
		out = append(out, FastLaunchImageItem{ImageID: imageID, State: stateEnabledFastLaunch})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ImageID < out[j].ImageID })

	return out
}

// EnableFastSnapshotRestores enables fast snapshot restores for given AZ/snapshot combos.
func (b *InMemoryBackend) EnableFastSnapshotRestores(
	snapshotIDs, availabilityZones []string,
) error {
	b.mu.Lock("EnableFastSnapshotRestores")
	defer b.mu.Unlock()

	for _, snap := range snapshotIDs {
		for _, az := range availabilityZones {
			b.fastSnapshotRestores[snap+":"+az] = true
		}
	}

	return nil
}

// DisableFastSnapshotRestores disables fast snapshot restores.
func (b *InMemoryBackend) DisableFastSnapshotRestores(
	snapshotIDs, availabilityZones []string,
) error {
	b.mu.Lock("DisableFastSnapshotRestores")
	defer b.mu.Unlock()

	for _, snap := range snapshotIDs {
		for _, az := range availabilityZones {
			delete(b.fastSnapshotRestores, snap+":"+az)
		}
	}

	return nil
}

// FastSnapshotRestoreItem holds fast snapshot restore config for a snapshot/AZ combo.
type FastSnapshotRestoreItem struct {
	SnapshotID       string
	AvailabilityZone string
	State            string
}

// DescribeFastSnapshotRestores returns fast snapshot restore enabled items.
func (b *InMemoryBackend) DescribeFastSnapshotRestores() []FastSnapshotRestoreItem {
	b.mu.RLock("DescribeFastSnapshotRestores")
	defer b.mu.RUnlock()

	var out []FastSnapshotRestoreItem
	for key := range b.fastSnapshotRestores {
		// Key is "snapshotID:az"
		parts := splitKey(key)
		if len(parts) == magicSplitLen {
			out = append(out, FastSnapshotRestoreItem{
				SnapshotID:       parts[0],
				AvailabilityZone: parts[1],
				State:            stateEnabledFastLaunch,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SnapshotID != out[j].SnapshotID {
			return out[i].SnapshotID < out[j].SnapshotID
		}

		return out[i].AvailabilityZone < out[j].AvailabilityZone
	})

	return out
}

// splitKey splits a colon-separated key string into two parts.
func splitKey(key string) []string {
	for i, c := range key {
		if c == ':' {
			return []string{key[:i], key[i+1:]}
		}
	}

	return []string{key}
}

// ---- GetPasswordData ----

// GetPasswordData returns the password data for a Windows instance (empty in mock).
func (b *InMemoryBackend) GetPasswordData(instanceID string) (string, time.Time, error) {
	if instanceID == "" {
		return "", time.Time{}, fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetPasswordData")
	defer b.mu.RUnlock()

	if _, ok := b.instances[instanceID]; !ok {
		return "", time.Time{}, fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	return "", time.Now().UTC(), nil
}

// ---- GetConsoleScreenshot ----

// GetConsoleScreenshot returns an empty base64-encoded console screenshot.
func (b *InMemoryBackend) GetConsoleScreenshot(instanceID string) (string, error) {
	if instanceID == "" {
		return "", fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetConsoleScreenshot")
	defer b.mu.RUnlock()

	if _, ok := b.instances[instanceID]; !ok {
		return "", fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	return "", nil
}

// ---- GetInstanceTypesFromInstanceRequirements ----

// GetInstanceTypesFromInstanceRequirements returns a static list of instance types
// matching the given requirements (simplified mock).
func (b *InMemoryBackend) GetInstanceTypesFromInstanceRequirements() []string {
	return []string{
		instanceTypeT3Micro, instanceTypeT3Small, instanceTypeT3Medium, "m5.large",
		instanceTypeM5Xlarge,
	}
}

// ---- GetSubnetCidrReservations ----

// GetSubnetCidrReservations returns CIDR reservations for a subnet.
func (b *InMemoryBackend) GetSubnetCidrReservations(
	subnetID string,
) ([]*SubnetCIDRReservation, error) {
	if subnetID == "" {
		return nil, fmt.Errorf("%w: SubnetId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetSubnetCidrReservations")
	defer b.mu.RUnlock()

	reservations := b.subnetCIDRReservations[subnetID]
	out := make([]*SubnetCIDRReservation, len(reservations))
	for i, r := range reservations {
		cp := *r
		out[i] = &cp
	}

	return out, nil
}

// ---- GetSecurityGroupsForVpc ----

// SecurityGroupForVpcItem is a SG returned by GetSecurityGroupsForVpc.
type SecurityGroupForVpcItem struct {
	GroupID     string
	GroupName   string
	Description string
	VPCID       string
}

// GetSecurityGroupsForVpc returns security groups associated with a VPC.
func (b *InMemoryBackend) GetSecurityGroupsForVpc(vpcID string) ([]SecurityGroupForVpcItem, error) {
	if vpcID == "" {
		return nil, fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetSecurityGroupsForVpc")
	defer b.mu.RUnlock()

	var out []SecurityGroupForVpcItem
	for _, sg := range b.securityGroups {
		if sg.VPCID == vpcID {
			out = append(out, SecurityGroupForVpcItem{
				GroupID:     sg.ID,
				GroupName:   sg.Name,
				Description: sg.Description,
				VPCID:       sg.VPCID,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GroupID < out[j].GroupID })

	return out, nil
}

// ---- ReplaceRoute ----

// ReplaceRoute replaces an existing route in a route table.
func (b *InMemoryBackend) ReplaceRoute(rtID, destCIDR, gatewayID, natGatewayID string) error {
	if rtID == "" || destCIDR == "" {
		return fmt.Errorf(
			"%w: RouteTableId and DestinationCidrBlock are required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("ReplaceRoute")
	defer b.mu.Unlock()

	rt, ok := b.routeTables[rtID]
	if !ok {
		return fmt.Errorf("%w: route table %s not found", ErrInvalidParameter, rtID)
	}

	for i, route := range rt.Routes {
		if route.DestinationCIDR == destCIDR {
			rt.Routes[i] = Route{
				DestinationCIDR: destCIDR,
				GatewayID:       gatewayID,
				NatGatewayID:    natGatewayID,
				State:           stateActive,
			}

			return nil
		}
	}

	return fmt.Errorf("%w: route %s not found in %s", ErrInvalidParameter, destCIDR, rtID)
}

// ---- RegisterInstanceEventNotificationAttributes ----

// RegisterInstanceEventNotificationAttributes enables all-tag notification for instance events.
func (b *InMemoryBackend) RegisterInstanceEventNotificationAttributes(includeAllTags bool) {
	b.mu.Lock("RegisterInstanceEventNotificationAttributes")
	defer b.mu.Unlock()

	b.instanceEventNotifAttrs = &InstanceEventNotificationAttributes{
		IncludeAllTagsOfInstance: includeAllTags,
	}
}

// ---- ResetEbsDefaultKmsKeyID ----

// ResetEbsDefaultKmsKeyID resets the EBS default KMS key to the AWS-managed key.
func (b *InMemoryBackend) ResetEbsDefaultKmsKeyID() {
	b.mu.Lock("ResetEbsDefaultKmsKeyID")
	defer b.mu.Unlock()

	b.ebsDefaultKmsKeyID = ""
}

// ---- UpdateSecurityGroupRuleDescriptions ----

// UpdateSecurityGroupRuleDescriptionsIngress updates descriptions of ingress rules.
func (b *InMemoryBackend) UpdateSecurityGroupRuleDescriptionsIngress(
	groupID string,
	_ []SecurityGroupRule,
) error {
	if groupID == "" {
		return fmt.Errorf("%w: GroupId is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateSecurityGroupRuleDescriptionsIngress")
	defer b.mu.Unlock()

	if _, ok := b.securityGroups[groupID]; !ok {
		return fmt.Errorf("%w: %s", ErrSecurityGroupNotFound, groupID)
	}

	return nil
}

// UpdateSecurityGroupRuleDescriptionsEgress updates descriptions of egress rules.
func (b *InMemoryBackend) UpdateSecurityGroupRuleDescriptionsEgress(
	groupID string,
	_ []SecurityGroupRule,
) error {
	if groupID == "" {
		return fmt.Errorf("%w: GroupId is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateSecurityGroupRuleDescriptionsEgress")
	defer b.mu.Unlock()

	if _, ok := b.securityGroups[groupID]; !ok {
		return fmt.Errorf("%w: %s", ErrSecurityGroupNotFound, groupID)
	}

	return nil
}

// ---- Volume recycle bin ----

// ListVolumesInRecycleBin returns soft-deleted volumes.
func (b *InMemoryBackend) ListVolumesInRecycleBin(volumeIDs []string) []*RecycleBinVolume {
	b.mu.RLock("ListVolumesInRecycleBin")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(volumeIDs))
	for _, id := range volumeIDs {
		filter[id] = true
	}

	var out []*RecycleBinVolume
	for _, vol := range b.recycleBinVolumes {
		if len(filter) > 0 && !filter[vol.VolumeID] {
			continue
		}
		cp := *vol
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].VolumeID < out[j].VolumeID })

	return out
}

// RestoreVolumeFromRecycleBin restores a volume from recycle bin.
func (b *InMemoryBackend) RestoreVolumeFromRecycleBin(volumeID string) error {
	if volumeID == "" {
		return fmt.Errorf("%w: VolumeId is required", ErrInvalidParameter)
	}

	b.mu.Lock("RestoreVolumeFromRecycleBin")
	defer b.mu.Unlock()

	if _, ok := b.recycleBinVolumes[volumeID]; !ok {
		return fmt.Errorf("%w: %s", ErrVolumeNotFound, volumeID)
	}
	delete(b.recycleBinVolumes, volumeID)

	return nil
}

// ---- RestoreAddressToClassic ----

// RestoreAddressToClassic moves an EIP from VPC to EC2-Classic (no-op in mock).
func (b *InMemoryBackend) RestoreAddressToClassic(publicIP string) error {
	if publicIP == "" {
		return fmt.Errorf("%w: PublicIp is required", ErrInvalidParameter)
	}

	return nil
}

// ---- ReportInstanceStatus ----

// ReportInstanceStatus records a custom status for an instance.
func (b *InMemoryBackend) ReportInstanceStatus(instanceID string, _ string, _ string) error {
	if instanceID == "" {
		return fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	b.mu.RLock("ReportInstanceStatus")
	defer b.mu.RUnlock()

	if _, ok := b.instances[instanceID]; !ok {
		return fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	return nil
}

// ---- VPN operations ----

// ModifyVpnConnection updates properties of a VPN connection.
func (b *InMemoryBackend) ModifyVpnConnection(vpnConnectionID string, _ string) error {
	if vpnConnectionID == "" {
		return fmt.Errorf("%w: VpnConnectionId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyVpnConnection")
	defer b.mu.Unlock()

	if _, ok := b.vpnConnections[vpnConnectionID]; !ok {
		return fmt.Errorf("%w: %s", ErrInvalidParameter, vpnConnectionID)
	}

	return nil
}

// VpnConnectionRoute represents a static route in a VPN connection.
type VpnConnectionRoute struct {
	VpnConnectionID string `json:"vpnConnectionId"`
	DestinationCIDR string `json:"destinationCidrBlock"`
	State           string `json:"state"`
}

// CreateVpnConnectionRoute adds a static route to a VPN connection.
func (b *InMemoryBackend) CreateVpnConnectionRoute(
	vpnConnectionID, destinationCIDR string,
) (*VpnConnectionRoute, error) {
	if vpnConnectionID == "" || destinationCIDR == "" {
		return nil, fmt.Errorf(
			"%w: VpnConnectionId and DestinationCidrBlock are required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("CreateVpnConnectionRoute")
	defer b.mu.Unlock()

	if _, ok := b.vpnConnections[vpnConnectionID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrInvalidParameter, vpnConnectionID)
	}

	route := &VpnConnectionRoute{
		VpnConnectionID: vpnConnectionID,
		DestinationCIDR: destinationCIDR,
		State:           stateActive,
	}
	b.vpnConnectionRoutes[vpnConnectionID+":"+destinationCIDR] = route

	return route, nil
}

// DeleteVpnConnectionRoute removes a static route from a VPN connection.
func (b *InMemoryBackend) DeleteVpnConnectionRoute(vpnConnectionID, destinationCIDR string) error {
	if vpnConnectionID == "" || destinationCIDR == "" {
		return fmt.Errorf(
			"%w: VpnConnectionId and DestinationCidrBlock are required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("DeleteVpnConnectionRoute")
	defer b.mu.Unlock()

	key := vpnConnectionID + ":" + destinationCIDR
	if _, ok := b.vpnConnectionRoutes[key]; !ok {
		return fmt.Errorf("%w: route %s not found", ErrInvalidParameter, destinationCIDR)
	}
	delete(b.vpnConnectionRoutes, key)

	return nil
}

// ---- ModifyTransitGateway ----

// ModifyTransitGateway updates properties of a transit gateway.
func (b *InMemoryBackend) ModifyTransitGateway(tgwID, description string) error {
	if tgwID == "" {
		return fmt.Errorf("%w: TransitGatewayId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyTransitGateway")
	defer b.mu.Unlock()

	tgw, ok := b.transitGateways[tgwID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrInvalidParameter, tgwID)
	}
	if description != "" {
		tgw.Description = description
	}

	return nil
}
