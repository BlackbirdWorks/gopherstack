package ec2

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// ModifyVolume records a volume modification request and immediately applies
// the target type/size so DescribeVolumes reflects the change.
func (b *InMemoryBackend) ModifyVolume(
	volumeID, volumeType string,
	size, iops int,
) (*VolumeModification, error) {
	if volumeID == "" {
		return nil, fmt.Errorf("%w: VolumeId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyVolume")
	defer b.mu.Unlock()

	vol, ok := b.volumes.Get(volumeID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrVolumeNotFound, volumeID)
	}

	mod := &VolumeModification{
		VolumeID:          volumeID,
		ModificationState: stateCompleted,
		OrigVolumeType:    vol.VolumeType,
		OrigSize:          vol.Size,
		TargetVolumeType:  volumeType,
		TargetSize:        size,
		TargetIops:        iops,
		StartTime:         time.Now().UTC(),
		Progress:          progressComplete,
	}
	mod.EndTime = mod.StartTime

	if volumeType != "" {
		vol.VolumeType = volumeType
	} else {
		mod.TargetVolumeType = vol.VolumeType
	}

	if size > 0 {
		vol.Size = size
	} else {
		mod.TargetSize = vol.Size
	}
	b.volumeModifications.Put(mod)

	return mod, nil
}

// ---- DescribeVolumeStatus ----

// VolumeStatusItem represents the status of a single EBS volume.
type VolumeStatusItem struct {
	VolumeID         string `json:"volumeID,omitempty"`
	AvailabilityZone string `json:"availabilityZone,omitempty"`
	VolumeStatus     string `json:"volumeStatus,omitempty"`
	StatusMessage    string `json:"statusMessage,omitempty"`
}

// DescribeVolumeStatus returns health status for the given volumes (all if ids is empty).
func (b *InMemoryBackend) DescribeVolumeStatus(ids []string) []VolumeStatusItem {
	b.mu.RLock("DescribeVolumeStatus")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []VolumeStatusItem
	for _, vol := range b.volumes.All() {
		if len(filter) > 0 && !filter[vol.ID] {
			continue
		}
		az := vol.AZ
		if az == "" {
			az = b.Region + "a"
		}
		out = append(out, VolumeStatusItem{
			VolumeID:         vol.ID,
			AvailabilityZone: az,
			VolumeStatus:     "ok",
			StatusMessage:    "",
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].VolumeID < out[j].VolumeID })

	return out
}

// ---- DescribeVolumesModifications ----

// DescribeVolumesModifications returns stored modification records.
func (b *InMemoryBackend) DescribeVolumesModifications(ids []string) []*VolumeModification {
	b.mu.RLock("DescribeVolumesModifications")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []*VolumeModification
	for _, mod := range b.volumeModifications.All() {
		if len(filter) > 0 && !filter[mod.VolumeID] {
			continue
		}
		cp := *mod
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].VolumeID < out[j].VolumeID })

	return out
}

// ---- CopySnapshot ----

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

	if _, ok := b.volumes.Get(volumeID); !ok {
		return fmt.Errorf("%w: %s", ErrVolumeNotFound, volumeID)
	}

	return nil
}

// ---- Snapshot locking ----

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
		if _, ok := b.volumes.Get(id); !ok {
			return nil, fmt.Errorf("%w: %s", ErrVolumeNotFound, id)
		}
	}

	results := make([]CopyVolumesResult, 0, len(volumeIDs))
	for _, id := range volumeIDs {
		src, _ := b.volumes.Get(id)
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
		b.volumes.Put(newVol)
		results = append(results, CopyVolumesResult{SourceVolumeID: id, DestVolumeID: newVol.ID})
	}

	return results, nil
}

// ---- VPC CIDR disassociation ----

// CreateReplaceRootVolumeTask creates a task to replace an instance's root volume.
func (b *InMemoryBackend) CreateReplaceRootVolumeTask(
	instanceID, snapshotID string,
) (*ReplaceRootVolumeTask, error) {
	if instanceID == "" {
		return nil, fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateReplaceRootVolumeTask")
	defer b.mu.Unlock()

	if _, ok := b.instances.Get(instanceID); !ok {
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
	b.replaceRootVolumeTasks.Put(task)

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
	for _, task := range b.replaceRootVolumeTasks.All() {
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

// ResetEbsDefaultKmsKeyID resets the EBS default KMS key to the AWS-managed key.
func (b *InMemoryBackend) ResetEbsDefaultKmsKeyID() {
	b.mu.Lock("ResetEbsDefaultKmsKeyID")
	defer b.mu.Unlock()

	b.ebsDefaultKmsKeyID = ""
}

// ---- UpdateSecurityGroupRuleDescriptions ----

// ListVolumesInRecycleBin returns soft-deleted volumes.
func (b *InMemoryBackend) ListVolumesInRecycleBin(volumeIDs []string) []*RecycleBinVolume {
	b.mu.RLock("ListVolumesInRecycleBin")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(volumeIDs))
	for _, id := range volumeIDs {
		filter[id] = true
	}

	var out []*RecycleBinVolume
	for _, vol := range b.recycleBinVolumes.All() {
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

	if _, ok := b.recycleBinVolumes.Get(volumeID); !ok {
		return fmt.Errorf("%w: %s", ErrVolumeNotFound, volumeID)
	}
	b.recycleBinVolumes.Delete(volumeID)

	return nil
}

// ---- RestoreAddressToClassic ----

// SetVolumeEncryption marks a volume as encrypted and records its KMS key ID.
// If encrypted is false the KMS key ID is cleared. If encrypted is true and
// kmsKeyID is empty an AWS-managed key ("aws/ebs") is implied.
func (b *InMemoryBackend) SetVolumeEncryption(
	volumeID string,
	encrypted bool,
	kmsKeyID string,
) error {
	b.mu.Lock("SetVolumeEncryption")
	defer b.mu.Unlock()

	vol, ok := b.volumes.Get(volumeID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrVolumeNotFound, volumeID)
	}

	vol.Encrypted = encrypted

	if encrypted {
		if kmsKeyID == "" {
			kmsKeyID = "alias/aws/ebs"
		}

		vol.KmsKeyID = kmsKeyID
	} else {
		vol.KmsKeyID = ""
	}

	return nil
}

// SetVolumePerformance sets the IOPS and throughput (MB/s) on an existing volume.
// A value of 0 leaves that field unchanged.
func (b *InMemoryBackend) SetVolumePerformance(volumeID string, iops, throughput int) error {
	b.mu.Lock("SetVolumePerformance")
	defer b.mu.Unlock()

	vol, ok := b.volumes.Get(volumeID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrVolumeNotFound, volumeID)
	}

	if iops > 0 {
		vol.Iops = iops
	}

	if throughput > 0 {
		vol.Throughput = throughput
	}

	return nil
}

// spotPriceBaseTable holds per-instance-type baseline prices in USD/hr.
// Values are approximate AWS on-demand prices used as a seed for spot history.
//
//nolint:gochecknoglobals,mnd // lookup table of AWS published prices
var spotPriceBaseTable = map[string]float64{
	"t2.micro":                   0.0116,
	"t2.small":                   0.023,
	"t2.medium":                  0.0464,
	"t2.large":                   0.0928,
	instanceTypeT3Micro:          0.0104,
	instanceTypeT3Small:          0.0208,
	instanceTypeT3Medium:         0.0416,
	"t3.large":                   0.0832,
	"t3.xlarge":                  0.1664,
	"t3.2xlarge":                 0.3328,
	spotFleetDefaultInstanceType: 0.096,
	instanceTypeM5Xlarge:         0.192,
	"m5.2xlarge":                 0.384,
	"m5.4xlarge":                 0.768,
	"c5.large":                   0.085,
	instanceTypeC5XL:             0.17,
	"c5.2xlarge":                 0.34,
	"r5.large":                   0.126,
	"r5.xlarge":                  0.252,
	"r5.2xlarge":                 0.504,
	"p3.2xlarge":                 3.06,
	"p3.8xlarge":                 12.24,
	"g4dn.xlarge":                0.526,
}
