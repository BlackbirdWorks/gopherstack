package ecs

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	attachmentTypeEBS = "AmazonElasticBlockStorage"

	attachmentStatusAttaching = "ATTACHING"
	attachmentStatusAttached  = "ATTACHED"
	attachmentStatusDetached  = "DETACHED"
	attachmentStatusDeleted   = "DELETED"

	detailVolumeName          = "volumeName"
	detailVolumeID            = "volumeId"
	detailRoleArn             = "roleArn"
	detailDeleteOnTermination = "deleteOnTermination"
)

var (
	errEBSUnavailable = errors.New("managed EBS volumes are unavailable: the EC2 service is not enabled")
	errVolumeConfig   = errors.New("invalid volumeConfigurations")

	ebsVolumeTypes = []string{"gp2", "gp3", "io1", "io2", "sc1", "st1", "standard"}
	ebsFilesystems = []string{"ext3", "ext4", "xfs", "ntfs"}
)

// ManagedEBSVolume is types.TaskManagedEBSVolumeConfiguration and types.ServiceManagedEBSVolumeConfiguration;
// terminationPolicy applies to tasks only.
type ManagedEBSVolume struct {
	Encrypted                *bool                 `json:"encrypted,omitempty"`
	Iops                     *int                  `json:"iops,omitempty"`
	SizeInGiB                *int                  `json:"sizeInGiB,omitempty"`
	Throughput               *int                  `json:"throughput,omitempty"`
	VolumeInitializationRate *int                  `json:"volumeInitializationRate,omitempty"`
	TerminationPolicy        *EBSTerminationPolicy `json:"terminationPolicy,omitempty"`
	RoleArn                  string                `json:"roleArn,omitempty"`
	FilesystemType           string                `json:"filesystemType,omitempty"`
	KMSKeyID                 string                `json:"kmsKeyId,omitempty"`
	SnapshotID               string                `json:"snapshotId,omitempty"`
	VolumeType               string                `json:"volumeType,omitempty"`
	TagSpecifications        []EBSTagSpecification `json:"tagSpecifications,omitempty"`
}

// EBSTerminationPolicy is types.TaskManagedEBSVolumeTerminationPolicy.
type EBSTerminationPolicy struct {
	DeleteOnTermination bool `json:"deleteOnTermination"`
}

// EBSTagSpecification is types.EBSTagSpecification.
type EBSTagSpecification struct {
	ResourceType  string `json:"resourceType"`
	PropagateTags string `json:"propagateTags,omitempty"`
	Tags          []Tag  `json:"tags,omitempty"`
}

// VolumeConfiguration is types.TaskVolumeConfiguration and types.ServiceVolumeConfiguration.
type VolumeConfiguration struct {
	ManagedEBSVolume *ManagedEBSVolume `json:"managedEBSVolume,omitempty"`
	Name             string            `json:"name"`
}

// EBSVolumeSpec is one EBS volume ECS asks the EC2 service to create for a task.
type EBSVolumeSpec struct {
	Encrypted                *bool
	Iops                     int
	Throughput               int
	VolumeInitializationRate int
	Region                   string
	VolumeType               string
	SnapshotID               string
	KMSKeyID                 string
	Tags                     []Tag
	SizeInGiB                int
}

// EBSProvisioner creates and deletes the EBS volumes ECS manages for tasks.
type EBSProvisioner interface {
	CreateTaskVolume(spec EBSVolumeSpec) (string, error)
	DeleteTaskVolume(region, volumeID string) error
}

// ebsMount is a task's managed EBS volume as the runtime mounts it.
type ebsMount struct {
	volumeID            string
	deleteOnTermination bool
}

// SetEBSProvisioner wires the EC2 service so configuredAtLaunch volumes become real EBS volumes.
func (b *InMemoryBackend) SetEBSProvisioner(p EBSProvisioner) {
	b.mu.Lock("SetEBSProvisioner")
	defer b.mu.Unlock()

	b.ebs = p
}

// validateVolumeConfigurations checks volumeConfigurations against the task definition's configuredAtLaunch
// volumes and the managedEBSVolume parameter rules.
func validateVolumeConfigurations(td *TaskDefinition, cfgs []VolumeConfiguration, forService bool) error {
	atLaunch := map[string]bool{}

	for _, v := range td.Volumes {
		if v.ConfiguredAtLaunch {
			atLaunch[v.Name] = true
		}
	}

	seen := map[string]bool{}

	for _, c := range cfgs {
		if !atLaunch[c.Name] {
			return fmt.Errorf("%w: %w: volume %q is not configuredAtLaunch in the task definition",
				ErrInvalidParameter, errVolumeConfig, c.Name)
		}

		if seen[c.Name] {
			return fmt.Errorf("%w: %w: volume %q is configured twice", ErrInvalidParameter, errVolumeConfig, c.Name)
		}

		seen[c.Name] = true

		if err := validateManagedEBS(c, forService); err != nil {
			return err
		}
	}

	for name := range atLaunch {
		if !seen[name] {
			return fmt.Errorf("%w: %w: volume %q is configuredAtLaunch and needs a volume configuration",
				ErrInvalidParameter, errVolumeConfig, name)
		}
	}

	return nil
}

func validateManagedEBS(c VolumeConfiguration, forService bool) error {
	e := c.ManagedEBSVolume
	if e == nil {
		return fmt.Errorf("%w: %w: volume %q needs managedEBSVolume", ErrInvalidParameter, errVolumeConfig, c.Name)
	}

	bad := func(format string, args ...any) error {
		return fmt.Errorf("%w: %w: volume %q: %s", ErrInvalidParameter, errVolumeConfig, c.Name,
			fmt.Sprintf(format, args...))
	}

	switch {
	case e.RoleArn == "":
		return bad("managedEBSVolume.roleArn is required")
	case e.SizeInGiB == nil && e.SnapshotID == "":
		return bad("either sizeInGiB or snapshotId must be specified")
	case e.SizeInGiB != nil && *e.SizeInGiB <= 0:
		return bad("sizeInGiB must be positive")
	case e.VolumeType != "" && !slices.Contains(ebsVolumeTypes, e.VolumeType):
		return bad("volumeType %q is not one of %v", e.VolumeType, ebsVolumeTypes)
	case e.FilesystemType != "" && !slices.Contains(ebsFilesystems, e.FilesystemType):
		return bad("filesystemType %q is not one of %v", e.FilesystemType, ebsFilesystems)
	case forService && e.TerminationPolicy != nil:
		return bad("terminationPolicy is not supported for services")
	}

	return nil
}

// provisionTaskVolumesLocked creates the task's managed EBS volumes and records them as ATTACHING attachments.
// Volumes created before a failure are deleted again.
func (b *InMemoryBackend) provisionTaskVolumesLocked(task *Task, cfgs []VolumeConfiguration) error {
	if len(cfgs) == 0 {
		return nil
	}

	if b.ebs == nil {
		return errEBSUnavailable
	}

	mounts := make(map[string]ebsMount, len(cfgs))

	var created []string

	for _, c := range cfgs {
		e := c.ManagedEBSVolume

		del := e.TerminationPolicy == nil || e.TerminationPolicy.DeleteOnTermination

		id, err := b.ebs.CreateTaskVolume(ebsSpecFor(b.region, e))
		if err != nil {
			for _, vid := range created {
				_ = b.ebs.DeleteTaskVolume(b.region, vid)
			}

			return fmt.Errorf("create EBS volume for %q: %w", c.Name, err)
		}

		created = append(created, id)
		mounts[c.Name] = ebsMount{volumeID: id, deleteOnTermination: del}
		task.Attachments = append(task.Attachments, TaskAttachment{
			ID: uuid.NewString(), Type: attachmentTypeEBS, Status: attachmentStatusAttaching,
			Details: []KeyValuePair{
				{Name: detailRoleArn, Value: e.RoleArn},
				{Name: detailDeleteOnTermination, Value: strconv.FormatBool(del)},
				{Name: detailVolumeName, Value: c.Name},
				{Name: detailVolumeID, Value: id},
			},
		})
	}

	task.ebsVolumes = mounts

	return nil
}

func ebsSpecFor(region string, e *ManagedEBSVolume) EBSVolumeSpec {
	spec := EBSVolumeSpec{
		Region: region, VolumeType: e.VolumeType, SnapshotID: e.SnapshotID, KMSKeyID: e.KMSKeyID,
		Encrypted: e.Encrypted,
	}

	if e.SizeInGiB != nil {
		spec.SizeInGiB = *e.SizeInGiB
	}

	if e.Iops != nil {
		spec.Iops = *e.Iops
	}

	if e.Throughput != nil {
		spec.Throughput = *e.Throughput
	}

	if e.VolumeInitializationRate != nil {
		spec.VolumeInitializationRate = *e.VolumeInitializationRate
	}

	for _, ts := range e.TagSpecifications {
		if ts.ResourceType == "volume" {
			spec.Tags = append(spec.Tags, ts.Tags...)
		}
	}

	return spec
}

// markTaskVolumesAttachedLocked flips ATTACHING EBS attachments to ATTACHED once the task runs.
func markTaskVolumesAttachedLocked(task *Task) {
	for i := range task.Attachments {
		a := &task.Attachments[i]
		if a.Type == attachmentTypeEBS && a.Status == attachmentStatusAttaching {
			a.Status = attachmentStatusAttached
		}
	}
}

// releaseTaskVolumesLocked deletes a stopped task's EBS volumes whose terminationPolicy asks for it and marks
// the attachments DELETED or DETACHED. Safe to call more than once.
func (b *InMemoryBackend) releaseTaskVolumesLocked(task *Task) {
	for i := range task.Attachments {
		a := &task.Attachments[i]
		if a.Type != attachmentTypeEBS || a.Status == attachmentStatusDeleted || a.Status == attachmentStatusDetached {
			continue
		}

		if attachmentDetail(*a, detailDeleteOnTermination) != "true" {
			a.Status = attachmentStatusDetached

			continue
		}

		a.Status = attachmentStatusDeleted

		if b.ebs == nil {
			continue
		}

		id := attachmentDetail(*a, detailVolumeID)
		if err := b.ebs.DeleteTaskVolume(b.region, id); err != nil {
			logger.Load(context.Background()).Warn("ecs: failed to delete task EBS volume",
				"task", task.TaskArn, "volume", id, "error", err)
		}
	}
}

func attachmentDetail(a TaskAttachment, name string) string {
	for _, d := range a.Details {
		if d.Name == name {
			return d.Value
		}
	}

	return ""
}
