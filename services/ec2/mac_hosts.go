package ec2

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// mac_hosts.go implements the EC2 Mac Dedicated Host and Mac
// modification task family: DescribeMacHosts, CreateMacSystemIntegrityProtectionModificationTask,
// CreateDelegateMacVolumeOwnershipTask, and DescribeMacModificationTasks.
//
// EC2 Mac Dedicated Hosts have no separate creation API: a Mac Dedicated Host
// is a regular Dedicated Host (see accept_ops.go's AllocateHosts)
// allocated with a mac1/mac2/mac-m* instance type. DescribeMacHosts therefore
// derives its results from the existing b.dedicatedHosts state rather than
// maintaining a second, parallel resource map.

// ErrMacInstanceRequired is returned when a Mac-only operation targets an
// instance that is not a Mac (mac1/mac2/mac-m*) instance type.
var ErrMacInstanceRequired = errors.New("InvalidParameterValue")

// Mac modification task lifecycle/type constants, mirroring the AWS-modeled
// MacModificationTaskState / MacModificationTaskType enums.
const (
	macTaskTypeSIP              = "sip-modification"
	macTaskTypeVolumeDelegation = "volume-ownership-delegation"
	macTaskStatePending         = "pending"
	macTaskStateSuccessful      = "successful"

	// resourceTypeMacModificationTask is the TagSpecification ResourceType
	// shared by CreateMacSystemIntegrityProtectionModificationTask and
	// CreateDelegateMacVolumeOwnershipTask.
	resourceTypeMacModificationTask = "mac-modification-task"
)

// MacHost represents an EC2 Mac Dedicated Host, derived on read from the
// dedicated host allocated for it.
type MacHost struct {
	HostID                       string   `json:"hostId,omitempty"`
	MacOSLatestSupportedVersions []string `json:"macOSLatestSupportedVersions,omitempty"`
}

// MacSIPConfig captures the resolved per-setting System Integrity Protection
// configuration recorded by a sip-modification MacModificationTask.
type MacSIPConfig struct {
	AppleInternal         string `json:"appleInternal,omitempty"`
	BaseSystem            string `json:"baseSystem,omitempty"`
	DTraceRestrictions    string `json:"dTraceRestrictions,omitempty"`
	DebuggingRestrictions string `json:"debuggingRestrictions,omitempty"`
	FilesystemProtections string `json:"filesystemProtections,omitempty"`
	KextSigning           string `json:"kextSigning,omitempty"`
	NvramProtections      string `json:"nvramProtections,omitempty"`
	Status                string `json:"status,omitempty"`
}

// MacModificationTask represents a System Integrity Protection (SIP)
// modification task or volume ownership delegation task for an EC2 Mac
// instance.
type MacModificationTask struct {
	StartTime             time.Time     `json:"startTime"`
	SIPConfig             *MacSIPConfig `json:"sipConfig,omitempty"`
	MacModificationTaskID string        `json:"macModificationTaskId,omitempty"`
	InstanceID            string        `json:"instanceId,omitempty"`
	TaskType              string        `json:"taskType,omitempty"`
	TaskState             string        `json:"taskState,omitempty"`
}

// resetMacHostMapsLocked re-initialises the Mac modification task map. Must be
// called with b.mu held. (Mac Hosts themselves are derived on read from
// b.dedicatedHosts, so there is no separate map to reset for them.)
func (b *InMemoryBackend) resetMacHostMapsLocked() {
}

// isMacInstanceType reports whether instanceType is an EC2 Mac Dedicated Host
// compatible instance type (mac1.*, mac2.*, mac2-m2.*, mac2-m2pro.*, mac-m4*, ...).
func isMacInstanceType(instanceType string) bool {
	return strings.HasPrefix(instanceType, "mac1") ||
		strings.HasPrefix(instanceType, "mac2") ||
		strings.HasPrefix(instanceType, "mac-m")
}

// macOSLatestSupportedVersions returns the deterministic set of macOS versions
// that a Mac Dedicated Host of the given instance type can launch without an
// upgrade.
func macOSLatestSupportedVersions(instanceType string) []string {
	switch {
	case strings.HasPrefix(instanceType, "mac2"):
		return []string{"15.3.1", "14.7.3", "13.6.7"}
	case strings.HasPrefix(instanceType, "mac-m"):
		return []string{"15.3.1", "14.7.3"}
	case strings.HasPrefix(instanceType, "mac1"):
		return []string{"12.7.6", "11.7.10"}
	default:
		return nil
	}
}

// DescribeMacHosts returns the Mac Dedicated Hosts (i.e. Dedicated Hosts
// allocated with a mac1/mac2/mac-m* instance type), optionally filtered by
// host ID.
func (b *InMemoryBackend) DescribeMacHosts(ids []string) []*MacHost {
	b.mu.RLock("DescribeMacHosts")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*MacHost, 0, b.dedicatedHosts.Len())

	for _, h := range b.dedicatedHosts.All() {
		if !isMacInstanceType(h.InstanceType) {
			continue
		}

		if len(idSet) > 0 && !idSet[h.HostID] {
			continue
		}

		out = append(out, &MacHost{
			HostID:                       h.HostID,
			MacOSLatestSupportedVersions: macOSLatestSupportedVersions(h.InstanceType),
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].HostID < out[j].HostID })

	return out
}

// requireMacInstanceLocked validates that instanceID exists and refers to a
// Mac (mac1/mac2/mac-m*) instance. Must be called with b.mu held.
func (b *InMemoryBackend) requireMacInstanceLocked(instanceID string) error {
	inst, ok := b.instances.Get(instanceID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	if !isMacInstanceType(inst.InstanceType) {
		return fmt.Errorf("%w: %s is not a Mac instance", ErrMacInstanceRequired, instanceID)
	}

	return nil
}

// CreateMacSystemIntegrityProtectionModificationTask creates a SIP
// modification task for the given Mac instance.
func (b *InMemoryBackend) CreateMacSystemIntegrityProtectionModificationTask(
	instanceID, status string, config *MacSIPConfig, tags map[string]string,
) (*MacModificationTask, error) {
	if instanceID == "" {
		return nil, fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	if status == "" {
		return nil, fmt.Errorf("%w: MacSystemIntegrityProtectionStatus is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateMacSystemIntegrityProtectionModificationTask")
	defer b.mu.Unlock()

	if err := b.requireMacInstanceLocked(instanceID); err != nil {
		return nil, err
	}

	if config == nil {
		config = &MacSIPConfig{}
	}

	config.Status = status

	task := &MacModificationTask{
		MacModificationTaskID: newMacModificationTaskID(),
		InstanceID:            instanceID,
		TaskType:              macTaskTypeSIP,
		TaskState:             macTaskStatePending,
		StartTime:             time.Now().UTC(),
		SIPConfig:             config,
	}
	b.macModificationTasks.Put(task)
	b.setTagsLocked(task.MacModificationTaskID, tags)

	cp := *task

	return &cp, nil
}

// CreateDelegateMacVolumeOwnershipTask creates a volume ownership delegation
// task for the given Apple silicon Mac instance.
func (b *InMemoryBackend) CreateDelegateMacVolumeOwnershipTask(
	instanceID, macCredentials string, tags map[string]string,
) (*MacModificationTask, error) {
	if instanceID == "" {
		return nil, fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	if macCredentials == "" {
		return nil, fmt.Errorf("%w: MacCredentials is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateDelegateMacVolumeOwnershipTask")
	defer b.mu.Unlock()

	if err := b.requireMacInstanceLocked(instanceID); err != nil {
		return nil, err
	}

	task := &MacModificationTask{
		MacModificationTaskID: newMacModificationTaskID(),
		InstanceID:            instanceID,
		TaskType:              macTaskTypeVolumeDelegation,
		TaskState:             macTaskStatePending,
		StartTime:             time.Now().UTC(),
	}
	b.macModificationTasks.Put(task)
	b.setTagsLocked(task.MacModificationTaskID, tags)

	cp := *task

	return &cp, nil
}

// DescribeMacModificationTasks returns Mac modification tasks, optionally
// filtered by ID. Pending tasks settle to "successful" as a side effect of
// being described, mirroring how these tasks complete asynchronously in AWS.
func (b *InMemoryBackend) DescribeMacModificationTasks(ids []string) []*MacModificationTask {
	b.mu.Lock("DescribeMacModificationTasks")
	defer b.mu.Unlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*MacModificationTask, 0, b.macModificationTasks.Len())

	for _, t := range b.macModificationTasks.All() {
		if len(idSet) > 0 && !idSet[t.MacModificationTaskID] {
			continue
		}

		if t.TaskState == macTaskStatePending {
			t.TaskState = macTaskStateSuccessful
		}

		cp := *t
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].MacModificationTaskID < out[j].MacModificationTaskID })

	return out
}
