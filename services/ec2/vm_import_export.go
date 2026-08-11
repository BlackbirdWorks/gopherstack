package ec2

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

// vm_import_export.go: VM Import/Export, Bundle Instance, and Conversion Task
// family. ExportImage/DescribeExportImageTasks integrate with the
// b.exportImageTasks map populated by ExportImage in backend_batch3.go;
// CancelImportTask operates across batch3's imageImportTasks/snapshotImportTasks.

// ---- Errors ----

var (
	// ErrBundleTaskNotFound is returned when a bundle task ID does not exist.
	ErrBundleTaskNotFound = errors.New("InvalidBundleID.NotFound")

	// ErrConversionTaskNotFound is returned when a conversion task ID does not exist.
	ErrConversionTaskNotFound = errors.New("InvalidConversionTaskId.NotFound")

	// ErrExportTaskNotFound is returned when an instance export task ID does not exist.
	ErrExportTaskNotFound = errors.New("InvalidExportTaskID.NotFound")

	// ErrImportTaskNotFound is returned when an ImportImage/ImportSnapshot task ID does not
	// exist in either import task map.
	ErrImportTaskNotFound = errors.New("InvalidParameterValue")

	// ErrTaskNotCancellable is returned when Cancel* is called on a task that has already
	// reached a terminal state (completed/cancelled/failed).
	ErrTaskNotCancellable = errors.New("IncorrectState")
)

// Lifecycle state constants shared by the bundle/conversion/export task families. Values
// mirror the AWS-modeled enums (BundleTaskState, ConversionTaskState, ExportTaskState).
const (
	bundleStatePending           = "pending"
	bundleStateComplete          = "complete"
	vmTaskStateActive            = "active"
	vmTaskStateCancelling        = "cancelling"
	vmTaskStateCancelled         = "cancelled"
	taskFullProgress             = "100%"
	taskZeroProgress             = "0%"
	conversionKindInstance       = "instance"
	conversionKindVolume         = "volume"
	conversionTaskExpiryWindow   = 30 * 24 * time.Hour
	defaultExportImageFormat     = "vmdk"
	defaultExportDiskImageFormat = "VMDK"
)

// ---- Data types ----

// BundleTask represents a BundleInstance task that packages an instance-store backed
// Windows instance's root volume into an AMI manifest stored in Amazon S3.
type BundleTask struct {
	BundleID     string    `json:"bundleId,omitempty"`
	InstanceID   string    `json:"instanceId,omitempty"`
	State        string    `json:"state,omitempty"`
	Progress     string    `json:"progress,omitempty"`
	S3Bucket     string    `json:"s3Bucket,omitempty"`
	S3Prefix     string    `json:"s3Prefix,omitempty"`
	StartTime    time.Time `json:"startTime"`
	UpdateTime   time.Time `json:"updateTime"`
	ErrorCode    string    `json:"errorCode,omitempty"`
	ErrorMessage string    `json:"errorMessage,omitempty"`
}

// ConversionTask represents an ImportInstance or ImportVolume conversion task.
type ConversionTask struct {
	ExpirationTime    time.Time `json:"expirationTime"`
	InstanceID        string    `json:"instanceId,omitempty"`
	State             string    `json:"state,omitempty"`
	StatusMessage     string    `json:"statusMessage,omitempty"`
	Kind              string    `json:"kind,omitempty"`
	Description       string    `json:"description,omitempty"`
	ConversionTaskID  string    `json:"conversionTaskId,omitempty"`
	Platform          string    `json:"platform,omitempty"`
	AvailabilityZone  string    `json:"availabilityZone,omitempty"`
	VolumeID          string    `json:"volumeId,omitempty"`
	ImageFormat       string    `json:"imageFormat,omitempty"`
	ImportManifestURL string    `json:"importManifestUrl,omitempty"`
	VolumeSize        int64     `json:"volumeSize,omitempty"`
	ImageBytes        int64     `json:"imageBytes,omitempty"`
}

// ExportTask represents a CreateInstanceExportTask (instance export) task.
type ExportTask struct {
	ExportTaskID      string `json:"exportTaskId,omitempty"`
	Description       string `json:"description,omitempty"`
	State             string `json:"state,omitempty"`
	StatusMessage     string `json:"statusMessage,omitempty"`
	InstanceID        string `json:"instanceId,omitempty"`
	TargetEnvironment string `json:"targetEnvironment,omitempty"`
	DiskImageFormat   string `json:"diskImageFormat,omitempty"`
	ContainerFormat   string `json:"containerFormat,omitempty"`
	S3Bucket          string `json:"s3Bucket,omitempty"`
	S3Key             string `json:"s3Key,omitempty"`
}

// ExportImageTaskRec represents an ExportImage task (AMI export to Amazon S3). Populated by
// ExportImage in backend_batch3.go and settled by DescribeExportImageTasks below.
type ExportImageTaskRec struct {
	ExportImageTaskID string `json:"exportImageTaskId,omitempty"`
	Description       string `json:"description,omitempty"`
	ImageID           string `json:"imageId,omitempty"`
	DiskImageFormat   string `json:"diskImageFormat,omitempty"`
	Progress          string `json:"progress,omitempty"`
	Status            string `json:"status,omitempty"`
	StatusMessage     string `json:"statusMessage,omitempty"`
	S3Bucket          string `json:"s3Bucket,omitempty"`
	S3Prefix          string `json:"s3Prefix,omitempty"`
	RoleName          string `json:"roleName,omitempty"`
}

// ---- Reset ----

// resetVMImportExportMapsLocked re-initialises all maps owned by this file. Must be called
// with b.mu held.
func (b *InMemoryBackend) resetVMImportExportMapsLocked() {
}

// ---- Bundle tasks ----

// BundleInstance starts a bundle task for an instance-store backed Windows instance.
func (b *InMemoryBackend) BundleInstance(instanceID, s3Bucket, s3Prefix string) (*BundleTask, error) {
	if instanceID == "" {
		return nil, fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	if s3Bucket == "" {
		return nil, fmt.Errorf("%w: Storage.S3.Bucket is required", ErrInvalidParameter)
	}

	b.mu.Lock("BundleInstance")
	defer b.mu.Unlock()

	if _, ok := b.instances.Get(instanceID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	now := time.Now().UTC()
	task := &BundleTask{
		BundleID:   "bundle-" + uuid.New().String()[:8],
		InstanceID: instanceID,
		State:      bundleStatePending,
		Progress:   taskZeroProgress,
		S3Bucket:   s3Bucket,
		S3Prefix:   s3Prefix,
		StartTime:  now,
		UpdateTime: now,
	}
	b.bundleTasks.Put(task)

	cp := *task

	return &cp, nil
}

// CancelBundleTask cancels a bundle task that has not yet reached a terminal state.
func (b *InMemoryBackend) CancelBundleTask(bundleID string) (*BundleTask, error) {
	if bundleID == "" {
		return nil, fmt.Errorf("%w: BundleId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CancelBundleTask")
	defer b.mu.Unlock()

	task, ok := b.bundleTasks.Get(bundleID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrBundleTaskNotFound, bundleID)
	}

	switch task.State {
	case bundleStateComplete, vmTaskStateCancelled, "failed":
		return nil, fmt.Errorf("%w: bundle task %s is already %s", ErrTaskNotCancellable, bundleID, task.State)
	}

	task.State = vmTaskStateCancelling
	task.UpdateTime = time.Now().UTC()

	cp := *task

	return &cp, nil
}

// DescribeBundleTasks returns bundle tasks, optionally filtered by ID. Non-terminal tasks
// settle to their terminal state as a side effect of being described, mirroring how bundle
// tasks in AWS complete asynchronously in the background.
func (b *InMemoryBackend) DescribeBundleTasks(ids []string) []*BundleTask {
	b.mu.Lock("DescribeBundleTasks")
	defer b.mu.Unlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	out := make([]*BundleTask, 0, b.bundleTasks.Len())

	for _, t := range b.bundleTasks.All() {
		if len(filter) > 0 && !filter[t.BundleID] {
			continue
		}

		settleBundleTask(t)
		cp := *t
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].BundleID < out[j].BundleID })

	return out
}

// settleBundleTask advances a bundle task to its terminal state. Must be called with b.mu
// held for writing.
func settleBundleTask(t *BundleTask) {
	switch t.State {
	case bundleStatePending, "waiting-for-shutdown", "bundling", "storing":
		t.State = bundleStateComplete
		t.Progress = taskFullProgress
		t.UpdateTime = time.Now().UTC()
	case vmTaskStateCancelling:
		t.State = vmTaskStateCancelled
		t.UpdateTime = time.Now().UTC()
	}
}

// ---- Conversion tasks (ImportInstance / ImportVolume) ----

// ImportInstance starts a conversion task that imports a VM image as a new instance.
func (b *InMemoryBackend) ImportInstance(
	description, platform, availabilityZone, diskFormat string, diskBytes, volumeSize int64,
) (*ConversionTask, error) {
	if platform == "" {
		return nil, fmt.Errorf("%w: Platform is required", ErrInvalidParameter)
	}

	b.mu.Lock("ImportInstance")
	defer b.mu.Unlock()

	if availabilityZone == "" {
		availabilityZone = b.Region + "a"
	}

	now := time.Now().UTC()
	task := &ConversionTask{
		ConversionTaskID: "import-i-" + uuid.New().String()[:8],
		Kind:             conversionKindInstance,
		State:            vmTaskStateActive,
		StatusMessage:    vmTaskStateActive,
		Description:      description,
		Platform:         platform,
		AvailabilityZone: availabilityZone,
		InstanceID:       newInstanceID(),
		VolumeSize:       volumeSize,
		ImageFormat:      diskFormat,
		ImageBytes:       diskBytes,
		ExpirationTime:   now.Add(conversionTaskExpiryWindow),
	}
	b.conversionTasks.Put(task)

	cp := *task

	return &cp, nil
}

// ImportVolume starts a conversion task that imports a disk image as a new EBS volume.
func (b *InMemoryBackend) ImportVolume(
	description, availabilityZone, diskFormat string, diskBytes, volumeSize int64,
) (*ConversionTask, error) {
	if availabilityZone == "" {
		return nil, fmt.Errorf("%w: AvailabilityZone is required", ErrInvalidParameter)
	}

	if volumeSize <= 0 {
		return nil, fmt.Errorf("%w: Volume.Size is required", ErrInvalidParameter)
	}

	b.mu.Lock("ImportVolume")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	task := &ConversionTask{
		ConversionTaskID: "import-vol-" + uuid.New().String()[:8],
		Kind:             conversionKindVolume,
		State:            vmTaskStateActive,
		StatusMessage:    vmTaskStateActive,
		Description:      description,
		AvailabilityZone: availabilityZone,
		VolumeID:         newVolumeID(),
		VolumeSize:       volumeSize,
		ImageFormat:      diskFormat,
		ImageBytes:       diskBytes,
		ExpirationTime:   now.Add(conversionTaskExpiryWindow),
	}
	b.conversionTasks.Put(task)

	cp := *task

	return &cp, nil
}

// DescribeConversionTasks returns conversion tasks, optionally filtered by ID. Active tasks
// settle to "completed" (or cancelling tasks to "cancelled") as a side effect of being
// described.
func (b *InMemoryBackend) DescribeConversionTasks(ids []string) []*ConversionTask {
	b.mu.Lock("DescribeConversionTasks")
	defer b.mu.Unlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	out := make([]*ConversionTask, 0, b.conversionTasks.Len())

	for _, t := range b.conversionTasks.All() {
		if len(filter) > 0 && !filter[t.ConversionTaskID] {
			continue
		}

		settleConversionTask(t)
		cp := *t
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ConversionTaskID < out[j].ConversionTaskID })

	return out
}

// CancelConversionTask cancels a conversion task that has not yet reached a terminal state.
func (b *InMemoryBackend) CancelConversionTask(conversionTaskID string) (*ConversionTask, error) {
	if conversionTaskID == "" {
		return nil, fmt.Errorf("%w: ConversionTaskId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CancelConversionTask")
	defer b.mu.Unlock()

	task, ok := b.conversionTasks.Get(conversionTaskID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrConversionTaskNotFound, conversionTaskID)
	}

	if task.State == stateTaskCompleted || task.State == vmTaskStateCancelled {
		return nil, fmt.Errorf(
			"%w: conversion task %s is already %s", ErrTaskNotCancellable, conversionTaskID, task.State,
		)
	}

	task.State = vmTaskStateCancelling
	task.StatusMessage = "Cancelling"

	cp := *task

	return &cp, nil
}

// settleConversionTask advances a conversion task to its terminal state. Must be called
// with b.mu held for writing.
func settleConversionTask(t *ConversionTask) {
	switch t.State {
	case vmTaskStateActive:
		t.State = stateTaskCompleted
		t.StatusMessage = "Completed"
	case vmTaskStateCancelling:
		t.State = vmTaskStateCancelled
		t.StatusMessage = "Cancelled"
	}
}

// ---- Instance export tasks ----

// CreateInstanceExportTask starts an export task that exports an instance to Amazon S3.
func (b *InMemoryBackend) CreateInstanceExportTask(
	instanceID, description, targetEnvironment, diskImageFormat, containerFormat, s3Bucket, s3Prefix string,
) (*ExportTask, error) {
	if instanceID == "" {
		return nil, fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateInstanceExportTask")
	defer b.mu.Unlock()

	if _, ok := b.instances.Get(instanceID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	if diskImageFormat == "" {
		diskImageFormat = defaultExportDiskImageFormat
	}

	id := "export-i-" + uuid.New().String()[:8]
	task := &ExportTask{
		ExportTaskID:      id,
		Description:       description,
		State:             vmTaskStateActive,
		StatusMessage:     vmTaskStateActive,
		InstanceID:        instanceID,
		TargetEnvironment: targetEnvironment,
		DiskImageFormat:   diskImageFormat,
		ContainerFormat:   containerFormat,
		S3Bucket:          s3Bucket,
		S3Key:             s3Prefix + id + "." + strings.ToLower(diskImageFormat),
	}
	b.exportTasks.Put(task)

	cp := *task

	return &cp, nil
}

// CancelExportTask cancels an instance or image export task that has not yet reached a
// terminal state.
func (b *InMemoryBackend) CancelExportTask(exportTaskID string) error {
	if exportTaskID == "" {
		return fmt.Errorf("%w: ExportTaskId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CancelExportTask")
	defer b.mu.Unlock()

	task, ok := b.exportTasks.Get(exportTaskID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrExportTaskNotFound, exportTaskID)
	}

	if task.State == stateTaskCompleted || task.State == vmTaskStateCancelled {
		return fmt.Errorf("%w: export task %s is already %s", ErrTaskNotCancellable, exportTaskID, task.State)
	}

	task.State = vmTaskStateCancelling
	task.StatusMessage = "Cancelling"

	return nil
}

// DescribeExportTasks returns instance export tasks, optionally filtered by ID. Active
// tasks settle to "completed" (or cancelling tasks to "cancelled") as a side effect of
// being described.
func (b *InMemoryBackend) DescribeExportTasks(ids []string) []*ExportTask {
	b.mu.Lock("DescribeExportTasks")
	defer b.mu.Unlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	out := make([]*ExportTask, 0, b.exportTasks.Len())

	for _, t := range b.exportTasks.All() {
		if len(filter) > 0 && !filter[t.ExportTaskID] {
			continue
		}

		settleExportTask(t)
		cp := *t
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ExportTaskID < out[j].ExportTaskID })

	return out
}

// settleExportTask advances an export task to its terminal state. Must be called with
// b.mu held for writing.
func settleExportTask(t *ExportTask) {
	switch t.State {
	case vmTaskStateActive:
		t.State = stateTaskCompleted
		t.StatusMessage = "Completed"
	case vmTaskStateCancelling:
		t.State = vmTaskStateCancelled
		t.StatusMessage = "Cancelled"
	}
}

// ---- Export image tasks ----

// DescribeExportImageTasks returns AMI export tasks, optionally filtered by ID. Active
// tasks settle to "completed" as a side effect of being described.
func (b *InMemoryBackend) DescribeExportImageTasks(ids []string) []*ExportImageTaskRec {
	b.mu.Lock("DescribeExportImageTasks")
	defer b.mu.Unlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	out := make([]*ExportImageTaskRec, 0, b.exportImageTasks.Len())

	for _, t := range b.exportImageTasks.All() {
		if len(filter) > 0 && !filter[t.ExportImageTaskID] {
			continue
		}

		if t.Status == vmTaskStateActive {
			t.Status = stateTaskCompleted
			t.StatusMessage = "completed"
			t.Progress = taskFullProgress
		}

		cp := *t
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ExportImageTaskID < out[j].ExportImageTaskID })

	return out
}

// ---- CancelImportTask (ImportImage / ImportSnapshot) ----

// CancelImportTask cancels an in-progress ImportImage or ImportSnapshot task, identified by
// its import task ID (import-ami-* or import-snap-*). Returns the task's previous and new
// state. Tasks that have already reached a terminal state cannot be cancelled.
func (b *InMemoryBackend) CancelImportTask(importTaskID string) (string, string, error) {
	if importTaskID == "" {
		return "", "", fmt.Errorf("%w: ImportTaskId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CancelImportTask")
	defer b.mu.Unlock()

	if task, ok := b.imageImportTasks.Get(importTaskID); ok {
		return cancelImportTaskStatus(importTaskID, &task.Status)
	}

	if task, ok := b.snapshotImportTasks.Get(importTaskID); ok {
		return cancelImportTaskStatus(importTaskID, &task.Status)
	}

	return "", "", fmt.Errorf("%w: %s", ErrImportTaskNotFound, importTaskID)
}

// cancelImportTaskStatus transitions an import task's status field to "cancelled" unless it
// has already reached a terminal state. Must be called with b.mu held for writing.
func cancelImportTaskStatus(importTaskID string, status *string) (string, string, error) {
	prev := *status
	if prev == stateTaskCompleted || prev == vmTaskStateCancelled {
		return "", "", fmt.Errorf("%w: import task %s is already %s", ErrTaskNotCancellable, importTaskID, prev)
	}

	*status = vmTaskStateCancelled

	return prev, *status, nil
}
