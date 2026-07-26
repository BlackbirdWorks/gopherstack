package ec2

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"
)

// image_ops.go implements three image-related families layered on top of the
// existing AMI state (b.images, backend_batch3.go / backend_refinement2.go): the
// account-region "Allowed AMIs" settings, Store/Restore Image tasks (AMI <-> S3 bucket), a
// bulk Image Usage Report feature, and ConfirmProductInstance.

// ---- Errors ----

// ErrImageNotFound is returned when an AMI ID does not exist.
var ErrImageNotFound = errors.New("InvalidAMIID.NotFound")

// ErrUsageReportNotFound is returned when an image usage report ID does not exist.
var ErrUsageReportNotFound = errors.New("InvalidParameterValue")

// Allowed Images Settings states.
const (
	allowedImagesStateEnabled   = "enabled"
	allowedImagesStateAudit     = "audit-mode"
	allowedImagesStateDisabled  = "disabled"
	allowedImagesManagedByAccnt = "account"
)

// ---- Allowed Images Settings ----

// CreationDateCondition bounds allowed images by maximum age since creation.
type CreationDateCondition struct {
	MaximumDaysSinceCreated int32 `json:"maximumDaysSinceCreated,omitempty"`
}

// DeprecationTimeCondition bounds allowed images by maximum time since deprecation.
type DeprecationTimeCondition struct {
	MaximumDaysSinceDeprecated int32 `json:"maximumDaysSinceDeprecated,omitempty"`
}

// ImageCriterion is a single set of criteria evaluated to determine whether AMIs are
// discoverable and usable in the account, per the Allowed AMIs feature.
type ImageCriterion struct {
	CreationDateCondition    *CreationDateCondition    `json:"creationDateCondition,omitempty"`
	DeprecationTimeCondition *DeprecationTimeCondition `json:"deprecationTimeCondition,omitempty"`
	ImageNames               []string                  `json:"imageNames,omitempty"`
	ImageProviders           []string                  `json:"imageProviders,omitempty"`
	MarketplaceProductCodes  []string                  `json:"marketplaceProductCodes,omitempty"`
}

// AllowedImagesSettings is the account-region singleton setting controlling which AMIs are
// discoverable and usable, as managed by Enable/Disable/ReplaceImageCriteria.
type AllowedImagesSettings struct {
	State         string           `json:"state,omitempty"`
	ManagedBy     string           `json:"managedBy,omitempty"`
	ImageCriteria []ImageCriterion `json:"imageCriteria,omitempty"`
}

// resetAllowedImagesSettingsLocked re-initialises the Allowed Images Settings. Must be
// called with b.mu held.
func (b *InMemoryBackend) resetAllowedImagesSettingsLocked() {
	b.allowedImagesSettings = &AllowedImagesSettings{
		State:     allowedImagesStateDisabled,
		ManagedBy: allowedImagesManagedByAccnt,
	}
}

// EnableAllowedImagesSettings enables (or sets to audit-mode) the Allowed AMIs setting.
func (b *InMemoryBackend) EnableAllowedImagesSettings(state string) (string, error) {
	if state != allowedImagesStateEnabled && state != allowedImagesStateAudit {
		return "", fmt.Errorf(
			"%w: AllowedImagesSettingsState must be %q or %q",
			ErrInvalidParameter, allowedImagesStateEnabled, allowedImagesStateAudit,
		)
	}

	b.mu.Lock("EnableAllowedImagesSettings")
	defer b.mu.Unlock()

	b.allowedImagesSettings.State = state
	b.allowedImagesSettings.ManagedBy = allowedImagesManagedByAccnt

	return state, nil
}

// DisableAllowedImagesSettings disables the Allowed AMIs setting, allowing all AMIs again.
func (b *InMemoryBackend) DisableAllowedImagesSettings() string {
	b.mu.Lock("DisableAllowedImagesSettings")
	defer b.mu.Unlock()

	b.allowedImagesSettings.State = allowedImagesStateDisabled
	b.allowedImagesSettings.ManagedBy = allowedImagesManagedByAccnt

	return allowedImagesStateDisabled
}

// GetAllowedImagesSettings returns the current Allowed AMIs setting.
func (b *InMemoryBackend) GetAllowedImagesSettings() *AllowedImagesSettings {
	b.mu.RLock("GetAllowedImagesSettings")
	defer b.mu.RUnlock()

	cp := *b.allowedImagesSettings
	cp.ImageCriteria = append([]ImageCriterion(nil), b.allowedImagesSettings.ImageCriteria...)

	return &cp
}

// ReplaceImageCriteriaInAllowedImagesSettings replaces the full list of image criteria.
func (b *InMemoryBackend) ReplaceImageCriteriaInAllowedImagesSettings(criteria []ImageCriterion) bool {
	b.mu.Lock("ReplaceImageCriteriaInAllowedImagesSettings")
	defer b.mu.Unlock()

	b.allowedImagesSettings.ImageCriteria = criteria

	return true
}

// ---- Store / Restore Image Tasks ----

// StoreImageTask represents an in-flight or completed CreateStoreImageTask operation,
// storing an AMI's contents as an object in an S3 bucket.
type StoreImageTask struct {
	TaskStartTime          time.Time `json:"taskStartTime"`
	AmiID                  string    `json:"amiId,omitempty"`
	Bucket                 string    `json:"bucket,omitempty"`
	S3ObjectKey            string    `json:"s3ObjectKey,omitempty"`
	StoreTaskState         string    `json:"storeTaskState,omitempty"`
	StoreTaskFailureReason string    `json:"storeTaskFailureReason,omitempty"`
	ProgressPercentage     int32     `json:"progressPercentage,omitempty"`
}

// storeImageTaskProgressComplete is the ProgressPercentage of a synchronously-completed
// store image task.
const storeImageTaskProgressComplete = 100

// resetImageTasksLocked re-initialises the store image task map. Must be called with b.mu
// held.
func (b *InMemoryBackend) resetImageTasksLocked() {
}

// CreateStoreImageTask stores an existing AMI's contents in an S3 bucket, returning the
// generated S3 object key. The task completes synchronously (100% progress) since no real
// S3 upload pipeline is modelled.
func (b *InMemoryBackend) CreateStoreImageTask(imageID, bucket string) (*StoreImageTask, error) {
	if imageID == "" || bucket == "" {
		return nil, fmt.Errorf("%w: ImageId and Bucket are required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateStoreImageTask")
	defer b.mu.Unlock()

	if _, ok := b.images.Get(imageID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrImageNotFound, imageID)
	}

	task := &StoreImageTask{
		AmiID:              imageID,
		Bucket:             bucket,
		S3ObjectKey:        imageID + ".bin",
		StoreTaskState:     stateTaskCompleted,
		ProgressPercentage: storeImageTaskProgressComplete,
		TaskStartTime:      time.Now().UTC(),
	}
	b.storeImageTasks.Put(task)

	cp := *task

	return &cp, nil
}

// DescribeStoreImageTasks returns store image tasks, optionally filtered by AMI ID.
func (b *InMemoryBackend) DescribeStoreImageTasks(imageIDs []string) []*StoreImageTask {
	b.mu.RLock("DescribeStoreImageTasks")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(imageIDs))
	for _, id := range imageIDs {
		filter[id] = true
	}

	out := make([]*StoreImageTask, 0, b.storeImageTasks.Len())

	for _, t := range b.storeImageTasks.All() {
		if len(filter) > 0 && !filter[t.AmiID] {
			continue
		}

		cp := *t
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].AmiID < out[j].AmiID })

	return out
}

// CreateRestoreImageTask restores an AMI previously stored via CreateStoreImageTask from its
// S3 bucket/object key, registering a new AMI.
func (b *InMemoryBackend) CreateRestoreImageTask(bucket, objectKey, name string) (*AMIStub, error) {
	if bucket == "" || objectKey == "" {
		return nil, fmt.Errorf("%w: Bucket and ObjectKey are required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateRestoreImageTask")
	defer b.mu.Unlock()

	var source *StoreImageTask

	for _, t := range b.storeImageTasks.All() {
		if t.Bucket == bucket && t.S3ObjectKey == objectKey {
			source = t

			break
		}
	}

	if source == nil {
		return nil, fmt.Errorf("%w: no stored AMI object %s/%s", ErrInvalidParameter, bucket, objectKey)
	}

	if name == "" {
		if orig, ok := b.images.Get(source.AmiID); ok {
			name = orig.Name
		}
	}

	img := &AMIStub{
		ImageID:      "ami-" + uuid.New().String()[:17],
		Name:         name,
		Architecture: archX8664,
		State:        stateAvailable,
	}
	b.images.Put(img)

	cp := *img

	return &cp, nil
}

// ---- Image Usage Reports ----

// UsageReportEntry is a single account/resource-type usage tally within an image usage
// report.
type UsageReportEntry struct {
	ReportCreationTime time.Time `json:"reportCreationTime"`
	ReportID           string    `json:"reportId,omitempty"`
	ImageID            string    `json:"imageId,omitempty"`
	AccountID          string    `json:"accountId,omitempty"`
	ResourceType       string    `json:"resourceType,omitempty"`
	UsageCount         int64     `json:"usageCount,omitempty"`
}

// UsageReport tracks a CreateImageUsageReport request and its computed entries.
type UsageReport struct {
	CreatedAt time.Time `json:"createdAt"`
	ReportID  string    `json:"reportId,omitempty"`
	ImageID   string    `json:"imageId,omitempty"`
}

// resetUsageReportMapsLocked re-initialises the usage report state maps. Must be called with
// b.mu held.
func (b *InMemoryBackend) resetUsageReportMapsLocked() {
	b.usageReportEntries = make(map[string][]*UsageReportEntry)
}

// CreateImageUsageReport creates a usage report for the given AMI, tallying how many
// instances and launch templates currently reference it across the requested resource
// types (and, if accountIDs is non-empty, restricted to those accounts).
func (b *InMemoryBackend) CreateImageUsageReport(
	imageID string, resourceTypes, accountIDs []string,
) (*UsageReport, error) {
	if imageID == "" {
		return nil, fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateImageUsageReport")
	defer b.mu.Unlock()

	if _, ok := b.images.Get(imageID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrImageNotFound, imageID)
	}

	wantAccounts := make(map[string]bool, len(accountIDs))
	for _, a := range accountIDs {
		wantAccounts[a] = true
	}

	wantTypes := make(map[string]bool, len(resourceTypes))
	for _, t := range resourceTypes {
		wantTypes[t] = true
	}

	now := time.Now().UTC()
	reportID := "imgusgrpt-" + uuid.New().String()[:8]
	report := &UsageReport{ReportID: reportID, ImageID: imageID, CreatedAt: now}
	b.usageReports.Put(report)

	accountWanted := len(wantAccounts) == 0 || wantAccounts[b.AccountID]

	var entries []*UsageReportEntry
	if accountWanted && (len(wantTypes) == 0 || wantTypes["ec2:Instance"]) {
		if count := b.countInstancesUsingImageLocked(imageID); count > 0 {
			entries = append(entries, &UsageReportEntry{
				ReportID: reportID, ImageID: imageID, AccountID: b.AccountID,
				ResourceType: "ec2:Instance", UsageCount: count, ReportCreationTime: now,
			})
		}
	}

	if accountWanted && (len(wantTypes) == 0 || wantTypes["ec2:LaunchTemplate"]) {
		if count := b.countLaunchTemplatesUsingImageLocked(imageID); count > 0 {
			entries = append(entries, &UsageReportEntry{
				ReportID: reportID, ImageID: imageID, AccountID: b.AccountID,
				ResourceType: "ec2:LaunchTemplate", UsageCount: count, ReportCreationTime: now,
			})
		}
	}

	b.usageReportEntries[reportID] = entries

	cp := *report

	return &cp, nil
}

// countInstancesUsingImageLocked counts instances referencing imageID. Must be called with
// b.mu held.
func (b *InMemoryBackend) countInstancesUsingImageLocked(imageID string) int64 {
	var count int64

	for _, inst := range b.instances.All() {
		if inst.ImageID == imageID {
			count++
		}
	}

	return count
}

// countLaunchTemplatesUsingImageLocked counts launch templates referencing imageID. Must be
// called with b.mu held.
func (b *InMemoryBackend) countLaunchTemplatesUsingImageLocked(imageID string) int64 {
	var count int64

	for _, lt := range b.launchTemplates.All() {
		if lt.ImageID == imageID {
			count++
		}
	}

	return count
}

// DeleteImageUsageReport removes a usage report and its entries.
func (b *InMemoryBackend) DeleteImageUsageReport(reportID string) error {
	if reportID == "" {
		return fmt.Errorf("%w: ReportId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteImageUsageReport")
	defer b.mu.Unlock()

	if _, ok := b.usageReports.Get(reportID); !ok {
		return fmt.Errorf("%w: %s", ErrUsageReportNotFound, reportID)
	}
	b.usageReports.Delete(reportID)
	delete(b.usageReportEntries, reportID)

	return nil
}

// DescribeImageUsageReportEntries returns usage report entries, optionally filtered by
// report ID and/or image ID.
func (b *InMemoryBackend) DescribeImageUsageReportEntries(reportIDs, imageIDs []string) []*UsageReportEntry {
	b.mu.RLock("DescribeImageUsageReportEntries")
	defer b.mu.RUnlock()

	reportFilter := make(map[string]bool, len(reportIDs))
	for _, id := range reportIDs {
		reportFilter[id] = true
	}

	imageFilter := make(map[string]bool, len(imageIDs))
	for _, id := range imageIDs {
		imageFilter[id] = true
	}

	var out []*UsageReportEntry

	for reportID, entries := range b.usageReportEntries {
		if len(reportFilter) > 0 && !reportFilter[reportID] {
			continue
		}

		for _, e := range entries {
			if len(imageFilter) > 0 && !imageFilter[e.ImageID] {
				continue
			}

			cp := *e
			out = append(out, &cp)
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].ReportID != out[j].ReportID {
			return out[i].ReportID < out[j].ReportID
		}

		return out[i].ResourceType < out[j].ResourceType
	})

	return out
}

// ---- ConfirmProductInstance ----

// ConfirmProductInstance verifies that the given instance exists and, if so, reports
// whether the given product code is associated with it. This mock does not currently
// populate per-instance product code associations, so it always returns false for an
// existing instance (matching AWS's behaviour for an instance with no matching product
// code) while still validating the instance's existence.
func (b *InMemoryBackend) ConfirmProductInstance(instanceID, productCode string) (bool, error) {
	if instanceID == "" || productCode == "" {
		return false, fmt.Errorf("%w: InstanceId and ProductCode are required", ErrInvalidParameter)
	}

	b.mu.RLock("ConfirmProductInstance")
	defer b.mu.RUnlock()

	if _, ok := b.instances.Get(instanceID); !ok {
		return false, fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	return slices.Contains(b.instanceProductCodes[instanceID], productCode), nil
}

// ---- Image Watermarks ----

// watermarkKey builds the accountId:watermarkName identifier AWS uses for a
// AttachImageWatermark-created watermark.
func watermarkKey(accountID, watermarkName string) string {
	return accountID + ":" + watermarkName
}

// AttachImageWatermark attaches a named watermark to an AMI, returning the
// generated accountId:watermarkName WatermarkKey. Idempotent: attaching the
// same watermark name again returns the same key without duplicating it.
func (b *InMemoryBackend) AttachImageWatermark(imageID, watermarkName string) (string, error) {
	if imageID == "" {
		return "", fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	if watermarkName == "" {
		return "", fmt.Errorf("%w: WatermarkName is required", ErrInvalidParameter)
	}

	b.mu.Lock("AttachImageWatermark")
	defer b.mu.Unlock()

	if b.lookupImageLocked(imageID) == nil {
		return "", fmt.Errorf("%w: %s", ErrImageNotFound, imageID)
	}

	key := watermarkKey(b.AccountID, watermarkName)
	if !slices.Contains(b.imageWatermarks[imageID], key) {
		b.imageWatermarks[imageID] = append(b.imageWatermarks[imageID], key)
	}

	return key, nil
}

// DetachImageWatermark removes a previously attached watermark from an AMI by
// its accountId:watermarkName WatermarkKey.
func (b *InMemoryBackend) DetachImageWatermark(imageID, watermarkKey string) error {
	if imageID == "" {
		return fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	if watermarkKey == "" {
		return fmt.Errorf("%w: WatermarkKey is required", ErrInvalidParameter)
	}

	b.mu.Lock("DetachImageWatermark")
	defer b.mu.Unlock()

	if b.lookupImageLocked(imageID) == nil {
		return fmt.Errorf("%w: %s", ErrImageNotFound, imageID)
	}

	kept := b.imageWatermarks[imageID][:0]
	for _, k := range b.imageWatermarks[imageID] {
		if k != watermarkKey {
			kept = append(kept, k)
		}
	}
	b.imageWatermarks[imageID] = kept

	return nil
}
