package iot

import (
	"encoding/json"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// OTAUpdate represents an AWS IoT OTA update.
type OTAUpdate struct {
	AdditionalParameters          map[string]string `json:"additionalParameters,omitempty"`
	AWSJobExecutionsRolloutConfig map[string]any    `json:"awsJobExecutionsRolloutConfig,omitempty"`
	AWSJobPresignedURLConfig      map[string]any    `json:"awsJobPresignedUrlConfig,omitempty"`
	OTAUpdateARN                  string            `json:"otaUpdateArn"`
	OTAUpdateID                   string            `json:"otaUpdateId"`
	Description                   string            `json:"description,omitempty"`
	RoleARN                       string            `json:"roleArn,omitempty"`
	Status                        string            `json:"otaUpdateStatus"`
	AWSIoTJobID                   string            `json:"awsIotJobId,omitempty"`
	AWSIoTJobARN                  string            `json:"awsIotJobArn,omitempty"`
	TargetSelection               string            `json:"targetSelection,omitempty"`
	Files                         []any             `json:"otaUpdateFiles,omitempty"`
	Targets                       []string          `json:"targets,omitempty"`
	Protocols                     []string          `json:"protocols,omitempty"`
	CreationDate                  float64           `json:"creationDate,omitempty"`
	LastModifiedDate              float64           `json:"lastModifiedDate,omitempty"`
}

// OTAUpdateOptions holds CreateOTAUpdate's optional members beyond files and targets.
type OTAUpdateOptions struct {
	AdditionalParameters          map[string]string
	AWSJobAbortConfig             map[string]any
	AWSJobExecutionsRolloutConfig map[string]any
	AWSJobPresignedURLConfig      map[string]any
	AWSJobTimeoutConfig           map[string]any
	TargetSelection               string
	Protocols                     []string
}

func cloneOTAUpdate(o *OTAUpdate) *OTAUpdate {
	cp := *o
	cp.Targets = append([]string(nil), o.Targets...)
	cp.Files = append([]any(nil), o.Files...)
	cp.Protocols = append([]string(nil), o.Protocols...)
	cp.AdditionalParameters = maps.Clone(o.AdditionalParameters)

	return &cp
}

func (b *InMemoryBackend) otaARN(id string) string {
	return arn.Build("iot", b.region, b.accountID, fmt.Sprintf("otaupdate/%s", id))
}

func (b *InMemoryBackend) CreateOTAUpdate(
	id, description, roleARN string,
	targets []string,
	files []any,
	tags map[string]string,
	opts OTAUpdateOptions,
) (*OTAUpdate, error) {
	if err := validateOTAUpdateOptions(opts); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateOTAUpdate")
	defer b.mu.Unlock()

	if b.otaUpdates.Has(id) {
		return nil, fmt.Errorf("OTA update %q already exists: %w", id, ErrAlreadyExists)
	}
	now := float64(time.Now().Unix())
	jobID := "AFR_OTA-" + id

	doc, docErr := json.Marshal(map[string]any{"afr_ota": map[string]any{"files": files}})
	if docErr != nil {
		return nil, fmt.Errorf("building OTA job document: %w", docErr)
	}

	targetSelection := firstNonEmptyString(opts.TargetSelection, otaTargetSnapshot)

	jobInput := otaJobInput(opts, roleARN)
	jobInput.JobID = jobID
	jobInput.Description = description
	jobInput.Document = string(doc)
	jobInput.Targets = targets
	jobInput.TargetSelection = targetSelection

	if _, err := b.createJobLocked(jobInput); err != nil {
		return nil, err
	}

	o := &OTAUpdate{
		OTAUpdateID:      id,
		OTAUpdateARN:     b.otaARN(id),
		Description:      description,
		RoleARN:          roleARN,
		Targets:          append([]string(nil), targets...),
		Files:            append([]any(nil), files...),
		Status:           "CREATE_COMPLETE",
		AWSIoTJobID:      jobID,
		AWSIoTJobARN:     b.jobARN(jobID),
		CreationDate:     now,
		LastModifiedDate: now,

		AdditionalParameters:          maps.Clone(opts.AdditionalParameters),
		AWSJobExecutionsRolloutConfig: opts.AWSJobExecutionsRolloutConfig,
		AWSJobPresignedURLConfig:      opts.AWSJobPresignedURLConfig,
		Protocols:                     append([]string(nil), opts.Protocols...),
		TargetSelection:               targetSelection,
	}
	b.otaUpdates.Put(o)
	b.putResourceTagsLocked(o.OTAUpdateARN, tags)

	return cloneOTAUpdate(o), nil
}

func (b *InMemoryBackend) GetOTAUpdate(id string) (*OTAUpdate, error) {
	b.mu.RLock("GetOTAUpdate")
	defer b.mu.RUnlock()

	o, ok := b.otaUpdates.Get(id)
	if !ok {
		return nil, fmt.Errorf("OTA update %q not found: %w", id, ErrResourceNotFound)
	}

	return cloneOTAUpdate(o), nil
}

func (b *InMemoryBackend) DeleteOTAUpdate(id string) error {
	return b.DeleteOTAUpdateWithOptions(id, true)
}

// DeleteOTAUpdateWithOptions deletes an OTA update and its job; a non-terminal job needs forceDeleteJob
// (api_op_DeleteOTAUpdate.go ForceDeleteAWSJob), else ErrInvalidStateTransition and nothing is deleted.
func (b *InMemoryBackend) DeleteOTAUpdateWithOptions(id string, forceDeleteJob bool) error {
	b.mu.Lock("DeleteOTAUpdate")
	defer b.mu.Unlock()

	o, ok := b.otaUpdates.Get(id)
	if !ok {
		return fmt.Errorf("OTA update %q not found: %w", id, ErrResourceNotFound)
	}

	if o.AWSIoTJobID != "" && b.jobs.Has(o.AWSIoTJobID) {
		if err := b.deleteJobLocked(o.AWSIoTJobID, forceDeleteJob); err != nil {
			return err
		}
	}

	b.otaUpdates.Delete(id)
	delete(b.resourceTags, b.otaARN(id))

	return nil
}

func (b *InMemoryBackend) ListOTAUpdates() []*OTAUpdate {
	b.mu.RLock("ListOTAUpdates")
	defer b.mu.RUnlock()

	items := b.otaUpdates.Snapshot()
	out := make([]*OTAUpdate, 0, len(items))
	for _, v := range items {
		out = append(out, cloneOTAUpdate(v))
	}

	return out
}
