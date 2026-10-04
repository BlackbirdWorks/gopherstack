package iot

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// OTAUpdate represents an AWS IoT OTA update.
type OTAUpdate struct {
	OTAUpdateARN     string   `json:"otaUpdateArn"`
	OTAUpdateID      string   `json:"otaUpdateId"`
	Description      string   `json:"description,omitempty"`
	RoleARN          string   `json:"roleArn,omitempty"`
	Status           string   `json:"otaUpdateStatus"`
	AWSIoTJobID      string   `json:"awsIotJobId,omitempty"`
	AWSIoTJobARN     string   `json:"awsIotJobArn,omitempty"`
	Files            []any    `json:"otaUpdateFiles,omitempty"`
	Targets          []string `json:"targets,omitempty"`
	CreationDate     float64  `json:"creationDate,omitempty"`
	LastModifiedDate float64  `json:"lastModifiedDate,omitempty"`
}

func cloneOTAUpdate(o *OTAUpdate) *OTAUpdate {
	cp := *o
	cp.Targets = append([]string(nil), o.Targets...)
	cp.Files = append([]any(nil), o.Files...)

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
) (*OTAUpdate, error) {
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

	if _, err := b.createJobLocked(&CreateJobInput{
		JobID:           jobID,
		Description:     description,
		Document:        string(doc),
		Targets:         targets,
		TargetSelection: "SNAPSHOT",
	}); err != nil {
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
