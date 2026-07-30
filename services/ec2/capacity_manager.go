package ec2

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// ---- Capacity Manager ----

// EnableCapacityManager enables Capacity Manager for the account, optionally
// enabling cross-account Organizations data aggregation.
func (b *InMemoryBackend) EnableCapacityManager(organizationsAccess bool) (string, bool) {
	b.mu.Lock("EnableCapacityManager")
	defer b.mu.Unlock()

	b.capacityManagerState.Status = capacityManagerStatusEnabled
	b.capacityManagerState.OrganizationsAccess = organizationsAccess

	return b.capacityManagerState.Status, b.capacityManagerState.OrganizationsAccess
}

// DisableCapacityManager disables Capacity Manager for the account, clearing
// any Organizations data aggregation setting.
func (b *InMemoryBackend) DisableCapacityManager() (string, bool) {
	b.mu.Lock("DisableCapacityManager")
	defer b.mu.Unlock()

	b.capacityManagerState.Status = capacityManagerStatusDisabled
	b.capacityManagerState.OrganizationsAccess = false

	return b.capacityManagerState.Status, b.capacityManagerState.OrganizationsAccess
}

// UpdateCapacityManagerOrganizationsAccess updates the Organizations
// cross-account data aggregation setting for Capacity Manager.
func (b *InMemoryBackend) UpdateCapacityManagerOrganizationsAccess(
	organizationsAccess bool,
) (string, bool) {
	b.mu.Lock("UpdateCapacityManagerOrganizationsAccess")
	defer b.mu.Unlock()

	b.capacityManagerState.OrganizationsAccess = organizationsAccess

	return b.capacityManagerState.Status, b.capacityManagerState.OrganizationsAccess
}

// CapacityManagerAttributes reports the current Capacity Manager status and
// data ingestion state for the account.
type CapacityManagerAttributes struct {
	Status                 string
	IngestionStatus        string
	IngestionStatusMessage string
	OrganizationsAccess    bool
	DataExportCount        int32
}

// GetCapacityManagerAttributes returns the current Capacity Manager status,
// data export count, and ingestion state for the account.
func (b *InMemoryBackend) GetCapacityManagerAttributes() *CapacityManagerAttributes {
	b.mu.RLock("GetCapacityManagerAttributes")
	defer b.mu.RUnlock()

	attrs := &CapacityManagerAttributes{
		Status:              b.capacityManagerState.Status,
		OrganizationsAccess: b.capacityManagerState.OrganizationsAccess,
		DataExportCount:     toInt32Clamped(b.capacityManagerDataExports.Len()),
	}

	if attrs.Status == capacityManagerStatusEnabled {
		attrs.IngestionStatus = "initial-ingestion-in-progress"
		attrs.IngestionStatusMessage = "Capacity Manager is ingesting historical data; this may take several hours."
	}

	return attrs
}

// GetCapacityManagerMetricData always returns an empty result set: this backend
// does not simulate the historical utilization pipeline Capacity Manager
// aggregates from, so there is no ingested data to report. The response shape
// matches AWS's for an account with no data points yet available.
func (b *InMemoryBackend) GetCapacityManagerMetricData() []MetricDataResult {
	return nil
}

// MetricDataResult is a placeholder for a single Capacity Manager metric data
// result. It is never populated by this backend (see GetCapacityManagerMetricData)
// but is defined so callers have a stable, documented return type.
type MetricDataResult struct {
	MetricName string
	Timestamps []time.Time
	Values     []float64
}

// GetCapacityManagerMetricDimensions always returns an empty result set for the
// same reason as GetCapacityManagerMetricData.
func (b *InMemoryBackend) GetCapacityManagerMetricDimensions() []CapacityManagerDimension {
	return nil
}

// CapacityManagerDimension is a placeholder for a single Capacity Manager
// dimension combination. Never populated by this backend.
type CapacityManagerDimension struct {
	Dimensions map[string]string
}

// CreateCapacityManagerDataExport configures a new periodic Capacity Manager
// data export to the given S3 bucket.
func (b *InMemoryBackend) CreateCapacityManagerDataExport(
	outputFormat, s3BucketName, s3BucketPrefix, schedule string,
	tags map[string]string,
) (*CapacityManagerDataExport, error) {
	if outputFormat == "" || s3BucketName == "" || schedule == "" {
		return nil, fmt.Errorf(
			"%w: OutputFormat, S3BucketName, and Schedule are required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("CreateCapacityManagerDataExport")
	defer b.mu.Unlock()

	export := &CapacityManagerDataExport{
		CapacityManagerDataExportID: "cmde-" + uuid.New().String()[:8],
		OutputFormat:                outputFormat,
		S3BucketName:                s3BucketName,
		S3BucketPrefix:              s3BucketPrefix,
		Schedule:                    schedule,
		CreateTime:                  time.Now().UTC(),
	}
	b.capacityManagerDataExports.Put(export)
	b.setTagsLocked(export.CapacityManagerDataExportID, tags)

	return export, nil
}

// DescribeCapacityManagerDataExports returns Capacity Manager data export
// configurations matching the given IDs (all, if empty).
func (b *InMemoryBackend) DescribeCapacityManagerDataExports(
	ids []string,
) []*CapacityManagerDataExport {
	b.mu.RLock("DescribeCapacityManagerDataExports")
	defer b.mu.RUnlock()

	idSet := toIDSet(ids)

	var result []*CapacityManagerDataExport

	for _, export := range b.capacityManagerDataExports.All() {
		if len(idSet) > 0 && !idSet[export.CapacityManagerDataExportID] {
			continue
		}

		cp := *export
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].CapacityManagerDataExportID < result[j].CapacityManagerDataExportID
	})

	return result
}

// GetCapacityManagerMonitoredTagKeys returns the tag keys currently
// monitored by Capacity Manager, sorted by tag key for deterministic output.
func (b *InMemoryBackend) GetCapacityManagerMonitoredTagKeys() []*CapacityManagerMonitoredTagKey {
	b.mu.RLock("GetCapacityManagerMonitoredTagKeys")
	defer b.mu.RUnlock()

	out := make([]*CapacityManagerMonitoredTagKey, 0, len(b.capacityManagerState.MonitoredTagKeys))
	for _, k := range b.capacityManagerState.MonitoredTagKeys {
		cp := *k
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].TagKey < out[j].TagKey })

	return out
}

// UpdateCapacityManagerMonitoredTagKeys activates and/or deactivates the
// given tag keys for Capacity Manager monitoring, returning every tag key
// touched by this call (activated then deactivated, matching request order).
// Activation is synchronous in this mock (state goes straight to "activated"
// rather than a transient "activating"); likewise deactivation goes straight
// to the terminal "suspended" state.
func (b *InMemoryBackend) UpdateCapacityManagerMonitoredTagKeys(
	activateTagKeys, deactivateTagKeys []string,
) ([]*CapacityManagerMonitoredTagKey, error) {
	if len(activateTagKeys) == 0 && len(deactivateTagKeys) == 0 {
		return nil, fmt.Errorf(
			"%w: at least one of ActivateTagKey/DeactivateTagKey is required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("UpdateCapacityManagerMonitoredTagKeys")
	defer b.mu.Unlock()

	if b.capacityManagerState.MonitoredTagKeys == nil {
		b.capacityManagerState.MonitoredTagKeys = make(map[string]*CapacityManagerMonitoredTagKey)
	}

	var touched []*CapacityManagerMonitoredTagKey

	for _, key := range activateTagKeys {
		k, ok := b.capacityManagerState.MonitoredTagKeys[key]
		if !ok {
			k = &CapacityManagerMonitoredTagKey{TagKey: key}
			b.capacityManagerState.MonitoredTagKeys[key] = k
		}
		k.Status = capacityManagerTagKeyStatusActivated
		k.StatusMessage = ""

		cp := *k
		touched = append(touched, &cp)
	}

	for _, key := range deactivateTagKeys {
		k, ok := b.capacityManagerState.MonitoredTagKeys[key]
		if !ok {
			k = &CapacityManagerMonitoredTagKey{TagKey: key}
			b.capacityManagerState.MonitoredTagKeys[key] = k
		}
		k.Status = capacityManagerTagKeyStatusSuspended

		cp := *k
		touched = append(touched, &cp)
	}

	return touched, nil
}

// DeleteCapacityManagerDataExport removes a Capacity Manager data export
// configuration, returning its ID.
func (b *InMemoryBackend) DeleteCapacityManagerDataExport(id string) (string, error) {
	if id == "" {
		return "", fmt.Errorf("%w: CapacityManagerDataExportId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteCapacityManagerDataExport")
	defer b.mu.Unlock()

	if _, ok := b.capacityManagerDataExports.Get(id); !ok {
		return "", fmt.Errorf("%w: %s", ErrCapacityManagerDataExportNotFound, id)
	}
	b.capacityManagerDataExports.Delete(id)
	delete(b.tags, id)

	return id, nil
}
