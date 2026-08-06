// Package dynamodb implements the AWS DynamoDB mock service.
// handler_backups.go implements the wire-JSON handlers for continuous
// backups/PITR, ExportTableToPointInTime/DescribeExport/ListExports, and
// DescribeTableReplicaAutoScaling. Routing (dispatchBackupOps) stays in
// handler.go; these are the leaf implementations it calls into.
package dynamodb

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

type pointInTimeRecoveryDescription struct {
	PointInTimeRecoveryStatus string `json:"PointInTimeRecoveryStatus"`
	// EarliestRestorableDateTime / LatestRestorableDateTime are Unix epoch
	// seconds (float64), matching AWS's wire format. Omitted when PITR is
	// disabled or no snapshots exist yet.
	EarliestRestorableDateTime float64 `json:"EarliestRestorableDateTime,omitempty"`
	LatestRestorableDateTime   float64 `json:"LatestRestorableDateTime,omitempty"`
}

type continuousBackupsDescriptionFields struct {
	ContinuousBackupsStatus        string                         `json:"ContinuousBackupsStatus"`
	PointInTimeRecoveryDescription pointInTimeRecoveryDescription `json:"PointInTimeRecoveryDescription"`
}

type describeContinuousBackupsOutput struct {
	ContinuousBackupsDescription continuousBackupsDescriptionFields `json:"ContinuousBackupsDescription"`
}

const (
	continuousBackupsStatusEnabled  = "ENABLED"
	continuousBackupsStatusDisabled = "DISABLED"
)

type describeContinuousBackupsInput struct {
	TableName string `json:"TableName"`
}

func (h *DynamoDBHandler) describeContinuousBackups(ctx context.Context, body []byte) (any, error) {
	var req describeContinuousBackupsInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if req.TableName == "" {
		return nil, NewValidationException("TableName is required")
	}

	pitrEnabled := false
	var earliest, latest time.Time

	if db, ok := h.Backend.(*InMemoryDB); ok {
		table, err := db.getTable(ctx, req.TableName)
		if err != nil {
			return nil, err
		}

		pitrEnabled, earliest, latest = pitrStateRLocked(table)
	}

	continuousStatus := continuousBackupsStatusEnabled
	pitrStatus := continuousBackupsStatusDisabled

	desc := pointInTimeRecoveryDescription{PointInTimeRecoveryStatus: pitrStatus}
	if pitrEnabled {
		desc.PointInTimeRecoveryStatus = continuousBackupsStatusEnabled
		if !earliest.IsZero() {
			desc.EarliestRestorableDateTime = float64(earliest.Unix())
			desc.LatestRestorableDateTime = float64(latest.Unix())
		}
	}

	return &describeContinuousBackupsOutput{
		ContinuousBackupsDescription: continuousBackupsDescriptionFields{
			ContinuousBackupsStatus:        continuousStatus,
			PointInTimeRecoveryDescription: desc,
		},
	}, nil
}

// pitrStateRLocked returns whether PITR is enabled and, when enabled with at
// least one snapshot taken, the earliest/latest restorable timestamps, under
// a defer-protected table.mu.RLock.
func pitrStateRLocked(table *Table) (bool, time.Time, time.Time) {
	table.mu.RLock(opDescribeContinuousBackups)
	defer table.mu.RUnlock()

	pitrEnabled := table.PITREnabled

	var earliest, latest time.Time
	// EarliestRestorableDateTime tracks the oldest available snapshot.
	// LatestRestorableDateTime is "now" while PITR is active — AWS
	// guarantees you can always recover to the current instant.
	if pitrEnabled && len(table.PITRSnapshots) > 0 {
		earliest = table.PITRSnapshots[0].Taken
		latest = time.Now().UTC()
	}

	return pitrEnabled, earliest, latest
}

// pointInTimeRecoverySpec holds the PITR enable/disable setting.
type pointInTimeRecoverySpec struct {
	PointInTimeRecoveryEnabled bool `json:"PointInTimeRecoveryEnabled"`
}

type updateContinuousBackupsInput struct {
	TableName                        string                  `json:"TableName"`
	PointInTimeRecoverySpecification pointInTimeRecoverySpec `json:"PointInTimeRecoverySpecification"`
}

// setPITREnabledLocked sets table.PITREnabled and, when disabling, releases
// the snapshot ring, under a defer-protected table.mu.Lock.
func setPITREnabledLocked(table *Table, pitrEnabled bool) {
	table.mu.Lock(opUpdateContinuousBackups)
	defer table.mu.Unlock()

	table.PITREnabled = pitrEnabled
	if !pitrEnabled {
		// Releasing memory the moment the feature is turned off keeps the
		// per-table footprint tight; re-enabling starts a fresh ring.
		table.PITRSnapshots = nil
	}
}

func (h *DynamoDBHandler) updateContinuousBackups(ctx context.Context, body []byte) (any, error) {
	var req updateContinuousBackupsInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if req.TableName == "" {
		return nil, NewValidationException("TableName is required")
	}

	pitrEnabled := req.PointInTimeRecoverySpecification.PointInTimeRecoveryEnabled

	if db, ok := h.Backend.(*InMemoryDB); ok {
		table, err := db.getTable(ctx, req.TableName)
		if err != nil {
			return nil, err
		}

		setPITREnabledLocked(table, pitrEnabled)
	}

	pitrStatus := continuousBackupsStatusDisabled
	if pitrEnabled {
		pitrStatus = continuousBackupsStatusEnabled
	}

	return &describeContinuousBackupsOutput{
		ContinuousBackupsDescription: continuousBackupsDescriptionFields{
			ContinuousBackupsStatus: continuousBackupsStatusEnabled,
			PointInTimeRecoveryDescription: pointInTimeRecoveryDescription{
				PointInTimeRecoveryStatus: pitrStatus,
			},
		},
	}, nil
}

type exportTableToPointInTimeInput struct {
	TableArn     string  `json:"TableArn"`
	S3Bucket     string  `json:"S3Bucket"`
	S3Prefix     string  `json:"S3Prefix,omitempty"`
	ExportFormat string  `json:"ExportFormat,omitempty"`
	ExportTime   float64 `json:"ExportTime,omitempty"`
}

type exportDescriptionFields struct {
	ExportArn       string  `json:"ExportArn"`
	ExportStatus    string  `json:"ExportStatus"`
	TableArn        string  `json:"TableArn,omitempty"`
	S3Bucket        string  `json:"S3Bucket,omitempty"`
	S3Prefix        string  `json:"S3Prefix,omitempty"`
	ExportFormat    string  `json:"ExportFormat,omitempty"`
	ExportType      string  `json:"ExportType,omitempty"`
	ExportManifest  string  `json:"ExportManifest,omitempty"`
	FailureCode     string  `json:"FailureCode,omitempty"`
	FailureMessage  string  `json:"FailureMessage,omitempty"`
	ExportTime      float64 `json:"ExportTime,omitempty"`
	StartTime       float64 `json:"StartTime,omitempty"`
	EndTime         float64 `json:"EndTime,omitempty"`
	BilledSizeBytes int64   `json:"BilledSizeBytes,omitempty"`
	ItemCount       int64   `json:"ItemCount,omitempty"`
}

type exportTableToPointInTimeOutput struct {
	ExportDescription exportDescriptionFields `json:"ExportDescription"`
}

type listExportsOutput struct {
	NextToken       string                    `json:"NextToken,omitempty"`
	ExportSummaries []exportDescriptionFields `json:"ExportSummaries"`
}

// exportIDSuffixLen is the number of characters taken from the UUID to form the
// second component of an export ID suffix. 16 characters is chosen to keep ARNs
// short while still providing enough randomness to avoid collisions.
const exportIDSuffixLen = 16

// exportARNRegionIdx is the zero-based position of the region field in a colon-split ARN.
const exportARNRegionIdx = 3

// exportARNAccountIdx is the zero-based position of the account-ID field in a colon-split ARN.
const exportARNAccountIdx = 4

// exportARNPartCount is the expected number of parts when splitting a full DynamoDB ARN on ":".
const exportARNPartCount = 6

// exportARNPathParts is the expected number of parts when splitting the resource portion of an ARN on "/".
const exportARNPathParts = 2

// generateExportID creates a short unique suffix for export ARNs.
// Format matches the AWS convention: a zero-padded Unix millisecond timestamp
// followed by a UUID-derived hex suffix.
func generateExportID() string {
	return fmt.Sprintf(
		"%016x-%s",
		time.Now().UnixMilli(),
		strings.ReplaceAll(uuid.New().String(), "-", "")[:exportIDSuffixLen],
	)
}

func (h *DynamoDBHandler) exportTableToPointInTime(ctx context.Context, body []byte) (any, error) {
	var req exportTableToPointInTimeInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	region, accountID := exportRegionAccount(req.TableArn)
	exportARN := buildExportARN(req.TableArn, region, accountID)

	exportFmt := req.ExportFormat
	if exportFmt == "" {
		exportFmt = "DYNAMODB_JSON"
	}
	now := time.Now()
	desc := exportDescriptionFields{
		ExportArn:    exportARN,
		ExportStatus: "IN_PROGRESS",
		TableArn:     req.TableArn,
		S3Bucket:     req.S3Bucket,
		S3Prefix:     req.S3Prefix,
		ExportFormat: exportFmt,
		ExportType:   "FULL_EXPORT",
		StartTime:    float64(now.Unix()),
		ExportTime:   req.ExportTime,
	}

	// Persist as IN_PROGRESS (AWS initial response), then complete asynchronously.
	// Real AWS takes minutes; the emulator finishes in microseconds.
	if b, ok := h.Backend.(*InMemoryDB); ok {
		b.storeExport(desc)
		reqCopy := req
		go b.completeExportSync(context.WithoutCancel(ctx), exportARN, &reqCopy)
	}

	return &exportTableToPointInTimeOutput{ExportDescription: desc}, nil
}

// exportRegionAccount extracts region and accountID from a DynamoDB table ARN.
func exportRegionAccount(tableARN string) (string, string) {
	region, accountID := config.DefaultRegion, config.DefaultAccountID
	if tableARN == "" {
		return region, accountID
	}
	parts := strings.SplitN(tableARN, ":", exportARNPartCount)
	if len(parts) >= exportARNRegionIdx+1 && parts[exportARNRegionIdx] != "" {
		region = parts[exportARNRegionIdx]
	}
	if len(parts) >= exportARNAccountIdx+1 && parts[exportARNAccountIdx] != "" {
		accountID = parts[exportARNAccountIdx]
	}

	return region, accountID
}

// buildExportARN constructs a unique export ARN from the table ARN.
func buildExportARN(tableARN, region, accountID string) string {
	tableSlug := "unknown"
	if tableARN != "" {
		if parts := strings.SplitN(tableARN, "/", exportARNPathParts); len(
			parts,
		) == exportARNPathParts {
			tableSlug = parts[1]
		}
	}
	exportID := fmt.Sprintf("%s/%s", tableSlug, generateExportID())

	return arn.Build("dynamodb", region, accountID, "table/"+exportID)
}

// completeExportSync performs the S3 write (if a bucket is configured) and
// updates the stored export record to its terminal state (COMPLETED or FAILED).
func (db *InMemoryDB) completeExportSync(
	ctx context.Context,
	exportARN string,
	req *exportTableToPointInTimeInput,
) {
	var (
		manifestKey string
		itemCount   int64
		billedBytes int64
		failCode    string
		failMsg     string
		finalStatus = "COMPLETED"
	)
	if req.S3Bucket != "" {
		manifestKey, itemCount, billedBytes, failCode, failMsg, finalStatus =
			db.exportToS3Bucket(ctx, req)
	} else {
		if n, err := db.countTableItems(ctx, req.TableArn); err == nil {
			itemCount = int64(n)
			billedBytes = itemCount * avgExportItemBytes
		}
	}
	db.updateExport(exportARN, finalStatus, manifestKey, failCode, failMsg, itemCount, billedBytes)
}

// exportToS3Bucket writes export data to S3 and returns completion metadata.
func (db *InMemoryDB) exportToS3Bucket(
	ctx context.Context,
	req *exportTableToPointInTimeInput,
) (string, int64, int64, string, string, string) {
	base := strings.TrimSuffix(req.S3Prefix, "/")
	if base != "" {
		base += "/"
	}
	objBase := fmt.Sprintf("%sAWSDynamoDB/%s", base, generateExportID())
	dataKey := objBase + "/data/00000.json.gz"
	manifestKey := objBase + "/manifest-summary.json"
	n, err := db.exportTableToS3(ctx, req.TableArn, req.S3Bucket, dataKey, manifestKey)
	if err != nil {
		return manifestKey, 0, 0, "ExportError", err.Error(), "FAILED"
	}
	itemCount := n
	billedBytes := itemCount * avgExportItemBytes

	return manifestKey, itemCount, billedBytes, "", "", "COMPLETED"
}

type describeExportInput struct {
	ExportArn string `json:"ExportArn"`
}

func (h *DynamoDBHandler) describeExport(ctx context.Context, body []byte) (any, error) {
	var req describeExportInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if req.ExportArn == "" {
		return nil, NewValidationException("ExportArn is required")
	}

	// Look up the stored export if the backend supports it, restricted to request region.
	if b, ok := h.Backend.(*InMemoryDB); ok {
		requestRegion := h.regionFromHandlerContext(ctx)
		if b.regionFromARN(req.ExportArn) != requestRegion {
			return nil, NewExportNotFoundException("Export not found: " + req.ExportArn)
		}

		if desc, found := b.lookupExport(req.ExportArn); found {
			return &exportTableToPointInTimeOutput{ExportDescription: desc}, nil
		}
	}

	// AWS returns ExportNotFoundException for an unknown ARN, not a fake COMPLETED.
	return nil, NewExportNotFoundException("Export not found: " + req.ExportArn)
}

// --- ListExports handler ---

type listExportsInput struct {
	TableArn   string `json:"TableArn,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
}

func (h *DynamoDBHandler) listExports(ctx context.Context, body []byte) (any, error) {
	var req listExportsInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if b, ok := h.Backend.(*InMemoryDB); ok {
		return b.listExportsWire(
			req.TableArn,
			req.NextToken,
			req.MaxResults,
			h.regionFromHandlerContext(ctx),
		), nil
	}

	return &listExportsOutput{ExportSummaries: []exportDescriptionFields{}}, nil
}

type describeTableReplicaAutoScalingInput struct {
	TableName string `json:"TableName"`
}

type replicaAutoScalingDescription struct {
	RegionName    string `json:"RegionName"`
	ReplicaStatus string `json:"ReplicaStatus"`
}

type tableAutoScalingDescription struct {
	TableName   string                          `json:"TableName"`
	TableStatus string                          `json:"TableStatus"`
	Replicas    []replicaAutoScalingDescription `json:"Replicas,omitempty"`
}

type describeTableReplicaAutoScalingOutput struct {
	TableAutoScalingDescription tableAutoScalingDescription `json:"TableAutoScalingDescription"`
}

// replicaAutoScalingDescriptionsRLocked copies table.Replicas into the wire
// shape under a defer-protected table.mu.RLock.
func replicaAutoScalingDescriptionsRLocked(table *Table) []replicaAutoScalingDescription {
	table.mu.RLock(opDescribeTableReplicaAutoScaling)
	defer table.mu.RUnlock()

	replicas := make([]replicaAutoScalingDescription, 0, len(table.Replicas))
	for _, r := range table.Replicas {
		replicas = append(replicas, replicaAutoScalingDescription{
			RegionName:    r.RegionName,
			ReplicaStatus: r.ReplicaStatus,
		})
	}

	return replicas
}

func (h *DynamoDBHandler) describeTableReplicaAutoScaling(
	ctx context.Context,
	body []byte,
) (any, error) {
	var req describeTableReplicaAutoScalingInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if req.TableName == "" {
		return nil, NewValidationException("TableName is required")
	}

	var replicas []replicaAutoScalingDescription

	if db, ok := h.Backend.(*InMemoryDB); ok {
		table, err := db.getTable(ctx, req.TableName)
		if err != nil {
			return nil, err
		}

		replicas = replicaAutoScalingDescriptionsRLocked(table)
	}

	return &describeTableReplicaAutoScalingOutput{
		TableAutoScalingDescription: tableAutoScalingDescription{
			TableName:   req.TableName,
			TableStatus: models.TableStatusActive,
			Replicas:    replicas,
		},
	}, nil
}
