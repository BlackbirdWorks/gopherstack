// Package dynamodb implements the AWS DynamoDB mock service.
// handler_backups.go implements the wire-JSON handlers for continuous
// backups/PITR, ExportTableToPointInTime/DescribeExport/ListExports, and
// DescribeTableReplicaAutoScaling. Routing (dispatchBackupOps) stays in
// handler.go; backend logic lives behind the StorageBackend interface in
// backup_interface.go, import_export_s3.go and autoscaling.go. These handlers
// do wire (un)marshalling and SDK-type conversion only.
package dynamodb

import (
	"context"
	"encoding/json"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdkdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	sdktypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
)

type pointInTimeRecoveryDescription struct {
	PointInTimeRecoveryStatus string `json:"PointInTimeRecoveryStatus"`
	// EarliestRestorableDateTime / LatestRestorableDateTime are Unix epoch
	// seconds (float64), matching AWS's wire format. Omitted when PITR is
	// disabled or no snapshots exist yet.
	EarliestRestorableDateTime float64 `json:"EarliestRestorableDateTime,omitempty"`
	LatestRestorableDateTime   float64 `json:"LatestRestorableDateTime,omitempty"`
	RecoveryPeriodInDays       int32   `json:"RecoveryPeriodInDays,omitempty"`
}

type continuousBackupsDescriptionFields struct {
	ContinuousBackupsStatus        string                         `json:"ContinuousBackupsStatus"`
	PointInTimeRecoveryDescription pointInTimeRecoveryDescription `json:"PointInTimeRecoveryDescription"`
}

type describeContinuousBackupsOutput struct {
	ContinuousBackupsDescription continuousBackupsDescriptionFields `json:"ContinuousBackupsDescription"`
}

type describeContinuousBackupsInput struct {
	TableName string `json:"TableName"`
}

// continuousBackupsOutputFromSDK converts the SDK ContinuousBackupsDescription
// into the wire shape shared by DescribeContinuousBackups and
// UpdateContinuousBackups.
func continuousBackupsOutputFromSDK(
	d *sdktypes.ContinuousBackupsDescription,
) *describeContinuousBackupsOutput {
	if d == nil {
		return &describeContinuousBackupsOutput{}
	}

	desc := pointInTimeRecoveryDescription{PointInTimeRecoveryStatus: continuousBackupsStatusDisabled}
	if d.PointInTimeRecoveryDescription != nil {
		pitr := d.PointInTimeRecoveryDescription
		desc.PointInTimeRecoveryStatus = string(pitr.PointInTimeRecoveryStatus)
		desc.RecoveryPeriodInDays = aws.ToInt32(pitr.RecoveryPeriodInDays)
		if pitr.EarliestRestorableDateTime != nil {
			desc.EarliestRestorableDateTime = float64(pitr.EarliestRestorableDateTime.Unix())
		}
		if pitr.LatestRestorableDateTime != nil {
			desc.LatestRestorableDateTime = float64(pitr.LatestRestorableDateTime.Unix())
		}
	}

	return &describeContinuousBackupsOutput{
		ContinuousBackupsDescription: continuousBackupsDescriptionFields{
			ContinuousBackupsStatus:        string(d.ContinuousBackupsStatus),
			PointInTimeRecoveryDescription: desc,
		},
	}
}

func (h *DynamoDBHandler) describeContinuousBackups(ctx context.Context, body []byte) (any, error) {
	var req describeContinuousBackupsInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if req.TableName == "" {
		return nil, NewValidationException("TableName is required")
	}

	out, err := h.Backend.DescribeContinuousBackups(ctx, &sdkdynamodb.DescribeContinuousBackupsInput{
		TableName: &req.TableName,
	})
	if err != nil {
		return nil, err
	}

	return continuousBackupsOutputFromSDK(out.ContinuousBackupsDescription), nil
}

// pointInTimeRecoverySpec holds the PITR enable/disable setting.
type pointInTimeRecoverySpec struct {
	RecoveryPeriodInDays       *int32 `json:"RecoveryPeriodInDays,omitempty"`
	PointInTimeRecoveryEnabled bool   `json:"PointInTimeRecoveryEnabled"`
}

type updateContinuousBackupsInput struct {
	TableName                        string                  `json:"TableName"`
	PointInTimeRecoverySpecification pointInTimeRecoverySpec `json:"PointInTimeRecoverySpecification"`
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

	out, err := h.Backend.UpdateContinuousBackups(ctx, &sdkdynamodb.UpdateContinuousBackupsInput{
		TableName: &req.TableName,
		PointInTimeRecoverySpecification: &sdktypes.PointInTimeRecoverySpecification{
			PointInTimeRecoveryEnabled: &pitrEnabled,
			RecoveryPeriodInDays:       req.PointInTimeRecoverySpecification.RecoveryPeriodInDays,
		},
	})
	if err != nil {
		return nil, err
	}

	return continuousBackupsOutputFromSDK(out.ContinuousBackupsDescription), nil
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

// exportSummaryFields is the wire shape of types.ExportSummary (see
// deserializers.go's awsAwsjson10_deserializeDocumentExportSummary), which
// carries only ExportArn, ExportStatus and ExportType -- unlike
// exportDescriptionFields, which mirrors the much richer ExportDescription
// returned by ExportTableToPointInTime/DescribeExport.
type exportSummaryFields struct {
	ExportArn    string `json:"ExportArn,omitempty"`
	ExportStatus string `json:"ExportStatus,omitempty"`
	ExportType   string `json:"ExportType,omitempty"`
}

func exportSummaryFieldsFromSDK(s sdktypes.ExportSummary) exportSummaryFields {
	return exportSummaryFields{
		ExportArn:    aws.ToString(s.ExportArn),
		ExportStatus: string(s.ExportStatus),
		ExportType:   string(s.ExportType),
	}
}

type listExportsOutput struct {
	NextToken       string                `json:"NextToken,omitempty"`
	ExportSummaries []exportSummaryFields `json:"ExportSummaries"`
}

// exportDescFieldsFromSDK converts the SDK ExportDescription into the wire shape.
func exportDescFieldsFromSDK(d *sdktypes.ExportDescription) exportDescriptionFields {
	if d == nil {
		return exportDescriptionFields{}
	}

	out := exportDescriptionFields{
		ExportArn:      aws.ToString(d.ExportArn),
		ExportStatus:   string(d.ExportStatus),
		TableArn:       aws.ToString(d.TableArn),
		S3Bucket:       aws.ToString(d.S3Bucket),
		S3Prefix:       aws.ToString(d.S3Prefix),
		ExportFormat:   string(d.ExportFormat),
		ExportType:     string(d.ExportType),
		ExportManifest: aws.ToString(d.ExportManifest),
		FailureCode:    aws.ToString(d.FailureCode),
		FailureMessage: aws.ToString(d.FailureMessage),
	}
	if d.ExportTime != nil {
		out.ExportTime = float64(d.ExportTime.Unix())
	}
	if d.StartTime != nil {
		out.StartTime = float64(d.StartTime.Unix())
	}
	if d.EndTime != nil {
		out.EndTime = float64(d.EndTime.Unix())
	}
	out.BilledSizeBytes = aws.ToInt64(d.BilledSizeBytes)
	out.ItemCount = aws.ToInt64(d.ItemCount)

	return out
}

func (h *DynamoDBHandler) exportTableToPointInTime(ctx context.Context, body []byte) (any, error) {
	var req exportTableToPointInTimeInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	sdkInput := &sdkdynamodb.ExportTableToPointInTimeInput{
		TableArn:     &req.TableArn,
		S3Bucket:     &req.S3Bucket,
		S3Prefix:     &req.S3Prefix,
		ExportFormat: sdktypes.ExportFormat(req.ExportFormat),
	}
	if req.ExportTime != 0 {
		t := time.Unix(int64(req.ExportTime), 0)
		sdkInput.ExportTime = &t
	}

	out, err := h.Backend.ExportTableToPointInTime(ctx, sdkInput)
	if err != nil {
		return nil, err
	}

	return &exportTableToPointInTimeOutput{
		ExportDescription: exportDescFieldsFromSDK(out.ExportDescription),
	}, nil
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

	out, err := h.Backend.DescribeExport(ctx, &sdkdynamodb.DescribeExportInput{
		ExportArn: &req.ExportArn,
	})
	if err != nil {
		return nil, err
	}

	return &exportTableToPointInTimeOutput{
		ExportDescription: exportDescFieldsFromSDK(out.ExportDescription),
	}, nil
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

	var maxResults *int32
	if req.MaxResults > 0 {
		mr := int32(req.MaxResults) // #nosec G115 -- MaxResults is a page-size hint, not a trust boundary
		maxResults = &mr
	}

	out, err := h.Backend.ListExports(ctx, &sdkdynamodb.ListExportsInput{
		TableArn:   ptrconv.NilIfEmpty(req.TableArn),
		NextToken:  ptrconv.NilIfEmpty(req.NextToken),
		MaxResults: maxResults,
	})
	if err != nil {
		return nil, err
	}

	// ExportSummary (the official SDK shape) carries only ExportArn,
	// ExportStatus and ExportType -- the backend already returns exactly
	// that (see (*InMemoryDB).ListExports), so no per-ARN DescribeExport
	// call is needed to fill this out.
	summaries := make([]exportSummaryFields, 0, len(out.ExportSummaries))
	for _, s := range out.ExportSummaries {
		summaries = append(summaries, exportSummaryFieldsFromSDK(s))
	}

	return &listExportsOutput{
		NextToken:       aws.ToString(out.NextToken),
		ExportSummaries: summaries,
	}, nil
}

type describeTableReplicaAutoScalingInput struct {
	TableName string `json:"TableName"`
}

type replicaAutoScalingDescription struct {
	WriteCapAutoScaling *autoScalingSettingsDescWire `json:"ReplicaProvisionedWriteCapacityAutoScalingSettings,omitempty"`
	RegionName          string                       `json:"RegionName"`
	ReplicaStatus       string                       `json:"ReplicaStatus"`
}

type tableAutoScalingDescription struct {
	TableName   string                          `json:"TableName"`
	TableStatus string                          `json:"TableStatus"`
	Replicas    []replicaAutoScalingDescription `json:"Replicas,omitempty"`
}

type describeTableReplicaAutoScalingOutput struct {
	TableAutoScalingDescription tableAutoScalingDescription `json:"TableAutoScalingDescription"`
}

// describeTableReplicaAutoScalingOutputFromSDK converts the SDK
// TableAutoScalingDescription into the wire shape.
func describeTableReplicaAutoScalingOutputFromSDK(
	d *sdktypes.TableAutoScalingDescription,
) *describeTableReplicaAutoScalingOutput {
	if d == nil {
		return &describeTableReplicaAutoScalingOutput{}
	}

	replicas := make([]replicaAutoScalingDescription, 0, len(d.Replicas))
	for _, r := range d.Replicas {
		replicas = append(replicas, replicaAutoScalingDescription{
			RegionName:    aws.ToString(r.RegionName),
			ReplicaStatus: string(r.ReplicaStatus),
			WriteCapAutoScaling: autoScalingSettingsDescWireFromSDK(
				r.ReplicaProvisionedWriteCapacityAutoScalingSettings,
			),
		})
	}

	return &describeTableReplicaAutoScalingOutput{
		TableAutoScalingDescription: tableAutoScalingDescription{
			TableName:   aws.ToString(d.TableName),
			TableStatus: string(d.TableStatus),
			Replicas:    replicas,
		},
	}
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

	out, err := h.Backend.DescribeTableReplicaAutoScaling(
		ctx,
		&sdkdynamodb.DescribeTableReplicaAutoScalingInput{TableName: &req.TableName},
	)
	if err != nil {
		return nil, err
	}

	return describeTableReplicaAutoScalingOutputFromSDK(out.TableAutoScalingDescription), nil
}
