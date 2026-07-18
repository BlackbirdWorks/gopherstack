// Package dynamodb implements the AWS DynamoDB mock service.
// handler_import.go implements the wire-JSON handlers for
// DescribeImport/ImportTable/ListImports. Routing (dispatchExtraOps) stays
// in handler.go; these are the leaf implementations it calls into. Backend
// logic lives in import_export_s3.go.
package dynamodb

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdkDDB "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

type describeImportInput struct {
	ImportArn string `json:"ImportArn"`
}

type importTableDescriptionWire struct {
	ImportArn          string `json:"ImportArn,omitempty"`
	ImportStatus       string `json:"ImportStatus,omitempty"`
	TableArn           string `json:"TableArn,omitempty"`
	InputFormat        string `json:"InputFormat,omitempty"`
	FailureCode        string `json:"FailureCode,omitempty"`
	FailureMessage     string `json:"FailureMessage,omitempty"`
	ImportedItemCount  int64  `json:"ImportedItemCount,omitempty"`
	ProcessedItemCount int64  `json:"ProcessedItemCount,omitempty"`
	ProcessedSizeBytes int64  `json:"ProcessedSizeBytes,omitempty"`
	ErrorCount         int64  `json:"ErrorCount,omitempty"`
}

// importDescriptionWireFromSDK maps the SDK import description to the wire shape.
func importDescriptionWireFromSDK(d *types.ImportTableDescription) importTableDescriptionWire {
	w := importTableDescriptionWire{}
	if d == nil {
		return w
	}
	w.ImportArn = ptrconv.String(d.ImportArn)
	w.ImportStatus = string(d.ImportStatus)
	w.TableArn = ptrconv.String(d.TableArn)
	w.InputFormat = string(d.InputFormat)
	w.FailureCode = ptrconv.String(d.FailureCode)
	w.FailureMessage = ptrconv.String(d.FailureMessage)
	w.ImportedItemCount = d.ImportedItemCount
	w.ProcessedItemCount = d.ProcessedItemCount
	w.ErrorCount = d.ErrorCount
	if d.ProcessedSizeBytes != nil {
		w.ProcessedSizeBytes = *d.ProcessedSizeBytes
	}

	return w
}

type describeImportOutput struct {
	ImportTableDescription importTableDescriptionWire `json:"ImportTableDescription"`
}

func (h *DynamoDBHandler) handleDescribeImport(ctx context.Context, body []byte) (any, error) {
	var req describeImportInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	out, err := h.Backend.DescribeImport(ctx, &sdkDDB.DescribeImportInput{
		ImportArn: &req.ImportArn,
	})
	if err != nil {
		return nil, err
	}

	return &describeImportOutput{
		ImportTableDescription: importDescriptionWireFromSDK(out.ImportTableDescription),
	}, nil
}

// --- ImportTable handler ---

type importTableS3BucketSourceWire struct {
	S3Bucket      string `json:"S3Bucket"`
	S3KeyPrefix   string `json:"S3KeyPrefix,omitempty"`
	S3BucketOwner string `json:"S3BucketOwner,omitempty"`
}

type importTableCsvOptionsWire struct {
	Delimiter  string   `json:"Delimiter,omitempty"`
	HeaderList []string `json:"HeaderList,omitempty"`
}

type importTableInputFormatOptionsWire struct {
	Csv *importTableCsvOptionsWire `json:"Csv,omitempty"`
}

type importTableInput struct {
	InputFormatOptions      *importTableInputFormatOptionsWire `json:"InputFormatOptions,omitempty"`
	S3BucketSource          importTableS3BucketSourceWire      `json:"S3BucketSource"`
	InputFormat             string                             `json:"InputFormat,omitempty"`
	InputCompressionType    string                             `json:"InputCompressionType,omitempty"`
	TableCreationParameters models.CreateTableInput            `json:"TableCreationParameters"`
}

type importTableOutput struct {
	ImportTableDescription importTableDescriptionWire `json:"ImportTableDescription"`
}

func (h *DynamoDBHandler) handleImportTable(ctx context.Context, body []byte) (any, error) {
	var req importTableInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	// Reuse the CreateTable conversion so KeySchema / AttributeDefinitions / GSIs /
	// throughput are all carried into the imported table.
	cti := models.ToSDKCreateTableInput(&req.TableCreationParameters)

	in := &sdkDDB.ImportTableInput{
		InputFormat:          types.InputFormat(req.InputFormat),
		InputCompressionType: types.InputCompressionType(req.InputCompressionType),
		S3BucketSource: &types.S3BucketSource{
			S3Bucket:      aws.String(req.S3BucketSource.S3Bucket),
			S3KeyPrefix:   aws.String(req.S3BucketSource.S3KeyPrefix),
			S3BucketOwner: aws.String(req.S3BucketSource.S3BucketOwner),
		},
		TableCreationParameters: &types.TableCreationParameters{
			TableName:              cti.TableName,
			KeySchema:              cti.KeySchema,
			AttributeDefinitions:   cti.AttributeDefinitions,
			BillingMode:            cti.BillingMode,
			GlobalSecondaryIndexes: cti.GlobalSecondaryIndexes,
			ProvisionedThroughput:  cti.ProvisionedThroughput,
		},
	}

	if req.InputFormatOptions != nil && req.InputFormatOptions.Csv != nil {
		in.InputFormatOptions = &types.InputFormatOptions{
			Csv: &types.CsvOptions{
				Delimiter:  aws.String(req.InputFormatOptions.Csv.Delimiter),
				HeaderList: req.InputFormatOptions.Csv.HeaderList,
			},
		}
	}

	out, err := h.Backend.ImportTable(ctx, in)
	if err != nil {
		return nil, err
	}

	return &importTableOutput{
		ImportTableDescription: importDescriptionWireFromSDK(out.ImportTableDescription),
	}, nil
}

// --- ListImports handler ---

type listImportsOutput struct {
	NextToken         string                       `json:"NextToken,omitempty"`
	ImportSummaryList []importTableDescriptionWire `json:"ImportSummaryList"`
}

type listImportsInput struct {
	TableArn  string `json:"TableArn,omitempty"`
	NextToken string `json:"NextToken,omitempty"`
	PageSize  int    `json:"PageSize,omitempty"`
}

func (h *DynamoDBHandler) handleListImports(ctx context.Context, body []byte) (any, error) {
	var req listImportsInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	region := h.regionFromHandlerContext(ctx)

	db, ok := h.Backend.(*InMemoryDB)
	if !ok {
		return &listImportsOutput{ImportSummaryList: []importTableDescriptionWire{}}, nil
	}

	all := db.listImportsStored()

	// Filter by region and optionally by TableArn.
	filtered := make([]storedImport, 0, len(all))
	for _, imp := range all {
		if db.regionFromARN(imp.ImportArn) != region {
			continue
		}
		if req.TableArn != "" && imp.TableArn != req.TableArn {
			continue
		}
		filtered = append(filtered, imp)
	}

	// Apply ExclusiveStart cursor (NextToken = last-seen import ARN).
	start := 0
	if req.NextToken != "" {
		for i, imp := range filtered {
			if imp.ImportArn == req.NextToken {
				start = i + 1

				break
			}
		}
	}
	filtered = filtered[start:]

	// Apply page size cap.
	const defaultPageSize = 25

	pageSize := defaultPageSize
	if req.PageSize > 0 {
		pageSize = req.PageSize
	}

	var outNextToken string
	if len(filtered) > pageSize {
		outNextToken = filtered[pageSize-1].ImportArn
		filtered = filtered[:pageSize]
	}

	summaries := make([]importTableDescriptionWire, 0, len(filtered))
	for _, imp := range filtered {
		summaries = append(summaries, importTableDescriptionWire{
			ImportArn:    imp.ImportArn,
			ImportStatus: imp.ImportStatus,
			TableArn:     imp.TableArn,
		})
	}

	return &listImportsOutput{
		ImportSummaryList: summaries,
		NextToken:         outNextToken,
	}, nil
}
