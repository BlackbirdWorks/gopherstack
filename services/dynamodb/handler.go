package dynamodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"net/http"
	"strconv"
	"strings"
	"time"

	sdkDDB "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

var ErrUnknownOperation = errors.New("UnknownOperationException")

// regionContextKey is used to store the AWS region in request context.
type regionContextKey struct{}

// AWS SigV4 credential format has at least 3 parts: AKID/date/region.
const minSigV4CredentialParts = 3

// extractRegionFromAuth extracts the AWS region from the Authorization header.
// AWS Signature Version 4 has format: Credential=AKID/date/region/service/aws4_request
// Falls back to X-Amz-Region header if present, or uses the default region.
func extractRegionFromAuth(r *http.Request, defaultRegion string) string {
	authHeader := r.Header.Get("Authorization")
	if authHeader != "" && strings.Contains(authHeader, "Credential=") {
		// Extract from "Credential=AKID/20230525/us-east-1/dynamodb/aws4_request"
		parts := strings.Split(authHeader, "Credential=")
		if len(parts) > 1 {
			credParts := strings.Split(parts[1], "/")
			if len(credParts) >= minSigV4CredentialParts {
				return credParts[2]
			}
		}
	}

	// Check for X-Amz-Region header as fallback
	if region := r.Header.Get("X-Amz-Region"); region != "" {
		return region
	}

	return defaultRegion
}

// DynamoDBHandler handles HTTP requests for DynamoDB operations.
//
//nolint:revive // Stuttering preferred here for clarity per Plan.md
type DynamoDBHandler struct {
	Backend       StorageBackend
	Streams       StreamsBackend
	janitor       *Janitor
	DefaultRegion string
}

// NewHandler creates a new DynamoDB handler with the given storage backend.
func NewHandler(backend StorageBackend) *DynamoDBHandler {
	h := &DynamoDBHandler{
		Backend:       backend,
		DefaultRegion: config.DefaultRegion,
	}

	if sb, ok := backend.(StreamsBackend); ok {
		h.Streams = sb
	}

	return h
}

// WithJanitor attaches a background janitor to the handler.
// The optional janitorTimeout parameter bounds each individual janitor task;
// zero (or omitted) disables per-task timeouts.
func (h *DynamoDBHandler) WithJanitor(settings Settings, janitorTimeout ...time.Duration) *DynamoDBHandler {
	h.DefaultRegion = settings.DefaultRegion
	if h.DefaultRegion == "" {
		h.DefaultRegion = config.DefaultRegion
	}
	if memBackend, ok := h.Backend.(*InMemoryDB); ok {
		memBackend.SetDefaultRegion(h.DefaultRegion)
		j := NewJanitor(memBackend, settings)
		if len(janitorTimeout) > 0 {
			j.TaskTimeout = janitorTimeout[0]
		}
		h.janitor = j
	}

	return h
}

// StartWorker starts the background janitor if it is configured.
func (h *DynamoDBHandler) StartWorker(ctx context.Context) error {
	if h.janitor != nil {
		go h.janitor.Run(ctx)
	}

	return nil
}

// GetSupportedOperations returns a sorted list of supported DynamoDB operations.
func (h *DynamoDBHandler) GetSupportedOperations() []string {
	return []string{
		"BatchExecuteStatement",
		"BatchGetItem",
		"BatchWriteItem",
		"CreateBackup",
		"CreateGlobalTable",
		"CreateTable",
		"DeleteBackup",
		"DeleteItem",
		"DeleteResourcePolicy",
		"DeleteTable",
		"DescribeBackup",
		"DescribeContributorInsights",
		"DescribeContinuousBackups",
		"DescribeEndpoints",
		"DescribeExport",
		"DescribeGlobalTable",
		"DescribeGlobalTableSettings",
		"DescribeImport",
		"DescribeKinesisStreamingDestination",
		"DescribeLimits",
		"DescribeTable",
		"DescribeTableReplicaAutoScaling",
		"DescribeTimeToLive",
		"DisableKinesisStreamingDestination",
		"EnableKinesisStreamingDestination",
		"ExecuteStatement",
		"ExportTableToPointInTime",
		"GetItem",
		"GetResourcePolicy",
		"ListBackups",
		"ListExports",
		"ListGlobalTables",
		"ListTables",
		"ListTagsOfResource",
		"PutItem",
		"PutResourcePolicy",
		"Query",
		"RestoreTableFromBackup",
		"RestoreTableToPointInTime",
		"Scan",
		"TagResource",
		"TransactGetItems",
		"TransactWriteItems",
		"UntagResource",
		"UpdateContinuousBackups",
		"UpdateGlobalTable",
		"UpdateItem",
		"UpdateTable",
		"UpdateTimeToLive",
	}
}

// Regions returns all regions with tables in the backend.
// Returns an empty slice when not using the in-memory backend.
func (h *DynamoDBHandler) Regions() []string {
	if b, ok := h.Backend.(*InMemoryDB); ok {
		return b.Regions()
	}

	return []string{}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *DynamoDBHandler) ChaosServiceName() string { return "dynamodb" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *DynamoDBHandler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this DynamoDB instance handles.
func (h *DynamoDBHandler) ChaosRegions() []string {
	regions := h.Regions()
	if len(regions) == 0 {
		return []string{h.DefaultRegion}
	}

	return regions
}

// TableNamesByRegion returns table names in the given region (all if empty).
// Returns an empty slice when not using the in-memory backend.
func (h *DynamoDBHandler) TableNamesByRegion(region string) []string {
	if b, ok := h.Backend.(*InMemoryDB); ok {
		return b.TableNamesByRegion(region)
	}

	return []string{}
}

// DescribeTableInRegion returns a table from the backend for a specific region.
// Returns nil when not using the in-memory backend or when the table is not found.
func (h *DynamoDBHandler) DescribeTableInRegion(region, tableName string) *Table {
	b, ok := h.Backend.(*InMemoryDB)
	if !ok {
		return nil
	}

	table, exists := b.GetTableInRegion(tableName, region)
	if !exists {
		return nil
	}

	return table
}

// Handler is the Echo HTTP handler for DynamoDB operations.
func (h *DynamoDBHandler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		if c.Request().Method == http.MethodGet && c.Request().URL.Path == "/" {
			ops := h.GetSupportedOperations()

			return c.JSON(http.StatusOK, ops)
		}

		if c.Request().Method != http.MethodPost {
			return c.String(http.StatusMethodNotAllowed, "Method not allowed")
		}

		target := c.Request().Header.Get("X-Amz-Target")
		if target == "" {
			return c.String(http.StatusBadRequest, "Missing X-Amz-Target")
		}

		const targetParts = 2
		parts := strings.Split(target, ".")
		if len(parts) != targetParts {
			return c.String(http.StatusBadRequest, "Invalid X-Amz-Target")
		}
		action := parts[1]

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "failed to read request body", "error", err)

			return c.String(http.StatusInternalServerError, "internal server error")
		}

		log.DebugContext(ctx, "DynamoDB request", "action", action, "body", string(body))

		// Extract region from request and add to context
		region := extractRegionFromAuth(c.Request(), h.DefaultRegion)
		ctx = context.WithValue(ctx, regionContextKey{}, region)

		response, reqErr := h.dispatch(ctx, action, body)
		if reqErr != nil {
			return h.handleError(ctx, c, action, reqErr)
		}

		payload, err := json.Marshal(response)
		if err != nil {
			log.ErrorContext(ctx, "failed to marshal JSON response", "error", err)

			return c.String(http.StatusInternalServerError, "internal server error")
		}

		checksum := crc32.ChecksumIEEE(payload)
		c.Response().Header().Set("X-Amz-Crc32", strconv.FormatUint(uint64(checksum), 10))
		c.Response().Header().Set("Content-Type", "application/x-amz-json-1.0")

		return c.JSONBlob(http.StatusOK, payload)
	}
}

// Name returns the service identifier.
func (h *DynamoDBHandler) Name() string {
	return "DynamoDB"
}

// RouteMatcher returns a matcher for DynamoDB requests (by X-Amz-Target header).
func (h *DynamoDBHandler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")

		return strings.HasPrefix(target, "DynamoDB_")
	}
}

// MatchPriority returns the priority for the DynamoDB matcher.
// Header-based matchers have high priority (100).
func (h *DynamoDBHandler) MatchPriority() int {
	return service.PriorityHeaderExact
}

// ExtractOperation extracts the DynamoDB operation from the X-Amz-Target header.
func (h *DynamoDBHandler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	parts := strings.Split(target, ".")
	const actionParts = 2
	if len(parts) == actionParts {
		return parts[1]
	}

	return "unknown"
}

// ExtractResource extracts the table name from the DynamoDB request body.
func (h *DynamoDBHandler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var data map[string]any
	if uerr := json.Unmarshal(body, &data); uerr != nil {
		return ""
	}

	if tbl, exists := data["TableName"]; exists {
		if tblStr, ok := tbl.(string); ok {
			return tblStr
		}
	}

	return ""
}

func (h *DynamoDBHandler) dispatch(ctx context.Context, action string, body []byte) (any, error) {
	switch action {
	case "CreateTable",
		"DeleteTable",
		"DescribeTable",
		"ListTables",
		"TagResource",
		"UntagResource",
		"ListTagsOfResource",
		"UpdateTable",
		"UpdateTimeToLive",
		"DescribeTimeToLive":
		return h.dispatchTableOps(ctx, action, body)
	case "PutItem",
		"GetItem",
		"DeleteItem",
		"UpdateItem",
		"Query",
		"Scan",
		"BatchGetItem",
		"BatchWriteItem":
		return h.dispatchItemOps(ctx, action, body)
	case "TransactWriteItems", "TransactGetItems":
		return h.dispatchTransactOps(ctx, action, body)
	case "DescribeStream", "GetShardIterator", "GetRecords", "ListStreams":
		return h.dispatchStreamsOps(ctx, action, body)
	case "ExecuteStatement":
		return h.handleExecuteStatement(ctx, body)
	case "BatchExecuteStatement":
		return h.handleBatchExecuteStatement(ctx, body)
	case "DescribeContinuousBackups",
		"UpdateContinuousBackups",
		"CreateBackup",
		"DescribeBackup",
		"DeleteBackup",
		"ListBackups",
		"RestoreTableFromBackup",
		"RestoreTableToPointInTime",
		"DescribeTableReplicaAutoScaling":
		return h.dispatchBackupOps(ctx, action, body)
	case "ExportTableToPointInTime":
		return h.exportTableToPointInTime(ctx, body)
	case "DescribeExport":
		return h.describeExport(ctx, body)
	case "ListExports":
		return &listExportsOutput{ExportSummaries: []exportDescriptionFields{}}, nil
	case "CreateGlobalTable",
		"DescribeGlobalTable",
		"DescribeGlobalTableSettings",
		"ListGlobalTables",
		"UpdateGlobalTable",
		"EnableKinesisStreamingDestination",
		"DescribeKinesisStreamingDestination",
		"DisableKinesisStreamingDestination",
		"DescribeLimits",
		"DescribeEndpoints",
		"DescribeContributorInsights",
		"DeleteResourcePolicy",
		"GetResourcePolicy",
		"PutResourcePolicy",
		"DescribeImport":
		return h.dispatchExtraOps(ctx, action, body)
	default:
		return nil, fmt.Errorf("%w:%s", ErrUnknownOperation, action)
	}
}

func (h *DynamoDBHandler) dispatchBackupOps(ctx context.Context, action string, body []byte) (any, error) {
	switch action {
	case "DescribeContinuousBackups":
		return h.describeContinuousBackups(ctx, body)
	case "UpdateContinuousBackups":
		return h.updateContinuousBackups(ctx, body)
	case "CreateBackup":
		return h.createBackup(ctx, body)
	case "DescribeBackup":
		return h.describeBackup(ctx, body)
	case "DeleteBackup":
		return h.deleteBackup(ctx, body)
	case "ListBackups":
		return h.listBackups(ctx, body)
	case "RestoreTableFromBackup":
		return h.restoreTableFromBackup(ctx, body)
	case "RestoreTableToPointInTime":
		return h.restoreTableToPointInTime(ctx, body)
	case "DescribeTableReplicaAutoScaling":
		return h.describeTableReplicaAutoScaling(ctx, body)
	default:
		return nil, fmt.Errorf("%w:%s", ErrUnknownOperation, action)
	}
}

// Helper for operations where Adapter allows error.
func handleOpErr[WireIn any, SDKIn any, SDKOut any, WireOut any](
	ctx context.Context,
	action string,
	body []byte,
	toSDK func(*WireIn) (*SDKIn, error),
	doOp func(context.Context, *SDKIn) (*SDKOut, error),
	fromSDK func(*SDKOut) *WireOut,
) (any, error) {
	log := logger.Load(ctx)

	var input WireIn
	if len(body) > 0 {
		if err := json.Unmarshal(body, &input); err != nil {
			return nil, err
		}
	}

	inputJSON, _ := json.Marshal(input)
	log.DebugContext(ctx, "handler input", "action", action, "input", string(inputJSON))

	sdkInput, err := toSDK(&input)
	if err != nil {
		return nil, err
	}
	sdkOutput, err := doOp(ctx, sdkInput)
	if err != nil {
		return nil, err
	}

	wireOutput := fromSDK(sdkOutput)

	outputJSON, _ := json.Marshal(wireOutput)
	log.DebugContext(ctx, "handler output", "action", action, "output", string(outputJSON))

	return wireOutput, nil
}

// Helper for operations where Adapter does not return error.
func handleOp[WireIn any, SDKIn any, SDKOut any, WireOut any](
	ctx context.Context,
	action string,
	body []byte,
	toSDK func(*WireIn) *SDKIn,
	doOp func(context.Context, *SDKIn) (*SDKOut, error),
	fromSDK func(*SDKOut) *WireOut,
) (any, error) {
	log := logger.Load(ctx)

	var input WireIn
	if len(body) > 0 {
		if err := json.Unmarshal(body, &input); err != nil {
			return nil, err
		}
	}

	inputJSON, _ := json.Marshal(input)
	log.DebugContext(ctx, "handler input", "action", action, "input", string(inputJSON))

	sdkInput := toSDK(&input)
	sdkOutput, err := doOp(ctx, sdkInput)
	if err != nil {
		return nil, err
	}

	wireOutput := fromSDK(sdkOutput)

	outputJSON, _ := json.Marshal(wireOutput)
	log.DebugContext(ctx, "handler output", "action", action, "output", string(outputJSON))

	return wireOutput, nil
}

func (h *DynamoDBHandler) dispatchTableOps(ctx context.Context, action string, body []byte) (any, error) {
	switch action {
	case "CreateTable":
		return handleOp(
			ctx, action, body,
			models.ToSDKCreateTableInput, h.Backend.CreateTable, models.FromSDKCreateTableOutput,
		)
	case "DeleteTable":
		return handleOp(
			ctx, action, body,
			models.ToSDKDeleteTableInput, h.Backend.DeleteTable, models.FromSDKDeleteTableOutput,
		)
	case "DescribeTable":
		return handleOp(
			ctx, action, body,
			models.ToSDKDescribeTableInput, h.Backend.DescribeTable, models.FromSDKDescribeTableOutput,
		)
	case "ListTables":
		return handleOp(
			ctx, action, body,
			models.ToSDKListTablesInput, h.Backend.ListTables, models.FromSDKListTablesOutput,
		)
	case "UpdateTable":
		return handleOpErr(
			ctx, action, body,
			models.ToSDKUpdateTableInput, h.Backend.UpdateTable, models.FromSDKUpdateTableOutput,
		)
	case "TagResource":
		return handleOpErr(
			ctx, action, body,
			models.ToSDKTagResourceInput, h.Backend.TagResource, models.FromSDKTagResourceOutput,
		)
	case "UntagResource":
		return handleOpErr(
			ctx, action, body,
			models.ToSDKUntagResourceInput, h.Backend.UntagResource, models.FromSDKUntagResourceOutput,
		)
	case "ListTagsOfResource":
		return handleOpErr(
			ctx, action, body,
			models.ToSDKListTagsOfResourceInput, h.Backend.ListTagsOfResource, models.FromSDKListTagsOfResourceOutput,
		)
	case "UpdateTimeToLive":
		return handleOp(
			ctx,
			action,
			body,
			models.ToSDKUpdateTimeToLiveInput,
			h.Backend.UpdateTimeToLive,
			models.FromSDKUpdateTimeToLiveOutput,
		)
	case "DescribeTimeToLive":
		return handleOp(
			ctx,
			action,
			body,
			models.ToSDKDescribeTimeToLiveInput,
			h.Backend.DescribeTimeToLive,
			models.FromSDKDescribeTimeToLiveOutput,
		)
	default:
		return nil, fmt.Errorf("%w:%s", ErrUnknownOperation, action)
	}
}

func (h *DynamoDBHandler) dispatchItemOps(ctx context.Context, action string, body []byte) (any, error) {
	switch action {
	case "PutItem":
		return handleOpErr(
			ctx, action, body,
			models.ToSDKPutItemInput, h.Backend.PutItem, models.FromSDKPutItemOutput,
		)
	case "GetItem":
		return handleOpErr(
			ctx, action, body,
			models.ToSDKGetItemInput, h.Backend.GetItem, models.FromSDKGetItemOutput,
		)
	case "DeleteItem":
		return handleOpErr(
			ctx, action, body,
			models.ToSDKDeleteItemInput, h.Backend.DeleteItem, models.FromSDKDeleteItemOutput,
		)
	case "Scan":
		return handleOpErr(
			ctx, action, body,
			models.ToSDKScanInput, h.Backend.Scan, models.FromSDKScanOutput,
		)
	case "UpdateItem":
		return handleOpErr(
			ctx, action, body,
			models.ToSDKUpdateItemInput, h.Backend.UpdateItem, models.FromSDKUpdateItemOutput,
		)
	case "Query":
		return handleOpErr(
			ctx, action, body,
			models.ToSDKQueryInput, h.Backend.Query, models.FromSDKQueryOutput,
		)
	case "BatchGetItem":
		return handleOpErr(
			ctx, action, body,
			models.ToSDKBatchGetItemInput, h.Backend.BatchGetItem, models.FromSDKBatchGetItemOutput,
		)
	case "BatchWriteItem":
		return handleOpErr(
			ctx,
			action,
			body,
			models.ToSDKBatchWriteItemInput,
			h.Backend.BatchWriteItem,
			models.FromSDKBatchWriteItemOutput,
		)
	default:
		return nil, fmt.Errorf("%w:%s", ErrUnknownOperation, action)
	}
}

func (h *DynamoDBHandler) dispatchTransactOps(
	ctx context.Context,
	action string,
	body []byte,
) (any, error) {
	switch action {
	case "TransactWriteItems":
		return handleOpErr(ctx,
			action,
			body,
			models.ToSDKTransactWriteItemsInput,
			h.Backend.TransactWriteItems,
			models.FromSDKTransactWriteItemsOutput,
		)
	case "TransactGetItems":
		return handleOpErr(ctx,
			action,
			body,
			models.ToSDKTransactGetItemsInput,
			h.Backend.TransactGetItems,
			models.FromSDKTransactGetItemsOutput,
		)
	default:
		return nil, fmt.Errorf("%w:%s", ErrUnknownOperation, action)
	}
}

func (h *DynamoDBHandler) dispatchStreamsOps(ctx context.Context, action string, body []byte) (any, error) {
	if h.Streams == nil {
		return nil, fmt.Errorf("%w:%s", ErrUnknownOperation, action)
	}

	log := logger.Load(ctx)
	log.DebugContext(ctx, "DynamoDB Streams request", "action", action)

	switch action {
	case "DescribeStream":
		return handleStreamsOp(ctx, body, h.Streams.DescribeStream)
	case "GetShardIterator":
		return handleStreamsOp(ctx, body, h.Streams.GetShardIterator)
	case "GetRecords":
		return handleStreamsGetRecords(ctx, body, h.Streams.GetRecords)
	case "ListStreams":
		return handleStreamsOp(ctx, body, h.Streams.ListStreams)
	default:
		return nil, fmt.Errorf("%w:%s", ErrUnknownOperation, action)
	}
}

func handleStreamsOp[In any, Out any](
	ctx context.Context,
	body []byte,
	op func(context.Context, *In) (*Out, error),
) (any, error) {
	var input In
	if len(body) > 0 {
		if err := json.Unmarshal(body, &input); err != nil {
			return nil, err
		}
	}

	return op(ctx, &input)
}

func handleStreamsGetRecords(
	ctx context.Context,
	body []byte,
	op func(context.Context, *dynamodbstreams.GetRecordsInput) (*dynamodbstreams.GetRecordsOutput, error),
) (any, error) {
	var input dynamodbstreams.GetRecordsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &input); err != nil {
			return nil, err
		}
	}

	out, err := op(ctx, &input)
	if err != nil {
		return nil, err
	}

	wireOut, err := toWireGetRecordsOutput(out)
	if err != nil {
		return nil, err
	}

	return wireOut, nil
}

func (h *DynamoDBHandler) handleError(
	ctx context.Context,
	c *echo.Context,
	action string,
	reqErr error,
) error {
	log := logger.Load(ctx)

	if strings.HasPrefix(reqErr.Error(), "UnknownOperationException:") {
		log.WarnContext(ctx, "Unknown action", "action", action)
		body := []byte(
			`{"__type":"com.amazon.coral.service#UnknownOperationException","message":"Action not supported"}`,
		)
		checksum := crc32.ChecksumIEEE(body)
		c.Response().Header().Set("X-Amz-Crc32", strconv.FormatUint(uint64(checksum), 10))
		c.Response().Header().Set("Content-Type", "application/x-amz-json-1.0")

		return c.JSONBlob(http.StatusBadRequest, body)
	}

	log.ErrorContext(ctx, "Error handling action", "action", action, "error", reqErr)

	statusCode, awsErr := h.classifyError(reqErr)

	c.Response().Header().Set("Content-Type", "application/x-amz-json-1.0")

	payload, _ := json.Marshal(awsErr)
	checksum := crc32.ChecksumIEEE(payload)
	c.Response().Header().Set("X-Amz-Crc32", strconv.FormatUint(uint64(checksum), 10))

	return c.JSONBlob(statusCode, payload)
}

func (h *DynamoDBHandler) classifyError(reqErr error) (int, *Error) {
	// Simple error classification wrapping
	// If it's already a DynamoDB error type/struct, use it.
	// But our internal implementation returns native go errors or custom structs.
	// We need to map them to Wire Error struct.

	var wireErr *Error
	if errors.As(reqErr, &wireErr) {
		// Map type to status code. Most DynamoDB errors return 400.
		if wireErr.Type == "com.amazonaws.dynamodb.v20120810#InternalServerError" {
			return http.StatusInternalServerError, wireErr
		}

		return http.StatusBadRequest, wireErr
	}

	// Fallback
	var syntaxErr *json.SyntaxError
	var unmarshalTypeError *json.UnmarshalTypeError
	if errors.As(reqErr, &syntaxErr) || errors.As(reqErr, &unmarshalTypeError) {
		return http.StatusBadRequest, NewValidationException(
			fmt.Sprintf("JSON Error: %s", reqErr.Error()),
		)
	}

	errStr := reqErr.Error()
	if strings.Contains(errStr, "json:") || strings.Contains(errStr, "unmarshal") {
		return http.StatusBadRequest, NewValidationException(fmt.Sprintf("JSON Error: %s", errStr))
	}

	return http.StatusInternalServerError, &Error{
		Type:    "com.amazonaws.dynamodb.v20120810#InternalServerError",
		Message: reqErr.Error(),
	}
}

type pointInTimeRecoveryDescription struct {
	PointInTimeRecoveryStatus string `json:"PointInTimeRecoveryStatus"`
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

	if db, ok := h.Backend.(*InMemoryDB); ok {
		table, err := db.getTable(ctx, req.TableName)
		if err != nil {
			return nil, err
		}

		table.mu.RLock("DescribeContinuousBackups")
		pitrEnabled = table.PITREnabled
		table.mu.RUnlock()
	}

	continuousStatus := continuousBackupsStatusEnabled
	pitrStatus := continuousBackupsStatusDisabled

	if pitrEnabled {
		pitrStatus = continuousBackupsStatusEnabled
	}

	return &describeContinuousBackupsOutput{
		ContinuousBackupsDescription: continuousBackupsDescriptionFields{
			ContinuousBackupsStatus:        continuousStatus,
			PointInTimeRecoveryDescription: pointInTimeRecoveryDescription{PointInTimeRecoveryStatus: pitrStatus},
		},
	}, nil
}

// pointInTimeRecoverySpec holds the PITR enable/disable setting.
type pointInTimeRecoverySpec struct {
	PointInTimeRecoveryEnabled bool `json:"PointInTimeRecoveryEnabled"`
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

	if db, ok := h.Backend.(*InMemoryDB); ok {
		table, err := db.getTable(ctx, req.TableName)
		if err != nil {
			return nil, err
		}

		table.mu.Lock("UpdateContinuousBackups")
		table.PITREnabled = pitrEnabled
		table.mu.Unlock()
	}

	pitrStatus := continuousBackupsStatusDisabled
	if pitrEnabled {
		pitrStatus = continuousBackupsStatusEnabled
	}

	return &describeContinuousBackupsOutput{
		ContinuousBackupsDescription: continuousBackupsDescriptionFields{
			ContinuousBackupsStatus:        continuousBackupsStatusEnabled,
			PointInTimeRecoveryDescription: pointInTimeRecoveryDescription{PointInTimeRecoveryStatus: pitrStatus},
		},
	}, nil
}

type exportTableToPointInTimeInput struct {
	TableArn string `json:"TableArn"`
	S3Bucket string `json:"S3Bucket"`
}

type exportDescriptionFields struct {
	ExportArn    string `json:"ExportArn"`
	ExportStatus string `json:"ExportStatus"`
	TableArn     string `json:"TableArn,omitempty"`
	S3Bucket     string `json:"S3Bucket,omitempty"`
}

type exportTableToPointInTimeOutput struct {
	ExportDescription exportDescriptionFields `json:"ExportDescription"`
}

type listExportsOutput struct {
	NextToken       string                    `json:"NextToken,omitempty"`
	ExportSummaries []exportDescriptionFields `json:"ExportSummaries"`
}

func (h *DynamoDBHandler) exportTableToPointInTime(_ context.Context, body []byte) (any, error) {
	var req exportTableToPointInTimeInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	exportArn := arn.Build("dynamodb", config.DefaultRegion, config.DefaultAccountID,
		"table/table/export/01000000-0000-0000-0000-000000000000")

	return &exportTableToPointInTimeOutput{
		ExportDescription: exportDescriptionFields{
			ExportArn:    exportArn,
			ExportStatus: "COMPLETED",
			TableArn:     req.TableArn,
			S3Bucket:     req.S3Bucket,
		},
	}, nil
}

type describeExportInput struct {
	ExportArn string `json:"ExportArn"`
}

func (h *DynamoDBHandler) describeExport(_ context.Context, body []byte) (any, error) {
	var req describeExportInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	return &exportTableToPointInTimeOutput{
		ExportDescription: exportDescriptionFields{
			ExportArn:    req.ExportArn,
			ExportStatus: "COMPLETED",
		},
	}, nil
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

func (h *DynamoDBHandler) describeTableReplicaAutoScaling(ctx context.Context, body []byte) (any, error) {
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

		table.mu.RLock("DescribeTableReplicaAutoScaling")
		for _, r := range table.Replicas {
			replicas = append(replicas, replicaAutoScalingDescription{
				RegionName:    r.RegionName,
				ReplicaStatus: r.ReplicaStatus,
			})
		}
		table.mu.RUnlock()
	}

	return &describeTableReplicaAutoScalingOutput{
		TableAutoScalingDescription: tableAutoScalingDescription{
			TableName:   req.TableName,
			TableStatus: models.TableStatusActive,
			Replicas:    replicas,
		},
	}, nil
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (h *DynamoDBHandler) Reset() {
	if db, ok := h.Backend.(*InMemoryDB); ok {
		db.Reset()
	}
}

// --- Wire types for the 10 new operations ---

type globalTableReplicaWire struct {
	RegionName string `json:"RegionName"`
}

type globalTableDescriptionWire struct {
	CreationDateTime  string                   `json:"CreationDateTime,omitempty"`
	GlobalTableArn    string                   `json:"GlobalTableArn,omitempty"`
	GlobalTableName   string                   `json:"GlobalTableName,omitempty"`
	GlobalTableStatus string                   `json:"GlobalTableStatus,omitempty"`
	ReplicationGroup  []globalTableReplicaWire `json:"ReplicationGroup,omitempty"`
}

type createGlobalTableInput struct {
	GlobalTableName  string                   `json:"GlobalTableName"`
	ReplicationGroup []globalTableReplicaWire `json:"ReplicationGroup"`
}

type createGlobalTableOutput struct {
	GlobalTableDescription globalTableDescriptionWire `json:"GlobalTableDescription"`
}

// updateGlobalTableReplicaActionWire wraps a Create or Delete action for a single region.
type updateGlobalTableReplicaActionWire struct {
	RegionName string `json:"RegionName,omitempty"`
}

// updateGlobalTableReplicaUpdateWire represents a single Create or Delete replica action.
type updateGlobalTableReplicaUpdateWire struct {
	Create *updateGlobalTableReplicaActionWire `json:"Create,omitempty"`
	Delete *updateGlobalTableReplicaActionWire `json:"Delete,omitempty"`
}

type updateGlobalTableInput struct {
	GlobalTableName string                               `json:"GlobalTableName"`
	ReplicaUpdates  []updateGlobalTableReplicaUpdateWire `json:"ReplicaUpdates"`
}

type updateGlobalTableOutput struct {
	GlobalTableDescription globalTableDescriptionWire `json:"GlobalTableDescription"`
}

type describeGlobalTableInput struct {
	GlobalTableName string `json:"GlobalTableName"`
}

type describeGlobalTableOutput struct {
	GlobalTableDescription globalTableDescriptionWire `json:"GlobalTableDescription"`
}

type describeGlobalTableSettingsInput struct {
	GlobalTableName string `json:"GlobalTableName"`
}

type replicaSettingsWire struct {
	RegionName                           string `json:"RegionName"`
	ReplicaStatus                        string `json:"ReplicaStatus,omitempty"`
	ReplicaProvisionedReadCapacityUnits  int64  `json:"ReplicaProvisionedReadCapacityUnits,omitempty"`
	ReplicaProvisionedWriteCapacityUnits int64  `json:"ReplicaProvisionedWriteCapacityUnits,omitempty"`
}

type describeGlobalTableSettingsOutput struct {
	GlobalTableName string                `json:"GlobalTableName,omitempty"`
	ReplicaSettings []replicaSettingsWire `json:"ReplicaSettings,omitempty"`
}

type describeKinesisInput struct {
	TableName string `json:"TableName"`
}

type kinesisDestinationWire struct {
	StreamArn         string `json:"StreamArn,omitempty"`
	DestinationStatus string `json:"DestinationStatus,omitempty"`
}

type describeKinesisOutput struct {
	TableName                     string                   `json:"TableName,omitempty"`
	KinesisDataStreamDestinations []kinesisDestinationWire `json:"KinesisDataStreamDestinations"`
}

type disableKinesisInput struct {
	TableName string `json:"TableName"`
	StreamArn string `json:"StreamArn"`
}

type disableKinesisOutput struct {
	TableName         string `json:"TableName,omitempty"`
	StreamArn         string `json:"StreamArn,omitempty"`
	DestinationStatus string `json:"DestinationStatus,omitempty"`
}

type describeLimitsOutput struct {
	AccountMaxReadCapacityUnits  int64 `json:"AccountMaxReadCapacityUnits"`
	AccountMaxWriteCapacityUnits int64 `json:"AccountMaxWriteCapacityUnits"`
	TableMaxReadCapacityUnits    int64 `json:"TableMaxReadCapacityUnits"`
	TableMaxWriteCapacityUnits   int64 `json:"TableMaxWriteCapacityUnits"`
}

type endpointWire struct {
	Address              string `json:"Address"`
	CachePeriodInMinutes int64  `json:"CachePeriodInMinutes"`
}

type describeEndpointsOutput struct {
	Endpoints []endpointWire `json:"Endpoints"`
}

type describeContributorInsightsInput struct {
	TableName string `json:"TableName"`
	IndexName string `json:"IndexName,omitempty"`
}

type describeContributorInsightsOutput struct {
	TableName                   string   `json:"TableName,omitempty"`
	IndexName                   string   `json:"IndexName,omitempty"`
	ContributorInsightsStatus   string   `json:"ContributorInsightsStatus,omitempty"`
	ContributorInsightsRuleList []string `json:"ContributorInsightsRuleList"`
}

type resourcePolicyInput struct {
	ResourceArn string `json:"ResourceArn"`
	Policy      string `json:"Policy,omitempty"`
}

type resourcePolicyOutput struct {
	RevisionID string `json:"RevisionId,omitempty"`
}

type globalTableWire struct {
	GlobalTableName  string                   `json:"GlobalTableName,omitempty"`
	ReplicationGroup []globalTableReplicaWire `json:"ReplicationGroup,omitempty"`
}

type listGlobalTablesInput struct {
	ExclusiveStartGlobalTableName string `json:"ExclusiveStartGlobalTableName,omitempty"`
	RegionName                    string `json:"RegionName,omitempty"`
	Limit                         int32  `json:"Limit,omitempty"`
}

type listGlobalTablesOutput struct {
	LastEvaluatedGlobalTableName string            `json:"LastEvaluatedGlobalTableName,omitempty"`
	GlobalTables                 []globalTableWire `json:"GlobalTables"`
}

type enableKinesisInput struct {
	TableName string `json:"TableName"`
	StreamArn string `json:"StreamArn"`
}

type enableKinesisOutput struct {
	TableName         string `json:"TableName,omitempty"`
	StreamArn         string `json:"StreamArn,omitempty"`
	DestinationStatus string `json:"DestinationStatus,omitempty"`
}

type deleteResourcePolicyInput struct {
	ResourceArn string `json:"ResourceArn"`
}

type deleteResourcePolicyOutput struct {
	RevisionID string `json:"RevisionId,omitempty"`
}

type describeImportInput struct {
	ImportArn string `json:"ImportArn"`
}

type importTableDescriptionWire struct {
	ImportArn    string `json:"ImportArn,omitempty"`
	ImportStatus string `json:"ImportStatus,omitempty"`
}

type describeImportOutput struct {
	ImportTableDescription importTableDescriptionWire `json:"ImportTableDescription"`
}

// dispatchExtraOps routes the extended DynamoDB operations to their handlers
// using a per-action dispatch map to keep complexity low.
func (h *DynamoDBHandler) dispatchExtraOps(
	ctx context.Context,
	action string,
	body []byte,
) (any, error) {
	type handlerFn func() (any, error)

	enableKinesis := func() (any, error) { return h.handleEnableKinesisStreamingDestination(ctx, body) }
	describeKinesis := func() (any, error) { return h.handleDescribeKinesisStreamingDestination(ctx, body) }
	disableKinesis := func() (any, error) { return h.handleDisableKinesisStreamingDestination(ctx, body) }

	handlers := map[string]handlerFn{
		"CreateGlobalTable":                   func() (any, error) { return h.handleCreateGlobalTable(ctx, body) },
		"DescribeGlobalTable":                 func() (any, error) { return h.handleDescribeGlobalTable(ctx, body) },
		"DescribeGlobalTableSettings":         func() (any, error) { return h.handleDescribeGlobalTableSettings(ctx, body) },
		"ListGlobalTables":                    func() (any, error) { return h.handleListGlobalTables(ctx, body) },
		"UpdateGlobalTable":                   func() (any, error) { return h.handleUpdateGlobalTable(ctx, body) },
		"EnableKinesisStreamingDestination":   enableKinesis,
		"DescribeKinesisStreamingDestination": describeKinesis,
		"DisableKinesisStreamingDestination":  disableKinesis,
		"DescribeLimits":                      func() (any, error) { return h.handleDescribeLimits(ctx) },
		"DescribeEndpoints":                   func() (any, error) { return h.handleDescribeEndpoints(ctx) },
		"DescribeContributorInsights":         func() (any, error) { return h.handleDescribeContributorInsights(ctx, body) },
		"GetResourcePolicy":                   func() (any, error) { return h.handleGetResourcePolicy(ctx, body) },
		"PutResourcePolicy":                   func() (any, error) { return h.handlePutResourcePolicy(ctx, body) },
		"DeleteResourcePolicy":                func() (any, error) { return h.handleDeleteResourcePolicy(ctx, body) },
		"DescribeImport":                      func() (any, error) { return h.handleDescribeImport(ctx, body) },
	}

	fn, ok := handlers[action]
	if !ok {
		return nil, fmt.Errorf("%w:%s", ErrUnknownOperation, action)
	}

	return fn()
}

func (h *DynamoDBHandler) handleCreateGlobalTable(ctx context.Context, body []byte) (any, error) {
	var req createGlobalTableInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	replicas := make([]types.Replica, 0, len(req.ReplicationGroup))

	for _, r := range req.ReplicationGroup {
		regionName := r.RegionName
		replicas = append(replicas, types.Replica{RegionName: &regionName})
	}

	out, err := h.Backend.CreateGlobalTable(ctx, &sdkDDB.CreateGlobalTableInput{
		GlobalTableName:  &req.GlobalTableName,
		ReplicationGroup: replicas,
	})
	if err != nil {
		return nil, err
	}

	d := out.GlobalTableDescription
	wire := buildGlobalTableDescriptionWire(d)

	return &createGlobalTableOutput{GlobalTableDescription: wire}, nil
}

func (h *DynamoDBHandler) handleDescribeGlobalTable(ctx context.Context, body []byte) (any, error) {
	var req describeGlobalTableInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	out, err := h.Backend.DescribeGlobalTable(ctx, &sdkDDB.DescribeGlobalTableInput{
		GlobalTableName: &req.GlobalTableName,
	})
	if err != nil {
		return nil, err
	}

	wire := buildGlobalTableDescriptionWire(out.GlobalTableDescription)

	return &describeGlobalTableOutput{GlobalTableDescription: wire}, nil
}

func (h *DynamoDBHandler) handleDescribeGlobalTableSettings(
	ctx context.Context,
	body []byte,
) (any, error) {
	var req describeGlobalTableSettingsInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	out, err := h.Backend.DescribeGlobalTableSettings(ctx, &sdkDDB.DescribeGlobalTableSettingsInput{
		GlobalTableName: &req.GlobalTableName,
	})
	if err != nil {
		return nil, err
	}

	replicaSettings := make([]replicaSettingsWire, 0, len(out.ReplicaSettings))
	for _, rs := range out.ReplicaSettings {
		w := replicaSettingsWire{
			RegionName:    derefStr(rs.RegionName),
			ReplicaStatus: string(rs.ReplicaStatus),
		}
		if rs.ReplicaProvisionedReadCapacityUnits != nil {
			w.ReplicaProvisionedReadCapacityUnits = *rs.ReplicaProvisionedReadCapacityUnits
		}

		if rs.ReplicaProvisionedWriteCapacityUnits != nil {
			w.ReplicaProvisionedWriteCapacityUnits = *rs.ReplicaProvisionedWriteCapacityUnits
		}

		replicaSettings = append(replicaSettings, w)
	}

	return &describeGlobalTableSettingsOutput{
		GlobalTableName: derefStr(out.GlobalTableName),
		ReplicaSettings: replicaSettings,
	}, nil
}

func (h *DynamoDBHandler) handleDescribeKinesisStreamingDestination(
	ctx context.Context,
	body []byte,
) (any, error) {
	var req describeKinesisInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	out, err := h.Backend.DescribeKinesisStreamingDestination(
		ctx,
		&sdkDDB.DescribeKinesisStreamingDestinationInput{TableName: &req.TableName},
	)
	if err != nil {
		return nil, err
	}

	destinations := make([]kinesisDestinationWire, 0, len(out.KinesisDataStreamDestinations))
	for _, d := range out.KinesisDataStreamDestinations {
		destinations = append(destinations, kinesisDestinationWire{
			StreamArn:         derefStr(d.StreamArn),
			DestinationStatus: string(d.DestinationStatus),
		})
	}

	return &describeKinesisOutput{
		TableName:                     derefStr(out.TableName),
		KinesisDataStreamDestinations: destinations,
	}, nil
}

func (h *DynamoDBHandler) handleDisableKinesisStreamingDestination(
	ctx context.Context,
	body []byte,
) (any, error) {
	var req disableKinesisInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	out, err := h.Backend.DisableKinesisStreamingDestination(
		ctx,
		&sdkDDB.DisableKinesisStreamingDestinationInput{
			TableName: &req.TableName,
			StreamArn: &req.StreamArn,
		},
	)
	if err != nil {
		return nil, err
	}

	return &disableKinesisOutput{
		TableName:         derefStr(out.TableName),
		StreamArn:         derefStr(out.StreamArn),
		DestinationStatus: string(out.DestinationStatus),
	}, nil
}

func (h *DynamoDBHandler) handleDescribeLimits(ctx context.Context) (any, error) {
	out, err := h.Backend.DescribeLimits(ctx, &sdkDDB.DescribeLimitsInput{})
	if err != nil {
		return nil, err
	}

	var accountRCU, accountWCU, tableRCU, tableWCU int64

	if out.AccountMaxReadCapacityUnits != nil {
		accountRCU = *out.AccountMaxReadCapacityUnits
	}

	if out.AccountMaxWriteCapacityUnits != nil {
		accountWCU = *out.AccountMaxWriteCapacityUnits
	}

	if out.TableMaxReadCapacityUnits != nil {
		tableRCU = *out.TableMaxReadCapacityUnits
	}

	if out.TableMaxWriteCapacityUnits != nil {
		tableWCU = *out.TableMaxWriteCapacityUnits
	}

	return &describeLimitsOutput{
		AccountMaxReadCapacityUnits:  accountRCU,
		AccountMaxWriteCapacityUnits: accountWCU,
		TableMaxReadCapacityUnits:    tableRCU,
		TableMaxWriteCapacityUnits:   tableWCU,
	}, nil
}

func (h *DynamoDBHandler) handleDescribeEndpoints(ctx context.Context) (any, error) {
	out, err := h.Backend.DescribeEndpoints(ctx, &sdkDDB.DescribeEndpointsInput{})
	if err != nil {
		return nil, err
	}

	endpoints := make([]endpointWire, 0, len(out.Endpoints))
	for _, e := range out.Endpoints {
		endpoints = append(endpoints, endpointWire{
			Address:              derefStr(e.Address),
			CachePeriodInMinutes: e.CachePeriodInMinutes,
		})
	}

	return &describeEndpointsOutput{Endpoints: endpoints}, nil
}

func (h *DynamoDBHandler) handleDescribeContributorInsights(
	ctx context.Context,
	body []byte,
) (any, error) {
	var req describeContributorInsightsInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	input := &sdkDDB.DescribeContributorInsightsInput{TableName: &req.TableName}
	if req.IndexName != "" {
		input.IndexName = &req.IndexName
	}

	out, err := h.Backend.DescribeContributorInsights(ctx, input)
	if err != nil {
		return nil, err
	}

	wire := &describeContributorInsightsOutput{
		TableName:                   derefStr(out.TableName),
		ContributorInsightsStatus:   string(out.ContributorInsightsStatus),
		ContributorInsightsRuleList: out.ContributorInsightsRuleList,
	}

	if out.IndexName != nil {
		wire.IndexName = *out.IndexName
	}

	return wire, nil
}

func (h *DynamoDBHandler) handleDeleteResourcePolicy(ctx context.Context, body []byte) (any, error) {
	var req deleteResourcePolicyInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	_, err := h.Backend.DeleteResourcePolicy(ctx, &sdkDDB.DeleteResourcePolicyInput{
		ResourceArn: &req.ResourceArn,
	})
	if err != nil {
		return nil, err
	}

	return &deleteResourcePolicyOutput{}, nil
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

	d := out.ImportTableDescription

	return &describeImportOutput{
		ImportTableDescription: importTableDescriptionWire{
			ImportArn:    derefStr(d.ImportArn),
			ImportStatus: string(d.ImportStatus),
		},
	}, nil
}

func (h *DynamoDBHandler) handleListGlobalTables(ctx context.Context, body []byte) (any, error) {
	var req listGlobalTablesInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, err
		}
	}

	sdkInput := &sdkDDB.ListGlobalTablesInput{}
	if req.ExclusiveStartGlobalTableName != "" {
		sdkInput.ExclusiveStartGlobalTableName = &req.ExclusiveStartGlobalTableName
	}

	if req.RegionName != "" {
		sdkInput.RegionName = &req.RegionName
	}

	if req.Limit > 0 {
		sdkInput.Limit = &req.Limit
	}

	out, err := h.Backend.ListGlobalTables(ctx, sdkInput)
	if err != nil {
		return nil, err
	}

	tables := make([]globalTableWire, 0, len(out.GlobalTables))
	for _, gt := range out.GlobalTables {
		replicas := make([]globalTableReplicaWire, 0, len(gt.ReplicationGroup))
		for _, r := range gt.ReplicationGroup {
			replicas = append(replicas, globalTableReplicaWire{RegionName: derefStr(r.RegionName)})
		}

		tables = append(tables, globalTableWire{
			GlobalTableName:  derefStr(gt.GlobalTableName),
			ReplicationGroup: replicas,
		})
	}

	wire := &listGlobalTablesOutput{GlobalTables: tables}
	if out.LastEvaluatedGlobalTableName != nil {
		wire.LastEvaluatedGlobalTableName = *out.LastEvaluatedGlobalTableName
	}

	return wire, nil
}

func (h *DynamoDBHandler) handleUpdateGlobalTable(ctx context.Context, body []byte) (any, error) {
	var req updateGlobalTableInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	updates := make([]types.ReplicaUpdate, 0, len(req.ReplicaUpdates))
	for _, u := range req.ReplicaUpdates {
		var update types.ReplicaUpdate
		if u.Create != nil {
			regionName := u.Create.RegionName
			update.Create = &types.CreateReplicaAction{RegionName: &regionName}
		} else if u.Delete != nil {
			regionName := u.Delete.RegionName
			update.Delete = &types.DeleteReplicaAction{RegionName: &regionName}
		}

		updates = append(updates, update)
	}

	out, err := h.Backend.UpdateGlobalTable(ctx, &sdkDDB.UpdateGlobalTableInput{
		GlobalTableName: &req.GlobalTableName,
		ReplicaUpdates:  updates,
	})
	if err != nil {
		return nil, err
	}

	d := out.GlobalTableDescription
	wire := buildGlobalTableDescriptionWire(d)

	return &updateGlobalTableOutput{GlobalTableDescription: wire}, nil
}

func (h *DynamoDBHandler) handleEnableKinesisStreamingDestination(
	ctx context.Context,
	body []byte,
) (any, error) {
	var req enableKinesisInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	out, err := h.Backend.EnableKinesisStreamingDestination(
		ctx,
		&sdkDDB.EnableKinesisStreamingDestinationInput{
			TableName: &req.TableName,
			StreamArn: &req.StreamArn,
		},
	)
	if err != nil {
		return nil, err
	}

	return &enableKinesisOutput{
		TableName:         derefStr(out.TableName),
		StreamArn:         derefStr(out.StreamArn),
		DestinationStatus: string(out.DestinationStatus),
	}, nil
}

func (h *DynamoDBHandler) handleGetResourcePolicy(ctx context.Context, body []byte) (any, error) {
	var req resourcePolicyInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	_, err := h.Backend.GetResourcePolicy(ctx, &sdkDDB.GetResourcePolicyInput{
		ResourceArn: &req.ResourceArn,
	})
	if err != nil {
		return nil, err
	}

	return &resourcePolicyOutput{}, nil
}

func (h *DynamoDBHandler) handlePutResourcePolicy(ctx context.Context, body []byte) (any, error) {
	var req resourcePolicyInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	_, err := h.Backend.PutResourcePolicy(ctx, &sdkDDB.PutResourcePolicyInput{
		ResourceArn: &req.ResourceArn,
		Policy:      &req.Policy,
	})
	if err != nil {
		return nil, err
	}

	return &resourcePolicyOutput{}, nil
}

// buildGlobalTableDescriptionWire converts the SDK GlobalTableDescription to the wire format.
func buildGlobalTableDescriptionWire(d *types.GlobalTableDescription) globalTableDescriptionWire {
	if d == nil {
		return globalTableDescriptionWire{}
	}

	replicas := make([]globalTableReplicaWire, 0, len(d.ReplicationGroup))
	for _, r := range d.ReplicationGroup {
		replicas = append(replicas, globalTableReplicaWire{RegionName: derefStr(r.RegionName)})
	}

	wire := globalTableDescriptionWire{
		GlobalTableName:   derefStr(d.GlobalTableName),
		GlobalTableArn:    derefStr(d.GlobalTableArn),
		GlobalTableStatus: string(d.GlobalTableStatus),
		ReplicationGroup:  replicas,
	}

	if d.CreationDateTime != nil {
		wire.CreationDateTime = d.CreationDateTime.UTC().Format(time.RFC3339)
	}

	return wire
}

// derefStr safely dereferences a *string, returning "" if nil.
func derefStr(s *string) string {
	if s == nil {
		return ""
	}

	return *s
}
