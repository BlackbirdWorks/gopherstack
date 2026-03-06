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

	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
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
func (h *DynamoDBHandler) WithJanitor(settings Settings) *DynamoDBHandler {
	h.DefaultRegion = settings.DefaultRegion
	if h.DefaultRegion == "" {
		h.DefaultRegion = config.DefaultRegion
	}
	if memBackend, ok := h.Backend.(*InMemoryDB); ok {
		memBackend.SetDefaultRegion(h.DefaultRegion)
		h.janitor = NewJanitor(memBackend, settings)
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
		"BatchGetItem",
		"BatchWriteItem",
		"CreateBackup",
		"CreateTable",
		"DeleteBackup",
		"DeleteItem",
		"DeleteTable",
		"DescribeBackup",
		"DescribeContinuousBackups",
		"DescribeExport",
		"DescribeStream",
		"DescribeTable",
		"DescribeTableReplicaAutoScaling",
		"DescribeTimeToLive",
		"ExportTableToPointInTime",
		"GetItem",
		"GetRecords",
		"GetShardIterator",
		"ListBackups",
		"ListExports",
		"ListStreams",
		"ListTables",
		"ListTagsOfResource",
		"PutItem",
		"Query",
		"RestoreTableFromBackup",
		"RestoreTableToPointInTime",
		"Scan",
		"TagResource",
		"TransactGetItems",
		"TransactWriteItems",
		"UntagResource",
		"BatchExecuteStatement",
		"ExecuteStatement",
		"UpdateContinuousBackups",
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

		body, err := httputil.ReadBody(c.Request())
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

		return strings.HasPrefix(target, "DynamoDB_") ||
			strings.HasPrefix(target, "DynamoDBStreams_")
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
	body, err := httputil.ReadBody(c.Request())
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

type describeContinuousBackupsInput struct {
	TableName string `json:"TableName"`
}

func (h *DynamoDBHandler) describeContinuousBackups(ctx context.Context, body []byte) (any, error) {
	var req describeContinuousBackupsInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	pitrStatus := "DISABLED"

	if db, ok := h.Backend.(*InMemoryDB); ok {
		table, err := db.getTable(ctx, req.TableName)
		if err != nil {
			return nil, err
		}

		table.mu.RLock("DescribeContinuousBackups")
		if table.PITREnabled {
			pitrStatus = "ENABLED"
		}
		table.mu.RUnlock()
	}

	return &describeContinuousBackupsOutput{
		ContinuousBackupsDescription: continuousBackupsDescriptionFields{
			ContinuousBackupsStatus:        "ENABLED",
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

	status := "DISABLED"
	if pitrEnabled {
		status = "ENABLED"
	}

	return &describeContinuousBackupsOutput{
		ContinuousBackupsDescription: continuousBackupsDescriptionFields{
			ContinuousBackupsStatus:        "ENABLED",
			PointInTimeRecoveryDescription: pointInTimeRecoveryDescription{PointInTimeRecoveryStatus: status},
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
