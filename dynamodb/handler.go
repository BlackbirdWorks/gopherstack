package dynamodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"Gopherstack/dynamodb/models"
	"Gopherstack/pkgs/httputil"
	"Gopherstack/pkgs/logger"
	"Gopherstack/pkgs/service"
)

var ErrUnknownOperation = errors.New("UnknownOperationException")

// DynamoDBHandler handles HTTP requests for DynamoDB operations.
//
//nolint:revive // Stuttering preferred here for clarity per Plan.md
type DynamoDBHandler struct {
	Backend StorageBackend
	Logger  *slog.Logger
}

// NewHandler creates a new DynamoDB handler with the given storage backend.
func NewHandler(backend StorageBackend, logger *slog.Logger) *DynamoDBHandler {
	return &DynamoDBHandler{
		Backend: backend,
		Logger:  logger,
	}
}

// GetSupportedOperations returns a sorted list of supported DynamoDB operations.
func (h *DynamoDBHandler) GetSupportedOperations() []string {
	return []string{
		"BatchGetItem",
		"BatchWriteItem",
		"CreateTable",
		"DeleteItem",
		"DeleteTable",
		"DescribeTable",
		"DescribeTimeToLive",
		"GetItem",
		"ListTables",
		"PutItem",
		"Query",
		"Scan",
		"TransactGetItems",
		"TransactWriteItems",
		"UpdateItem",
		"UpdateTimeToLive",
	}
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
	const priority = 100

	return priority
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
