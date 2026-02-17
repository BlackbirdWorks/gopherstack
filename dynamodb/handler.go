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

	"Gopherstack/dynamodb/models"
	"Gopherstack/pkgs/httputils"
	"Gopherstack/pkgs/logger"
)

var ErrUnknownOperation = errors.New("UnknownOperationException")

// writeDynamoDBJSON marshals payload to JSON, sets the x-amz-crc32 header with
// the CRC32/IEEE checksum of the body (required by the DynamoDB protocol), and
// writes the response.
func writeDynamoDBJSON(logger *slog.Logger, w http.ResponseWriter, code int, payload any) {
	response, err := json.Marshal(payload)
	if err != nil {
		if logger != nil {
			logger.Error("failed to marshal JSON response", "error", err)
		}
		http.Error(w, "internal server error", http.StatusInternalServerError)

		return
	}

	checksum := crc32.ChecksumIEEE(response)
	w.Header().
		Set("X-Amz-Crc32", strconv.FormatUint(uint64(checksum), 10))

	w.WriteHeader(code)

	// False positive: this is marshaled JSON, not user input
	//nolint:gosec // this is marshaled JSON, not user input
	if _, wErr := w.Write(response); wErr != nil && logger != nil {
		logger.Error("failed to write JSON response", "error", wErr)
	}
}

// Handler handles HTTP requests for DynamoDB operations.
type Handler struct {
	DB     *InMemoryDB
	Logger *slog.Logger
}

// NewHandler creates a new DynamoDB handler.
func NewHandler(logger *slog.Logger) *Handler {
	return &Handler{
		DB:     NewInMemoryDB(),
		Logger: logger,
	}
}

// GetSupportedOperations returns a sorted list of supported DynamoDB operations.
func (h *Handler) GetSupportedOperations() []string {
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

// ServeHTTP implements [http.Handler] interface.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	log := logger.Load(ctx)

	if r.Method == http.MethodGet && r.URL.Path == "/" {
		ops := h.GetSupportedOperations()
		writeDynamoDBJSON(log, w, http.StatusOK, ops)

		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)

		return
	}

	target := r.Header.Get("X-Amz-Target")
	if target == "" {
		http.Error(w, "Missing X-Amz-Target", http.StatusBadRequest)

		return
	}

	const targetParts = 2
	parts := strings.Split(target, ".")
	if len(parts) != targetParts {
		http.Error(w, "Invalid X-Amz-Target", http.StatusBadRequest)

		return
	}
	action := parts[1]

	body, err := httputils.ReadBody(r)
	if err != nil {
		httputils.WriteError(log, w, r, err, http.StatusInternalServerError)

		return
	}

	log.DebugContext(ctx, "DynamoDB request", "action", action, "body", string(body))

	w.Header().Set("Content-Type", "application/x-amz-json-1.0")

	response, reqErr := h.dispatch(ctx, action, body)
	if reqErr != nil {
		h.handleError(ctx, w, r, action, reqErr)

		return
	}

	writeDynamoDBJSON(log, w, http.StatusOK, response)
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) (any, error) {
	switch action {
	case "CreateTable", "DeleteTable", "DescribeTable", "ListTables", "UpdateTimeToLive", "DescribeTimeToLive":
		return h.dispatchTableOps(ctx, action, body)
	case "PutItem", "GetItem", "DeleteItem", "UpdateItem", "Query", "Scan", "BatchGetItem", "BatchWriteItem":
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
	doOp func(*SDKIn) (*SDKOut, error),
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
	sdkOutput, err := doOp(sdkInput)
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
	doOp func(*SDKIn) (*SDKOut, error),
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
	sdkOutput, err := doOp(sdkInput)
	if err != nil {
		return nil, err
	}

	wireOutput := fromSDK(sdkOutput)

	outputJSON, _ := json.Marshal(wireOutput)
	log.DebugContext(ctx, "handler output", "action", action, "output", string(outputJSON))

	return wireOutput, nil
}

func (h *Handler) dispatchTableOps(ctx context.Context, action string, body []byte) (any, error) {
	switch action {
	case "CreateTable":
		return handleOp(
			ctx, action, body,
			models.ToSDKCreateTableInput, h.DB.CreateTable, models.FromSDKCreateTableOutput,
		)
	case "DeleteTable":
		return handleOp(
			ctx, action, body,
			models.ToSDKDeleteTableInput, h.DB.DeleteTable, models.FromSDKDeleteTableOutput,
		)
	case "DescribeTable":
		return handleOp(
			ctx, action, body,
			models.ToSDKDescribeTableInput, h.DB.DescribeTable, models.FromSDKDescribeTableOutput,
		)
	case "ListTables":
		return handleOp(
			ctx, action, body,
			models.ToSDKListTablesInput, h.DB.ListTables, models.FromSDKListTablesOutput,
		)
	case "UpdateTimeToLive":
		return handleOp(
			ctx,
			action,
			body,
			models.ToSDKUpdateTimeToLiveInput,
			h.DB.UpdateTimeToLive,
			models.FromSDKUpdateTimeToLiveOutput,
		)
	case "DescribeTimeToLive":
		return handleOp(
			ctx,
			action,
			body,
			models.ToSDKDescribeTimeToLiveInput,
			h.DB.DescribeTimeToLive,
			models.FromSDKDescribeTimeToLiveOutput,
		)
	default:
		return nil, fmt.Errorf("%w:%s", ErrUnknownOperation, action)
	}
}

func (h *Handler) dispatchItemOps(ctx context.Context, action string, body []byte) (any, error) {
	switch action {
	case "PutItem":
		return handleOpErr(
			ctx, action, body,
			models.ToSDKPutItemInput, h.DB.PutItem, models.FromSDKPutItemOutput,
		)
	case "GetItem":
		return handleOpErr(
			ctx, action, body,
			models.ToSDKGetItemInput, h.DB.GetItem, models.FromSDKGetItemOutput,
		)
	case "DeleteItem":
		return handleOpErr(
			ctx, action, body,
			models.ToSDKDeleteItemInput, h.DB.DeleteItem, models.FromSDKDeleteItemOutput,
		)
	case "Scan":
		return handleOpErr(
			ctx, action, body,
			models.ToSDKScanInput, h.DB.Scan, models.FromSDKScanOutput,
		)
	case "UpdateItem":
		return handleOpErr(
			ctx, action, body,
			models.ToSDKUpdateItemInput, h.DB.UpdateItem, models.FromSDKUpdateItemOutput,
		)
	case "Query":
		return handleOpErr(
			ctx, action, body,
			models.ToSDKQueryInput, h.DB.Query, models.FromSDKQueryOutput,
		)
	case "BatchGetItem":
		return handleOpErr(
			ctx, action, body,
			models.ToSDKBatchGetItemInput, h.DB.BatchGetItem, models.FromSDKBatchGetItemOutput,
		)
	case "BatchWriteItem":
		return handleOpErr(
			ctx,
			action,
			body,
			models.ToSDKBatchWriteItemInput,
			h.DB.BatchWriteItem,
			models.FromSDKBatchWriteItemOutput,
		)
	default:
		return nil, fmt.Errorf("%w:%s", ErrUnknownOperation, action)
	}
}

func (h *Handler) dispatchTransactOps(ctx context.Context, action string, body []byte) (any, error) {
	switch action {
	case "TransactWriteItems":
		return handleOpErr(ctx,
			action,
			body,
			models.ToSDKTransactWriteItemsInput,
			h.DB.TransactWriteItems,
			models.FromSDKTransactWriteItemsOutput,
		)
	case "TransactGetItems":
		return handleOpErr(ctx,
			action,
			body,
			models.ToSDKTransactGetItemsInput,
			h.DB.TransactGetItems,
			models.FromSDKTransactGetItemsOutput,
		)
	default:
		return nil, fmt.Errorf("%w:%s", ErrUnknownOperation, action)
	}
}

func (h *Handler) handleError(ctx context.Context, w http.ResponseWriter, _ *http.Request, action string, reqErr error) {
	log := logger.Load(ctx)

	if strings.HasPrefix(reqErr.Error(), "UnknownOperationException:") {
		log.WarnContext(ctx, "Unknown action", "action", action)
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		body := []byte(
			`{"__type":"com.amazon.coral.service#UnknownOperationException","message":"Action not supported"}`,
		)
		checksum := crc32.ChecksumIEEE(body)
		w.Header().
			Set("X-Amz-Crc32", strconv.FormatUint(uint64(checksum), 10))

		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write(body)

		return
	}

	log.ErrorContext(ctx, "Error handling action", "action", action, "error", reqErr)

	statusCode, awsErr := h.classifyError(reqErr)

	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	writeDynamoDBJSON(log, w, statusCode, awsErr)
}

func (h *Handler) classifyError(reqErr error) (int, *Error) {
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
		return http.StatusBadRequest, NewValidationException(fmt.Sprintf("JSON Error: %s", reqErr.Error()))
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
