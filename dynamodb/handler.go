package dynamodb

import (
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

	w.Header().Set("Content-Length", strconv.Itoa(len(response)))
	w.WriteHeader(code)

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
func NewHandler() *Handler {
	return &Handler{
		DB:     NewInMemoryDB(),
		Logger: slog.Default(),
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
	if r.Method == http.MethodGet && r.URL.Path == "/" {
		ops := h.GetSupportedOperations()
		writeDynamoDBJSON(h.Logger, w, http.StatusOK, ops)

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
		httputils.WriteError(h.Logger, w, r, err, http.StatusInternalServerError)

		return
	}

	w.Header().Set("Content-Type", "application/x-amz-json-1.0")

	response, reqErr := h.dispatch(action, body)
	if reqErr != nil {
		h.handleError(w, r, action, reqErr)

		return
	}

	writeDynamoDBJSON(h.Logger, w, http.StatusOK, response)
}

func (h *Handler) dispatch(action string, body []byte) (any, error) {
	switch action {
	case "CreateTable", "DeleteTable", "DescribeTable", "ListTables", "UpdateTimeToLive", "DescribeTimeToLive":
		return h.dispatchTableOps(action, body)
	case "PutItem", "GetItem", "DeleteItem", "UpdateItem", "Query", "Scan", "BatchGetItem", "BatchWriteItem":
		return h.dispatchItemOps(action, body)
	case "TransactWriteItems", "TransactGetItems":
		return h.dispatchTransactOps(action, body)
	default:
		return nil, fmt.Errorf("%w:%s", ErrUnknownOperation, action)
	}
}

// Helper for operations where Adapter allows error.
func handleOpErr[WireIn any, SDKIn any, SDKOut any, WireOut any](
	body []byte,
	toSDK func(*WireIn) (*SDKIn, error),
	doOp func(*SDKIn) (*SDKOut, error),
	fromSDK func(*SDKOut) *WireOut,
) (any, error) {
	var input WireIn
	if len(body) > 0 {
		if err := json.Unmarshal(body, &input); err != nil {
			return nil, err
		}
	}
	sdkInput, err := toSDK(&input)
	if err != nil {
		return nil, err
	}
	sdkOutput, err := doOp(sdkInput)
	if err != nil {
		return nil, err
	}

	return fromSDK(sdkOutput), nil
}

// Helper for operations where Adapter does not return error.
func handleOp[WireIn any, SDKIn any, SDKOut any, WireOut any](
	body []byte,
	toSDK func(*WireIn) *SDKIn,
	doOp func(*SDKIn) (*SDKOut, error),
	fromSDK func(*SDKOut) *WireOut,
) (any, error) {
	var input WireIn
	if len(body) > 0 {
		if err := json.Unmarshal(body, &input); err != nil {
			return nil, err
		}
	}
	sdkInput := toSDK(&input)
	sdkOutput, err := doOp(sdkInput)
	if err != nil {
		return nil, err
	}

	return fromSDK(sdkOutput), nil
}

func (h *Handler) dispatchTableOps(action string, body []byte) (any, error) {
	switch action {
	case "CreateTable":
		return handleOp(body, models.ToSDKCreateTableInput, h.DB.CreateTable, models.FromSDKCreateTableOutput)
	case "DeleteTable":
		return handleOp(body, models.ToSDKDeleteTableInput, h.DB.DeleteTable, models.FromSDKDeleteTableOutput)
	case "DescribeTable":
		return handleOp(body, models.ToSDKDescribeTableInput, h.DB.DescribeTable, models.FromSDKDescribeTableOutput)
	case "ListTables":
		return handleOp(body, models.ToSDKListTablesInput, h.DB.ListTables, models.FromSDKListTablesOutput)
	case "UpdateTimeToLive":
		return handleOp(
			body,
			models.ToSDKUpdateTimeToLiveInput,
			h.DB.UpdateTimeToLive,
			models.FromSDKUpdateTimeToLiveOutput,
		)
	case "DescribeTimeToLive":
		return handleOp(
			body,
			models.ToSDKDescribeTimeToLiveInput,
			h.DB.DescribeTimeToLive,
			models.FromSDKDescribeTimeToLiveOutput,
		)
	default:
		return nil, fmt.Errorf("%w:%s", ErrUnknownOperation, action)
	}
}

func (h *Handler) dispatchItemOps(action string, body []byte) (any, error) {
	switch action {
	case "PutItem":
		return handleOpErr(body, models.ToSDKPutItemInput, h.DB.PutItem, models.FromSDKPutItemOutput)
	case "GetItem":
		return handleOpErr(body, models.ToSDKGetItemInput, h.DB.GetItem, models.FromSDKGetItemOutput)
	case "DeleteItem":
		return handleOpErr(body, models.ToSDKDeleteItemInput, h.DB.DeleteItem, models.FromSDKDeleteItemOutput)
	case "Scan":
		return handleOpErr(body, models.ToSDKScanInput, h.DB.Scan, models.FromSDKScanOutput)
	case "UpdateItem":
		return handleOpErr(body, models.ToSDKUpdateItemInput, h.DB.UpdateItem, models.FromSDKUpdateItemOutput)
	case "Query":
		return handleOpErr(body, models.ToSDKQueryInput, h.DB.Query, models.FromSDKQueryOutput)
	case "BatchGetItem":
		return handleOpErr(body, models.ToSDKBatchGetItemInput, h.DB.BatchGetItem, models.FromSDKBatchGetItemOutput)
	case "BatchWriteItem":
		return handleOpErr(
			body,
			models.ToSDKBatchWriteItemInput,
			h.DB.BatchWriteItem,
			models.FromSDKBatchWriteItemOutput,
		)
	default:
		return nil, fmt.Errorf("%w:%s", ErrUnknownOperation, action)
	}
}

func (h *Handler) dispatchTransactOps(action string, body []byte) (any, error) {
	switch action {
	case "TransactWriteItems":
		return handleOpErr(
			body,
			models.ToSDKTransactWriteItemsInput,
			h.DB.TransactWriteItems,
			models.FromSDKTransactWriteItemsOutput,
		)
	case "TransactGetItems":
		return handleOpErr(
			body,
			models.ToSDKTransactGetItemsInput,
			h.DB.TransactGetItems,
			models.FromSDKTransactGetItemsOutput,
		)
	default:
		return nil, fmt.Errorf("%w:%s", ErrUnknownOperation, action)
	}
}

func (h *Handler) handleError(w http.ResponseWriter, _ *http.Request, action string, reqErr error) {
	if strings.HasPrefix(reqErr.Error(), "UnknownOperationException:") {
		h.Logger.Warn("Unknown action", "action", action)
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		body := []byte(
			`{"__type":"com.amazon.coral.service#UnknownOperationException","message":"Action not supported"}`,
		)
		checksum := crc32.ChecksumIEEE(body)
		w.Header().
			Set("X-Amz-Crc32", strconv.FormatUint(uint64(checksum), 10))

		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write(body)

		return
	}

	h.Logger.Error("Error handling action", "action", action, "error", reqErr)

	statusCode, awsErr := h.classifyError(reqErr)

	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	writeDynamoDBJSON(h.Logger, w, statusCode, awsErr)
}

func (h *Handler) classifyError(reqErr error) (int, *Error) {
	// Simple error classification wrapping
	// If it's already a DynamoDB error type/struct, use it.
	// But our internal implementation returns native go errors or custom structs.
	// We need to map them to Wire Error struct.

	// If reqErr is already *Error (Wire type), return it.
	var wireErr *Error
	if errors.As(reqErr, &wireErr) {
		// Map type to status code
		switch wireErr.Type {
		case "com.amazonaws.dynamodb.v20120810#InternalServerError":
			return http.StatusInternalServerError, wireErr
		case "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException":
			return http.StatusNotFound, wireErr
		case "com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException":
			return http.StatusBadRequest, wireErr
		case "com.amazonaws.dynamodb.v20120810#TransactionCanceledException":
			return http.StatusBadRequest, wireErr
		default:
			return http.StatusBadRequest, wireErr
		}
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
