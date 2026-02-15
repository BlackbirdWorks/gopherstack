package dynamodb

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"Gopherstack/pkgs/httputils"
)

var ErrUnknownOperation = errors.New("UnknownOperationException")

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
		httputils.WriteJSON(h.Logger, w, http.StatusOK, ops)
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

	httputils.WriteJSON(h.Logger, w, http.StatusOK, response)
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

// Helper for operations where Adapter allows error
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

// Helper for operations where Adapter does not return error
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
		return handleOp(body, ToSDKCreateTableInput, h.DB.CreateTable, FromSDKCreateTableOutput)
	case "DeleteTable":
		return handleOp(body, ToSDKDeleteTableInput, h.DB.DeleteTable, FromSDKDeleteTableOutput)
	case "DescribeTable":
		return handleOp(body, ToSDKDescribeTableInput, h.DB.DescribeTable, FromSDKDescribeTableOutput)
	case "ListTables":
		return handleOp(body, ToSDKListTablesInput, h.DB.ListTables, FromSDKListTablesOutput)
	case "UpdateTimeToLive":
		return handleOp(body, ToSDKUpdateTimeToLiveInput, h.DB.UpdateTimeToLive, FromSDKUpdateTimeToLiveOutput)
	case "DescribeTimeToLive":
		return handleOp(body, ToSDKDescribeTimeToLiveInput, h.DB.DescribeTimeToLive, FromSDKDescribeTimeToLiveOutput)
	default:
		return nil, fmt.Errorf("%w:%s", ErrUnknownOperation, action)
	}
}

func (h *Handler) dispatchItemOps(action string, body []byte) (any, error) {
	switch action {
	case "PutItem":
		return handleOpErr(body, ToSDKPutItemInput, h.DB.PutItem, FromSDKPutItemOutput)
	case "GetItem":
		return handleOpErr(body, ToSDKGetItemInput, h.DB.GetItem, FromSDKGetItemOutput)
	case "DeleteItem":
		return handleOpErr(body, ToSDKDeleteItemInput, h.DB.DeleteItem, FromSDKDeleteItemOutput)
	case "Scan":
		return handleOpErr(body, ToSDKScanInput, h.DB.Scan, FromSDKScanOutput)
	case "UpdateItem":
		return handleOpErr(body, ToSDKUpdateItemInput, h.DB.UpdateItem, FromSDKUpdateItemOutput)
	case "Query":
		return handleOpErr(body, ToSDKQueryInput, h.DB.Query, FromSDKQueryOutput)
	case "BatchGetItem":
		return handleOpErr(body, ToSDKBatchGetItemInput, h.DB.BatchGetItem, FromSDKBatchGetItemOutput)
	case "BatchWriteItem":
		return handleOpErr(body, ToSDKBatchWriteItemInput, h.DB.BatchWriteItem, FromSDKBatchWriteItemOutput)
	default:
		return nil, fmt.Errorf("%w:%s", ErrUnknownOperation, action)
	}
}

func (h *Handler) dispatchTransactOps(action string, body []byte) (any, error) {
	switch action {
	case "TransactWriteItems":
		return handleOpErr(body, ToSDKTransactWriteItemsInput, h.DB.TransactWriteItems, FromSDKTransactWriteItemsOutput)
	case "TransactGetItems":
		return handleOpErr(body, ToSDKTransactGetItemsInput, h.DB.TransactGetItems, FromSDKTransactGetItemsOutput)
	default:
		return nil, fmt.Errorf("%w:%s", ErrUnknownOperation, action)
	}
}

func (h *Handler) handleError(w http.ResponseWriter, _ *http.Request, action string, reqErr error) {
	if strings.HasPrefix(reqErr.Error(), "UnknownOperationException:") {
		h.Logger.Warn("Unknown action", "action", action)
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"__type":"com.amazon.coral.service#UnknownOperationException","message":"Action not supported"}`))
		return
	}

	h.Logger.Error("Error handling action", "action", action, "error", reqErr)

	statusCode, awsErr := h.classifyError(reqErr)

	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	httputils.WriteJSON(h.Logger, w, statusCode, awsErr)
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
