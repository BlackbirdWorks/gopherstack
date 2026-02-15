package dynamodb

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
)

var ErrUnknownOperation = errors.New("UnknownOperationException")

// Handler handles HTTP requests for DynamoDB operations.
type Handler struct {
	DB          *InMemoryDB
	Logger      *slog.Logger
	dispatchMap map[string]func([]byte) (any, error)
}

// NewHandler creates a new DynamoDB handler.
func NewHandler() *Handler {
	h := &Handler{
		DB:     NewInMemoryDB(),
		Logger: slog.Default(),
	}
	h.initDispatchMap()

	return h
}

func (h *Handler) initDispatchMap() {
	h.dispatchMap = map[string]func([]byte) (any, error){
		"CreateTable":        h.DB.CreateTable,
		"DeleteTable":        h.DB.DeleteTable,
		"DescribeTable":      h.DB.DescribeTable,
		"ListTables":         h.DB.ListTables,
		"PutItem":            h.DB.PutItem,
		"GetItem":            h.DB.GetItem,
		"DeleteItem":         h.DB.DeleteItem,
		"Scan":               h.DB.Scan,
		"UpdateItem":         h.DB.UpdateItem,
		"Query":              h.DB.Query,
		"BatchGetItem":       h.DB.BatchGetItem,
		"BatchWriteItem":     h.DB.BatchWriteItem,
		"UpdateTimeToLive":   h.DB.UpdateTimeToLive,
		"DescribeTimeToLive": h.DB.DescribeTimeToLive,
		"TransactWriteItems": h.DB.TransactWriteItems,
		"TransactGetItems":   h.DB.TransactGetItems,
	}
}

// GetSupportedOperations returns a sorted list of supported DynamoDB operations.
func (h *Handler) GetSupportedOperations() []string {
	ops := make([]string, 0, len(h.dispatchMap))
	for op := range h.dispatchMap {
		ops = append(ops, op)
	}
	sort.Strings(ops)

	return ops
}

// ServeHTTP implements [http.Handler] interface.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusInternalServerError)

		return
	}
	defer func() {
		_ = r.Body.Close()
	}()

	w.Header().Set("Content-Type", "application/x-amz-json-1.0")

	response, reqErr := h.dispatch(action, body)

	if reqErr != nil {
		h.handleError(w, action, reqErr)

		return
	}

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		h.Logger.Error("Failed to marshal response", "error", err)
		http.Error(w, "Failed to marshal response", http.StatusInternalServerError)

		return
	}
	w.WriteHeader(http.StatusOK)
	if _, wErr := w.Write(jsonResponse); wErr != nil {
		h.Logger.Error("Failed to write response", "error", wErr)
	}
}

func (h *Handler) dispatch(action string, body []byte) (any, error) {
	if handler, ok := h.dispatchMap[action]; ok {
		return handler(body)
	}

	return nil, fmt.Errorf("%w:%s", ErrUnknownOperation, action)
}

func (h *Handler) handleError(w http.ResponseWriter, action string, reqErr error) {
	if strings.HasPrefix(reqErr.Error(), "UnknownOperationException:") {
		h.Logger.Warn("Unknown action", "action", action)
		w.WriteHeader(http.StatusBadRequest)

		if _, err := w.Write(
			[]byte(`{"__type":"com.amazon.coral.service#UnknownOperationException","message":"Action not supported"}`),
		); err != nil {
			h.Logger.Error("Failed to write unknown operation response", "error", err)
		}

		return
	}

	h.Logger.Error("Error handling action", "action", action, "error", reqErr)

	awsErr := h.classifyError(w, reqErr)

	jsonBytes, err := json.Marshal(awsErr)
	if err != nil {
		h.Logger.Error("Failed to marshal AWS error", "error", err)

		return
	}

	if _, wErr := w.Write(jsonBytes); wErr != nil {
		h.Logger.Error("Failed to write error response", "error", wErr)
	}
}

func (h *Handler) classifyError(w http.ResponseWriter, reqErr error) *Error {
	var awsErr *Error
	if errors.As(reqErr, &awsErr) {
		switch awsErr.Type {
		case "com.amazonaws.dynamodb.v20120810#InternalServerError":
			w.WriteHeader(http.StatusInternalServerError)
		case "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException":
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusBadRequest)
		}

		return awsErr
	}

	if strings.Contains(reqErr.Error(), "json:") || strings.Contains(reqErr.Error(), "unmarshal") {
		w.WriteHeader(http.StatusBadRequest)

		return NewValidationException(fmt.Sprintf("The parameter cannot be converted to a JSON: %s", reqErr.Error()))
	}

	w.WriteHeader(http.StatusInternalServerError)

	return &Error{
		Type:    "com.amazonaws.dynamodb.v20120810#InternalServerError",
		Message: reqErr.Error(),
	}
}
