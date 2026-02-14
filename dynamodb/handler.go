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
		"CreateTable":    h.DB.CreateTable,
		"DeleteTable":    h.DB.DeleteTable,
		"DescribeTable":  h.DB.DescribeTable,
		"ListTables":     h.DB.ListTables,
		"PutItem":        h.DB.PutItem,
		"GetItem":        h.DB.GetItem,
		"DeleteItem":     h.DB.DeleteItem,
		"Scan":           h.DB.Scan,
		"UpdateItem":     h.DB.UpdateItem,
		"Query":          h.DB.Query,
		"BatchGetItem":   h.DB.BatchGetItem,
		"BatchWriteItem": h.DB.BatchWriteItem,
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
	w.Header().Set("Connection", "close")

	response, reqErr := h.dispatch(action, body)

	if reqErr != nil {
		h.handleError(w, action, reqErr)

		return
	}

	jsonResponse, _ := json.Marshal(response)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(jsonResponse)
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
		_, _ = w.Write(
			[]byte(`{"__type":"com.amazon.coral.service#UnknownOperationException","message":"Action not supported"}`),
		)

		return
	}

	h.Logger.Error("Error handling action", "action", action, "error", reqErr)

	var awsErr *Error
	if errors.As(reqErr, &awsErr) {
		if strings.Contains(awsErr.Type, "InternalServerError") {
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			w.WriteHeader(http.StatusBadRequest)
		}
		jsonBytes, _ := json.Marshal(awsErr)
		_, _ = w.Write(jsonBytes)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprintf(
			w,
			`{"__type":"com.amazonaws.dynamodb.v20120810#InternalServerError","message":"%v"}`,
			reqErr,
		)
	}
}
