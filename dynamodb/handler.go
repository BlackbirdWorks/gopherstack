package dynamodb

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
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
	switch action {
	case "CreateTable":
		return h.DB.CreateTable(body)
	case "DeleteTable":
		return h.DB.DeleteTable(body)
	case "DescribeTable":
		return h.DB.DescribeTable(body)
	case "ListTables":
		return h.DB.ListTables(body)
	case "PutItem":
		return h.DB.PutItem(body)
	case "GetItem":
		return h.DB.GetItem(body)
	case "DeleteItem":
		return h.DB.DeleteItem(body)
	case "Scan":
		return h.DB.Scan(body)
	case "UpdateItem":
		return h.DB.UpdateItem(body)
	case "Query":
		return h.DB.Query(body)
	case "BatchGetItem":
		return h.DB.BatchGetItem(body)
	case "BatchWriteItem":
		return h.DB.BatchWriteItem(body)
	default:
		return nil, fmt.Errorf("%w:%s", ErrUnknownOperation, action)
	}
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
