package s3tables

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func (h *Handler) handleDeleteTableReplication(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	tableArn := r.URL.Query().Get("tableArn")
	if tableArn == "" {
		return nil, fmt.Errorf("%w: tableArn is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteTableReplication(tableArn); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: deleted table replication", "tableArn", tableArn)

	return nil, nil
}

func (h *Handler) handleGetTableRecordExpirationConfiguration(
	ctx context.Context,
	r *http.Request,
	_ []byte,
) ([]byte, error) {
	tableArn := r.URL.Query().Get("tableArn")
	if tableArn == "" {
		return nil, fmt.Errorf("%w: tableArn is required", errInvalidRequest)
	}

	cfg, err := h.Backend.GetTableRecordExpirationConfiguration(tableArn)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: got table record expiration configuration", "tableArn", tableArn)

	return json.Marshal(map[string]any{
		keyTableARN:    tableArn,
		keyStatusField: cfg.Status,
	})
}

func (h *Handler) handleGetTableReplication(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	tableArn := r.URL.Query().Get("tableArn")
	if tableArn == "" {
		return nil, fmt.Errorf("%w: tableArn is required", errInvalidRequest)
	}

	cfg, err := h.Backend.GetTableReplicationConfig(tableArn)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: got table replication", "tableArn", tableArn)

	return json.Marshal(map[string]any{
		keyConfiguration: cfg,
		keyVersionToken:  "",
	})
}

// putTableReplicationRequest is the request body for PutTableReplication.
type putTableReplicationRequest struct {
	Configuration map[string]any `json:"configuration"`
}

func (h *Handler) handlePutTableReplication(ctx context.Context, r *http.Request, body []byte) ([]byte, error) {
	tableArn := r.URL.Query().Get("tableArn")
	if tableArn == "" {
		return nil, fmt.Errorf("%w: tableArn is required", errInvalidRequest)
	}

	var req putTableReplicationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.SetTableReplicationConfig(tableArn, req.Configuration); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: put table replication", "tableArn", tableArn)

	return nil, nil
}

func (h *Handler) handleGetTableReplicationStatus(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	tableArn := r.URL.Query().Get("tableArn")
	if tableArn == "" {
		return nil, fmt.Errorf("%w: tableArn is required", errInvalidRequest)
	}

	if err := h.Backend.ValidateTableExists(tableArn); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: got table replication status", "tableArn", tableArn)

	return json.Marshal(map[string]any{
		"sourceTableArn": tableArn,
		"destinations":   []any{},
	})
}

// putTableRecordExpirationRequest is the request body for PutTableRecordExpirationConfiguration.
type putTableRecordExpirationRequest struct {
	Value map[string]any `json:"value"`
}

func (h *Handler) handlePutTableRecordExpirationConfiguration(
	ctx context.Context,
	r *http.Request,
	body []byte,
) ([]byte, error) {
	tableArn := r.URL.Query().Get("tableArn")
	if tableArn == "" {
		return nil, fmt.Errorf("%w: tableArn is required", errInvalidRequest)
	}

	var req putTableRecordExpirationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	cfg := &TableRecordExpiryConfig{Status: "DISABLED"}
	if st, ok := req.Value[keyStatusField].(string); ok {
		cfg.Status = st
	}

	if err := h.Backend.PutTableRecordExpirationConfiguration(tableArn, cfg); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: put table record expiration configuration", "tableArn", tableArn)

	return nil, nil
}

func (h *Handler) handleGetTableRecordExpirationJobStatus(
	ctx context.Context,
	r *http.Request,
	_ []byte,
) ([]byte, error) {
	tableArn := r.URL.Query().Get("tableArn")
	if tableArn == "" {
		return nil, fmt.Errorf("%w: tableArn is required", errInvalidRequest)
	}

	if err := h.Backend.ValidateTableExists(tableArn); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: got table record expiration job status", "tableArn", tableArn)

	return json.Marshal(map[string]any{
		keyStatusField: "SUCCEEDED",
	})
}
