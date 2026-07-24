package s3tables

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func (h *Handler) handleDeleteTableReplication(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	tableArn := r.URL.Query().Get(keyTableArnLower)
	if tableArn == "" {
		return nil, fmt.Errorf("%w: tableArn is required", errInvalidRequest)
	}

	versionToken := r.URL.Query().Get(keyVersionToken)
	if versionToken == "" {
		return nil, fmt.Errorf("%w: versionToken is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteTableReplication(tableArn, versionToken); err != nil {
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
	tableArn := r.URL.Query().Get(keyTableArnLower)
	if tableArn == "" {
		return nil, fmt.Errorf("%w: tableArn is required", errInvalidRequest)
	}

	cfg, err := h.Backend.GetTableRecordExpirationConfiguration(tableArn)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: got table record expiration configuration", "tableArn", tableArn)

	// GetTableRecordExpirationConfigurationOutput has a single required
	// "configuration" member ({status, settings: {days}}) -- there is no
	// top-level tableARN or status field on the real output (confirmed via
	// deserializeOpDocumentGetTableRecordExpirationConfigurationOutput in
	// aws-sdk-go-v2/service/s3tables's deserializers.go).
	configuration := map[string]any{keyStatusField: cfg.Status}
	if cfg.Days > 0 {
		configuration[keySettings] = map[string]any{"days": cfg.Days}
	}

	return json.Marshal(map[string]any{
		keyConfiguration: configuration,
	})
}

func (h *Handler) handleGetTableReplication(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	tableArn := r.URL.Query().Get(keyTableArnLower)
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
		keyConfiguration: map[string]any{
			"role":  cfg.Role,
			"rules": cfg.Rules,
		},
		keyVersionToken: cfg.VersionToken,
	})
}

// putTableReplicationRequest is the request body for PutTableReplication.
type putTableReplicationRequest struct {
	Configuration map[string]any `json:"configuration"`
}

func (h *Handler) handlePutTableReplication(ctx context.Context, r *http.Request, body []byte) ([]byte, error) {
	tableArn := r.URL.Query().Get(keyTableArnLower)
	if tableArn == "" {
		return nil, fmt.Errorf("%w: tableArn is required", errInvalidRequest)
	}

	versionToken := r.URL.Query().Get(keyVersionToken)

	var req putTableReplicationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	role, rules := parseReplicationConfiguration(req.Configuration)

	cfg, err := h.Backend.PutTableReplication(tableArn, role, rules, versionToken)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: put table replication", "tableArn", tableArn)

	return json.Marshal(map[string]any{
		keyStatusField:  replicationStatusCompleted,
		keyVersionToken: cfg.VersionToken,
	})
}

func (h *Handler) handleGetTableReplicationStatus(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	tableArn := r.URL.Query().Get(keyTableArnLower)
	if tableArn == "" {
		return nil, fmt.Errorf("%w: tableArn is required", errInvalidRequest)
	}

	if err := h.Backend.ValidateTableExists(tableArn); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: got table replication status", "tableArn", tableArn)

	destinations := []map[string]any{}

	if cfg, err := h.Backend.GetTableReplicationConfig(tableArn); err == nil {
		for _, rule := range cfg.Rules {
			for _, dest := range rule.Destinations {
				destinations = append(destinations, map[string]any{
					"destinationTableBucketArn": dest.DestinationTableBucketARN,
					"replicationStatus":         replicationStatusCompletedLower,
				})
			}
		}
	}

	return json.Marshal(map[string]any{
		"sourceTableArn": tableArn,
		"destinations":   destinations,
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
	tableArn := r.URL.Query().Get(keyTableArnLower)
	if tableArn == "" {
		return nil, fmt.Errorf("%w: tableArn is required", errInvalidRequest)
	}

	var req putTableRecordExpirationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	cfg := &TableRecordExpiryConfig{Status: recordExpirationStatusDisabled}
	if st, ok := req.Value[keyStatusField].(string); ok {
		cfg.Status = st
	}

	if settings, ok := req.Value[keySettings].(map[string]any); ok {
		if days, isNum := settings["days"].(float64); isNum {
			cfg.Days = int(days)
		}
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
	tableArn := r.URL.Query().Get(keyTableArnLower)
	if tableArn == "" {
		return nil, fmt.Errorf("%w: tableArn is required", errInvalidRequest)
	}

	if err := h.Backend.ValidateTableExists(tableArn); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: got table record expiration job status", "tableArn", tableArn)

	// GetTableRecordExpirationJobStatusOutput.Status is the
	// types.TableRecordExpirationJobStatus enum ("NotYetRun"/"Successful"/
	// "Failed"/"Disabled" -- confirmed via enums.go), NOT the fabricated
	// "SUCCEEDED" this previously returned (which matches no enum value at
	// all). This in-memory backend runs no background expiration jobs, so a
	// table with expiration enabled reports "NotYetRun"; one with no
	// configuration (or explicitly disabled) reports "Disabled".
	status := recordExpirationJobStatusDisabled
	if cfg, err := h.Backend.GetTableRecordExpirationConfiguration(tableArn); err == nil &&
		cfg.Status == statusEnabled {
		status = recordExpirationJobStatusNotYetRun
	}

	return json.Marshal(map[string]any{
		keyStatusField: status,
	})
}
