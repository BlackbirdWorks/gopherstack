package wafv2

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// validLoggingDestinationPrefixes lists accepted ARN prefixes for logging destinations.
var validLoggingDestinationPrefixes = []string{ //nolint:gochecknoglobals // package-level lookup table
	"arn:aws:firehose:",
	"arn:aws:s3:::",
	"arn:aws:logs:",
}

// deleteLoggingConfigurationRequest is the request body for DeleteLoggingConfiguration.
type deleteLoggingConfigurationRequest struct {
	ResourceArn string `json:"ResourceArn"`
}

func (h *Handler) handleDeleteLoggingConfiguration(ctx context.Context, body []byte) ([]byte, error) {
	var req deleteLoggingConfigurationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteLoggingConfiguration(ctx, req.ResourceArn); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: deleted logging configuration", "resourceArn", req.ResourceArn)

	return nil, nil
}

// putLoggingConfigurationRequest is the request body for PutLoggingConfiguration.
type putLoggingConfigurationRequest struct {
	LoggingConfiguration json.RawMessage `json:"LoggingConfiguration"`
}

func (h *Handler) handlePutLoggingConfiguration(ctx context.Context, body []byte) ([]byte, error) {
	var req putLoggingConfigurationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	// Extract ResourceArn for validation.
	var cfg map[string]json.RawMessage
	if err := json.Unmarshal(req.LoggingConfiguration, &cfg); err != nil {
		return nil, fmt.Errorf("%w: invalid LoggingConfiguration JSON: %w", errInvalidRequest, err)
	}

	var resourceARN string
	if raw, ok := cfg["ResourceArn"]; ok {
		if err := json.Unmarshal(raw, &resourceARN); err != nil || resourceARN == "" {
			return nil, fmt.Errorf("%w: LoggingConfiguration.ResourceArn is required", errInvalidRequest)
		}
	} else {
		return nil, fmt.Errorf("%w: LoggingConfiguration.ResourceArn is required", errInvalidRequest)
	}

	// Validate destination ARN prefixes.
	if destRaw, ok := cfg["LogDestinationConfigs"]; ok {
		var destinations []string
		if unmarshalErr := json.Unmarshal(destRaw, &destinations); unmarshalErr == nil {
			for _, dest := range destinations {
				if destErr := validateLoggingDestination(dest); destErr != nil {
					return nil, destErr
				}
			}
		}
	}

	if err := h.Backend.PutLoggingConfiguration(ctx, resourceARN, req.LoggingConfiguration); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: put logging configuration", "resourceArn", resourceARN)

	var respCfg any
	if err := json.Unmarshal(req.LoggingConfiguration, &respCfg); err != nil {
		respCfg = map[string]any{"ResourceArn": resourceARN}
	}

	return json.Marshal(map[string]any{
		"LoggingConfiguration": respCfg,
	})
}

// validateLoggingDestination checks that a destination ARN has an accepted prefix.
func validateLoggingDestination(dest string) error {
	for _, prefix := range validLoggingDestinationPrefixes {
		if strings.HasPrefix(dest, prefix) {
			return nil
		}
	}

	return fmt.Errorf(
		"%w: LogDestinationConfig ARN %q must start with one of: firehose, s3, or CloudWatch Logs",
		errInvalidRequest,
		dest,
	)
}

// getLoggingConfigurationRequest is the request body for GetLoggingConfiguration.
type getLoggingConfigurationRequest struct {
	ResourceArn string `json:"ResourceArn"`
}

func (h *Handler) handleGetLoggingConfiguration(ctx context.Context, body []byte) ([]byte, error) {
	var req getLoggingConfigurationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	cfgJSON, err := h.Backend.GetLoggingConfiguration(ctx, req.ResourceArn)
	if err != nil {
		return nil, err
	}

	var cfg any
	if unmarshalErr := json.Unmarshal(cfgJSON, &cfg); unmarshalErr != nil {
		cfg = map[string]any{"ResourceArn": req.ResourceArn}
	}

	return json.Marshal(map[string]any{
		"LoggingConfiguration": cfg,
	})
}

// handleListLoggingConfigurations lists all logging configurations.
func (h *Handler) handleListLoggingConfigurations(ctx context.Context, _ []byte) ([]byte, error) {
	configs := h.Backend.ListLoggingConfigurations(ctx)
	items := make([]any, 0, len(configs))

	for _, cfg := range configs {
		var v any
		if err := json.Unmarshal(cfg, &v); err == nil {
			items = append(items, v)
		}
	}

	return json.Marshal(map[string]any{"LoggingConfigurations": items})
}

// loggingConfigDispatchOps returns the logging-configuration-family operation dispatch
// entries. Each entry is a bound method value -- handleDeleteLoggingConfiguration et al.
// already match the dispatchFn signature, so no wrapper closure is needed.
func (h *Handler) loggingConfigDispatchOps() map[string]dispatchFn {
	return map[string]dispatchFn{
		"DeleteLoggingConfiguration": h.handleDeleteLoggingConfiguration,
		"PutLoggingConfiguration":    h.handlePutLoggingConfiguration,
		"GetLoggingConfiguration":    h.handleGetLoggingConfiguration,
		"ListLoggingConfigurations":  h.handleListLoggingConfigurations,
	}
}
