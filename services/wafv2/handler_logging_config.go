package wafv2

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// defaultLogScope is LoggingConfiguration.LogScope's documented default
// ("Default: CUSTOMER", types.LoggingConfiguration doc comment,
// wafv2@v1.77.3 types/types.go) -- the SDK serializer omits LogScope from
// the wire entirely when it's the zero value (serializers.go:
// awsAwsjson11_serializeDocumentLoggingConfiguration, `if len(v.LogScope) >
// 0`), so a stored config with no LogScope key means CUSTOMER.
const defaultLogScope = "CUSTOMER"

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

// listLoggingConfigurationsRequest mirrors ListLoggingConfigurationsInput
// (wafv2@v1.77.3 api_op_ListLoggingConfigurations.go): Scope, Limit, LogScope, and
// NextMarker are all real request members, not just Scope.
type listLoggingConfigurationsRequest struct {
	Scope      string `json:"Scope"`
	LogScope   string `json:"LogScope"`
	NextMarker string `json:"NextMarker"`
	Limit      int    `json:"Limit"`
}

// loggingConfigEntry pairs a stored logging configuration's decoded document
// with the ResourceArn key used for LogScope filtering and marker-based
// pagination.
type loggingConfigEntry struct {
	doc map[string]any
	arn string
}

// filterLoggingConfigsByLogScope keeps only entries whose LogScope matches
// logScope (defaultLogScope when a document has none), or returns entries
// unchanged when logScope is empty (no filter requested).
func filterLoggingConfigsByLogScope(entries []loggingConfigEntry, logScope string) []loggingConfigEntry {
	if logScope == "" {
		return entries
	}

	filtered := make([]loggingConfigEntry, 0, len(entries))

	for _, e := range entries {
		docLogScope, _ := e.doc["LogScope"].(string)
		if docLogScope == "" {
			docLogScope = defaultLogScope
		}

		if docLogScope == logScope {
			filtered = append(filtered, e)
		}
	}

	return filtered
}

// skipToLoggingConfigMarker returns the entries after the one whose ARN
// equals nextMarker (an unknown marker yields no entries), or entries
// unchanged when nextMarker is empty.
func skipToLoggingConfigMarker(entries []loggingConfigEntry, nextMarker string) []loggingConfigEntry {
	if nextMarker == "" {
		return entries
	}

	for i, e := range entries {
		if e.arn == nextMarker {
			return entries[i+1:]
		}
	}

	return nil
}

// handleListLoggingConfigurations lists logging configurations for the request's Scope
// and LogScope, paginated by Limit/NextMarker.
func (h *Handler) handleListLoggingConfigurations(ctx context.Context, body []byte) ([]byte, error) {
	var req listLoggingConfigurationsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	configs := h.Backend.ListLoggingConfigurations(ctx, req.Scope)

	entries := make([]loggingConfigEntry, 0, len(configs))

	for _, cfg := range configs {
		var v map[string]any
		if err := json.Unmarshal(cfg, &v); err != nil {
			continue
		}

		arn, _ := v["ResourceArn"].(string)
		entries = append(entries, loggingConfigEntry{arn: arn, doc: v})
	}

	entries = filterLoggingConfigsByLogScope(entries, req.LogScope)
	entries = skipToLoggingConfigMarker(entries, req.NextMarker)

	nextMarker := ""
	if req.Limit > 0 && len(entries) > req.Limit {
		nextMarker = entries[req.Limit-1].arn
		entries = entries[:req.Limit]
	}

	items := make([]any, 0, len(entries))
	for _, e := range entries {
		items = append(items, e.doc)
	}

	resp := map[string]any{"LoggingConfigurations": items}
	if nextMarker != "" {
		resp["NextMarker"] = nextMarker
	}

	return json.Marshal(resp)
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
