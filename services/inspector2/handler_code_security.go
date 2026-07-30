package inspector2

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const (
	opCreateCodeSecurityIntegration = "CreateCodeSecurityIntegration"
	opDeleteCodeSecurityIntegration = "DeleteCodeSecurityIntegration"
	opGetCodeSecurityIntegration    = "GetCodeSecurityIntegration"
	opUpdateCodeSecurityIntegration = "UpdateCodeSecurityIntegration"
	opListCodeSecurityIntegrations  = "ListCodeSecurityIntegrations"

	opCreateCodeSecurityScanConfiguration            = "CreateCodeSecurityScanConfiguration"
	opDeleteCodeSecurityScanConfiguration            = "DeleteCodeSecurityScanConfiguration"
	opGetCodeSecurityScanConfiguration               = "GetCodeSecurityScanConfiguration"
	opUpdateCodeSecurityScanConfiguration            = "UpdateCodeSecurityScanConfiguration"
	opListCodeSecurityScanConfigurations             = "ListCodeSecurityScanConfigurations"
	opBatchAssociateCodeSecurityScanConfiguration    = "BatchAssociateCodeSecurityScanConfiguration"
	opBatchDisassociateCodeSecurityScanConfiguration = "BatchDisassociateCodeSecurityScanConfiguration"
	opListCodeSecurityScanConfigurationAssociations  = "ListCodeSecurityScanConfigurationAssociations"
	opStartCodeSecurityScan                          = "StartCodeSecurityScan"
	opGetCodeSecurityScan                            = "GetCodeSecurityScan"

	pathCodeSecurityIntegrationCreate = "/codesecurity/integration/create"
	pathCodeSecurityIntegrationDelete = "/codesecurity/integration/delete"
	pathCodeSecurityIntegrationGet    = "/codesecurity/integration/get"
	pathCodeSecurityIntegrationUpdate = "/codesecurity/integration/update"
	pathCodeSecurityIntegrationList   = "/codesecurity/integration/list"

	pathCodeSecurityScanConfigCreate        = "/codesecurity/scan-configuration/create"
	pathCodeSecurityScanConfigDelete        = "/codesecurity/scan-configuration/delete"
	pathCodeSecurityScanConfigGet           = "/codesecurity/scan-configuration/get"
	pathCodeSecurityScanConfigUpdate        = "/codesecurity/scan-configuration/update"
	pathCodeSecurityScanConfigList          = "/codesecurity/scan-configuration/list"
	pathCodeSecurityScanConfigBatchAssoc    = "/codesecurity/scan-configuration/batch/associate"
	pathCodeSecurityScanConfigBatchDisassoc = "/codesecurity/scan-configuration/batch/disassociate"
	pathCodeSecurityScanConfigAssocList     = "/codesecurity/scan-configuration/associations/list"
	pathCodeSecurityScanStart               = "/codesecurity/scan/start"
	pathCodeSecurityScanGet                 = "/codesecurity/scan/get"

	keyIntegrationArn = "integrationArn"
)

func (h *Handler) handleCreateCodeSecurityIntegration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		Tags    map[string]string `json:"tags"`
		Details map[string]any    `json:"details"`
		Name    string            `json:"name"`
		Type    string            `json:"type"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	integ, createErr := h.Backend.CreateCodeSecurityIntegration(
		req.Name,
		req.Type,
		req.Tags,
		req.Details,
	)
	if createErr != nil {
		return h.mapError(c, createErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyIntegrationArn: integ.IntegrationArn,
		keyStatus:         integ.Status,
	})
}

func (h *Handler) handleDeleteCodeSecurityIntegration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		IntegrationArn string `json:"integrationArn"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if deleteErr := h.Backend.DeleteCodeSecurityIntegration(req.IntegrationArn); deleteErr != nil {
		return h.mapError(c, deleteErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetCodeSecurityIntegration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		IntegrationArn string `json:"integrationArn"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	integ, getErr := h.Backend.GetCodeSecurityIntegration(req.IntegrationArn)
	if getErr != nil {
		return h.mapError(c, getErr)
	}

	return c.JSON(http.StatusOK, codeSecurityIntegrationToWire(integ))
}

// codeSecurityIntegrationToWire renders a CodeSecurityIntegration in its
// Inspector2 wire shape. Get/ListCodeSecurityIntegrations both use
// createdOn/lastUpdateOn (epoch-seconds DateTimeTimestamp members, see
// pkgs/awstime) rather than the domain struct's internal createdAt/updatedAt
// field names -- marshaling the struct directly would both use the wrong
// keys and emit RFC3339 strings, either of which leaves the real fields
// unpopulated on the client.
func codeSecurityIntegrationToWire(integ *CodeSecurityIntegration) map[string]any {
	return map[string]any{
		keyIntegrationArn: integ.IntegrationArn,
		keyName:           integ.Name,
		keyStatus:         integ.Status,
		"createdOn":       awstime.Epoch(integ.CreatedAt),
		"lastUpdateOn":    awstime.Epoch(integ.UpdatedAt),
	}
}

func (h *Handler) handleUpdateCodeSecurityIntegration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		Details        map[string]any `json:"details"`
		IntegrationArn string         `json:"integrationArn"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	integ, updateErr := h.Backend.UpdateCodeSecurityIntegration(req.IntegrationArn, req.Details)
	if updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyIntegrationArn: integ.IntegrationArn,
		keyStatus:         integ.Status,
	})
}

func (h *Handler) handleListCodeSecurityIntegrations(c *echo.Context) error {
	integrations, err := h.Backend.ListCodeSecurityIntegrations()
	if err != nil {
		return h.mapError(c, err)
	}

	wire := make([]map[string]any, 0, len(integrations))
	for _, integ := range integrations {
		wire = append(wire, codeSecurityIntegrationToWire(integ))
	}

	return c.JSON(http.StatusOK, map[string]any{"integrations": wire})
}

func (h *Handler) handleCreateCodeSecurityScanConfiguration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	// Real CreateCodeSecurityScanConfigurationInput nests ruleSetCategories,
	// periodicScanConfiguration, and continuousIntegrationScanConfiguration
	// under a required top-level "configuration" object (confirmed via
	// serializers.go); level/name/scopeSettings/tags sit alongside it, not
	// inside it. Decoding those three fields at the top level (as an earlier
	// revision did) silently drops them on every real request.
	var req struct {
		ScopeSettings map[string]any    `json:"scopeSettings"`
		Tags          map[string]string `json:"tags"`
		Name          string            `json:"name"`
		Level         string            `json:"level"`
		Configuration struct {
			ContinuousIntegrationScanConfiguration map[string]any `json:"continuousIntegrationScanConfiguration"`
			PeriodicScanConfiguration              map[string]any `json:"periodicScanConfiguration"`
			RuleSetCategories                      []string       `json:"ruleSetCategories"`
		} `json:"configuration"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	cfg, createErr := h.Backend.CreateCodeSecurityScanConfiguration(
		req.Name, req.Level, req.Configuration.RuleSetCategories,
		req.Configuration.ContinuousIntegrationScanConfiguration,
		req.Configuration.PeriodicScanConfiguration, req.ScopeSettings, req.Tags,
	)
	if createErr != nil {
		return h.mapError(c, createErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyScanConfigurationArn: cfg.Arn})
}

func (h *Handler) handleDeleteCodeSecurityScanConfiguration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ScanConfigurationArn string `json:"scanConfigurationArn"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if deleteErr := h.Backend.DeleteCodeSecurityScanConfiguration(req.ScanConfigurationArn); deleteErr != nil {
		return h.mapError(c, deleteErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetCodeSecurityScanConfiguration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ScanConfigurationArn string `json:"scanConfigurationArn"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	cfg, getErr := h.Backend.GetCodeSecurityScanConfiguration(req.ScanConfigurationArn)
	if getErr != nil {
		return h.mapError(c, getErr)
	}

	return c.JSON(http.StatusOK, codeSecurityScanConfigToWire(cfg))
}

// codeSecurityScanConfigToWire renders a CodeSecurityScanConfiguration in its
// Inspector2 wire shape. GetCodeSecurityScanConfiguration's real
// createdAt/lastUpdatedAt members are epoch-seconds DateTimeTimestamps (see
// pkgs/awstime); the domain struct's own "updatedAt" JSON tag additionally
// disagrees with the real "lastUpdatedAt" key name, so both must be
// converted here rather than marshaling the struct directly. ruleSetCategories/
// periodicScanConfiguration/continuousIntegrationScanConfiguration nest under
// a "configuration" object on the real wire (see
// deserializers.go's awsRestjson1_deserializeOpDocumentGetCodeSecurityScanConfigurationOutput);
// level/scopeSettings/name/tags/createdAt/lastUpdatedAt/scanConfigurationArn
// are siblings of "configuration", not nested inside it. There is no "status"
// member on the real GetCodeSecurityScanConfigurationOutput shape.
func codeSecurityScanConfigToWire(cfg *CodeSecurityScanConfiguration) map[string]any {
	configuration := map[string]any{
		"ruleSetCategories": cfg.RuleSetCategories,
	}

	if cfg.PeriodicScanConfig != nil {
		configuration["periodicScanConfiguration"] = cfg.PeriodicScanConfig
	}

	if cfg.ContinuousIntegrationScanConfig != nil {
		configuration["continuousIntegrationScanConfiguration"] = cfg.ContinuousIntegrationScanConfig
	}

	entry := map[string]any{
		keyScanConfigurationArn: cfg.Arn,
		keyName:                 cfg.Name,
		keyLevel:                cfg.Level,
		"configuration":         configuration,
		keyCreatedAt:            awstime.Epoch(cfg.CreatedAt),
		"lastUpdatedAt":         awstime.Epoch(cfg.UpdatedAt),
	}

	if cfg.ScopeSettings != nil {
		entry["scopeSettings"] = cfg.ScopeSettings
	}

	if len(cfg.Tags) > 0 {
		entry["tags"] = cfg.Tags
	}

	return entry
}

func (h *Handler) handleUpdateCodeSecurityScanConfiguration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	// Real UpdateCodeSecurityScanConfigurationInput only carries "configuration"
	// (nesting ruleSetCategories/periodicScanConfiguration/
	// continuousIntegrationScanConfiguration) and "scanConfigurationArn" --
	// there is no top-level scopeSettings on update, that member is
	// create-time-only.
	var req struct {
		ScanConfigurationArn string `json:"scanConfigurationArn"`
		Configuration        struct {
			ContinuousIntegrationScanConfiguration map[string]any `json:"continuousIntegrationScanConfiguration"`
			PeriodicScanConfiguration              map[string]any `json:"periodicScanConfiguration"`
			RuleSetCategories                      []string       `json:"ruleSetCategories"`
		} `json:"configuration"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	cfg, updateErr := h.Backend.UpdateCodeSecurityScanConfiguration(
		req.ScanConfigurationArn, req.Configuration.RuleSetCategories,
		req.Configuration.ContinuousIntegrationScanConfiguration,
		req.Configuration.PeriodicScanConfiguration,
	)
	if updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyScanConfigurationArn: cfg.Arn})
}

// codeSecurityScanConfigSummaryToWire renders a CodeSecurityScanConfiguration
// in ListCodeSecurityScanConfigurationsOutput's real
// CodeSecurityScanConfigurationSummary shape, which is flat -- unlike
// GetCodeSecurityScanConfigurationOutput, there is no nested "configuration"
// object: ruleSetCategories, continuousIntegrationScanSupportedEvents (a bare
// event-name list, not the nested continuousIntegrationScanConfiguration
// object), frequencyExpression, periodicScanFrequency, and scopeSettings all
// sit directly on the summary (confirmed via
// deserializers.go's awsRestjson1_deserializeDocumentCodeSecurityScanConfigurationSummary).
// There is no tags/createdAt/lastUpdatedAt/level/status on the summary shape.
func codeSecurityScanConfigSummaryToWire(cfg *CodeSecurityScanConfiguration, ownerAccountID string) map[string]any {
	entry := map[string]any{
		keyScanConfigurationArn: cfg.Arn,
		keyName:                 cfg.Name,
		"ownerAccountId":        ownerAccountID,
		"ruleSetCategories":     cfg.RuleSetCategories,
	}

	if cfg.ScopeSettings != nil {
		entry["scopeSettings"] = cfg.ScopeSettings
	}

	if events, ok := cfg.ContinuousIntegrationScanConfig["supportedEvents"]; ok {
		entry["continuousIntegrationScanSupportedEvents"] = events
	}

	if freq, ok := cfg.PeriodicScanConfig["frequency"]; ok {
		entry["periodicScanFrequency"] = freq
	}

	if expr, ok := cfg.PeriodicScanConfig["frequencyExpression"]; ok {
		entry["frequencyExpression"] = expr
	}

	return entry
}

func (h *Handler) handleListCodeSecurityScanConfigurations(c *echo.Context) error {
	cfgs, err := h.Backend.ListCodeSecurityScanConfigurations()
	if err != nil {
		return h.mapError(c, err)
	}

	ownerAccountID := h.Backend.AccountID()

	wire := make([]map[string]any, 0, len(cfgs))
	for _, cfg := range cfgs {
		wire = append(wire, codeSecurityScanConfigSummaryToWire(cfg, ownerAccountID))
	}

	// Real ListCodeSecurityScanConfigurationsOutput's list member wire key is
	// "configurations" (confirmed via deserializers.go), not "scanConfigurations"
	// -- that key belongs to the unrelated CIS/connector scan-configuration
	// list endpoints (see keyScanConfigurations's other call sites), reusing it
	// here would leave a real client's Configurations field unpopulated.
	return c.JSON(http.StatusOK, map[string]any{"configurations": wire})
}

func (h *Handler) handleBatchAssociateCodeSecurityScanConfiguration( //nolint:dupl // existing issue.
	c *echo.Context,
) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ScanConfigurationArn           string           `json:"scanConfigurationArn"`
		AssociateConfigurationRequests []map[string]any `json:"associateConfigurationRequests"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	resources := make([]string, 0, len(req.AssociateConfigurationRequests))
	for _, r := range req.AssociateConfigurationRequests {
		if res, ok := r["resource"].(string); ok {
			resources = append(resources, res)
		}
	}

	failed, assocErr := h.Backend.BatchAssociateCodeSecurityScanConfiguration(
		req.ScanConfigurationArn,
		resources,
	)
	if assocErr != nil {
		return h.mapError(c, assocErr)
	}

	return c.JSON(http.StatusOK, map[string]any{"failedAssociations": failed})
}

func (h *Handler) handleBatchDisassociateCodeSecurityScanConfiguration( //nolint:dupl // existing issue.
	c *echo.Context,
) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ScanConfigurationArn              string           `json:"scanConfigurationArn"`
		DisassociateConfigurationRequests []map[string]any `json:"disassociateConfigurationRequests"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	resources := make([]string, 0, len(req.DisassociateConfigurationRequests))
	for _, r := range req.DisassociateConfigurationRequests {
		if res, ok := r["resource"].(string); ok {
			resources = append(resources, res)
		}
	}

	failed, disErr := h.Backend.BatchDisassociateCodeSecurityScanConfiguration(
		req.ScanConfigurationArn,
		resources,
	)
	if disErr != nil {
		return h.mapError(c, disErr)
	}

	return c.JSON(http.StatusOK, map[string]any{"failedDisassociations": failed})
}

func (h *Handler) handleListCodeSecurityScanConfigurationAssociations(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ScanConfigurationArn string `json:"scanConfigurationArn"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	assocs, listErr := h.Backend.ListCodeSecurityScanConfigurationAssociations(
		req.ScanConfigurationArn,
	)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	if assocs == nil {
		assocs = []*CodeSecurityScanConfigurationAssociation{}
	}

	return c.JSON(http.StatusOK, map[string]any{"associations": assocs})
}

func (h *Handler) handleStartCodeSecurityScan(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ResourceID string `json:"resourceId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	result, startErr := h.Backend.StartCodeSecurityScan(req.ResourceID)
	if startErr != nil {
		return h.mapError(c, startErr)
	}

	return c.JSON(http.StatusOK, result)
}

func (h *Handler) handleGetCodeSecurityScan(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ScanID string `json:"scanId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	scan, getErr := h.Backend.GetCodeSecurityScan(req.ScanID)
	if getErr != nil {
		return h.mapError(c, getErr)
	}

	return c.JSON(http.StatusOK, scan)
}
