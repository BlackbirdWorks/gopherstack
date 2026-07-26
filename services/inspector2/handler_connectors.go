package inspector2

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const (
	opCreateConnector                  = "CreateConnector"
	opUpdateConnector                  = "UpdateConnector"
	opDeleteConnector                  = "DeleteConnector"
	opListConnectors                   = "ListConnectors"
	opListConnectorScanConfigurations  = "ListConnectorScanConfigurations"
	opUpdateConnectorScanConfiguration = "UpdateConnectorScanConfiguration"

	pathConnectorCreate           = "/connector/create"
	pathConnectorUpdate           = "/connector/update"
	pathConnectorDelete           = "/connector/delete"
	pathConnectorList             = "/connector/list"
	pathConnectorScanConfigList   = "/connectorscanconfigurations/list"
	pathConnectorScanConfigUpdate = "/connectorscanconfiguration/update"

	keyConnectorArn = "connectorArn"
)

// azureScopeSettingWire is the wire shape of one ScopeConfigurationInput
// entry (real field names scopeType/scopeValues), shared by
// CreateConnector/UpdateConnector's providerDetail.azure.scopeConfiguration.
type azureScopeSettingWire struct {
	ScopeType   string   `json:"scopeType"`
	ScopeValues []string `json:"scopeValues"`
}

func (w *azureScopeSettingWire) toDomain() *ConnectorScopeSetting {
	if w == nil {
		return nil
	}

	return &ConnectorScopeSetting{ScopeType: w.ScopeType, ScopeValues: w.ScopeValues}
}

// azureScopeConfigWire is the wire shape of AzureScopeConfigurationInput.
type azureScopeConfigWire struct {
	ContainerImageScanning *azureScopeSettingWire `json:"containerImageScanning"`
	ServerlessScanning     *azureScopeSettingWire `json:"serverlessScanning"`
	VMScanning             *azureScopeSettingWire `json:"vmScanning"`
}

func (w *azureScopeConfigWire) toDomain() *ConnectorScopeConfiguration {
	if w == nil {
		return nil
	}

	return &ConnectorScopeConfiguration{
		ContainerImageScanning: w.ContainerImageScanning.toDomain(),
		ServerlessScanning:     w.ServerlessScanning.toDomain(),
		VMScanning:             w.VMScanning.toDomain(),
	}
}

// azureProviderDetailCreateWire is the wire shape of AzureProviderDetailCreate.
type azureProviderDetailCreateWire struct {
	ScopeConfiguration    *azureScopeConfigWire `json:"scopeConfiguration"`
	AutoInstallVMScanner  *bool                 `json:"autoInstallVMScanner"`
	AwsConfigConnectorArn string                `json:"awsConfigConnectorArn"`
	AzureRegions          []string              `json:"azureRegions"`
}

// createConnectorRequest is the wire shape of CreateConnectorInput.
// ClientToken is accepted (idempotency-token semantics are a client-side
// retry aid, not backend state this in-memory emulator needs to dedupe on)
// but otherwise unused, matching how other idempotency tokens in this
// package are handled.
type createConnectorRequest struct {
	ProviderDetail struct {
		Azure *azureProviderDetailCreateWire `json:"azure"`
	} `json:"providerDetail"`
	Tags        map[string]string `json:"tags"`
	ClientToken string            `json:"clientToken"`
	Description string            `json:"description"`
	Name        string            `json:"name"`
	Provider    string            `json:"provider"`
}

// handleCreateConnector implements CreateConnector. Real CreateConnectorOutput
// has exactly one member (connectorArn) -- see connectors.go's CreateConnector
// doc comment for why this backend returns only that, matching the real
// asynchronous contract rather than inventing a full Connector echo.
func (h *Handler) handleCreateConnector(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse(errValidation, "invalid body"))
	}

	var req createConnectorRequest
	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse(errValidation, "invalid JSON"))
	}

	var (
		awsConfigConnectorArn string
		azureRegions          []string
		autoInstallVMScanner  *bool
		scopeConfig           *ConnectorScopeConfiguration
	)

	if az := req.ProviderDetail.Azure; az != nil {
		awsConfigConnectorArn = az.AwsConfigConnectorArn
		azureRegions = az.AzureRegions
		autoInstallVMScanner = az.AutoInstallVMScanner
		scopeConfig = az.ScopeConfiguration.toDomain()
	}

	connector, createErr := h.Backend.CreateConnector(
		req.Name, req.Description, req.Provider, req.Tags,
		awsConfigConnectorArn, azureRegions, autoInstallVMScanner, scopeConfig,
	)
	if createErr != nil {
		return h.mapError(c, createErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyConnectorArn: connector.ConnectorArn})
}

// azureProviderDetailUpdateWire is the wire shape of AzureProviderDetailUpdate.
type azureProviderDetailUpdateWire struct {
	ScopeConfiguration   *azureScopeConfigWire `json:"scopeConfiguration"`
	AutoInstallVMScanner *bool                 `json:"autoInstallVMScanner"`
	AzureRegions         []string              `json:"azureRegions"`
}

// updateConnectorRequest is the wire shape of UpdateConnectorInput.
type updateConnectorRequest struct {
	ProviderDetail struct {
		Azure *azureProviderDetailUpdateWire `json:"azure"`
	} `json:"providerDetail"`
	Description  *string `json:"description"`
	ConnectorArn string  `json:"connectorArn"`
}

// handleUpdateConnector implements UpdateConnector. Real UpdateConnectorOutput
// has exactly one (optional) member (connectorArn).
func (h *Handler) handleUpdateConnector(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse(errValidation, "invalid body"))
	}

	var req updateConnectorRequest
	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse(errValidation, "invalid JSON"))
	}

	var (
		azureRegions         []string
		autoInstallVMScanner *bool
		scopeConfig          *ConnectorScopeConfiguration
	)

	if az := req.ProviderDetail.Azure; az != nil {
		azureRegions = az.AzureRegions
		autoInstallVMScanner = az.AutoInstallVMScanner
		scopeConfig = az.ScopeConfiguration.toDomain()
	}

	connector, updateErr := h.Backend.UpdateConnector(
		req.ConnectorArn, req.Description, azureRegions, autoInstallVMScanner, scopeConfig,
	)
	if updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyConnectorArn: connector.ConnectorArn})
}

// handleDeleteConnector implements DeleteConnector. Real DeleteConnectorOutput
// has no members at all.
func (h *Handler) handleDeleteConnector(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse(errValidation, "invalid body"))
	}

	var req struct {
		ConnectorArn string `json:"connectorArn"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse(errValidation, "invalid JSON"))
	}

	if deleteErr := h.Backend.DeleteConnector(req.ConnectorArn); deleteErr != nil {
		return h.mapError(c, deleteErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// connectorScopeSettingToWire renders one ConnectorScopeSetting in its real
// wire shape (ScopeConfiguration), omitting the key entirely when nil,
// matching a real API response where the scanning type was never configured.
func connectorScopeSettingToWire(s *ConnectorScopeSetting) map[string]any {
	if s == nil {
		return nil
	}

	w := map[string]any{"scopeType": s.ScopeType}

	if len(s.ScopeValues) > 0 {
		w["scopeValues"] = s.ScopeValues
	}

	if s.State != "" {
		w["state"] = s.State
	}

	if s.StateReason != "" {
		w["stateReason"] = s.StateReason
	}

	return w
}

// connectorScopeConfigToWire renders a ConnectorScopeConfiguration in its
// real AzureScopeConfiguration wire shape.
func connectorScopeConfigToWire(cfg *ConnectorScopeConfiguration) map[string]any {
	if cfg == nil {
		return nil
	}

	w := map[string]any{}

	if v := connectorScopeSettingToWire(cfg.ContainerImageScanning); v != nil {
		w["containerImageScanning"] = v
	}

	if v := connectorScopeSettingToWire(cfg.ServerlessScanning); v != nil {
		w["serverlessScanning"] = v
	}

	if v := connectorScopeSettingToWire(cfg.VMScanning); v != nil {
		w["vmScanning"] = v
	}

	return w
}

// connectorHealthToWire renders a ConnectorHealth in its real wire shape.
// LastCheckedAt is a real "date-time" (RFC 3339 string) timestamp shape, not
// the unixTimestamp epoch-seconds shape pkgs/awstime.Epoch targets -- Connector
// deserializers.go parses createdAt/updatedAt/lastCheckedAt via
// smithytime.ParseDateTime, which rejects a JSON number.
func connectorHealthToWire(h *ConnectorHealth) map[string]any {
	if h == nil {
		return nil
	}

	w := map[string]any{"connectorStatus": h.ConnectorStatus}

	if !h.LastCheckedAt.IsZero() {
		w["lastCheckedAt"] = h.LastCheckedAt.Format(time.RFC3339)
	}

	if h.Message != "" {
		w["message"] = h.Message
	}

	return w
}

// connectorToWire renders a Connector in its real wire shape (field-diffed
// against deserializers.go's awsRestjson1_deserializeDocumentConnector).
// createdAt/updatedAt are date-time strings, not epoch numbers -- see
// connectorHealthToWire's doc comment.
func connectorToWire(connector *Connector) map[string]any {
	w := map[string]any{
		keyConnectorArn:        connector.ConnectorArn,
		keyName:                connector.Name,
		"provider":             connector.Provider,
		keyCreatedAt:           connector.CreatedAt.Format(time.RFC3339),
		"updatedAt":            connector.UpdatedAt.Format(time.RFC3339),
		"enablementStatus":     connector.EnablementStatus,
		"autoInstallVMScanner": connector.AutoInstallVMScanner,
	}

	if connector.Description != "" {
		w["description"] = connector.Description
	}

	if connector.AwsConfigConnectorArn != "" {
		w["awsConfigConnectorArn"] = connector.AwsConfigConnectorArn
	}

	if len(connector.AzureRegions) > 0 {
		w["azureRegions"] = connector.AzureRegions
	}

	if connector.EnablementStatusReason != "" {
		w["enablementStatusReason"] = connector.EnablementStatusReason
	}

	if health := connectorHealthToWire(connector.Health); health != nil {
		w["health"] = health
	}

	if scope := connectorScopeConfigToWire(connector.ScopeConfiguration); len(scope) > 0 {
		w["scopeConfiguration"] = scope
	}

	if len(connector.Tags) > 0 {
		w["tags"] = connector.Tags
	}

	return w
}

// connectorStringFilterWire is the wire shape shared by StringFilter/
// ConnectorArnFilter/AwsConfigConnectorArnFilter/ProviderFilter --
// comparison is accepted but not decoded: every one of those real filter
// types' Comparison enum has exactly one value (EQUALS), so there is no
// other comparison semantic for this backend to emulate.
type connectorStringFilterWire struct {
	Value string `json:"value"`
}

// connectorFilterCriteriaWire is the wire shape of ConnectorFilterCriteria,
// limited to the three facets this backend evaluates (see ListConnectors'
// doc comment in connectors.go for the ones it doesn't).
type connectorFilterCriteriaWire struct {
	Provider               []connectorStringFilterWire `json:"provider"`
	ConnectorArns          []connectorStringFilterWire `json:"connectorArns"`
	AwsConfigConnectorArns []connectorStringFilterWire `json:"awsConfigConnectorArns"`
}

// listConnectorsRequest is the wire shape of ListConnectorsInput.
type listConnectorsRequest struct {
	FilterCriteria *connectorFilterCriteriaWire `json:"filterCriteria"`
	NextToken      string                       `json:"nextToken"`
	MaxResults     int32                        `json:"maxResults"`
}

// connectorFilterValues extracts the non-empty filter values from fs.
func connectorFilterValues(fs []connectorStringFilterWire) []string {
	out := make([]string, 0, len(fs))

	for _, f := range fs {
		if f.Value != "" {
			out = append(out, f.Value)
		}
	}

	return out
}

// handleListConnectors implements ListConnectors.
func (h *Handler) handleListConnectors(c *echo.Context) error {
	req, ok := decodeListConnectorsRequest(c)
	if !ok {
		return nil
	}

	var providers, connectorArns, awsConfigConnectorArns []string

	if req.FilterCriteria != nil {
		providers = connectorFilterValues(req.FilterCriteria.Provider)
		connectorArns = connectorFilterValues(req.FilterCriteria.ConnectorArns)
		awsConfigConnectorArns = connectorFilterValues(req.FilterCriteria.AwsConfigConnectorArns)
	}

	connectors, next, listErr := h.Backend.ListConnectors(
		providers, connectorArns, awsConfigConnectorArns, req.MaxResults, req.NextToken,
	)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	items := make([]map[string]any, 0, len(connectors))
	for _, connector := range connectors {
		items = append(items, connectorToWire(connector))
	}

	resp := map[string]any{"items": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

// decodeListConnectorsRequest reads and decodes a ListConnectorsInput body.
// On a malformed body it writes the error response itself and returns ok=false.
func decodeListConnectorsRequest(c *echo.Context) (listConnectorsRequest, bool) {
	var req listConnectorsRequest

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		_ = c.JSON(http.StatusBadRequest, errorResponse(errValidation, "invalid body"))

		return req, false
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			_ = c.JSON(http.StatusBadRequest, errorResponse(errValidation, "invalid JSON"))

			return req, false
		}
	}

	return req, true
}

// connectorContainerImageScanConfigToWire renders a
// ConnectorContainerImageScanConfig in its real wire shape.
func connectorContainerImageScanConfigToWire(cfg *ConnectorContainerImageScanConfig) map[string]any {
	if cfg == nil {
		return nil
	}

	w := map[string]any{}

	if cfg.PullDuration != "" {
		w["pullDuration"] = cfg.PullDuration
	}

	if cfg.PushDuration != "" {
		w["pushDuration"] = cfg.PushDuration
	}

	return w
}

// connectorScanConfigItemToWire renders a ConnectorScanConfigurationItem in
// its real wire shape.
func connectorScanConfigItemToWire(item *ConnectorScanConfigurationItem) map[string]any {
	connArns := item.ConnectorArns
	if connArns == nil {
		connArns = []string{}
	}

	w := map[string]any{
		"awsConfigConnectorArn": item.AwsConfigConnectorArn,
		"connectorArns":         connArns,
	}

	if item.ScanConfiguration != nil {
		sc := map[string]any{}
		if v := connectorContainerImageScanConfigToWire(item.ScanConfiguration.ContainerImageScanning); len(v) > 0 {
			sc["containerImageScanning"] = v
		}

		w["scanConfiguration"] = sc
	}

	return w
}

// listConnectorScanConfigurationsRequest is the wire shape of
// ListConnectorScanConfigurationsInput.
type listConnectorScanConfigurationsRequest struct {
	NextToken              string   `json:"nextToken"`
	AwsConfigConnectorArns []string `json:"awsConfigConnectorArns"`
	MaxResults             int32    `json:"maxResults"`
}

// handleListConnectorScanConfigurations implements
// ListConnectorScanConfigurations.
func (h *Handler) handleListConnectorScanConfigurations(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse(errValidation, "invalid body"))
	}

	var req listConnectorScanConfigurationsRequest

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errorResponse(errValidation, "invalid JSON"))
		}
	}

	items, next, listErr := h.Backend.ListConnectorScanConfigurations(
		req.AwsConfigConnectorArns, req.MaxResults, req.NextToken,
	)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	wire := make([]map[string]any, 0, len(items))
	for _, item := range items {
		wire = append(wire, connectorScanConfigItemToWire(item))
	}

	resp := map[string]any{keyScanConfigurations: wire}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

// connectorContainerImageScanConfigWire is the wire shape of
// ConnectorContainerImageScanConfiguration.
type connectorContainerImageScanConfigWire struct {
	PullDuration string `json:"pullDuration"`
	PushDuration string `json:"pushDuration"`
}

// connectorScanConfigWire is the wire shape of ConnectorScanConfiguration.
type connectorScanConfigWire struct {
	ContainerImageScanning *connectorContainerImageScanConfigWire `json:"containerImageScanning"`
}

// updateConnectorScanConfigurationRequest is the wire shape of
// UpdateConnectorScanConfigurationInput.
type updateConnectorScanConfigurationRequest struct {
	ScanConfiguration     *connectorScanConfigWire `json:"scanConfiguration"`
	AwsConfigConnectorArn string                   `json:"awsConfigConnectorArn"`
}

// handleUpdateConnectorScanConfiguration implements
// UpdateConnectorScanConfiguration. Real
// UpdateConnectorScanConfigurationOutput has no members at all.
func (h *Handler) handleUpdateConnectorScanConfiguration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse(errValidation, "invalid body"))
	}

	var req updateConnectorScanConfigurationRequest
	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse(errValidation, "invalid JSON"))
	}

	var cfg *ConnectorContainerImageScanConfig

	if req.ScanConfiguration != nil && req.ScanConfiguration.ContainerImageScanning != nil {
		cis := req.ScanConfiguration.ContainerImageScanning
		cfg = &ConnectorContainerImageScanConfig{
			PullDuration: cis.PullDuration,
			PushDuration: cis.PushDuration,
		}
	}

	if updateErr := h.Backend.UpdateConnectorScanConfiguration(req.AwsConfigConnectorArn, cfg); updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}
