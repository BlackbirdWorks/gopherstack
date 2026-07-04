package iotwireless

// handler_ops.go — real handler implementations for IoT Wireless operations.

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

// ============================================================
// Group 1 — Multicast Group operations
// ============================================================

func (h *Handler) createMulticastGroup(c *echo.Context) error {
	var req struct {
		Tags        map[string]string `json:"Tags"`
		Name        string            `json:"Name"`
		Description string            `json:"Description"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	mg, err := h.Backend.CreateMulticastGroup(
		h.AccountID,
		h.DefaultRegion,
		req.Name,
		req.Description,
		req.Tags,
	)
	if err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusCreated, createMulticastGroupResponse{
		Arn: mg.ARN,
		ID:  mg.ID,
	})
}

func (h *Handler) getMulticastGroup(c *echo.Context, id string) error {
	mg, err := h.Backend.GetMulticastGroup(h.AccountID, h.DefaultRegion, id)
	if err != nil {
		return handleError(c, err)
	}

	return writeJSON(c, http.StatusOK, getMulticastGroupResponse{
		Arn:    mg.ARN,
		ID:     mg.ID,
		Name:   mg.Name,
		Status: mg.Status,
	})
}

func (h *Handler) listMulticastGroups(c *echo.Context) error {
	groups := h.Backend.ListMulticastGroups(h.AccountID, h.DefaultRegion)
	entries := make([]multicastGroupEntry, 0, len(groups))

	for _, mg := range groups {
		entries = append(entries, multicastGroupEntry{
			Arn:  mg.ARN,
			ID:   mg.ID,
			Name: mg.Name,
		})
	}

	return writeJSON(c, http.StatusOK, listMulticastGroupsResponse{
		MulticastGroupList: entries,
	})
}

func (h *Handler) deleteMulticastGroup(c *echo.Context, id string) error {
	if err := h.Backend.DeleteMulticastGroup(h.AccountID, h.DefaultRegion, id); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) updateMulticastGroup(c *echo.Context, id string) error {
	var req struct {
		Name        string `json:"Name"`
		Description string `json:"Description"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	if err := h.Backend.UpdateMulticastGroup(h.AccountID, h.DefaultRegion, id, req.Name, req.Description); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) listMulticastGroupsByFuotaTask(c *echo.Context, fuotaTaskID string) error {
	groups := h.Backend.ListMulticastGroupsByFuotaTask(h.AccountID, h.DefaultRegion, fuotaTaskID)
	entries := make([]multicastGroupEntry, 0, len(groups))

	for _, mg := range groups {
		entries = append(entries, multicastGroupEntry{
			Arn:  mg.ARN,
			ID:   mg.ID,
			Name: mg.Name,
		})
	}

	return writeJSON(c, http.StatusOK, listMulticastGroupsByFuotaTaskResponse{
		MulticastGroupList: entries,
	})
}

func (h *Handler) disassociateMulticastGroupFromFuotaTask(
	c *echo.Context,
	fuotaTaskID, _ string,
) error {
	if err := h.Backend.DisassociateMulticastGroupFromFuotaTask(fuotaTaskID); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) disassociateWirelessDeviceFromMulticastGroup(
	c *echo.Context,
	multicastGroupID, _ string,
) error {
	if err := h.Backend.DisassociateWirelessDeviceFromMulticastGroup(multicastGroupID); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) getMulticastGroupSession(c *echo.Context, id string) error {
	startedAt, err := h.Backend.GetMulticastGroupSession(id)
	if err != nil {
		return handleError(c, err)
	}

	return writeJSON(c, http.StatusOK, getMulticastGroupSessionResponse{
		LoRaWAN: map[string]any{
			"SessionStartTime": startedAt.Format(time.RFC3339),
		},
	})
}

func (h *Handler) startMulticastGroupSession(c *echo.Context, id string) error {
	if err := h.Backend.StartMulticastGroupSession(id); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

// ============================================================
// Group 2 — Network Analyzer operations
// ============================================================

func (h *Handler) createNetworkAnalyzerConfiguration(c *echo.Context) error {
	var req struct {
		Tags             map[string]string `json:"Tags"`
		Description      string            `json:"Description"`
		Name             string            `json:"Name"`
		WirelessDevices  []string          `json:"WirelessDevices"`
		WirelessGateways []string          `json:"WirelessGateways"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	nc, err := h.Backend.CreateNetworkAnalyzerConfig(
		h.AccountID, h.DefaultRegion,
		req.Name, req.Description,
		req.WirelessDevices, req.WirelessGateways,
		req.Tags,
	)
	if err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusCreated, createNetworkAnalyzerConfigurationResponse{
		Arn:  nc.ARN,
		Name: nc.Name,
	})
}

func (h *Handler) getNetworkAnalyzerConfiguration(c *echo.Context, name string) error {
	nc, err := h.Backend.GetNetworkAnalyzerConfig(h.AccountID, h.DefaultRegion, name)
	if err != nil {
		return handleError(c, err)
	}

	return writeJSON(c, http.StatusOK, getNetworkAnalyzerConfigurationResponse{
		Arn:              nc.ARN,
		Name:             nc.Name,
		Description:      nc.Description,
		WirelessDevices:  nc.WirelessDevices,
		WirelessGateways: nc.WirelessGateways,
	})
}

func (h *Handler) listNetworkAnalyzerConfigurations(c *echo.Context) error {
	configs := h.Backend.ListNetworkAnalyzerConfigs(h.AccountID, h.DefaultRegion)

	entries := make([]struct {
		Arn  string `json:"Arn"`
		Name string `json:"Name"`
	}, 0, len(configs))

	for _, nc := range configs {
		entries = append(entries, struct {
			Arn  string `json:"Arn"`
			Name string `json:"Name"`
		}{Arn: nc.ARN, Name: nc.Name})
	}

	return writeJSON(c, http.StatusOK, listNetworkAnalyzerConfigurationsResponse{
		NetworkAnalyzerConfigurationList: entries,
	})
}

func (h *Handler) deleteNetworkAnalyzerConfiguration(c *echo.Context, name string) error {
	if err := h.Backend.DeleteNetworkAnalyzerConfig(h.AccountID, h.DefaultRegion, name); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) updateNetworkAnalyzerConfiguration(c *echo.Context, name string) error {
	var req struct {
		Description      string   `json:"Description"`
		WirelessDevices  []string `json:"WirelessDevices"`
		WirelessGateways []string `json:"WirelessGateways"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	if err := h.Backend.UpdateNetworkAnalyzerConfig(
		h.AccountID, h.DefaultRegion, name,
		req.Description, req.WirelessDevices, req.WirelessGateways,
	); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

// ============================================================
// Group 3 — FuotaTask operations
// ============================================================

func (h *Handler) updateFuotaTask(c *echo.Context, id string) error {
	var req struct {
		Name        string `json:"Name"`
		Description string `json:"Description"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	if err := h.Backend.UpdateFuotaTask(h.AccountID, h.DefaultRegion, id, req.Name, req.Description); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) startFuotaTask(c *echo.Context, id string) error {
	if err := h.Backend.StartFuotaTask(h.AccountID, h.DefaultRegion, id); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) disassociateWirelessDeviceFromFuotaTask(
	c *echo.Context,
	fuotaTaskID, _ string,
) error {
	if err := h.Backend.DisassociateWirelessDeviceFromFuotaTask(fuotaTaskID); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

// ============================================================
// Group 4 — WirelessGateway misc operations
// ============================================================

func (h *Handler) updateWirelessGateway(c *echo.Context, id string) error {
	var req struct {
		Name        string `json:"Name"`
		Description string `json:"Description"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	if err := h.Backend.UpdateWirelessGateway(h.AccountID, h.DefaultRegion, id, req.Name, req.Description); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) disassociateWirelessGatewayFromCertificate(c *echo.Context, id string) error {
	if err := h.Backend.DisassociateWirelessGatewayFromCertificate(h.AccountID, h.DefaultRegion, id); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) disassociateWirelessGatewayFromThing(c *echo.Context, id string) error {
	if err := h.Backend.DisassociateWirelessGatewayFromThing(h.AccountID, h.DefaultRegion, id); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) getWirelessGatewayCertificate(c *echo.Context, id string) error {
	certID, err := h.Backend.GetWirelessGatewayCertificate(h.AccountID, h.DefaultRegion, id)
	if err != nil {
		return handleError(c, err)
	}

	return writeJSON(c, http.StatusOK, getWirelessGatewayCertificateResponse{
		IotCertificateID: certID,
	})
}

func (h *Handler) getWirelessGatewayStatistics(c *echo.Context, id string) error {
	return writeJSON(c, http.StatusOK, getWirelessGatewayStatisticsResponse{
		WirelessGatewayID: id,
		ConnectionStatus:  "Connected",
	})
}

func (h *Handler) getWirelessGatewayFirmwareInformation(c *echo.Context, _ string) error {
	return writeJSON(c, http.StatusOK, getWirelessGatewayFirmwareInformationResponse{})
}

// ============================================================
// Group 5 — WirelessDevice misc operations
// ============================================================

func (h *Handler) updateWirelessDevice(c *echo.Context, id string) error {
	var req struct {
		Name            string `json:"Name"`
		Description     string `json:"Description"`
		DestinationName string `json:"DestinationName"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	if err := h.Backend.UpdateWirelessDevice(
		h.AccountID, h.DefaultRegion, id,
		req.Name, req.Description, req.DestinationName,
	); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) deregisterWirelessDevice(c *echo.Context, id string) error {
	if err := h.Backend.DeleteWirelessDevice(h.AccountID, h.DefaultRegion, id); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) disassociateWirelessDeviceFromThing(c *echo.Context, id string) error {
	if err := h.Backend.DisassociateWirelessDeviceFromThing(h.AccountID, h.DefaultRegion, id); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) sendDataToWirelessDevice(c *echo.Context, wirelessDeviceID string) error {
	var req struct {
		PayloadData string `json:"PayloadData"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	messageID := uuid.NewString()

	// Real cross-op state: queue the downlink message so a subsequent
	// ListQueuedMessages reflects it, instead of silently discarding it.
	h.Backend.EnqueueMessage(wirelessDeviceID, QueuedMessage{
		MessageID:     messageID,
		PayloadBase64: req.PayloadData,
		ReceivedAt:    time.Now(),
	})

	return writeJSON(c, http.StatusCreated, sendDataToWirelessDeviceResponse{
		MessageID: messageID,
	})
}

func (h *Handler) getWirelessDeviceStatistics(c *echo.Context, id string) error {
	return writeJSON(c, http.StatusOK, getWirelessDeviceStatisticsResponse{
		WirelessDeviceID: id,
	})
}

func (h *Handler) testWirelessDevice(c *echo.Context, _ string) error {
	return writeJSON(c, http.StatusOK, testWirelessDeviceResponse{
		Result: "PASS",
	})
}

// ============================================================
// Group 6 — Destination update
// ============================================================

func (h *Handler) updateDestination(c *echo.Context, name string) error {
	var req struct {
		Expression     string `json:"Expression"`
		ExpressionType string `json:"ExpressionType"`
		RoleArn        string `json:"RoleArn"`
		Description    string `json:"Description"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	if err := h.Backend.UpdateDestination(
		h.AccountID, h.DefaultRegion, name,
		req.Expression, req.ExpressionType, req.RoleArn, req.Description,
	); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

// ============================================================
// Group 7 — Log level operations
// ============================================================

func (h *Handler) getLogLevelsByResourceTypes(c *echo.Context) error {
	level := h.Backend.GetLogLevelsByResourceTypes()

	return writeJSON(c, http.StatusOK, getLogLevelsByResourceTypesResponse{
		DefaultLogLevel:           level,
		WirelessGatewayLogOptions: []struct{}{},
		WirelessDeviceLogOptions:  []struct{}{},
	})
}

func (h *Handler) updateLogLevelsByResourceTypes(c *echo.Context) error {
	var req struct {
		DefaultLogLevel string `json:"DefaultLogLevel"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	if err := h.Backend.UpdateLogLevelsByResourceTypes(req.DefaultLogLevel); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) resetAllResourceLogLevels(c *echo.Context) error {
	if err := h.Backend.ResetAllResourceLogLevels(); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) getResourceLogLevel(c *echo.Context, id string) error {
	level := h.Backend.GetResourceLogLevel(id)

	return writeJSON(c, http.StatusOK, getResourceLogLevelResponse{
		LogLevel:   level,
		ResourceID: id,
	})
}

func (h *Handler) putResourceLogLevel(c *echo.Context, id string) error {
	var req struct {
		LogLevel     string `json:"LogLevel"`
		ResourceType string `json:"ResourceType"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	if err := h.Backend.PutResourceLogLevel(id, req.LogLevel); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) resetResourceLogLevel(c *echo.Context, id string) error {
	if err := h.Backend.ResetResourceLogLevel(id); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

// ============================================================
// Group 8 — Event configuration operations (real backend state)
// ============================================================

// eventConfigDocFromBody unmarshals an EventConfigDoc from a raw JSON request
// body, ignoring malformed input (matching the package's existing convention
// of treating unparsed bodies as empty).
func eventConfigDocFromBody(body []byte) *EventConfigDoc {
	var doc EventConfigDoc

	_ = json.Unmarshal(body, &doc)

	return &doc
}

func (h *Handler) getEventConfigurationByResourceTypes(c *echo.Context) error {
	doc := h.Backend.GetEventConfigurationByResourceTypes()

	return writeJSON(c, http.StatusOK, eventConfigDocResponseFrom(doc))
}

func (h *Handler) updateEventConfigurationByResourceTypes(c *echo.Context) error {
	body := readStubBody(c)
	h.Backend.UpdateEventConfigurationByResourceTypes(eventConfigDocFromBody(body))

	return stubNoContent(c)
}

func (h *Handler) listEventConfigurations(c *echo.Context) error {
	resourceType := c.QueryParam("resourceType")
	entries := h.Backend.ListEventConfigurations(resourceType)

	items := make([]eventConfigurationItemResponse, 0, len(entries))
	for _, e := range entries {
		items = append(items, eventConfigurationItemResponseFrom(e))
	}

	return writeJSON(c, http.StatusOK, listEventConfigurationsResponse{
		EventConfigurationsList: items,
	})
}

func (h *Handler) getResourceEventConfiguration(c *echo.Context, identifier string) error {
	entry, ok := h.Backend.GetResourceEventConfiguration(identifier)
	if !ok {
		return writeJSON(c, http.StatusOK, eventConfigDocResponse{})
	}

	return writeJSON(c, http.StatusOK, eventConfigDocResponseFrom(&entry.Config))
}

func (h *Handler) updateResourceEventConfiguration(c *echo.Context, identifier string) error {
	identifierType := c.QueryParam("identifierType")
	partnerType := c.QueryParam("partnerType")
	body := readStubBody(c)

	h.Backend.UpdateResourceEventConfiguration(identifier, identifierType, partnerType, eventConfigDocFromBody(body))

	return stubNoContent(c)
}

// ============================================================
// Group 9 — Partner account operations
// ============================================================

func (h *Handler) getPartnerAccount(c *echo.Context, partnerAccountID string) error {
	arn, err := h.Backend.GetPartnerAccount(partnerAccountID)
	if err != nil {
		// GetPartnerAccount reports link status rather than 404ing on an
		// unlinked account — matching AWS's use of it as a link-status check.
		return writeJSON(c, http.StatusOK, getPartnerAccountResponse{AccountLinked: false})
	}

	return writeJSON(c, http.StatusOK, getPartnerAccountResponse{
		AccountLinked: true,
		Sidewalk:      &sidewalkAccountInfo{AmazonID: partnerAccountID, Arn: arn},
	})
}

func (h *Handler) listPartnerAccounts(c *echo.Context) error {
	accounts := h.Backend.ListPartnerAccounts()

	sidewalk := make([]sidewalkAccountInfo, 0, len(accounts))
	for id, arn := range accounts {
		sidewalk = append(sidewalk, sidewalkAccountInfo{AmazonID: id, Arn: arn})
	}

	return writeJSON(c, http.StatusOK, listPartnerAccountsResponse{Sidewalk: sidewalk})
}

func (h *Handler) disassociateAwsAccountFromPartnerAccount(
	c *echo.Context,
	partnerAccountID string,
) error {
	// Ignore not-found; return 204 for idempotency.
	_ = h.Backend.DisassociateAwsAccountFromPartnerAccount(partnerAccountID)

	return stubNoContent(c)
}

func (h *Handler) updatePartnerAccount(c *echo.Context, partnerAccountID string) error {
	if _, err := h.Backend.GetPartnerAccount(partnerAccountID); err != nil {
		return handleError(c, err)
	}

	return stubNoContent(c)
}

// ============================================================
// Group 10 — Gateway task operations
// ============================================================

func (h *Handler) createWirelessGatewayTask(c *echo.Context, gatewayID string) error {
	var req struct {
		WirelessGatewayTaskDefinitionID string `json:"WirelessGatewayTaskDefinitionId"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	task, err := h.Backend.CreateWirelessGatewayTask(gatewayID, req.WirelessGatewayTaskDefinitionID)
	if err != nil {
		return handleError(c, err)
	}

	return writeJSON(c, http.StatusCreated, createWirelessGatewayTaskResponse{
		WirelessGatewayID:               task.WirelessGatewayID,
		WirelessGatewayTaskDefinitionID: task.TaskDefID,
		Status:                          task.Status,
	})
}

func (h *Handler) getWirelessGatewayTask(c *echo.Context, gatewayID string) error {
	task, err := h.Backend.GetWirelessGatewayTask(gatewayID)
	if err != nil {
		return handleError(c, err)
	}

	return writeJSON(c, http.StatusOK, getWirelessGatewayTaskResponse{
		WirelessGatewayID:               task.WirelessGatewayID,
		WirelessGatewayTaskDefinitionID: task.TaskDefID,
		Status:                          task.Status,
	})
}

func (h *Handler) deleteWirelessGatewayTask(c *echo.Context, gatewayID string) error {
	// Ignore not-found for idempotency.
	_ = h.Backend.DeleteWirelessGatewayTask(gatewayID)

	return stubNoContent(c)
}

func (h *Handler) createWirelessGatewayTaskDefinition(c *echo.Context) error {
	var req struct {
		Name            string `json:"Name"`
		AutoCreateTasks bool   `json:"AutoCreateTasks"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	def, err := h.Backend.CreateWirelessGatewayTaskDefinition(req.Name, req.AutoCreateTasks)
	if err != nil {
		return handleError(c, err)
	}

	return writeJSON(c, http.StatusCreated, createWirelessGatewayTaskDefinitionResponse{
		Arn: def.ARN,
		ID:  def.ID,
	})
}

func (h *Handler) getWirelessGatewayTaskDefinition(c *echo.Context, id string) error {
	def, err := h.Backend.GetWirelessGatewayTaskDefinition(id)
	if err != nil {
		return handleError(c, err)
	}

	return writeJSON(c, http.StatusOK, getWirelessGatewayTaskDefinitionResponse{
		Arn:             def.ARN,
		Name:            def.Name,
		AutoCreateTasks: def.AutoCreateTasks,
	})
}

func (h *Handler) listWirelessGatewayTaskDefinitions(c *echo.Context) error {
	defs := h.Backend.ListWirelessGatewayTaskDefinitions()

	type taskDefEntry struct {
		ID              string `json:"Id"`
		Arn             string `json:"Arn"`
		Name            string `json:"Name"`
		AutoCreateTasks bool   `json:"AutoCreateTasks"`
	}

	entries := make([]taskDefEntry, 0, len(defs))

	for _, def := range defs {
		entries = append(entries, taskDefEntry{
			ID:              def.ID,
			Arn:             def.ARN,
			Name:            def.Name,
			AutoCreateTasks: def.AutoCreateTasks,
		})
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"TaskDefinitions": entries,
		"NextToken":       "",
	})
}

func (h *Handler) deleteWirelessGatewayTaskDefinition(c *echo.Context, id string) error {
	// Ignore not-found; idempotent delete.
	_ = h.Backend.DeleteWirelessGatewayTaskDefinition(id)

	return stubNoContent(c)
}

// ============================================================
// Group 11 — Position operations
// ============================================================

// positionCoords extracts a []float64 from a stored position map's
// "Position" key (a []any of JSON numbers, since the map is populated by
// generic json.Unmarshal).
func positionCoords(pos map[string]any) []float64 {
	raw, ok := pos["Position"].([]any)
	if !ok {
		return nil
	}

	coords := make([]float64, 0, len(raw))

	for _, v := range raw {
		if f, isFloat := v.(float64); isFloat {
			coords = append(coords, f)
		}
	}

	return coords
}

func (h *Handler) getPosition(c *echo.Context, id string) error {
	pos := h.Backend.GetPosition(id)

	coords := positionCoords(pos)
	if coords == nil {
		// No position data has ever been submitted for this resource: return
		// the correct "no data available" shape (empty Position, no Accuracy).
		return writeJSON(c, http.StatusOK, getPositionResponse{Position: []float64{}})
	}

	// A value of 0.0 indicates that position data is available (see
	// GetPositionOutput.Accuracy doc); this is a manual override so no solver
	// metadata is reported.
	accuracy := 0.0

	return writeJSON(c, http.StatusOK, getPositionResponse{
		Position: coords,
		Accuracy: &accuracy,
	})
}

func (h *Handler) updatePosition(c *echo.Context, id string) error {
	var req map[string]any

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)
	_ = h.Backend.UpdatePosition(id, req)

	return stubNoContent(c)
}

func (h *Handler) getPositionConfiguration(c *echo.Context, id string) error {
	entry, ok := h.Backend.GetPositionConfiguration(id)
	if !ok {
		return writeJSON(c, http.StatusOK, getPositionConfigurationResponse{})
	}

	return writeJSON(c, http.StatusOK, getPositionConfigurationResponse{
		Destination: entry.Destination,
		Solvers:     entry.Solvers,
	})
}

func (h *Handler) putPositionConfiguration(c *echo.Context, id string) error {
	resourceType := c.QueryParam("resourceType")

	var req struct {
		Solvers     map[string]any `json:"Solvers"`
		Destination string         `json:"Destination"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	if err := h.Backend.PutPositionConfiguration(id, resourceType, req.Destination, req.Solvers); err != nil {
		return handleError(c, err)
	}

	return stubNoContent(c)
}

func (h *Handler) listPositionConfigurations(c *echo.Context) error {
	resourceType := c.QueryParam("resourceType")
	entries := h.Backend.ListPositionConfigurations(resourceType)

	items := make([]positionConfigurationItemResponse, 0, len(entries))
	for _, e := range entries {
		items = append(items, positionConfigurationItemResponseFrom(e))
	}

	return writeJSON(c, http.StatusOK, listPositionConfigurationsResponse{
		PositionConfigurationList: items,
	})
}

// ============================================================
// Group 12 — Queued messages operations
// ============================================================

func (h *Handler) listQueuedMessages(c *echo.Context, wirelessDeviceID string) error {
	msgs := h.Backend.ListQueuedMessages(wirelessDeviceID)

	items := make([]downlinkQueueMessageResponse, 0, len(msgs))
	for _, m := range msgs {
		items = append(items, downlinkQueueMessageResponse{
			MessageID:    m.MessageID,
			ReceivedAt:   m.ReceivedAt.UTC().Format(time.RFC3339),
			TransmitMode: m.TransmitMode,
		})
	}

	return writeJSON(c, http.StatusOK, listQueuedMessagesResponse{
		DownlinkQueueMessagesList: items,
	})
}

func (h *Handler) deleteQueuedMessages(c *echo.Context, wirelessDeviceID string) error {
	if err := h.Backend.DeleteQueuedMessages(wirelessDeviceID); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

// ============================================================
// Group 13-14 — Misc operations
// ============================================================

func (h *Handler) getMetricConfiguration(c *echo.Context) error {
	status := h.Backend.GetMetricConfigurationStatus()

	return writeJSON(c, http.StatusOK, getMetricConfigurationResponse{
		SummaryMetric: summaryMetricConfigurationResponse{Status: status},
	})
}

func (h *Handler) updateMetricConfiguration(c *echo.Context) error {
	var req struct {
		SummaryMetric struct {
			Status string `json:"Status"`
		} `json:"SummaryMetric"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	if err := h.Backend.UpdateMetricConfigurationStatus(req.SummaryMetric.Status); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

// getServiceEndpoint returns the CUPS or LNS endpoint for the requested
// serviceType and the handler's configured region. AWS defaults to CUPS when
// serviceType is omitted.
func (h *Handler) getServiceEndpoint(c *echo.Context) error {
	serviceType := c.QueryParam("serviceType")
	if serviceType == "" {
		serviceType = stubServiceType
	}

	region := h.DefaultRegion
	if region == "" {
		region = "us-east-1"
	}

	var host string

	switch serviceType {
	case "CUPS":
		host = "cups.lorawan." + region + ".amazonaws.com"
	case "LNS":
		host = "lns.lorawan." + region + ".amazonaws.com"
	default:
		return writeError(c, http.StatusBadRequest, "ValidationException: invalid serviceType "+serviceType)
	}

	return writeJSON(c, http.StatusOK, getServiceEndpointResponse{
		ServiceType:     serviceType,
		ServiceEndpoint: "https://" + host,
		ServerTrust:     "",
	})
}

// ============================================================
// Group 15 — Wireless Device Import Task operations
// ============================================================

func (h *Handler) startWirelessDeviceImportTask(c *echo.Context) error {
	var req struct {
		DestinationName string `json:"DestinationName"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	task, err := h.Backend.StartWirelessDeviceImportTask(h.AccountID, h.DefaultRegion, req.DestinationName)
	if err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusCreated, startWirelessDeviceImportTaskResponse{
		Arn: task.ARN,
		ID:  task.ID,
	})
}

func (h *Handler) startSingleWirelessDeviceImportTask(c *echo.Context) error {
	var req struct {
		DestinationName string `json:"DestinationName"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	task, err := h.Backend.StartSingleWirelessDeviceImportTask(h.AccountID, h.DefaultRegion, req.DestinationName)
	if err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusCreated, startSingleWirelessDeviceImportTaskResponse{
		Arn:              task.ARN,
		WirelessDeviceID: task.WirelessDeviceID,
	})
}

func (h *Handler) getWirelessDeviceImportTask(c *echo.Context, id string) error {
	task, err := h.Backend.GetWirelessDeviceImportTask(id)
	if err != nil {
		return handleError(c, err)
	}

	return writeJSON(c, http.StatusOK, getWirelessDeviceImportTaskResponse{
		Arn:                            task.ARN,
		ID:                             task.ID,
		DestinationName:                task.DestinationName,
		Status:                         task.Status,
		StatusReason:                   task.StatusReason,
		InitializedImportedDeviceCount: task.InitializedImportedDeviceCount,
		PendingImportedDeviceCount:     task.PendingImportedDeviceCount,
		OnboardedImportedDeviceCount:   task.OnboardedImportedDeviceCount,
		FailedImportedDeviceCount:      task.FailedImportedDeviceCount,
	})
}

func (h *Handler) deleteWirelessDeviceImportTask(c *echo.Context, id string) error {
	if err := h.Backend.DeleteWirelessDeviceImportTask(id); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) updateWirelessDeviceImportTask(c *echo.Context, id string) error {
	var req struct {
		DestinationName string `json:"DestinationName"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	if err := h.Backend.UpdateWirelessDeviceImportTask(id, req.DestinationName); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) listWirelessDeviceImportTasks(c *echo.Context) error {
	tasks := h.Backend.ListWirelessDeviceImportTasks()

	entries := make([]getWirelessDeviceImportTaskResponse, 0, len(tasks))

	for _, task := range tasks {
		entries = append(entries, getWirelessDeviceImportTaskResponse{
			Arn:                            task.ARN,
			ID:                             task.ID,
			DestinationName:                task.DestinationName,
			Status:                         task.Status,
			StatusReason:                   task.StatusReason,
			InitializedImportedDeviceCount: task.InitializedImportedDeviceCount,
			PendingImportedDeviceCount:     task.PendingImportedDeviceCount,
			OnboardedImportedDeviceCount:   task.OnboardedImportedDeviceCount,
			FailedImportedDeviceCount:      task.FailedImportedDeviceCount,
		})
	}

	return writeJSON(c, http.StatusOK, listWirelessDeviceImportTasksResponse{
		WirelessDeviceImportTaskList: entries,
	})
}

func (h *Handler) listDevicesForWirelessDeviceImportTask(c *echo.Context) error {
	id := c.QueryParam("id")
	if id == "" {
		// The Id query parameter is required by AWS, but clients that omit it
		// still get a well-formed (empty) list rather than a validation error,
		// matching this package's existing lenient-parsing convention.
		return writeJSON(c, http.StatusOK, listDevicesForWirelessDeviceImportTaskResponse{
			ImportedWirelessDeviceList: []struct{}{},
		})
	}

	task, err := h.Backend.GetWirelessDeviceImportTask(id)
	if err != nil {
		return handleError(c, err)
	}

	return writeJSON(c, http.StatusOK, listDevicesForWirelessDeviceImportTaskResponse{
		DestinationName:            task.DestinationName,
		Positioning:                "Disabled",
		ImportedWirelessDeviceList: []struct{}{},
	})
}

// ============================================================
// Group 16 — Multicast send operations
// ============================================================

func (h *Handler) sendDataToMulticastGroup(c *echo.Context, _ string) error {
	return writeJSON(c, http.StatusCreated, sendDataToMulticastGroupResponse{
		MessageID: uuid.NewString(),
	})
}

// ============================================================
// Group 17 — Metric, position, misc stateless operations
// ============================================================

// getMetrics maps each requested SummaryMetricQuery to a corresponding
// result entry (QueryId echoed, status Succeeded with no aggregated values),
// since this emulator does not ingest telemetry to compute real metric
// values. This mirrors AWS's per-query response shape rather than fabricating
// a query-count-independent empty list.
func (h *Handler) getMetrics(c *echo.Context) error {
	var req struct {
		SummaryMetricQueries []struct {
			QueryID    string `json:"QueryId"`
			MetricName string `json:"MetricName"`
		} `json:"SummaryMetricQueries"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	results := make([]summaryMetricQueryResultResponse, 0, len(req.SummaryMetricQueries))
	for _, q := range req.SummaryMetricQueries {
		results = append(results, summaryMetricQueryResultResponse{
			QueryID:     q.QueryID,
			QueryStatus: "Succeeded",
			MetricName:  q.MetricName,
			Values:      []float64{},
		})
	}

	return writeJSON(c, http.StatusOK, getMetricsResponse{
		SummaryMetricQueryResults: results,
	})
}

// getPositionEstimate computes a position estimate from the submitted
// Wi-Fi/cellular/GNSS/IP measurement data. This emulator cannot perform real
// satellite/RF geolocation solving, so it returns a structurally correct
// GeoJSON payload (matching AWS's wire shape) rather than a null/empty body.
func (h *Handler) getPositionEstimate(c *echo.Context) error {
	geoJSON := geoJSONPointPayload([]float64{0, 0}, map[string]any{
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})

	return writeJSON(c, http.StatusOK, getPositionEstimateResponse{GeoJSONPayload: geoJSON})
}

// getResourcePosition echoes back the raw GeoJSON payload most recently
// submitted via UpdateResourcePosition for this resource.
func (h *Handler) getResourcePosition(c *echo.Context, id string) error {
	pos := h.Backend.GetPosition(id)

	raw, ok := pos["GeoJsonPayload"].(string)
	if !ok || raw == "" {
		return writeJSON(c, http.StatusOK, getResourcePositionResponse{})
	}

	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return writeJSON(c, http.StatusOK, getResourcePositionResponse{})
	}

	return writeJSON(c, http.StatusOK, getResourcePositionResponse{GeoJSONPayload: decoded})
}

// geoJSONPointPayload builds a GeoJSON Point Feature payload for the given
// coordinates, matching the wire shape AWS returns for GeoJsonPayload fields.
func geoJSONPointPayload(coords []float64, properties map[string]any) []byte {
	if properties == nil {
		properties = map[string]any{}
	}

	payload := map[string]any{
		"type": "Feature",
		"geometry": map[string]any{
			"type":        "Point",
			"coordinates": coords,
		},
		"properties": properties,
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return []byte("{}")
	}

	return b
}
