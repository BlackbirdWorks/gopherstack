package iotwireless

// handler_ops.go — real handler implementations for IoT Wireless operations.

import (
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

func (h *Handler) getWirelessGatewayStatistics(c *echo.Context, _ string) error {
	return writeJSON(c, http.StatusOK, getWirelessGatewayStatisticsResponse{
		ConnectionStatus: "Connected",
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

func (h *Handler) sendDataToWirelessDevice(c *echo.Context, _ string) error {
	return writeJSON(c, http.StatusCreated, sendDataToWirelessDeviceResponse{
		MessageID: uuid.NewString(),
	})
}

func (h *Handler) getWirelessDeviceStatistics(c *echo.Context, _ string) error {
	return writeJSON(c, http.StatusOK, getWirelessDeviceStatisticsResponse{})
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
// Group 8 — Event configuration operations (stateless stubs)
// ============================================================

func (h *Handler) getEventConfigurationByResourceTypes(c *echo.Context) error {
	return writeJSON(c, http.StatusOK, getEventConfigurationByResourceTypesResponse{})
}

func (h *Handler) updateEventConfigurationByResourceTypes(c *echo.Context) error {
	return stubNoContent(c)
}

func (h *Handler) listEventConfigurations(c *echo.Context) error {
	return writeJSON(c, http.StatusOK, listEventConfigurationsResponse{
		EventConfigurationsList: []struct{}{},
	})
}

func (h *Handler) getResourceEventConfiguration(c *echo.Context, _ string) error {
	return writeJSON(c, http.StatusOK, getResourceEventConfigurationResponse{})
}

func (h *Handler) updateResourceEventConfiguration(c *echo.Context, _ string) error {
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

func (h *Handler) updatePartnerAccount(c *echo.Context, _ string) error {
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
		// Return stub response if not found, for API compatibility.
		return writeJSON(c, http.StatusOK, getWirelessGatewayTaskResponse{
			WirelessGatewayID: gatewayID,
			Status:            "PENDING",
		})
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

func (h *Handler) getPosition(c *echo.Context, id string) error {
	_ = h.Backend.GetPosition(id)

	return writeJSON(c, http.StatusOK, getPositionResponse{
		Position: []float64{},
	})
}

func (h *Handler) updatePosition(c *echo.Context, id string) error {
	var req map[string]any

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)
	_ = h.Backend.UpdatePosition(id, req)

	return stubNoContent(c)
}

func (h *Handler) getPositionConfiguration(c *echo.Context, _ string) error {
	return writeJSON(c, http.StatusOK, getPositionConfigurationResponse{})
}

func (h *Handler) putPositionConfiguration(c *echo.Context, _ string) error {
	return stubNoContent(c)
}

func (h *Handler) listPositionConfigurations(c *echo.Context) error {
	return writeJSON(c, http.StatusOK, listPositionConfigurationsResponse{
		PositionConfigurationList: []struct{}{},
	})
}

// ============================================================
// Group 12 — Queued messages operations
// ============================================================

func (h *Handler) listQueuedMessages(c *echo.Context, wirelessDeviceID string) error {
	_ = h.Backend.ListQueuedMessages(wirelessDeviceID)

	return writeJSON(c, http.StatusOK, listQueuedMessagesResponse{
		DownlinkQueueMessagesList: []struct{}{},
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
	return writeJSON(c, http.StatusOK, getMetricConfigurationResponse{})
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
	return writeJSON(c, http.StatusOK, listDevicesForWirelessDeviceImportTaskResponse{
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

func (h *Handler) getMetrics(c *echo.Context) error {
	return writeJSON(c, http.StatusOK, getMetricsResponse{
		SummaryMetricQueryResults: []struct{}{},
	})
}

func (h *Handler) getPositionEstimate(c *echo.Context) error {
	return writeJSON(c, http.StatusOK, getPositionEstimateResponse{})
}

func (h *Handler) getResourcePosition(c *echo.Context, id string) error {
	_ = h.Backend.GetPosition(id)

	return writeJSON(c, http.StatusOK, getResourcePositionResponse{})
}
