package iotwireless

// handler_stubs.go — stub HTTP handlers for the 75 IoT Wireless operations that
// are acknowledged in sdk_completeness_test.go.  Each stub returns the minimum
// response shape required by the AWS SDK so that SDK clients can successfully
// unmarshal the response.

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- Shared stub constants ---

const (
	stubArn              = "arn:aws:iotwireless:us-east-1:123456789012:Resource/stub"
	stubImportTaskArn    = "arn:aws:iotwireless:us-east-1:123456789012:ImportTask/stub"
	stubImportTaskID     = "stub-import-task-id"
	stubMulticastArn     = "arn:aws:iotwireless:us-east-1:123456789012:MulticastGroup/stub"
	stubMulticastGroupID = "stub-multicast-group-id"
	stubNetworkAnalArn   = "arn:aws:iotwireless:us-east-1:123456789012:NetworkAnalyzerConfiguration/stub"
	stubGatewayTaskArn   = "arn:aws:iotwireless:us-east-1:123456789012:WirelessGatewayTaskDefinition/stub"
	stubGatewayTaskDefID = "stub-task-def-id"
	stubName             = "stub"
	stubMessageID        = "stub-message-id"
	stubCertID           = "stub-cert-id"
	stubWirelessDeviceID = "stub-wireless-device-id"
	stubServiceEndpoint  = "https://cups.lorawan.us-east-1.amazonaws.com"
	stubServiceType      = "CUPS"
)

// maxStubBodyBytes caps stub request body reads to prevent unbounded memory
// usage on attacker-controlled inputs. IoT Wireless API payloads are far below
// 1 MiB; cap conservatively.
const maxStubBodyBytes = 1 << 20

// readStubBody returns the request body capped at maxStubBodyBytes. Errors are
// swallowed because stubs treat unparsed input as empty (matching prior behavior).
func readStubBody(c *echo.Context) []byte {
	body, _ := io.ReadAll(http.MaxBytesReader(c.Response(), c.Request().Body, maxStubBodyBytes))

	return body
}

// stubNoContent writes 204 No Content and returns nil.
func stubNoContent(c *echo.Context) error {
	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

// --- Stub response types ---

type createMulticastGroupResponse struct {
	Arn string `json:"Arn"`
	ID  string `json:"Id"`
}

type createNetworkAnalyzerConfigurationResponse struct {
	Arn  string `json:"Arn"`
	Name string `json:"Name"`
}

type createWirelessGatewayTaskResponse struct {
	WirelessGatewayTaskDefinitionID string `json:"WirelessGatewayTaskDefinitionId"`
	WirelessGatewayID               string `json:"WirelessGatewayId"`
	Status                          string `json:"Status"`
}

type createWirelessGatewayTaskDefinitionResponse struct {
	Arn string `json:"Arn"`
	ID  string `json:"Id"`
}

type getMulticastGroupResponse struct {
	Arn    string `json:"Arn"`
	ID     string `json:"Id"`
	Name   string `json:"Name"`
	Status string `json:"Status"`
}

type getMulticastGroupSessionResponse struct {
	LoRaWAN map[string]any `json:"LoRaWAN"`
}

type getNetworkAnalyzerConfigurationResponse struct {
	Arn              string   `json:"Arn"`
	Name             string   `json:"Name"`
	Description      string   `json:"Description"`
	WirelessDevices  []string `json:"WirelessDevices"`
	WirelessGateways []string `json:"WirelessGateways"`
}

type getPartnerAccountResponse struct {
	Arn       string `json:"Arn"`
	AccountID string `json:"AccountId"`
}

type getPositionResponse struct {
	SolverType     string    `json:"SolverType"`
	SolverVersion  string    `json:"SolverVersion"`
	SolverProvider string    `json:"SolverProvider"`
	Timestamp      string    `json:"Timestamp"`
	Position       []float64 `json:"Position"`
}

type getPositionConfigurationResponse struct {
	Destination string `json:"Destination"`
	SolverType  string `json:"SolverType"`
}

type getPositionEstimateResponse struct {
	GeoJSONPayload []byte `json:"GeoJsonPayload"`
}

type getResourceEventConfigurationResponse struct {
	Identifier     string `json:"Identifier"`
	IdentifierType string `json:"IdentifierType"`
	PartnerType    string `json:"PartnerType"`
}

type getResourceLogLevelResponse struct {
	LogLevel     string `json:"LogLevel"`
	ResourceType string `json:"ResourceType"`
	ResourceID   string `json:"ResourceId"`
}

type getResourcePositionResponse struct {
	GeoJSONPayload []byte `json:"GeoJsonPayload"`
}

type getServiceEndpointResponse struct {
	ServiceType     string `json:"ServiceType"`
	ServiceEndpoint string `json:"ServiceEndpoint"`
	ServerTrust     string `json:"ServerTrust"`
}

type getWirelessDeviceImportTaskResponse struct {
	Arn                            string `json:"Arn"`
	ID                             string `json:"Id"`
	DestinationName                string `json:"DestinationName"`
	Status                         string `json:"Status"`
	StatusReason                   string `json:"StatusReason"`
	InitializedImportedDeviceCount int64  `json:"InitializedImportedDeviceCount"`
	PendingImportedDeviceCount     int64  `json:"PendingImportedDeviceCount"`
	OnboardedImportedDeviceCount   int64  `json:"OnboardedImportedDeviceCount"`
	FailedImportedDeviceCount      int64  `json:"FailedImportedDeviceCount"`
}

type getWirelessDeviceStatisticsResponse struct {
	WirelessDeviceID     string `json:"WirelessDeviceId"`
	LastUplinkReceivedAt string `json:"LastUplinkReceivedAt"`
}

type getWirelessGatewayCertificateResponse struct {
	IotCertificateID                  string `json:"IotCertificateId"`
	LoRaWANNetworkServerCertificateID string `json:"LoRaWANNetworkServerCertificateId"`
}

type getWirelessGatewayFirmwareInformationResponse struct{}

type getWirelessGatewayStatisticsResponse struct {
	WirelessGatewayID    string `json:"WirelessGatewayId"`
	LastUplinkReceivedAt string `json:"LastUplinkReceivedAt"`
	ConnectionStatus     string `json:"ConnectionStatus"`
}

type getWirelessGatewayTaskResponse struct {
	WirelessGatewayID               string `json:"WirelessGatewayId"`
	WirelessGatewayTaskDefinitionID string `json:"WirelessGatewayTaskDefinitionId"`
	LastUplinkReceivedAt            string `json:"LastUplinkReceivedAt"`
	TaskCreatedAt                   string `json:"TaskCreatedAt"`
	Status                          string `json:"Status"`
}

type getWirelessGatewayTaskDefinitionResponse struct {
	Arn             string `json:"Arn"`
	Name            string `json:"Name"`
	AutoCreateTasks bool   `json:"AutoCreateTasks"`
}

type getEventConfigurationByResourceTypesResponse struct{}

type getLogLevelsByResourceTypesResponse struct {
	DefaultLogLevel           string     `json:"DefaultLogLevel"`
	WirelessGatewayLogOptions []struct{} `json:"WirelessGatewayLogOptions"`
	WirelessDeviceLogOptions  []struct{} `json:"WirelessDeviceLogOptions"`
}

type getMetricConfigurationResponse struct{}

type getMetricsResponse struct {
	SummaryMetricQueryResults []struct{} `json:"SummaryMetricQueryResults"`
}

type multicastGroupEntry struct {
	Arn  string `json:"Arn"`
	ID   string `json:"Id"`
	Name string `json:"Name"`
}

type listMulticastGroupsResponse struct {
	NextToken          string                `json:"NextToken"`
	MulticastGroupList []multicastGroupEntry `json:"MulticastGroupList"`
}

type listMulticastGroupsByFuotaTaskResponse struct {
	NextToken          string                `json:"NextToken"`
	MulticastGroupList []multicastGroupEntry `json:"MulticastGroupList"`
}

type listNetworkAnalyzerConfigurationsResponse struct {
	NextToken                        string `json:"NextToken"`
	NetworkAnalyzerConfigurationList []struct {
		Arn  string `json:"Arn"`
		Name string `json:"Name"`
	} `json:"NetworkAnalyzerConfigurationList"`
}

type listPartnerAccountsResponse struct {
	NextToken string     `json:"NextToken"`
	Sidewalk  []struct{} `json:"Sidewalk"`
}

type listPositionConfigurationsResponse struct {
	NextToken                 string     `json:"NextToken"`
	PositionConfigurationList []struct{} `json:"PositionConfigurationList"`
}

type listQueuedMessagesResponse struct {
	NextToken                 string     `json:"NextToken"`
	DownlinkQueueMessagesList []struct{} `json:"DownlinkQueueMessagesList"`
}

type listEventConfigurationsResponse struct {
	NextToken               string     `json:"NextToken"`
	EventConfigurationsList []struct{} `json:"EventConfigurationsList"`
}

type listWirelessDeviceImportTasksResponse struct {
	NextToken                    string     `json:"NextToken"`
	WirelessDeviceImportTaskList []struct{} `json:"WirelessDeviceImportTaskList"`
}

type listDevicesForWirelessDeviceImportTaskResponse struct {
	NextToken                  string     `json:"NextToken"`
	DestinationName            string     `json:"DestinationName"`
	ImportedWirelessDeviceList []struct{} `json:"ImportedWirelessDeviceList"`
}

type listWirelessGatewayTaskDefinitionsResponse struct {
	NextToken       string     `json:"NextToken"`
	TaskDefinitions []struct{} `json:"TaskDefinitions"`
}

type sendDataToMulticastGroupResponse struct {
	MessageID string `json:"MessageId"`
}

type sendDataToWirelessDeviceResponse struct {
	MessageID string `json:"MessageId"`
}

type startWirelessDeviceImportTaskResponse struct {
	Arn string `json:"Arn"`
	ID  string `json:"Id"`
}

type startSingleWirelessDeviceImportTaskResponse struct {
	Arn              string `json:"Arn"`
	WirelessDeviceID string `json:"WirelessDeviceId"`
}

type testWirelessDeviceResponse struct {
	Result string `json:"Result"`
}

// --- Stub handlers ---

func (h *Handler) createMulticastGroup(c *echo.Context) error {
	var req struct {
		Tags        map[string]string `json:"Tags"`
		Name        string            `json:"Name"`
		Description string            `json:"Description"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	mg, err := h.Backend.CreateMulticastGroup(h.AccountID, h.DefaultRegion, req.Name, req.Description, req.Tags)
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

func (h *Handler) getMulticastGroupSession(c *echo.Context, _ string) error {
	return writeJSON(c, http.StatusOK, getMulticastGroupSessionResponse{})
}

func (h *Handler) startMulticastGroupSession(c *echo.Context, _ string) error {
	return stubNoContent(c)
}

func (h *Handler) listMulticastGroupsByFuotaTask(c *echo.Context, _ string) error {
	return writeJSON(c, http.StatusOK, listMulticastGroupsByFuotaTaskResponse{
		MulticastGroupList: []multicastGroupEntry{},
	})
}

func (h *Handler) sendDataToMulticastGroup(c *echo.Context, _ string) error {
	return writeJSON(c, http.StatusCreated, sendDataToMulticastGroupResponse{
		MessageID: stubMessageID,
	})
}

func (h *Handler) startBulkAssociateWirelessDeviceWithMulticastGroup(c *echo.Context, _ string) error {
	return stubNoContent(c)
}

func (h *Handler) startBulkDisassociateWirelessDeviceFromMulticastGroup(c *echo.Context, _ string) error {
	return stubNoContent(c)
}

func (h *Handler) disassociateWirelessDeviceFromMulticastGroup(c *echo.Context, _, _ string) error {
	return stubNoContent(c)
}

func (h *Handler) disassociateMulticastGroupFromFuotaTask(c *echo.Context, _, _ string) error {
	return stubNoContent(c)
}

func (h *Handler) disassociateWirelessDeviceFromFuotaTask(c *echo.Context, _, _ string) error {
	return stubNoContent(c)
}

func (h *Handler) startFuotaTask(c *echo.Context, _ string) error {
	return stubNoContent(c)
}

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

func (h *Handler) getLogLevelsByResourceTypes(c *echo.Context) error {
	return writeJSON(c, http.StatusOK, getLogLevelsByResourceTypesResponse{
		DefaultLogLevel:           "INFO",
		WirelessGatewayLogOptions: []struct{}{},
		WirelessDeviceLogOptions:  []struct{}{},
	})
}

func (h *Handler) updateLogLevelsByResourceTypes(c *echo.Context) error {
	return stubNoContent(c)
}

func (h *Handler) resetAllResourceLogLevels(c *echo.Context) error {
	return stubNoContent(c)
}

func (h *Handler) getResourceLogLevel(c *echo.Context, _ string) error {
	return writeJSON(c, http.StatusOK, getResourceLogLevelResponse{
		LogLevel: "INFO",
	})
}

func (h *Handler) putResourceLogLevel(c *echo.Context, _ string) error {
	return stubNoContent(c)
}

func (h *Handler) resetResourceLogLevel(c *echo.Context, _ string) error {
	return stubNoContent(c)
}

func (h *Handler) getMetricConfiguration(c *echo.Context) error {
	return writeJSON(c, http.StatusOK, getMetricConfigurationResponse{})
}

func (h *Handler) updateMetricConfiguration(c *echo.Context) error {
	return stubNoContent(c)
}

func (h *Handler) getMetrics(c *echo.Context) error {
	return writeJSON(c, http.StatusOK, getMetricsResponse{
		SummaryMetricQueryResults: []struct{}{},
	})
}

func (h *Handler) getPosition(c *echo.Context, _ string) error {
	return writeJSON(c, http.StatusOK, getPositionResponse{
		Position: []float64{},
	})
}

func (h *Handler) updatePosition(c *echo.Context, _ string) error {
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

func (h *Handler) getPositionEstimate(c *echo.Context) error {
	return writeJSON(c, http.StatusOK, getPositionEstimateResponse{})
}

func (h *Handler) getResourcePosition(c *echo.Context, _ string) error {
	return writeJSON(c, http.StatusOK, getResourcePositionResponse{})
}

func (h *Handler) updateResourcePosition(c *echo.Context, _ string) error {
	return stubNoContent(c)
}

func (h *Handler) createWirelessGatewayTask(c *echo.Context, _ string) error {
	return writeJSON(c, http.StatusCreated, createWirelessGatewayTaskResponse{
		Status: "PENDING",
	})
}

func (h *Handler) getWirelessGatewayTask(c *echo.Context, _ string) error {
	return writeJSON(c, http.StatusOK, getWirelessGatewayTaskResponse{
		Status: "PENDING",
	})
}

func (h *Handler) deleteWirelessGatewayTask(c *echo.Context, _ string) error {
	return stubNoContent(c)
}

func (h *Handler) createWirelessGatewayTaskDefinition(c *echo.Context) error {
	return writeJSON(c, http.StatusCreated, createWirelessGatewayTaskDefinitionResponse{
		Arn: stubGatewayTaskArn,
		ID:  stubGatewayTaskDefID,
	})
}

func (h *Handler) getWirelessGatewayTaskDefinition(c *echo.Context, _ string) error {
	return writeJSON(c, http.StatusOK, getWirelessGatewayTaskDefinitionResponse{
		Arn:  stubGatewayTaskArn,
		Name: stubName,
	})
}

func (h *Handler) listWirelessGatewayTaskDefinitions(c *echo.Context) error {
	return writeJSON(c, http.StatusOK, listWirelessGatewayTaskDefinitionsResponse{
		TaskDefinitions: []struct{}{},
	})
}

func (h *Handler) deleteWirelessGatewayTaskDefinition(c *echo.Context, _ string) error {
	return stubNoContent(c)
}

func (h *Handler) getWirelessGatewayCertificate(c *echo.Context, _ string) error {
	return writeJSON(c, http.StatusOK, getWirelessGatewayCertificateResponse{
		IotCertificateID: stubCertID,
	})
}

func (h *Handler) getWirelessGatewayFirmwareInformation(c *echo.Context, _ string) error {
	return writeJSON(c, http.StatusOK, getWirelessGatewayFirmwareInformationResponse{})
}

func (h *Handler) getWirelessGatewayStatistics(c *echo.Context, _ string) error {
	return writeJSON(c, http.StatusOK, getWirelessGatewayStatisticsResponse{
		ConnectionStatus: "Connected",
	})
}

func (h *Handler) disassociateWirelessGatewayFromCertificate(c *echo.Context, _ string) error {
	return stubNoContent(c)
}

func (h *Handler) disassociateWirelessGatewayFromThing(c *echo.Context, _ string) error {
	return stubNoContent(c)
}

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

func (h *Handler) getWirelessDeviceStatistics(c *echo.Context, _ string) error {
	return writeJSON(c, http.StatusOK, getWirelessDeviceStatisticsResponse{})
}

func (h *Handler) sendDataToWirelessDevice(c *echo.Context, _ string) error {
	return writeJSON(c, http.StatusCreated, sendDataToWirelessDeviceResponse{
		MessageID: stubMessageID,
	})
}

func (h *Handler) testWirelessDevice(c *echo.Context, _ string) error {
	return writeJSON(c, http.StatusOK, testWirelessDeviceResponse{
		Result: "PASS",
	})
}

func (h *Handler) deregisterWirelessDevice(c *echo.Context, _ string) error {
	return stubNoContent(c)
}

func (h *Handler) updateWirelessDevice(c *echo.Context, _ string) error {
	return stubNoContent(c)
}

func (h *Handler) disassociateWirelessDeviceFromThing(c *echo.Context, _ string) error {
	return stubNoContent(c)
}

func (h *Handler) deleteQueuedMessages(c *echo.Context, _ string) error {
	return stubNoContent(c)
}

func (h *Handler) listQueuedMessages(c *echo.Context, _ string) error {
	return writeJSON(c, http.StatusOK, listQueuedMessagesResponse{
		DownlinkQueueMessagesList: []struct{}{},
	})
}

func (h *Handler) startWirelessDeviceImportTask(c *echo.Context) error {
	return writeJSON(c, http.StatusCreated, startWirelessDeviceImportTaskResponse{
		Arn: stubImportTaskArn,
		ID:  stubImportTaskID,
	})
}

func (h *Handler) startSingleWirelessDeviceImportTask(c *echo.Context) error {
	return writeJSON(c, http.StatusCreated, startSingleWirelessDeviceImportTaskResponse{
		Arn:              stubImportTaskArn,
		WirelessDeviceID: stubWirelessDeviceID,
	})
}

func (h *Handler) getWirelessDeviceImportTask(c *echo.Context, _ string) error {
	return writeJSON(c, http.StatusOK, getWirelessDeviceImportTaskResponse{
		Arn:    stubImportTaskArn,
		ID:     stubImportTaskID,
		Status: "Initialized",
	})
}

func (h *Handler) deleteWirelessDeviceImportTask(c *echo.Context, _ string) error {
	return stubNoContent(c)
}

func (h *Handler) updateWirelessDeviceImportTask(c *echo.Context, _ string) error {
	return stubNoContent(c)
}

func (h *Handler) listWirelessDeviceImportTasks(c *echo.Context) error {
	return writeJSON(c, http.StatusOK, listWirelessDeviceImportTasksResponse{
		WirelessDeviceImportTaskList: []struct{}{},
	})
}

func (h *Handler) listDevicesForWirelessDeviceImportTask(c *echo.Context) error {
	return writeJSON(c, http.StatusOK, listDevicesForWirelessDeviceImportTaskResponse{
		ImportedWirelessDeviceList: []struct{}{},
	})
}

func (h *Handler) getPartnerAccount(c *echo.Context, _ string) error {
	return writeJSON(c, http.StatusOK, getPartnerAccountResponse{})
}

func (h *Handler) listPartnerAccounts(c *echo.Context) error {
	return writeJSON(c, http.StatusOK, listPartnerAccountsResponse{
		Sidewalk: []struct{}{},
	})
}

func (h *Handler) disassociateAwsAccountFromPartnerAccount(c *echo.Context, _ string) error {
	return stubNoContent(c)
}

func (h *Handler) updatePartnerAccount(c *echo.Context, _ string) error {
	return stubNoContent(c)
}

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

func (h *Handler) getServiceEndpoint(c *echo.Context) error {
	return writeJSON(c, http.StatusOK, getServiceEndpointResponse{
		ServiceType:     stubServiceType,
		ServiceEndpoint: stubServiceEndpoint,
		ServerTrust:     "",
	})
}
