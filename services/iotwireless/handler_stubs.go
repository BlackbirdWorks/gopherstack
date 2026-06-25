package iotwireless

// handler_stubs.go — stub HTTP handlers for the IoT Wireless operations that
// have not yet been promoted to handler_ops.go.  Each stub returns the minimum
// response shape required by the AWS SDK so that SDK clients can successfully
// unmarshal the response.

import (
	"io"
	"net/http"

	"github.com/labstack/echo/v5"
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

type getPositionEstimateResponse struct {
	GeoJSONPayload []byte `json:"GeoJsonPayload"`
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

type getLogLevelsByResourceTypesResponse struct {
	DefaultLogLevel           string     `json:"DefaultLogLevel"`
	WirelessGatewayLogOptions []struct{} `json:"WirelessGatewayLogOptions"`
	WirelessDeviceLogOptions  []struct{} `json:"WirelessDeviceLogOptions"`
}

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
	NextToken                 string           `json:"NextToken"`
	PositionConfigurationList []map[string]any `json:"PositionConfigurationList"`
}

type listQueuedMessagesResponse struct {
	NextToken                 string           `json:"NextToken"`
	DownlinkQueueMessagesList []map[string]any `json:"DownlinkQueueMessagesList"`
}

type listEventConfigurationsResponse struct {
	NextToken               string           `json:"NextToken"`
	EventConfigurationsList []map[string]any `json:"EventConfigurationsList"`
}

type listWirelessDeviceImportTasksResponse struct {
	NextToken                    string                                `json:"NextToken"`
	WirelessDeviceImportTaskList []getWirelessDeviceImportTaskResponse `json:"WirelessDeviceImportTaskList"`
}

type listDevicesForWirelessDeviceImportTaskResponse struct {
	NextToken                  string     `json:"NextToken"`
	DestinationName            string     `json:"DestinationName"`
	ImportedWirelessDeviceList []struct{} `json:"ImportedWirelessDeviceList"`
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
