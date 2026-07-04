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

// --- Shared stub constants ---

// stubServiceType is the default ServiceType (CUPS) GetServiceEndpoint returns
// when the request omits the serviceType query parameter, matching AWS's
// documented default.
const stubServiceType = "CUPS"

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

type sidewalkAccountInfo struct {
	AmazonID string `json:"AmazonId,omitempty"`
	Arn      string `json:"Arn,omitempty"`
}

type getPartnerAccountResponse struct {
	Sidewalk      *sidewalkAccountInfo `json:"Sidewalk,omitempty"`
	AccountLinked bool                 `json:"AccountLinked"`
}

type getPositionResponse struct {
	Accuracy       *float64  `json:"Accuracy,omitempty"`
	SolverType     string    `json:"SolverType,omitempty"`
	SolverVersion  string    `json:"SolverVersion,omitempty"`
	SolverProvider string    `json:"SolverProvider,omitempty"`
	Timestamp      string    `json:"Timestamp,omitempty"`
	Position       []float64 `json:"Position"`
}

type getPositionConfigurationResponse struct {
	Solvers     map[string]any `json:"Solvers,omitempty"`
	Destination string         `json:"Destination,omitempty"`
}

type getPositionEstimateResponse struct {
	GeoJSONPayload []byte `json:"GeoJsonPayload"`
}

// eventConfigDocResponse mirrors AWS's five event-type configuration fields,
// shared verbatim by GetEventConfigurationByResourceTypes and
// GetResourceEventConfiguration (neither of which includes an identifier).
type eventConfigDocResponse struct {
	ConnectionStatus        map[string]any `json:"ConnectionStatus,omitempty"`
	DeviceRegistrationState map[string]any `json:"DeviceRegistrationState,omitempty"`
	Join                    map[string]any `json:"Join,omitempty"`
	MessageDeliveryStatus   map[string]any `json:"MessageDeliveryStatus,omitempty"`
	Proximity               map[string]any `json:"Proximity,omitempty"`
}

func eventConfigDocResponseFrom(doc *EventConfigDoc) eventConfigDocResponse {
	if doc == nil {
		return eventConfigDocResponse{}
	}

	return eventConfigDocResponse{
		ConnectionStatus:        doc.ConnectionStatus,
		DeviceRegistrationState: doc.DeviceRegistrationState,
		Join:                    doc.Join,
		MessageDeliveryStatus:   doc.MessageDeliveryStatus,
		Proximity:               doc.Proximity,
	}
}

type eventConfigurationItemResponse struct {
	Events         eventConfigDocResponse `json:"Events"`
	Identifier     string                 `json:"Identifier"`
	IdentifierType string                 `json:"IdentifierType"`
	PartnerType    string                 `json:"PartnerType,omitempty"`
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
	WirelessDeviceID     string `json:"WirelessDeviceId,omitempty"`
	LastUplinkReceivedAt string `json:"LastUplinkReceivedAt,omitempty"`
}

type getWirelessGatewayCertificateResponse struct {
	IotCertificateID                  string `json:"IotCertificateId"`
	LoRaWANNetworkServerCertificateID string `json:"LoRaWANNetworkServerCertificateId"`
}

type getWirelessGatewayFirmwareInformationResponse struct{}

type getWirelessGatewayStatisticsResponse struct {
	WirelessGatewayID    string `json:"WirelessGatewayId,omitempty"`
	LastUplinkReceivedAt string `json:"LastUplinkReceivedAt,omitempty"`
	ConnectionStatus     string `json:"ConnectionStatus,omitempty"`
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

type summaryMetricConfigurationResponse struct {
	Status string `json:"Status,omitempty"`
}

type getMetricConfigurationResponse struct {
	SummaryMetric summaryMetricConfigurationResponse `json:"SummaryMetric"`
}

// summaryMetricQueryResultResponse mirrors one entry of GetMetrics's
// SummaryMetricQueryResults: a per-query echo of the QueryId with a status,
// since this emulator does not ingest telemetry to aggregate real metric
// values.
type summaryMetricQueryResultResponse struct {
	QueryID     string    `json:"QueryId,omitempty"`
	QueryStatus string    `json:"QueryStatus,omitempty"`
	MetricName  string    `json:"MetricName,omitempty"`
	Values      []float64 `json:"Values,omitempty"`
}

type getMetricsResponse struct {
	SummaryMetricQueryResults []summaryMetricQueryResultResponse `json:"SummaryMetricQueryResults"`
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
	NextToken string                `json:"NextToken"`
	Sidewalk  []sidewalkAccountInfo `json:"Sidewalk"`
}

type positionConfigurationItemResponse struct {
	Solvers            map[string]any `json:"Solvers,omitempty"`
	ResourceIdentifier string         `json:"ResourceIdentifier,omitempty"`
	ResourceType       string         `json:"ResourceType,omitempty"`
	Destination        string         `json:"Destination,omitempty"`
}

func positionConfigurationItemResponseFrom(e *PositionConfigEntry) positionConfigurationItemResponse {
	return positionConfigurationItemResponse{
		ResourceIdentifier: e.ResourceIdentifier,
		ResourceType:       e.ResourceType,
		Destination:        e.Destination,
		Solvers:            e.Solvers,
	}
}

type listPositionConfigurationsResponse struct {
	NextToken                 string                              `json:"NextToken"`
	PositionConfigurationList []positionConfigurationItemResponse `json:"PositionConfigurationList"`
}

type downlinkQueueMessageResponse struct {
	MessageID    string `json:"MessageId,omitempty"`
	ReceivedAt   string `json:"ReceivedAt,omitempty"`
	TransmitMode int32  `json:"TransmitMode"`
}

type listQueuedMessagesResponse struct {
	NextToken                 string                         `json:"NextToken"`
	DownlinkQueueMessagesList []downlinkQueueMessageResponse `json:"DownlinkQueueMessagesList"`
}

func eventConfigurationItemResponseFrom(e *ResourceEventConfigEntry) eventConfigurationItemResponse {
	return eventConfigurationItemResponse{
		Identifier:     e.Identifier,
		IdentifierType: e.IdentifierType,
		PartnerType:    e.PartnerType,
		Events:         eventConfigDocResponseFrom(&e.Config),
	}
}

type listEventConfigurationsResponse struct {
	NextToken               string                           `json:"NextToken"`
	EventConfigurationsList []eventConfigurationItemResponse `json:"EventConfigurationsList"`
}

type listWirelessDeviceImportTasksResponse struct {
	NextToken                    string                                `json:"NextToken"`
	WirelessDeviceImportTaskList []getWirelessDeviceImportTaskResponse `json:"WirelessDeviceImportTaskList"`
}

type listDevicesForWirelessDeviceImportTaskResponse struct {
	NextToken                  string     `json:"NextToken"`
	DestinationName            string     `json:"DestinationName"`
	Positioning                string     `json:"Positioning,omitempty"`
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
