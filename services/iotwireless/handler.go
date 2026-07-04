package iotwireless

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	opAssociateAwsAccountWithPartnerAccount     = "AssociateAwsAccountWithPartnerAccount"
	opAssociateMulticastGroupWithFuotaTask      = "AssociateMulticastGroupWithFuotaTask"
	opAssociateWirelessDeviceWithFuotaTask      = "AssociateWirelessDeviceWithFuotaTask"
	opAssociateWirelessDeviceWithMulticastGroup = "AssociateWirelessDeviceWithMulticastGroup"
	opAssociateWirelessDeviceWithThing          = "AssociateWirelessDeviceWithThing"
	opAssociateWirelessGatewayWithCertificate   = "AssociateWirelessGatewayWithCertificate"
	opAssociateWirelessGatewayWithThing         = "AssociateWirelessGatewayWithThing"
	opCancelMulticastGroupSession               = "CancelMulticastGroupSession"
	opListTagsForResource                       = "ListTagsForResource"
	opTagResource                               = "TagResource"
	opUntagResource                             = "UntagResource"
)

// Additional operation name constants for stub ops.
const (
	opCreateMulticastGroup                                  = "CreateMulticastGroup"
	opGetMulticastGroup                                     = "GetMulticastGroup"
	opListMulticastGroups                                   = "ListMulticastGroups"
	opDeleteMulticastGroup                                  = "DeleteMulticastGroup"
	opUpdateMulticastGroup                                  = "UpdateMulticastGroup"
	opGetMulticastGroupSession                              = "GetMulticastGroupSession"
	opStartMulticastGroupSession                            = "StartMulticastGroupSession"
	opListMulticastGroupsByFuotaTask                        = "ListMulticastGroupsByFuotaTask"
	opSendDataToMulticastGroup                              = "SendDataToMulticastGroup"
	opStartBulkAssociateWirelessDeviceWithMulticastGroup    = "StartBulkAssociateWirelessDeviceWithMulticastGroup"
	opStartBulkDisassociateWirelessDeviceFromMulticastGroup = "StartBulkDisassociateWirelessDeviceFromMulticastGroup"
	opDisassociateWirelessDeviceFromMulticastGroup          = "DisassociateWirelessDeviceFromMulticastGroup"
	opDisassociateMulticastGroupFromFuotaTask               = "DisassociateMulticastGroupFromFuotaTask"
	opDisassociateWirelessDeviceFromFuotaTask               = "DisassociateWirelessDeviceFromFuotaTask"
	opUpdateFuotaTask                                       = "UpdateFuotaTask"
	opStartFuotaTask                                        = "StartFuotaTask"
	opCreateNetworkAnalyzerConfiguration                    = "CreateNetworkAnalyzerConfiguration"
	opGetNetworkAnalyzerConfiguration                       = "GetNetworkAnalyzerConfiguration"
	opListNetworkAnalyzerConfigurations                     = "ListNetworkAnalyzerConfigurations"
	opDeleteNetworkAnalyzerConfiguration                    = "DeleteNetworkAnalyzerConfiguration"
	opUpdateNetworkAnalyzerConfiguration                    = "UpdateNetworkAnalyzerConfiguration"
	opGetEventConfigurationByResourceTypes                  = "GetEventConfigurationByResourceTypes"
	opUpdateEventConfigurationByResourceTypes               = "UpdateEventConfigurationByResourceTypes"
	opListEventConfigurations                               = "ListEventConfigurations"
	opGetResourceEventConfiguration                         = "GetResourceEventConfiguration"
	opUpdateResourceEventConfiguration                      = "UpdateResourceEventConfiguration"
	opGetLogLevelsByResourceTypes                           = "GetLogLevelsByResourceTypes"
	opUpdateLogLevelsByResourceTypes                        = "UpdateLogLevelsByResourceTypes"
	opResetAllResourceLogLevels                             = "ResetAllResourceLogLevels"
	opGetResourceLogLevel                                   = "GetResourceLogLevel"
	opPutResourceLogLevel                                   = "PutResourceLogLevel"
	opResetResourceLogLevel                                 = "ResetResourceLogLevel"
	opGetMetricConfiguration                                = "GetMetricConfiguration"
	opUpdateMetricConfiguration                             = "UpdateMetricConfiguration"
	opGetMetrics                                            = "GetMetrics"
	opGetPosition                                           = "GetPosition"
	opUpdatePosition                                        = "UpdatePosition"
	opGetPositionConfiguration                              = "GetPositionConfiguration"
	opPutPositionConfiguration                              = "PutPositionConfiguration"
	opListPositionConfigurations                            = "ListPositionConfigurations"
	opGetPositionEstimate                                   = "GetPositionEstimate"
	opGetResourcePosition                                   = "GetResourcePosition"
	opUpdateResourcePosition                                = "UpdateResourcePosition"
	opCreateWirelessGatewayTask                             = "CreateWirelessGatewayTask"
	opGetWirelessGatewayTask                                = "GetWirelessGatewayTask"
	opDeleteWirelessGatewayTask                             = "DeleteWirelessGatewayTask"
	opCreateWirelessGatewayTaskDefinition                   = "CreateWirelessGatewayTaskDefinition"
	opGetWirelessGatewayTaskDefinition                      = "GetWirelessGatewayTaskDefinition"
	opListWirelessGatewayTaskDefinitions                    = "ListWirelessGatewayTaskDefinitions"
	opDeleteWirelessGatewayTaskDefinition                   = "DeleteWirelessGatewayTaskDefinition"
	opGetWirelessGatewayCertificate                         = "GetWirelessGatewayCertificate"
	opGetWirelessGatewayFirmwareInformation                 = "GetWirelessGatewayFirmwareInformation"
	opGetWirelessGatewayStatistics                          = "GetWirelessGatewayStatistics"
	opDisassociateWirelessGatewayFromCertificate            = "DisassociateWirelessGatewayFromCertificate"
	opDisassociateWirelessGatewayFromThing                  = "DisassociateWirelessGatewayFromThing"
	opUpdateWirelessGateway                                 = "UpdateWirelessGateway"
	opGetWirelessDeviceStatistics                           = "GetWirelessDeviceStatistics"
	opSendDataToWirelessDevice                              = "SendDataToWirelessDevice"
	opTestWirelessDevice                                    = "TestWirelessDevice"
	opDeregisterWirelessDevice                              = "DeregisterWirelessDevice"
	opUpdateWirelessDevice                                  = "UpdateWirelessDevice"
	opDisassociateWirelessDeviceFromThing                   = "DisassociateWirelessDeviceFromThing"
	opDeleteQueuedMessages                                  = "DeleteQueuedMessages"
	opListQueuedMessages                                    = "ListQueuedMessages"
	opStartWirelessDeviceImportTask                         = "StartWirelessDeviceImportTask"
	opStartSingleWirelessDeviceImportTask                   = "StartSingleWirelessDeviceImportTask"
	opGetWirelessDeviceImportTask                           = "GetWirelessDeviceImportTask"
	opDeleteWirelessDeviceImportTask                        = "DeleteWirelessDeviceImportTask"
	opUpdateWirelessDeviceImportTask                        = "UpdateWirelessDeviceImportTask"
	opListWirelessDeviceImportTasks                         = "ListWirelessDeviceImportTasks"
	opListDevicesForWirelessDeviceImportTask                = "ListDevicesForWirelessDeviceImportTask"
	opGetPartnerAccount                                     = "GetPartnerAccount"
	opListPartnerAccounts                                   = "ListPartnerAccounts"
	opDisassociateAwsAccountFromPartnerAccount              = "DisassociateAwsAccountFromPartnerAccount"
	opUpdatePartnerAccount                                  = "UpdatePartnerAccount"
	opUpdateDestination                                     = "UpdateDestination"
	opGetServiceEndpoint                                    = "GetServiceEndpoint"

	// Destination operation name constants (used in GetSupportedOperations, routing, and dispatch).
	opCreateDestination = "CreateDestination"
	opGetDestination    = "GetDestination"
	opListDestinations  = "ListDestinations"
	opDeleteDestination = "DeleteDestination"

	// path sub-segment constants.
	pathSubSession = "session"
)

const (
	iotwirelessService       = "iotwireless"
	iotwirelessMatchPriority = 86
)

// Handler is the HTTP handler for the IoT Wireless REST API.
type Handler struct {
	Backend       StorageBackend
	AccountID     string
	DefaultRegion string
}

// NewHandler creates a new IoT Wireless handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Reset clears the handler's backend state, returning it to a pristine condition.
func (h *Handler) Reset() {
	if r, ok := h.Backend.(interface{ Reset() }); ok {
		r.Reset()
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "IoTWireless" }

// GetSupportedOperations returns the list of supported IoT Wireless operations.
func (h *Handler) GetSupportedOperations() []string { //nolint:funlen // exhaustive ops list
	return []string{
		"CreateWirelessDevice",
		"GetWirelessDevice",
		"ListWirelessDevices",
		"DeleteWirelessDevice",
		"UpdateWirelessDevice",
		"DeregisterWirelessDevice",
		"GetWirelessDeviceStatistics",
		"SendDataToWirelessDevice",
		"TestWirelessDevice",
		"DisassociateWirelessDeviceFromThing",
		"DeleteQueuedMessages",
		"ListQueuedMessages",
		"CreateWirelessGateway",
		"GetWirelessGateway",
		"ListWirelessGateways",
		"DeleteWirelessGateway",
		"UpdateWirelessGateway",
		"GetWirelessGatewayCertificate",
		"GetWirelessGatewayFirmwareInformation",
		"GetWirelessGatewayStatistics",
		"GetWirelessGatewayTask",
		"DeleteWirelessGatewayTask",
		"CreateWirelessGatewayTask",
		"DisassociateWirelessGatewayFromCertificate",
		"DisassociateWirelessGatewayFromThing",
		"CreateWirelessGatewayTaskDefinition",
		"GetWirelessGatewayTaskDefinition",
		"ListWirelessGatewayTaskDefinitions",
		"DeleteWirelessGatewayTaskDefinition",
		"CreateServiceProfile",
		"GetServiceProfile",
		"ListServiceProfiles",
		"DeleteServiceProfile",
		opCreateDestination,
		opGetDestination,
		opListDestinations,
		opDeleteDestination,
		opUpdateDestination,
		opTagResource,
		opUntagResource,
		opListTagsForResource,
		opAssociateAwsAccountWithPartnerAccount,
		opAssociateMulticastGroupWithFuotaTask,
		opAssociateWirelessDeviceWithFuotaTask,
		opAssociateWirelessDeviceWithMulticastGroup,
		opAssociateWirelessDeviceWithThing,
		opAssociateWirelessGatewayWithCertificate,
		opAssociateWirelessGatewayWithThing,
		opCancelMulticastGroupSession,
		"CreateDeviceProfile",
		"GetDeviceProfile",
		"ListDeviceProfiles",
		"DeleteDeviceProfile",
		"CreateFuotaTask",
		"GetFuotaTask",
		"ListFuotaTasks",
		"DeleteFuotaTask",
		"UpdateFuotaTask",
		"StartFuotaTask",
		"DisassociateMulticastGroupFromFuotaTask",
		"DisassociateWirelessDeviceFromFuotaTask",
		"CreateMulticastGroup",
		"GetMulticastGroup",
		"ListMulticastGroups",
		"DeleteMulticastGroup",
		"UpdateMulticastGroup",
		"GetMulticastGroupSession",
		"StartMulticastGroupSession",
		"ListMulticastGroupsByFuotaTask",
		"SendDataToMulticastGroup",
		"StartBulkAssociateWirelessDeviceWithMulticastGroup",
		"StartBulkDisassociateWirelessDeviceFromMulticastGroup",
		"DisassociateWirelessDeviceFromMulticastGroup",
		"CreateNetworkAnalyzerConfiguration",
		"GetNetworkAnalyzerConfiguration",
		"ListNetworkAnalyzerConfigurations",
		"DeleteNetworkAnalyzerConfiguration",
		"UpdateNetworkAnalyzerConfiguration",
		"GetEventConfigurationByResourceTypes",
		"UpdateEventConfigurationByResourceTypes",
		"ListEventConfigurations",
		"GetResourceEventConfiguration",
		"UpdateResourceEventConfiguration",
		"GetLogLevelsByResourceTypes",
		"UpdateLogLevelsByResourceTypes",
		"ResetAllResourceLogLevels",
		"GetResourceLogLevel",
		"PutResourceLogLevel",
		"ResetResourceLogLevel",
		"GetMetricConfiguration",
		"UpdateMetricConfiguration",
		"GetMetrics",
		"GetPosition",
		"UpdatePosition",
		"GetPositionConfiguration",
		"PutPositionConfiguration",
		"ListPositionConfigurations",
		"GetPositionEstimate",
		"GetResourcePosition",
		"UpdateResourcePosition",
		"GetWirelessDeviceImportTask",
		"DeleteWirelessDeviceImportTask",
		"UpdateWirelessDeviceImportTask",
		"ListWirelessDeviceImportTasks",
		"ListDevicesForWirelessDeviceImportTask",
		"StartWirelessDeviceImportTask",
		"StartSingleWirelessDeviceImportTask",
		"GetPartnerAccount",
		"ListPartnerAccounts",
		"DisassociateAwsAccountFromPartnerAccount",
		"UpdatePartnerAccount",
		"GetServiceEndpoint",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return iotwirelessService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this IoT Wireless instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// RouteMatcher returns a function that matches IoT Wireless REST API requests.
// All paths are disambiguated via the SigV4 credential-scope service name to
// prevent mis-routing with other REST-JSON services that share similar paths.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		for _, prefix := range []string{
			"/" + pathBaseWirelessDevices,
			"/" + pathBaseWirelessGateways,
			"/" + pathBaseServiceProfiles,
			"/" + pathBaseDestinations,
			"/" + pathBaseDeviceProfiles,
			"/" + pathBaseFuotaTasks,
			"/" + pathBaseMulticastGroups,
			"/" + pathBasePartnerAccounts,
			"/" + pathBaseNetworkAnalyzerConfigs,
			"/" + pathBaseEventConfigsResourceTypes,
			"/" + pathBaseEventConfigs,
			"/" + pathBaseLogLevels,
			"/" + pathBaseMetricConfiguration,
			"/" + pathBaseMetrics,
			"/" + pathBasePositions,
			"/" + pathBasePositionConfigurations,
			"/" + pathBasePositionEstimate,
			"/" + pathBaseResourcePositions,
			"/" + pathBaseWirelessGatewayTaskDefs,
			"/" + pathBaseWirelessDeviceImportTask,
			"/" + pathBaseWirelessDeviceImportTasks,
			"/" + pathBaseSingleWirelessDeviceImport,
			"/" + pathBaseServiceEndpoint,
		} {
			if path == prefix || strings.HasPrefix(path, prefix+"/") {
				return httputils.ExtractServiceFromRequest(c.Request()) == iotwirelessService
			}
		}

		if strings.HasPrefix(path, "/tags/") {
			return httputils.ExtractServiceFromRequest(c.Request()) == iotwirelessService
		}

		return false
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return iotwirelessMatchPriority }

// ExtractOperation extracts the IoT Wireless operation name from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _ := parseIoTWirelessPath(c.Request().Method, c.Request().URL.Path)

	return op
}

// ExtractResource extracts the resource ID from the URL path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	_, resource := parseIoTWirelessPath(c.Request().Method, c.Request().URL.Path)

	return resource
}

// Handler returns the Echo handler function for IoT Wireless requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		method := c.Request().Method
		path := c.Request().URL.Path

		op, resource := parseIoTWirelessPath(method, path)
		if op == "" {
			return writeError(c, http.StatusNotFound, "resource not found")
		}

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "iotwireless: failed to read request body", "error", err)

			return writeError(c, http.StatusInternalServerError, "failed to read request body")
		}

		log.DebugContext(ctx, "iotwireless request", "op", op, "resource", resource)

		return h.dispatch(c, op, resource, body, c.Request().URL.Query())
	}
}

const (
	// maxPathParts is the maximum number of path segments to split when parsing IoT Wireless paths.
	maxPathParts = 3
	// idSegmentIndex is the index of the resource ID segment in the split path.
	idSegmentIndex = 2

	// path base segment constants used in route matching and path parsing.
	pathBaseWirelessDevices            = "wireless-devices"
	pathBaseWirelessGateways           = "wireless-gateways"
	pathBaseServiceProfiles            = "service-profiles"
	pathBaseDestinations               = "destinations"
	pathBaseDeviceProfiles             = "device-profiles"
	pathBaseFuotaTasks                 = "fuota-tasks"
	pathBaseMulticastGroups            = "multicast-groups"
	pathBasePartnerAccounts            = "partner-accounts"
	pathBaseNetworkAnalyzerConfigs     = "network-analyzer-configurations"
	pathBaseEventConfigsResourceTypes  = "event-configurations-resource-types"
	pathBaseEventConfigs               = "event-configurations"
	pathBaseLogLevels                  = "log-levels"
	pathBaseMetricConfiguration        = "metric-configuration"
	pathBaseMetrics                    = "metrics"
	pathBasePositions                  = "positions"
	pathBasePositionConfigurations     = "position-configurations"
	pathBasePositionEstimate           = "position-estimate"
	pathBaseResourcePositions          = "resource-positions"
	pathBaseWirelessGatewayTaskDefs    = "wireless-gateway-task-definitions"
	pathBaseWirelessDeviceImportTask   = "wireless_device_import_task"
	pathBaseWirelessDeviceImportTasks  = "wireless_device_import_tasks"
	pathBaseSingleWirelessDeviceImport = "wireless_single_device_import_task"
	pathBaseServiceEndpoint            = "service-endpoint"
)

// parseIoTWirelessPath maps a method+path to an operation and resource identifier.
func parseIoTWirelessPath(method, path string) (string, string) {
	// Strip leading slash and split into at most maxPathParts segments.
	trimmed := strings.TrimPrefix(path, "/")
	parts := strings.SplitN(trimmed, "/", maxPathParts)

	base := parts[0]
	hasID := len(parts) >= idSegmentIndex && parts[1] != ""

	// Handle /tags/{ResourceArn}
	if base == "tags" {
		if !hasID {
			return "", ""
		}

		arnEncoded := strings.Join(parts[1:], "/")

		switch method {
		case http.MethodGet:
			return opListTagsForResource, arnEncoded
		case http.MethodPost:
			return opTagResource, arnEncoded
		case http.MethodDelete:
			return opUntagResource, arnEncoded
		}

		return "", ""
	}

	id := ""
	if hasID {
		id = parts[1]
	}

	subPath := ""
	if len(parts) == maxPathParts {
		subPath = parts[2]
	}

	return parseIoTWirelessBase(method, base, id, subPath, hasID)
}

// parseIoTWirelessBase dispatches path parsing based on the first path segment.
func parseIoTWirelessBase(method, base, id, subPath string, hasID bool) (string, string) {
	if op, resource := parseIoTWirelessCoreGroup(method, base, id, subPath, hasID); op != "" {
		return op, resource
	}

	return parseIoTWirelessExtGroup(method, base, id, hasID)
}

// parseIoTWirelessCoreGroup handles core resource path segments.
func parseIoTWirelessCoreGroup(method, base, id, subPath string, hasID bool) (string, string) {
	switch base {
	case pathBaseWirelessDevices:
		return parseWirelessDevicePath(method, id, subPath, hasID)
	case pathBaseWirelessGateways:
		return parseWirelessGatewayPath(method, id, subPath, hasID)
	case pathBaseServiceProfiles:
		return parseCollectionPath(method, "ServiceProfile", hasID, id)
	case pathBaseDestinations:
		return parseDestinationPath(method, id, hasID)
	case pathBaseDeviceProfiles:
		return parseCollectionPath(method, "DeviceProfile", hasID, id)
	case pathBaseFuotaTasks:
		return parseFuotaTaskPath(method, id, subPath, hasID)
	case pathBaseMulticastGroups:
		return parseMulticastGroupPath(method, id, subPath, hasID)
	case pathBasePartnerAccounts:
		return parsePartnerAccountPath(method, id, hasID)
	case pathBaseNetworkAnalyzerConfigs:
		return parseNetworkAnalyzerPath(method, id, hasID)
	case pathBaseEventConfigsResourceTypes:
		return parseEventConfigsResourceTypesPath(method)
	case pathBaseEventConfigs:
		return parseEventConfigsPath(method, id, hasID)
	case pathBaseLogLevels:
		return parseLogLevelsPath(method, id, hasID)
	}

	return "", ""
}

// parseIoTWirelessExtGroup handles extended path segments (metrics, position, tasks, etc.).
// parseIoTWirelessExtGroup handles extended path segments (metrics, position, tasks, etc.).
func parseIoTWirelessExtGroup(method, base, id string, hasID bool) (string, string) {
	if op, res := parseIoTWirelessMetricsAndPosition(method, base, id, hasID); op != "" {
		return op, res
	}

	return parseIoTWirelessTasksAndImport(method, base, id, hasID)
}

// parseIoTWirelessMetricsAndPosition handles metrics and position paths.
func parseIoTWirelessMetricsAndPosition(method, base, id string, hasID bool) (string, string) {
	switch base {
	case pathBaseMetricConfiguration:
		return parseMetricConfigPath(method)
	case pathBaseMetrics:
		if method == http.MethodPost {
			return opGetMetrics, ""
		}
	case pathBasePositions:
		return parsePositionsPath(method, id, hasID)
	case pathBasePositionConfigurations:
		return parsePositionConfigPath(method, id, hasID)
	case pathBasePositionEstimate:
		if method == http.MethodPost {
			return opGetPositionEstimate, ""
		}
	case pathBaseResourcePositions:
		return parseResourcePositionsPath(method, id, hasID)
	}

	return "", ""
}

// parseIoTWirelessTasksAndImport handles gateway task definitions, import tasks, and service endpoint.
func parseIoTWirelessTasksAndImport(method, base, id string, hasID bool) (string, string) {
	switch base {
	case pathBaseWirelessGatewayTaskDefs:
		return parseGatewayTaskDefPath(method, id, hasID)
	case pathBaseWirelessDeviceImportTask:
		return parseImportTaskPath(method, id, hasID)
	case pathBaseWirelessDeviceImportTasks:
		if method == http.MethodGet {
			return opListWirelessDeviceImportTasks, ""
		}
	case pathBaseSingleWirelessDeviceImport:
		if method == http.MethodPost {
			return opStartSingleWirelessDeviceImportTask, ""
		}
	case pathBaseServiceEndpoint:
		if method == http.MethodGet {
			return opGetServiceEndpoint, ""
		}
	}

	return "", ""
}

// parseMetricConfigPath routes metric configuration paths.
func parseMetricConfigPath(method string) (string, string) {
	switch method {
	case http.MethodGet:
		return opGetMetricConfiguration, ""
	case http.MethodPut:
		return opUpdateMetricConfiguration, ""
	}

	return "", ""
}

// parsePositionsPath routes positions paths.
func parsePositionsPath(method, id string, hasID bool) (string, string) {
	if !hasID {
		return "", ""
	}

	switch method {
	case http.MethodGet:
		return opGetPosition, id
	case http.MethodPut:
		return opUpdatePosition, id
	}

	return "", ""
}

// parseResourcePositionsPath routes resource positions paths.
func parseResourcePositionsPath(method, id string, hasID bool) (string, string) {
	if !hasID {
		return "", ""
	}

	switch method {
	case http.MethodGet:
		return opGetResourcePosition, id
	case http.MethodPut:
		return opUpdateResourcePosition, id
	}

	return "", ""
}

// parseDestinationPath handles destinations sub-path routing including UpdateDestination.
func parseDestinationPath(method, id string, hasID bool) (string, string) {
	if hasID {
		switch method {
		case http.MethodGet:
			return opGetDestination, id
		case http.MethodDelete:
			return opDeleteDestination, id
		case http.MethodPatch:
			return opUpdateDestination, id
		}

		return "", ""
	}

	switch method {
	case http.MethodPost:
		return opCreateDestination, ""
	case http.MethodGet:
		return opListDestinations, ""
	}

	return "", ""
}

// parsePartnerAccountPath handles partner-accounts sub-path routing.
func parsePartnerAccountPath(method, id string, hasID bool) (string, string) {
	if !hasID {
		if method == http.MethodGet {
			return opListPartnerAccounts, ""
		}

		return "", ""
	}

	switch method {
	case http.MethodPut:
		return opAssociateAwsAccountWithPartnerAccount, id
	case http.MethodGet:
		return opGetPartnerAccount, id
	case http.MethodDelete:
		return opDisassociateAwsAccountFromPartnerAccount, id
	case http.MethodPatch:
		return opUpdatePartnerAccount, id
	}

	return "", ""
}

// parseNetworkAnalyzerPath handles network-analyzer-configurations routing.
func parseNetworkAnalyzerPath(method, id string, hasID bool) (string, string) {
	if !hasID {
		switch method {
		case http.MethodPost:
			return opCreateNetworkAnalyzerConfiguration, ""
		case http.MethodGet:
			return opListNetworkAnalyzerConfigurations, ""
		}

		return "", ""
	}

	switch method {
	case http.MethodGet:
		return opGetNetworkAnalyzerConfiguration, id
	case http.MethodDelete:
		return opDeleteNetworkAnalyzerConfiguration, id
	case http.MethodPatch:
		return opUpdateNetworkAnalyzerConfiguration, id
	}

	return "", ""
}

// parseEventConfigsResourceTypesPath handles event-configurations-resource-types routing.
func parseEventConfigsResourceTypesPath(method string) (string, string) {
	switch method {
	case http.MethodGet:
		return opGetEventConfigurationByResourceTypes, ""
	case http.MethodPost:
		return opUpdateEventConfigurationByResourceTypes, ""
	}

	return "", ""
}

// parseEventConfigsPath handles event-configurations routing.
func parseEventConfigsPath(method, id string, hasID bool) (string, string) {
	if !hasID {
		if method == http.MethodGet {
			return opListEventConfigurations, ""
		}

		return "", ""
	}

	switch method {
	case http.MethodGet:
		return opGetResourceEventConfiguration, id
	case http.MethodPatch:
		return opUpdateResourceEventConfiguration, id
	}

	return "", ""
}

// parseLogLevelsPath handles log-levels routing.
func parseLogLevelsPath(method, id string, hasID bool) (string, string) {
	if !hasID {
		switch method {
		case http.MethodGet:
			return opGetLogLevelsByResourceTypes, ""
		case http.MethodPost:
			return opUpdateLogLevelsByResourceTypes, ""
		case http.MethodDelete:
			return opResetAllResourceLogLevels, ""
		}

		return "", ""
	}

	switch method {
	case http.MethodGet:
		return opGetResourceLogLevel, id
	case http.MethodPut:
		return opPutResourceLogLevel, id
	case http.MethodDelete:
		return opResetResourceLogLevel, id
	}

	return "", ""
}

// parsePositionConfigPath handles position-configurations routing.
func parsePositionConfigPath(method, id string, hasID bool) (string, string) {
	if !hasID {
		if method == http.MethodGet {
			return opListPositionConfigurations, ""
		}

		return "", ""
	}

	switch method {
	case http.MethodGet:
		return opGetPositionConfiguration, id
	case http.MethodPut:
		return opPutPositionConfiguration, id
	}

	return "", ""
}

// parseGatewayTaskDefPath handles wireless-gateway-task-definitions routing.
func parseGatewayTaskDefPath(method, id string, hasID bool) (string, string) {
	if !hasID {
		switch method {
		case http.MethodPost:
			return opCreateWirelessGatewayTaskDefinition, ""
		case http.MethodGet:
			return opListWirelessGatewayTaskDefinitions, ""
		}

		return "", ""
	}

	switch method {
	case http.MethodGet:
		return opGetWirelessGatewayTaskDefinition, id
	case http.MethodDelete:
		return opDeleteWirelessGatewayTaskDefinition, id
	}

	return "", ""
}

// parseImportTaskPath handles wireless_device_import_task routing.
func parseImportTaskPath(method, id string, hasID bool) (string, string) {
	if !hasID {
		switch method {
		case http.MethodPost:
			return opStartWirelessDeviceImportTask, ""
		case http.MethodGet:
			return opListDevicesForWirelessDeviceImportTask, ""
		}

		return "", ""
	}

	switch method {
	case http.MethodGet:
		return opGetWirelessDeviceImportTask, id
	case http.MethodDelete:
		return opDeleteWirelessDeviceImportTask, id
	case http.MethodPatch:
		return opUpdateWirelessDeviceImportTask, id
	}

	return "", ""
}

// parseWirelessDevicePath handles wireless-devices sub-path routing.
func parseWirelessDevicePath(method, id, subPath string, hasID bool) (string, string) {
	if hasID {
		if op := parseWirelessDeviceSubPath(method, subPath); op != "" {
			return op, id
		}
	}

	return parseCollectionPath(method, "WirelessDevice", hasID, id)
}

// parseWirelessDeviceSubPath routes sub-paths of a wireless device resource.
func parseWirelessDeviceSubPath(method, subPath string) string {
	switch subPath {
	case "thing":
		switch method {
		case http.MethodPut:
			return opAssociateWirelessDeviceWithThing
		case http.MethodDelete:
			return opDisassociateWirelessDeviceFromThing
		}
	case "statistics":
		if method == http.MethodGet {
			return opGetWirelessDeviceStatistics
		}
	case "data":
		return parseWirelessDeviceDataPath(method)
	case "test":
		if method == http.MethodPost {
			return opTestWirelessDevice
		}
	case "deregister":
		if method == http.MethodPatch {
			return opDeregisterWirelessDevice
		}
	case "":
		if method == http.MethodPatch {
			return opUpdateWirelessDevice
		}
	}

	return ""
}

// parseWirelessDeviceDataPath routes data-related paths for a wireless device.
func parseWirelessDeviceDataPath(method string) string {
	switch method {
	case http.MethodPost:
		return opSendDataToWirelessDevice
	case http.MethodDelete:
		return opDeleteQueuedMessages
	case http.MethodGet:
		return opListQueuedMessages
	}

	return ""
}

// parseWirelessGatewayPath handles wireless-gateways sub-path routing.
//
// parseWirelessGatewayPath handles wireless-gateways sub-path routing.
func parseWirelessGatewayPath(method, id, subPath string, hasID bool) (string, string) {
	if hasID {
		if op := parseWirelessGatewaySubPath(method, subPath); op != "" {
			return op, id
		}
	}

	return parseCollectionPath(method, "WirelessGateway", hasID, id)
}

// parseWirelessGatewaySubPath routes sub-paths of a wireless gateway resource.
func parseWirelessGatewaySubPath(method, subPath string) string {
	switch subPath {
	case "certificate":
		return parseGatewayCertPath(method)
	case "thing":
		return parseGatewayThingPath(method)
	case "firmware-information":
		if method == http.MethodGet {
			return opGetWirelessGatewayFirmwareInformation
		}
	case "statistics":
		if method == http.MethodGet {
			return opGetWirelessGatewayStatistics
		}
	case "tasks":
		return parseGatewayTasksPath(method)
	case "":
		if method == http.MethodPatch {
			return opUpdateWirelessGateway
		}
	}

	return ""
}

// parseGatewayCertPath routes gateway certificate paths.
func parseGatewayCertPath(method string) string {
	switch method {
	case http.MethodPut:
		return opAssociateWirelessGatewayWithCertificate
	case http.MethodGet:
		return opGetWirelessGatewayCertificate
	case http.MethodDelete:
		return opDisassociateWirelessGatewayFromCertificate
	}

	return ""
}

// parseGatewayThingPath routes gateway thing paths.
func parseGatewayThingPath(method string) string {
	switch method {
	case http.MethodPut:
		return opAssociateWirelessGatewayWithThing
	case http.MethodDelete:
		return opDisassociateWirelessGatewayFromThing
	}

	return ""
}

// parseGatewayTasksPath routes gateway tasks paths.
func parseGatewayTasksPath(method string) string {
	switch method {
	case http.MethodPost:
		return opCreateWirelessGatewayTask
	case http.MethodGet:
		return opGetWirelessGatewayTask
	case http.MethodDelete:
		return opDeleteWirelessGatewayTask
	}

	return ""
}

// parseFuotaTaskPath handles fuota-tasks sub-path routing.
func parseFuotaTaskPath(method, id, subPath string, hasID bool) (string, string) {
	if !hasID {
		return parseCollectionPath(method, "FuotaTask", hasID, id)
	}

	if op := parseFuotaTaskSubPath(method, id, subPath); op != "" {
		return op, id
	}

	return parseCollectionPath(method, "FuotaTask", hasID, id)
}

// parseFuotaTaskSubPath routes sub-paths of a FUOTA task resource.
func parseFuotaTaskSubPath(method, id, subPath string) string {
	switch {
	case subPath == pathBaseMulticastGroups && method == http.MethodPut:
		return opAssociateMulticastGroupWithFuotaTask
	case subPath == pathBaseMulticastGroups && method == http.MethodGet:
		return opListMulticastGroupsByFuotaTask
	case strings.HasPrefix(subPath, "multicast-groups/") && method == http.MethodDelete:
		return opDisassociateMulticastGroupFromFuotaTask
	case subPath == pathBaseWirelessDevices && method == http.MethodPut:
		return opAssociateWirelessDeviceWithFuotaTask
	case strings.HasPrefix(subPath, "wireless-devices/") && method == http.MethodDelete:
		return opDisassociateWirelessDeviceFromFuotaTask
	case subPath == "" && method == http.MethodPut:
		return opStartFuotaTask
	case subPath == "" && method == http.MethodPatch:
		return opUpdateFuotaTask
	}

	_ = id // suppress unused warning

	return ""
}

// parseMulticastGroupPath handles multicast-groups sub-path routing.
func parseMulticastGroupPath(method, id, subPath string, hasID bool) (string, string) {
	if !hasID {
		switch method {
		case http.MethodPost:
			return opCreateMulticastGroup, ""
		case http.MethodGet:
			return opListMulticastGroups, ""
		}

		return "", ""
	}

	if op := parseMulticastGroupSubPath(method, subPath); op != "" {
		return op, id
	}

	// Use parseCollectionPath for default CRUD on the resource itself.
	if subPath == "" {
		switch method {
		case http.MethodGet:
			return opGetMulticastGroup, id
		case http.MethodDelete:
			return opDeleteMulticastGroup, id
		case http.MethodPatch:
			return opUpdateMulticastGroup, id
		}
	}

	return "", ""
}

// parseMulticastGroupSubPath routes sub-paths of a multicast group resource.
func parseMulticastGroupSubPath(method, subPath string) string {
	if op := parseMulticastWirelessDeviceSubPath(method, subPath); op != "" {
		return op
	}

	return parseMulticastSessionBulkSubPath(method, subPath)
}

// parseMulticastWirelessDeviceSubPath routes wireless device association sub-paths.
func parseMulticastWirelessDeviceSubPath(method, subPath string) string {
	switch {
	case subPath == pathBaseWirelessDevices && method == http.MethodPut:
		return opAssociateWirelessDeviceWithMulticastGroup
	case strings.HasPrefix(subPath, "wireless-devices/") && method == http.MethodDelete:
		return opDisassociateWirelessDeviceFromMulticastGroup
	case subPath == "data" && method == http.MethodPost:
		return opSendDataToMulticastGroup
	}

	return ""
}

// parseMulticastSessionBulkSubPath routes session and bulk operation sub-paths.
func parseMulticastSessionBulkSubPath(method, subPath string) string {
	switch {
	case subPath == pathSubSession && method == http.MethodDelete:
		return opCancelMulticastGroupSession
	case subPath == pathSubSession && method == http.MethodPut:
		return opStartMulticastGroupSession
	case subPath == pathSubSession && method == http.MethodGet:
		return opGetMulticastGroupSession
	case subPath == "bulk" && method == http.MethodPatch:
		return opStartBulkAssociateWirelessDeviceWithMulticastGroup
	case subPath == "bulk" && method == http.MethodPost:
		return opStartBulkDisassociateWirelessDeviceFromMulticastGroup
	}

	return ""
}

// parseCollectionPath handles standard CRUD routing for a resource collection.
func parseCollectionPath(method, resourceType string, hasID bool, id string) (string, string) {
	if !hasID {
		switch method {
		case http.MethodPost:
			return "Create" + resourceType, ""
		case http.MethodGet:
			return "List" + resourceType + "s", ""
		}

		return "", ""
	}

	switch method {
	case http.MethodGet:
		return "Get" + resourceType, id
	case http.MethodDelete:
		return "Delete" + resourceType, id
	}

	return "", ""
}

// dispatch routes to the appropriate handler based on the operation name.
func (h *Handler) dispatch(c *echo.Context, op, resource string, body []byte, query url.Values) error {
	if handled, result := h.dispatchCoreOps(c, op, resource, body); handled {
		return result
	}

	if handled, result := h.dispatchTagOps(c, op, resource, body, query); handled {
		return result
	}

	return writeError(c, http.StatusNotFound, "unknown operation")
}

// dispatchCoreOps routes all non-tag operations.
func (h *Handler) dispatchCoreOps(c *echo.Context, op, resource string, body []byte) (bool, error) {
	if handled, result := h.dispatchWirelessDevice(c, op, resource, body); handled {
		return true, result
	}

	if handled, result := h.dispatchWirelessGateway(c, op, resource, body); handled {
		return true, result
	}

	if handled, result := h.dispatchServiceProfile(c, op, resource, body); handled {
		return true, result
	}

	if handled, result := h.dispatchDestination(c, op, resource, body); handled {
		return true, result
	}

	if handled, result := h.dispatchNewOps(c, op, resource, body); handled {
		return true, result
	}

	if handled, result := h.dispatchMulticastOps(c, op, resource, body); handled {
		return true, result
	}

	if handled, result := h.dispatchNetworkAnalyzerOps(c, op, resource, body); handled {
		return true, result
	}

	return h.dispatchExtendedOpsGroup(c, op, resource, body)
}

// dispatchExtendedOpsGroup routes metrics, position, gateway task, import task, and partner ops.
func (h *Handler) dispatchExtendedOpsGroup(c *echo.Context, op, resource string, body []byte) (bool, error) {
	if handled, result := h.dispatchMetricsAndLogOps(c, op, resource, body); handled {
		return true, result
	}

	if handled, result := h.dispatchPositionOps(c, op, resource); handled {
		return true, result
	}

	if handled, result := h.dispatchGatewayTaskOps(c, op, resource, body); handled {
		return true, result
	}

	if handled, result := h.dispatchImportTaskOps(c, op, resource); handled {
		return true, result
	}

	return h.dispatchPartnerAndMiscOps(c, op, resource, body)
}

// dispatchTagOps handles resource tagging operations.
func (h *Handler) dispatchTagOps(c *echo.Context, op, resource string, body []byte, query url.Values) (bool, error) {
	switch op {
	case opListTagsForResource:
		return true, h.listTagsForResource(c, resource)
	case opTagResource:
		return true, h.tagResource(c, resource, body)
	case opUntagResource:
		return true, h.untagResource(c, resource, query)
	}

	return false, nil
}

// dispatchMulticastOps handles multicast group operations.
func (h *Handler) dispatchMulticastOps(c *echo.Context, op, resource string, body []byte) (bool, error) {
	if handled, result := h.dispatchMulticastGroupCRUDOps(c, op, resource); handled {
		return true, result
	}

	return h.dispatchMulticastAssocOps(c, op, resource, body)
}

// dispatchMulticastGroupCRUDOps handles multicast group CRUD and session operations.
func (h *Handler) dispatchMulticastGroupCRUDOps(c *echo.Context, op, resource string) (bool, error) {
	switch op {
	case opCreateMulticastGroup:
		return true, h.createMulticastGroup(c)
	case opGetMulticastGroup:
		return true, h.getMulticastGroup(c, resource)
	case opListMulticastGroups:
		return true, h.listMulticastGroups(c)
	case opDeleteMulticastGroup:
		return true, h.deleteMulticastGroup(c, resource)
	case opUpdateMulticastGroup:
		return true, h.updateMulticastGroup(c, resource)
	case opGetMulticastGroupSession:
		return true, h.getMulticastGroupSession(c, resource)
	case opStartMulticastGroupSession:
		return true, h.startMulticastGroupSession(c, resource)
	case opListMulticastGroupsByFuotaTask:
		return true, h.listMulticastGroupsByFuotaTask(c, resource)
	case opSendDataToMulticastGroup:
		return true, h.sendDataToMulticastGroup(c, resource)
	}

	return false, nil
}

// dispatchMulticastAssocOps handles multicast group association/disassociation and FUOTA task operations.
func (h *Handler) dispatchMulticastAssocOps(c *echo.Context, op, resource string, _ []byte) (bool, error) {
	switch op {
	case opStartBulkAssociateWirelessDeviceWithMulticastGroup,
		opStartBulkDisassociateWirelessDeviceFromMulticastGroup:
		c.Response().WriteHeader(http.StatusNoContent)

		return true, nil
	case opDisassociateWirelessDeviceFromMulticastGroup:
		return true, h.disassociateWirelessDeviceFromMulticastGroup(c, resource, "")
	case opDisassociateMulticastGroupFromFuotaTask:
		return true, h.disassociateMulticastGroupFromFuotaTask(c, resource, "")
	case opDisassociateWirelessDeviceFromFuotaTask:
		return true, h.disassociateWirelessDeviceFromFuotaTask(c, resource, "")
	case opStartFuotaTask:
		return true, h.startFuotaTask(c, resource)
	case opUpdateFuotaTask:
		return true, h.updateFuotaTask(c, resource)
	}

	return false, nil
}

// dispatchNetworkAnalyzerOps handles network analyzer configuration operations.
func (h *Handler) dispatchNetworkAnalyzerOps(c *echo.Context, op, resource string, _ []byte) (bool, error) {
	switch op {
	case opCreateNetworkAnalyzerConfiguration:
		return true, h.createNetworkAnalyzerConfiguration(c)
	case opGetNetworkAnalyzerConfiguration:
		return true, h.getNetworkAnalyzerConfiguration(c, resource)
	case opListNetworkAnalyzerConfigurations:
		return true, h.listNetworkAnalyzerConfigurations(c)
	case opDeleteNetworkAnalyzerConfiguration:
		return true, h.deleteNetworkAnalyzerConfiguration(c, resource)
	case opUpdateNetworkAnalyzerConfiguration:
		return true, h.updateNetworkAnalyzerConfiguration(c, resource)
	case opGetEventConfigurationByResourceTypes:
		return true, h.getEventConfigurationByResourceTypes(c)
	case opUpdateEventConfigurationByResourceTypes:
		return true, h.updateEventConfigurationByResourceTypes(c)
	case opListEventConfigurations:
		return true, h.listEventConfigurations(c)
	case opGetResourceEventConfiguration:
		return true, h.getResourceEventConfiguration(c, resource)
	case opUpdateResourceEventConfiguration:
		return true, h.updateResourceEventConfiguration(c, resource)
	}

	return false, nil
}

// dispatchMetricsAndLogOps handles metrics, log level, and event config operations.
func (h *Handler) dispatchMetricsAndLogOps(c *echo.Context, op, resource string, _ []byte) (bool, error) {
	switch op {
	case opGetLogLevelsByResourceTypes:
		return true, h.getLogLevelsByResourceTypes(c)
	case opUpdateLogLevelsByResourceTypes:
		return true, h.updateLogLevelsByResourceTypes(c)
	case opResetAllResourceLogLevels:
		return true, h.resetAllResourceLogLevels(c)
	case opGetResourceLogLevel:
		return true, h.getResourceLogLevel(c, resource)
	case opPutResourceLogLevel:
		return true, h.putResourceLogLevel(c, resource)
	case opResetResourceLogLevel:
		return true, h.resetResourceLogLevel(c, resource)
	case opGetMetricConfiguration:
		return true, h.getMetricConfiguration(c)
	case opUpdateMetricConfiguration:
		c.Response().WriteHeader(http.StatusNoContent)

		return true, nil
	case opGetMetrics:
		return true, h.getMetrics(c)
	}

	return false, nil
}

// dispatchPositionOps handles position and location operations.
func (h *Handler) dispatchPositionOps(c *echo.Context, op, resource string) (bool, error) {
	switch op {
	case opGetPosition:
		return true, h.getPosition(c, resource)
	case opUpdatePosition:
		return true, h.updatePosition(c, resource)
	case opGetPositionConfiguration:
		return true, h.getPositionConfiguration(c, resource)
	case opPutPositionConfiguration:
		return true, h.putPositionConfiguration(c, resource)
	case opListPositionConfigurations:
		return true, h.listPositionConfigurations(c)
	case opGetPositionEstimate:
		return true, h.getPositionEstimate(c)
	case opGetResourcePosition:
		return true, h.getResourcePosition(c, resource)
	case opUpdateResourcePosition:
		var req map[string]any

		body := readStubBody(c)
		_ = json.Unmarshal(body, &req)
		_ = h.Backend.UpdatePosition(resource, req)

		c.Response().WriteHeader(http.StatusNoContent)

		return true, nil
	}

	return false, nil
}

// dispatchGatewayTaskOps handles gateway task and task definition operations.
func (h *Handler) dispatchGatewayTaskOps(c *echo.Context, op, resource string, _ []byte) (bool, error) {
	switch op {
	case opCreateWirelessGatewayTask:
		return true, h.createWirelessGatewayTask(c, resource)
	case opGetWirelessGatewayTask:
		return true, h.getWirelessGatewayTask(c, resource)
	case opDeleteWirelessGatewayTask:
		return true, h.deleteWirelessGatewayTask(c, resource)
	case opCreateWirelessGatewayTaskDefinition:
		return true, h.createWirelessGatewayTaskDefinition(c)
	case opGetWirelessGatewayTaskDefinition:
		return true, h.getWirelessGatewayTaskDefinition(c, resource)
	case opListWirelessGatewayTaskDefinitions:
		return true, h.listWirelessGatewayTaskDefinitions(c)
	case opDeleteWirelessGatewayTaskDefinition:
		return true, h.deleteWirelessGatewayTaskDefinition(c, resource)
	}

	return false, nil
}

// dispatchImportTaskOps handles wireless device import task operations.
func (h *Handler) dispatchImportTaskOps(c *echo.Context, op, resource string) (bool, error) {
	switch op {
	case opStartWirelessDeviceImportTask:
		return true, h.startWirelessDeviceImportTask(c)
	case opStartSingleWirelessDeviceImportTask:
		return true, h.startSingleWirelessDeviceImportTask(c)
	case opGetWirelessDeviceImportTask:
		return true, h.getWirelessDeviceImportTask(c, resource)
	case opDeleteWirelessDeviceImportTask:
		return true, h.deleteWirelessDeviceImportTask(c, resource)
	case opUpdateWirelessDeviceImportTask:
		return true, h.updateWirelessDeviceImportTask(c, resource)
	case opListWirelessDeviceImportTasks:
		return true, h.listWirelessDeviceImportTasks(c)
	case opListDevicesForWirelessDeviceImportTask:
		return true, h.listDevicesForWirelessDeviceImportTask(c)
	}

	return false, nil
}

// dispatchPartnerAndMiscOps handles partner account, destination update, and misc operations.
func (h *Handler) dispatchPartnerAndMiscOps(c *echo.Context, op, resource string, _ []byte) (bool, error) {
	if handled, result := h.dispatchPartnerOps(c, op, resource); handled {
		return true, result
	}

	return h.dispatchGatewayDeviceMiscOps(c, op, resource)
}

// dispatchPartnerOps handles partner account and destination operations.
func (h *Handler) dispatchPartnerOps(c *echo.Context, op, resource string) (bool, error) {
	switch op {
	case opGetPartnerAccount:
		return true, h.getPartnerAccount(c, resource)
	case opListPartnerAccounts:
		return true, h.listPartnerAccounts(c)
	case opDisassociateAwsAccountFromPartnerAccount:
		return true, h.disassociateAwsAccountFromPartnerAccount(c, resource)
	case opUpdatePartnerAccount:
		return true, h.updatePartnerAccount(c, resource)
	case opUpdateDestination:
		return true, h.updateDestination(c, resource)
	case opGetServiceEndpoint:
		return true, h.getServiceEndpoint(c)
	case opGetWirelessGatewayCertificate:
		return true, h.getWirelessGatewayCertificate(c, resource)
	case opGetWirelessGatewayFirmwareInformation:
		return true, h.getWirelessGatewayFirmwareInformation(c, resource)
	case opGetWirelessGatewayStatistics:
		return true, h.getWirelessGatewayStatistics(c, resource)
	case opDisassociateWirelessGatewayFromCertificate:
		return true, h.disassociateWirelessGatewayFromCertificate(c, resource)
	}

	return false, nil
}

// dispatchGatewayDeviceMiscOps handles wireless gateway and device miscellaneous operations.
func (h *Handler) dispatchGatewayDeviceMiscOps(c *echo.Context, op, resource string) (bool, error) {
	switch op {
	case opDisassociateWirelessGatewayFromThing:
		return true, h.disassociateWirelessGatewayFromThing(c, resource)
	case opUpdateWirelessGateway:
		return true, h.updateWirelessGateway(c, resource)
	case opGetWirelessDeviceStatistics:
		return true, h.getWirelessDeviceStatistics(c, resource)
	case opSendDataToWirelessDevice:
		return true, h.sendDataToWirelessDevice(c, resource)
	case opTestWirelessDevice:
		return true, h.testWirelessDevice(c, resource)
	case opDeregisterWirelessDevice:
		return true, h.deregisterWirelessDevice(c, resource)
	case opUpdateWirelessDevice:
		return true, h.updateWirelessDevice(c, resource)
	case opDisassociateWirelessDeviceFromThing:
		return true, h.disassociateWirelessDeviceFromThing(c, resource)
	case opDeleteQueuedMessages:
		return true, h.deleteQueuedMessages(c, resource)
	case opListQueuedMessages:
		return true, h.listQueuedMessages(c, resource)
	}

	return false, nil
}

// dispatchWirelessDevice handles wireless device operations.
func (h *Handler) dispatchWirelessDevice(c *echo.Context, op, resource string, body []byte) (bool, error) {
	switch op {
	case "CreateWirelessDevice":
		return true, h.createWirelessDevice(c, body)
	case "GetWirelessDevice":
		return true, h.getWirelessDevice(c, resource)
	case "ListWirelessDevices":
		return true, h.listWirelessDevices(c)
	case "DeleteWirelessDevice":
		return true, h.deleteWirelessDevice(c, resource)
	}

	return false, nil
}

// dispatchWirelessGateway handles wireless gateway operations.
func (h *Handler) dispatchWirelessGateway(c *echo.Context, op, resource string, body []byte) (bool, error) {
	switch op {
	case "CreateWirelessGateway":
		return true, h.createWirelessGateway(c, body)
	case "GetWirelessGateway":
		return true, h.getWirelessGateway(c, resource)
	case "ListWirelessGateways":
		return true, h.listWirelessGateways(c)
	case "DeleteWirelessGateway":
		return true, h.deleteWirelessGateway(c, resource)
	}

	return false, nil
}

// dispatchServiceProfile handles service profile operations.
func (h *Handler) dispatchServiceProfile(c *echo.Context, op, resource string, body []byte) (bool, error) {
	switch op {
	case "CreateServiceProfile":
		return true, h.createServiceProfile(c, body)
	case "GetServiceProfile":
		return true, h.getServiceProfile(c, resource)
	case "ListServiceProfiles":
		return true, h.listServiceProfiles(c)
	case "DeleteServiceProfile":
		return true, h.deleteServiceProfile(c, resource)
	}

	return false, nil
}

// dispatchDestination handles destination operations.
func (h *Handler) dispatchDestination(c *echo.Context, op, resource string, body []byte) (bool, error) {
	switch op {
	case opCreateDestination:
		return true, h.createDestination(c, body)
	case opGetDestination:
		return true, h.getDestination(c, resource)
	case opListDestinations:
		return true, h.listDestinations(c)
	case opDeleteDestination:
		return true, h.deleteDestination(c, resource)
	}

	return false, nil
}

// dispatchNewOps handles operations added in this implementation.
func (h *Handler) dispatchNewOps(c *echo.Context, op, resource string, body []byte) (bool, error) {
	if handled, err := h.dispatchNewCRUDOps(c, op, resource, body); handled {
		return true, err
	}

	return h.dispatchAssociationOps(c, op, resource, body)
}

// dispatchNewCRUDOps handles CRUD operations for DeviceProfile and FuotaTask.
func (h *Handler) dispatchNewCRUDOps(c *echo.Context, op, resource string, body []byte) (bool, error) {
	switch op {
	case "CreateDeviceProfile":
		return true, h.createDeviceProfile(c, body)
	case "GetDeviceProfile":
		return true, h.getDeviceProfile(c, resource)
	case "ListDeviceProfiles":
		return true, h.listDeviceProfiles(c)
	case "DeleteDeviceProfile":
		return true, h.deleteDeviceProfile(c, resource)
	case "CreateFuotaTask":
		return true, h.createFuotaTask(c, body)
	case "GetFuotaTask":
		return true, h.getFuotaTask(c, resource)
	case "ListFuotaTasks":
		return true, h.listFuotaTasks(c)
	case "DeleteFuotaTask":
		return true, h.deleteFuotaTask(c, resource)
	}

	return false, nil
}

// dispatchAssociationOps handles AWS IoT Wireless resource-association operations.
func (h *Handler) dispatchAssociationOps(c *echo.Context, op, resource string, body []byte) (bool, error) {
	switch op {
	case opAssociateAwsAccountWithPartnerAccount:
		return true, h.associateAwsAccountWithPartnerAccount(c, resource, body)
	case opAssociateMulticastGroupWithFuotaTask:
		return true, h.associateMulticastGroupWithFuotaTask(c, resource, body)
	case opAssociateWirelessDeviceWithFuotaTask:
		return true, h.associateWirelessDeviceWithFuotaTask(c, resource, body)
	case opAssociateWirelessDeviceWithMulticastGroup:
		return true, h.associateWirelessDeviceWithMulticastGroup(c, resource, body)
	case opAssociateWirelessDeviceWithThing:
		return true, h.associateWirelessDeviceWithThing(c, resource, body)
	case opAssociateWirelessGatewayWithCertificate:
		return true, h.associateWirelessGatewayWithCertificate(c, resource, body)
	case opAssociateWirelessGatewayWithThing:
		return true, h.associateWirelessGatewayWithThing(c, resource, body)
	case opCancelMulticastGroupSession:
		return true, h.cancelMulticastGroupSession(c, resource)
	}

	return false, nil
}

// JSON request/response types.

type createWirelessDeviceRequest struct {
	Tags            map[string]string `json:"Tags"`
	Name            string            `json:"Name"`
	Type            string            `json:"Type"`
	DestinationName string            `json:"DestinationName"`
	Description     string            `json:"Description"`
}

type createWirelessDeviceResponse struct {
	Arn string `json:"Arn"`
	ID  string `json:"Id"`
}

type wirelessDeviceEntry struct {
	Arn             string `json:"Arn"`
	ID              string `json:"Id"`
	Name            string `json:"Name"`
	Type            string `json:"Type"`
	DestinationName string `json:"DestinationName"`
	Description     string `json:"Description"`
}

type listWirelessDevicesResponse struct {
	WirelessDeviceList []wirelessDeviceEntry `json:"WirelessDeviceList"`
}

type createWirelessGatewayRequest struct {
	Tags        map[string]string `json:"Tags"`
	Name        string            `json:"Name"`
	Description string            `json:"Description"`
}

type createWirelessGatewayResponse struct {
	Arn string `json:"Arn"`
	ID  string `json:"Id"`
}

type wirelessGatewayEntry struct {
	Arn         string `json:"Arn"`
	ID          string `json:"Id"`
	Name        string `json:"Name"`
	Description string `json:"Description"`
}

type listWirelessGatewaysResponse struct {
	WirelessGatewayList []wirelessGatewayEntry `json:"WirelessGatewayList"`
}

type createServiceProfileRequest struct {
	Tags map[string]string `json:"Tags"`
	Name string            `json:"Name"`
}

type createServiceProfileResponse struct {
	Arn string `json:"Arn"`
	ID  string `json:"Id"`
}

type serviceProfileEntry struct {
	Arn  string `json:"Arn"`
	ID   string `json:"Id"`
	Name string `json:"Name"`
}

type listServiceProfilesResponse struct {
	ServiceProfileList []serviceProfileEntry `json:"ServiceProfileList"`
}

type createDestinationRequest struct {
	Tags           map[string]string `json:"Tags"`
	Name           string            `json:"Name"`
	Expression     string            `json:"Expression"`
	ExpressionType string            `json:"ExpressionType"`
	RoleArn        string            `json:"RoleArn"`
	Description    string            `json:"Description"`
}

type destinationEntry struct {
	Arn            string `json:"Arn"`
	Name           string `json:"Name"`
	Expression     string `json:"Expression"`
	ExpressionType string `json:"ExpressionType"`
	RoleArn        string `json:"RoleArn"`
	Description    string `json:"Description"`
}

type listDestinationsResponse struct {
	DestinationList []destinationEntry `json:"DestinationList"`
}

type tagResourceRequest struct {
	Tags map[string]string `json:"Tags"`
}

type listTagsResponse struct {
	Tags map[string]string `json:"Tags"`
}

type errorResponse struct {
	Message string `json:"Message"`
}

// --- Request/response types for new operations ---

type createDeviceProfileRequest struct {
	Tags map[string]string `json:"Tags"`
	Name string            `json:"Name"`
}

type createDeviceProfileResponse struct {
	Arn string `json:"Arn"`
	ID  string `json:"Id"`
}

type createFuotaTaskRequest struct {
	Tags                map[string]string `json:"Tags"`
	Name                string            `json:"Name"`
	Description         string            `json:"Description"`
	FirmwareUpdateImage string            `json:"FirmwareUpdateImage"`
	FirmwareUpdateRole  string            `json:"FirmwareUpdateRole"`
}

type createFuotaTaskResponse struct {
	Arn string `json:"Arn"`
	ID  string `json:"Id"`
}

type deviceProfileEntry struct {
	Arn  string `json:"Arn"`
	ID   string `json:"Id"`
	Name string `json:"Name"`
}

type listDeviceProfilesResponse struct {
	DeviceProfileList []deviceProfileEntry `json:"DeviceProfileList"`
}

type fuotaTaskEntry struct {
	Arn                 string `json:"Arn"`
	ID                  string `json:"Id"`
	Name                string `json:"Name"`
	Description         string `json:"Description,omitempty"`
	FirmwareUpdateImage string `json:"FirmwareUpdateImage,omitempty"`
	FirmwareUpdateRole  string `json:"FirmwareUpdateRole,omitempty"`
}

type listFuotaTasksResponse struct {
	FuotaTaskList []fuotaTaskEntry `json:"FuotaTaskList"`
}

type associatePartnerAccountRequest struct {
	Tags map[string]string `json:"Tags"`
}

type associatePartnerAccountResponse struct {
	Arn string `json:"Arn"`
}

type associateMulticastGroupRequest struct {
	MulticastGroupID string `json:"MulticastGroupId"`
}

type associateWirelessDeviceWithFuotaRequest struct {
	WirelessDeviceID string `json:"WirelessDeviceId"`
}

type associateWirelessDeviceWithMulticastRequest struct {
	WirelessDeviceID string `json:"WirelessDeviceId"`
}

type associateWirelessDeviceWithThingRequest struct {
	ThingArn string `json:"ThingArn"`
}

type associateWirelessGatewayWithCertificateRequest struct {
	IotCertificateID string `json:"IotCertificateId"`
}

type associateWirelessGatewayWithCertificateResponse struct {
	IotCertificateArn string `json:"IotCertificateArn"`
}

type associateWirelessGatewayWithThingRequest struct {
	ThingArn string `json:"ThingArn"`
}

// writeError writes a JSON error response.
func writeError(c *echo.Context, status int, message string) error {
	c.Response().Header().Set("Content-Type", "application/json")
	c.Response().WriteHeader(status)

	_ = json.NewEncoder(c.Response()).Encode(errorResponse{Message: message})

	return nil
}

// writeJSON writes a JSON response with the given status code.
func writeJSON(c *echo.Context, status int, v any) error {
	c.Response().Header().Set("Content-Type", "application/json")
	c.Response().WriteHeader(status)

	_ = json.NewEncoder(c.Response()).Encode(v)

	return nil
}

// decodeNotFoundError maps not-found sentinel errors to 404.
func isNotFound(err error) bool {
	return errors.Is(err, ErrDeviceNotFound) ||
		errors.Is(err, ErrGatewayNotFound) ||
		errors.Is(err, ErrServiceProfileNotFound) ||
		errors.Is(err, ErrDestinationNotFound) ||
		errors.Is(err, ErrDeviceProfileNotFound) ||
		errors.Is(err, ErrFuotaTaskNotFound) ||
		errors.Is(err, ErrMulticastGroupNotFound) ||
		errors.Is(err, ErrNetworkAnalyzerConfigNotFound) ||
		errors.Is(err, ErrImportTaskNotFound) ||
		errors.Is(err, ErrGatewayTaskDefNotFound) ||
		errors.Is(err, ErrMulticastGroupSessionNotFound)
}

// handleError writes an appropriate HTTP error response for a backend error.
func handleError(c *echo.Context, err error) error {
	if isNotFound(err) {
		return writeError(c, http.StatusNotFound, err.Error())
	}

	if errors.Is(err, ErrValidation) {
		return writeError(c, http.StatusBadRequest, err.Error())
	}

	return writeError(c, http.StatusInternalServerError, err.Error())
}

// decodeARN URL-decodes an ARN path segment.
func decodeARN(encoded string) string {
	decoded, err := url.PathUnescape(encoded)
	if err != nil {
		return encoded
	}

	return decoded
}

// --- Wireless Device handlers ---

func (h *Handler) createWirelessDevice(c *echo.Context, body []byte) error {
	var req createWirelessDeviceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	d, err := h.Backend.CreateWirelessDevice(
		h.AccountID, h.DefaultRegion,
		req.Name, req.Type, req.DestinationName, req.Description, req.Tags,
	)
	if err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusCreated, createWirelessDeviceResponse{Arn: d.ARN, ID: d.ID})
}

func (h *Handler) getWirelessDevice(c *echo.Context, id string) error {
	d, err := h.Backend.GetWirelessDevice(h.AccountID, h.DefaultRegion, id)
	if err != nil {
		return handleError(c, err)
	}

	return writeJSON(c, http.StatusOK, wirelessDeviceEntry{
		Arn:             d.ARN,
		ID:              d.ID,
		Name:            d.Name,
		Type:            d.Type,
		DestinationName: d.DestinationName,
		Description:     d.Description,
	})
}

func (h *Handler) listWirelessDevices(c *echo.Context) error {
	devices := h.Backend.ListWirelessDevices(h.AccountID, h.DefaultRegion)
	entries := make([]wirelessDeviceEntry, 0, len(devices))

	for _, d := range devices {
		entries = append(entries, wirelessDeviceEntry{
			Arn:             d.ARN,
			ID:              d.ID,
			Name:            d.Name,
			Type:            d.Type,
			DestinationName: d.DestinationName,
			Description:     d.Description,
		})
	}

	return writeJSON(c, http.StatusOK, listWirelessDevicesResponse{WirelessDeviceList: entries})
}

func (h *Handler) deleteWirelessDevice(c *echo.Context, id string) error {
	err := h.Backend.DeleteWirelessDevice(h.AccountID, h.DefaultRegion, id)
	if err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

// --- Wireless Gateway handlers ---

func (h *Handler) createWirelessGateway(c *echo.Context, body []byte) error {
	var req createWirelessGatewayRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	gw, err := h.Backend.CreateWirelessGateway(
		h.AccountID, h.DefaultRegion,
		req.Name, req.Description, req.Tags,
	)
	if err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusCreated, createWirelessGatewayResponse{Arn: gw.ARN, ID: gw.ID})
}

func (h *Handler) getWirelessGateway(c *echo.Context, id string) error {
	gw, err := h.Backend.GetWirelessGateway(h.AccountID, h.DefaultRegion, id)
	if err != nil {
		return handleError(c, err)
	}

	return writeJSON(c, http.StatusOK, wirelessGatewayEntry{
		Arn:         gw.ARN,
		ID:          gw.ID,
		Name:        gw.Name,
		Description: gw.Description,
	})
}

func (h *Handler) listWirelessGateways(c *echo.Context) error {
	gws := h.Backend.ListWirelessGateways(h.AccountID, h.DefaultRegion)
	entries := make([]wirelessGatewayEntry, 0, len(gws))

	for _, gw := range gws {
		entries = append(entries, wirelessGatewayEntry{
			Arn:         gw.ARN,
			ID:          gw.ID,
			Name:        gw.Name,
			Description: gw.Description,
		})
	}

	return writeJSON(c, http.StatusOK, listWirelessGatewaysResponse{WirelessGatewayList: entries})
}

func (h *Handler) deleteWirelessGateway(c *echo.Context, id string) error {
	err := h.Backend.DeleteWirelessGateway(h.AccountID, h.DefaultRegion, id)
	if err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

// --- Service Profile handlers ---

func (h *Handler) createServiceProfile(c *echo.Context, body []byte) error {
	var req createServiceProfileRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	sp, err := h.Backend.CreateServiceProfile(h.AccountID, h.DefaultRegion, req.Name, req.Tags)
	if err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusCreated, createServiceProfileResponse{Arn: sp.ARN, ID: sp.ID})
}

func (h *Handler) getServiceProfile(c *echo.Context, id string) error {
	sp, err := h.Backend.GetServiceProfile(h.AccountID, h.DefaultRegion, id)
	if err != nil {
		return handleError(c, err)
	}

	return writeJSON(c, http.StatusOK, serviceProfileEntry{
		Arn:  sp.ARN,
		ID:   sp.ID,
		Name: sp.Name,
	})
}

func (h *Handler) listServiceProfiles(c *echo.Context) error {
	profiles := h.Backend.ListServiceProfiles(h.AccountID, h.DefaultRegion)
	entries := make([]serviceProfileEntry, 0, len(profiles))

	for _, sp := range profiles {
		entries = append(entries, serviceProfileEntry{
			Arn:  sp.ARN,
			ID:   sp.ID,
			Name: sp.Name,
		})
	}

	return writeJSON(c, http.StatusOK, listServiceProfilesResponse{ServiceProfileList: entries})
}

func (h *Handler) deleteServiceProfile(c *echo.Context, id string) error {
	err := h.Backend.DeleteServiceProfile(h.AccountID, h.DefaultRegion, id)
	if err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

// --- Destination handlers ---

func (h *Handler) createDestination(c *echo.Context, body []byte) error {
	var req createDestinationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	dest, err := h.Backend.CreateDestination(
		h.AccountID, h.DefaultRegion,
		req.Name, req.Expression, req.ExpressionType, req.RoleArn, req.Description, req.Tags,
	)
	if err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusCreated, destinationEntry{
		Arn:            dest.ARN,
		Name:           dest.Name,
		Expression:     dest.Expression,
		ExpressionType: dest.ExpressionType,
		RoleArn:        dest.RoleArn,
		Description:    dest.Description,
	})
}

func (h *Handler) getDestination(c *echo.Context, name string) error {
	dest, err := h.Backend.GetDestination(h.AccountID, h.DefaultRegion, name)
	if err != nil {
		return handleError(c, err)
	}

	return writeJSON(c, http.StatusOK, destinationEntry{
		Arn:            dest.ARN,
		Name:           dest.Name,
		Expression:     dest.Expression,
		ExpressionType: dest.ExpressionType,
		RoleArn:        dest.RoleArn,
		Description:    dest.Description,
	})
}

func (h *Handler) listDestinations(c *echo.Context) error {
	dests := h.Backend.ListDestinations(h.AccountID, h.DefaultRegion)
	entries := make([]destinationEntry, 0, len(dests))

	for _, dest := range dests {
		entries = append(entries, destinationEntry{
			Arn:            dest.ARN,
			Name:           dest.Name,
			Expression:     dest.Expression,
			ExpressionType: dest.ExpressionType,
			RoleArn:        dest.RoleArn,
			Description:    dest.Description,
		})
	}

	return writeJSON(c, http.StatusOK, listDestinationsResponse{DestinationList: entries})
}

func (h *Handler) deleteDestination(c *echo.Context, name string) error {
	err := h.Backend.DeleteDestination(h.AccountID, h.DefaultRegion, name)
	if err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

// --- Tag handlers ---

func (h *Handler) listTagsForResource(c *echo.Context, arnEncoded string) error {
	arn := decodeARN(arnEncoded)

	tags, err := h.Backend.ListTagsForResource(arn)
	if err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusOK, listTagsResponse{Tags: tags})
}

func (h *Handler) tagResource(c *echo.Context, arnEncoded string, body []byte) error {
	arn := decodeARN(arnEncoded)

	var req tagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	if err := h.Backend.TagResource(arn, req.Tags); err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) untagResource(c *echo.Context, arnEncoded string, query url.Values) error {
	arn := decodeARN(arnEncoded)
	tagKeys := query["tagKeys"]

	if err := h.Backend.UntagResource(arn, tagKeys); err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

// --- Device Profile handlers ---

func (h *Handler) createDeviceProfile(c *echo.Context, body []byte) error {
	var req createDeviceProfileRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	dp, err := h.Backend.CreateDeviceProfile(h.AccountID, h.DefaultRegion, req.Name, req.Tags)
	if err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusCreated, createDeviceProfileResponse{Arn: dp.ARN, ID: dp.ID})
}

// --- FUOTA Task handlers ---

func (h *Handler) createFuotaTask(c *echo.Context, body []byte) error {
	var req createFuotaTaskRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	ft, err := h.Backend.CreateFuotaTask(
		h.AccountID, h.DefaultRegion,
		req.Name, req.Description, req.FirmwareUpdateImage, req.FirmwareUpdateRole,
		req.Tags,
	)
	if err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusCreated, createFuotaTaskResponse{Arn: ft.ARN, ID: ft.ID})
}

func (h *Handler) getFuotaTask(c *echo.Context, id string) error {
	ft, err := h.Backend.GetFuotaTask(h.AccountID, h.DefaultRegion, id)
	if err != nil {
		return handleError(c, err)
	}

	return writeJSON(c, http.StatusOK, fuotaTaskEntry{
		Arn:                 ft.ARN,
		ID:                  ft.ID,
		Name:                ft.Name,
		Description:         ft.Description,
		FirmwareUpdateImage: ft.FirmwareUpdateImage,
		FirmwareUpdateRole:  ft.FirmwareUpdateRole,
	})
}

func (h *Handler) listFuotaTasks(c *echo.Context) error {
	tasks := h.Backend.ListFuotaTasks(h.AccountID, h.DefaultRegion)
	entries := make([]fuotaTaskEntry, 0, len(tasks))

	for _, ft := range tasks {
		entries = append(entries, fuotaTaskEntry{
			Arn:                 ft.ARN,
			ID:                  ft.ID,
			Name:                ft.Name,
			Description:         ft.Description,
			FirmwareUpdateImage: ft.FirmwareUpdateImage,
			FirmwareUpdateRole:  ft.FirmwareUpdateRole,
		})
	}

	return writeJSON(c, http.StatusOK, listFuotaTasksResponse{FuotaTaskList: entries})
}

func (h *Handler) deleteFuotaTask(c *echo.Context, id string) error {
	err := h.Backend.DeleteFuotaTask(h.AccountID, h.DefaultRegion, id)
	if err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) getDeviceProfile(c *echo.Context, id string) error {
	dp, err := h.Backend.GetDeviceProfile(h.AccountID, h.DefaultRegion, id)
	if err != nil {
		return handleError(c, err)
	}

	return writeJSON(c, http.StatusOK, deviceProfileEntry{
		Arn:  dp.ARN,
		ID:   dp.ID,
		Name: dp.Name,
	})
}

func (h *Handler) listDeviceProfiles(c *echo.Context) error {
	profiles := h.Backend.ListDeviceProfiles(h.AccountID, h.DefaultRegion)
	entries := make([]deviceProfileEntry, 0, len(profiles))

	for _, dp := range profiles {
		entries = append(entries, deviceProfileEntry{
			Arn:  dp.ARN,
			ID:   dp.ID,
			Name: dp.Name,
		})
	}

	return writeJSON(c, http.StatusOK, listDeviceProfilesResponse{DeviceProfileList: entries})
}

func (h *Handler) deleteDeviceProfile(c *echo.Context, id string) error {
	err := h.Backend.DeleteDeviceProfile(h.AccountID, h.DefaultRegion, id)
	if err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

// --- Association handlers ---

func (h *Handler) associateAwsAccountWithPartnerAccount(c *echo.Context, partnerAccountID string, body []byte) error {
	var req associatePartnerAccountRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	arn, err := h.Backend.AssociateAwsAccountWithPartnerAccount(
		h.AccountID,
		h.DefaultRegion,
		partnerAccountID,
		req.Tags,
	)
	if err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusOK, associatePartnerAccountResponse{Arn: arn})
}

func (h *Handler) associateMulticastGroupWithFuotaTask(c *echo.Context, fuotaTaskID string, body []byte) error {
	var req associateMulticastGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	if err := h.Backend.AssociateMulticastGroupWithFuotaTask(fuotaTaskID, req.MulticastGroupID); err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) associateWirelessDeviceWithFuotaTask(c *echo.Context, fuotaTaskID string, body []byte) error {
	var req associateWirelessDeviceWithFuotaRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	if err := h.Backend.AssociateWirelessDeviceWithFuotaTask(fuotaTaskID, req.WirelessDeviceID); err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) associateWirelessDeviceWithMulticastGroup(
	c *echo.Context, multicastGroupID string, body []byte,
) error {
	var req associateWirelessDeviceWithMulticastRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	if err := h.Backend.AssociateWirelessDeviceWithMulticastGroup(multicastGroupID, req.WirelessDeviceID); err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) associateWirelessDeviceWithThing(c *echo.Context, wirelessDeviceID string, body []byte) error {
	var req associateWirelessDeviceWithThingRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	err := h.Backend.AssociateWirelessDeviceWithThing(h.AccountID, h.DefaultRegion, wirelessDeviceID, req.ThingArn)
	if err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) associateWirelessGatewayWithCertificate(c *echo.Context, gatewayID string, body []byte) error {
	var req associateWirelessGatewayWithCertificateRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	certARN, err := h.Backend.AssociateWirelessGatewayWithCertificate(
		h.AccountID, h.DefaultRegion, gatewayID, req.IotCertificateID,
	)
	if err != nil {
		return handleError(c, err)
	}

	return writeJSON(c, http.StatusOK, associateWirelessGatewayWithCertificateResponse{IotCertificateArn: certARN})
}

func (h *Handler) associateWirelessGatewayWithThing(c *echo.Context, gatewayID string, body []byte) error {
	var req associateWirelessGatewayWithThingRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	err := h.Backend.AssociateWirelessGatewayWithThing(h.AccountID, h.DefaultRegion, gatewayID, req.ThingArn)
	if err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) cancelMulticastGroupSession(c *echo.Context, multicastGroupID string) error {
	if err := h.Backend.CancelMulticastGroupSession(multicastGroupID); err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}
