package iotwireless

import (
	"net/http"
	"strings"
)

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

	// Handle /tags (never /tags/{ResourceArn} — real AWS binds the resource
	// ARN as the "resourceArn" query parameter, not a path segment; see
	// dispatchTagOps, which reads it from the query instead of this
	// function's resource return value).
	if base == "tags" {
		switch method {
		case http.MethodGet:
			return opListTagsForResource, ""
		case http.MethodPost:
			return opTagResource, ""
		case http.MethodDelete:
			return opUntagResource, ""
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
// AssociateAwsAccountWithPartnerAccount binds to the bare collection path
// (POST /partner-accounts, partner account ID in the body's Sidewalk.AmazonId
// — never a path parameter), unlike Get/Update/Disassociate which all bind
// PartnerAccountId as a path parameter.
func parsePartnerAccountPath(method, id string, hasID bool) (string, string) {
	if !hasID {
		switch method {
		case http.MethodGet:
			return opListPartnerAccounts, ""
		case http.MethodPost:
			return opAssociateAwsAccountWithPartnerAccount, ""
		}

		return "", ""
	}

	switch method {
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
	case subPath == pathSubMulticastGroup && method == http.MethodPut:
		return opAssociateMulticastGroupWithFuotaTask
	case subPath == pathBaseMulticastGroups && method == http.MethodGet:
		return opListMulticastGroupsByFuotaTask
	case strings.HasPrefix(subPath, "multicast-groups/") && method == http.MethodDelete:
		return opDisassociateMulticastGroupFromFuotaTask
	case subPath == pathSubWirelessDevice && method == http.MethodPut:
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
	case subPath == pathSubWirelessDevice && method == http.MethodPut:
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
