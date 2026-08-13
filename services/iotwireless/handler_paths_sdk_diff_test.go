package iotwireless_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
)

// sdkRouteCases is the authoritative method+path for every real iotwireless
// operation, extracted from iotwireless@v1.59.4 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in
// for any {Param} URI label -- the router does not validate ID shape, so the
// literal value doesn't matter here, only that the path matches Op.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"AssociateAwsAccountWithPartnerAccount", "POST", "/partner-accounts"},
		{"AssociateMulticastGroupWithFuotaTask", "PUT", "/fuota-tasks/PLACEHOLDER/multicast-group"},
		{"AssociateWirelessDeviceWithFuotaTask", "PUT", "/fuota-tasks/PLACEHOLDER/wireless-device"},
		{"AssociateWirelessDeviceWithMulticastGroup", "PUT", "/multicast-groups/PLACEHOLDER/wireless-device"},
		{"AssociateWirelessDeviceWithThing", "PUT", "/wireless-devices/PLACEHOLDER/thing"},
		{"AssociateWirelessGatewayWithCertificate", "PUT", "/wireless-gateways/PLACEHOLDER/certificate"},
		{"AssociateWirelessGatewayWithThing", "PUT", "/wireless-gateways/PLACEHOLDER/thing"},
		{"CancelMulticastGroupSession", "DELETE", "/multicast-groups/PLACEHOLDER/session"},
		{"CreateDestination", "POST", "/destinations"},
		{"CreateDeviceProfile", "POST", "/device-profiles"},
		{"CreateFuotaTask", "POST", "/fuota-tasks"},
		{"CreateMulticastGroup", "POST", "/multicast-groups"},
		{"CreateNetworkAnalyzerConfiguration", "POST", "/network-analyzer-configurations"},
		{"CreateServiceProfile", "POST", "/service-profiles"},
		{"CreateWirelessDevice", "POST", "/wireless-devices"},
		{"CreateWirelessGateway", "POST", "/wireless-gateways"},
		{"CreateWirelessGatewayTask", "POST", "/wireless-gateways/PLACEHOLDER/tasks"},
		{"CreateWirelessGatewayTaskDefinition", "POST", "/wireless-gateway-task-definitions"},
		{"DeleteDestination", "DELETE", "/destinations/PLACEHOLDER"},
		{"DeleteDeviceProfile", "DELETE", "/device-profiles/PLACEHOLDER"},
		{"DeleteFuotaTask", "DELETE", "/fuota-tasks/PLACEHOLDER"},
		{"DeleteMulticastGroup", "DELETE", "/multicast-groups/PLACEHOLDER"},
		{"DeleteNetworkAnalyzerConfiguration", "DELETE", "/network-analyzer-configurations/PLACEHOLDER"},
		{"DeleteQueuedMessages", "DELETE", "/wireless-devices/PLACEHOLDER/data"},
		{"DeleteServiceProfile", "DELETE", "/service-profiles/PLACEHOLDER"},
		{"DeleteWirelessDevice", "DELETE", "/wireless-devices/PLACEHOLDER"},
		{"DeleteWirelessDeviceImportTask", "DELETE", "/wireless_device_import_task/PLACEHOLDER"},
		{"DeleteWirelessGateway", "DELETE", "/wireless-gateways/PLACEHOLDER"},
		{"DeleteWirelessGatewayTask", "DELETE", "/wireless-gateways/PLACEHOLDER/tasks"},
		{"DeleteWirelessGatewayTaskDefinition", "DELETE", "/wireless-gateway-task-definitions/PLACEHOLDER"},
		{"DeregisterWirelessDevice", "PATCH", "/wireless-devices/PLACEHOLDER/deregister"},
		{"DisassociateAwsAccountFromPartnerAccount", "DELETE", "/partner-accounts/PLACEHOLDER"},
		{"DisassociateMulticastGroupFromFuotaTask", "DELETE", "/fuota-tasks/PLACEHOLDER/multicast-groups/PLACEHOLDER"},
		{"DisassociateWirelessDeviceFromFuotaTask", "DELETE", "/fuota-tasks/PLACEHOLDER/wireless-devices/PLACEHOLDER"},
		{
			"DisassociateWirelessDeviceFromMulticastGroup", "DELETE",
			"/multicast-groups/PLACEHOLDER/wireless-devices/PLACEHOLDER",
		},
		{"DisassociateWirelessDeviceFromThing", "DELETE", "/wireless-devices/PLACEHOLDER/thing"},
		{"DisassociateWirelessGatewayFromCertificate", "DELETE", "/wireless-gateways/PLACEHOLDER/certificate"},
		{"DisassociateWirelessGatewayFromThing", "DELETE", "/wireless-gateways/PLACEHOLDER/thing"},
		{"GetDestination", "GET", "/destinations/PLACEHOLDER"},
		{"GetDeviceProfile", "GET", "/device-profiles/PLACEHOLDER"},
		{"GetEventConfigurationByResourceTypes", "GET", "/event-configurations-resource-types"},
		{"GetFuotaTask", "GET", "/fuota-tasks/PLACEHOLDER"},
		{"GetLogLevelsByResourceTypes", "GET", "/log-levels"},
		{"GetMetricConfiguration", "GET", "/metric-configuration"},
		{"GetMetrics", "POST", "/metrics"},
		{"GetMulticastGroup", "GET", "/multicast-groups/PLACEHOLDER"},
		{"GetMulticastGroupSession", "GET", "/multicast-groups/PLACEHOLDER/session"},
		{"GetNetworkAnalyzerConfiguration", "GET", "/network-analyzer-configurations/PLACEHOLDER"},
		{"GetPartnerAccount", "GET", "/partner-accounts/PLACEHOLDER"},
		{"GetPosition", "GET", "/positions/PLACEHOLDER"},
		{"GetPositionConfiguration", "GET", "/position-configurations/PLACEHOLDER"},
		{"GetPositionEstimate", "POST", "/position-estimate"},
		{"GetResourceEventConfiguration", "GET", "/event-configurations/PLACEHOLDER"},
		{"GetResourceLogLevel", "GET", "/log-levels/PLACEHOLDER"},
		{"GetResourcePosition", "GET", "/resource-positions/PLACEHOLDER"},
		{"GetServiceEndpoint", "GET", "/service-endpoint"},
		{"GetServiceProfile", "GET", "/service-profiles/PLACEHOLDER"},
		{"GetWirelessDevice", "GET", "/wireless-devices/PLACEHOLDER"},
		{"GetWirelessDeviceImportTask", "GET", "/wireless_device_import_task/PLACEHOLDER"},
		{"GetWirelessDeviceStatistics", "GET", "/wireless-devices/PLACEHOLDER/statistics"},
		{"GetWirelessGateway", "GET", "/wireless-gateways/PLACEHOLDER"},
		{"GetWirelessGatewayCertificate", "GET", "/wireless-gateways/PLACEHOLDER/certificate"},
		{"GetWirelessGatewayFirmwareInformation", "GET", "/wireless-gateways/PLACEHOLDER/firmware-information"},
		{"GetWirelessGatewayStatistics", "GET", "/wireless-gateways/PLACEHOLDER/statistics"},
		{"GetWirelessGatewayTask", "GET", "/wireless-gateways/PLACEHOLDER/tasks"},
		{"GetWirelessGatewayTaskDefinition", "GET", "/wireless-gateway-task-definitions/PLACEHOLDER"},
		{"ListDestinations", "GET", "/destinations"},
		{"ListDeviceProfiles", "GET", "/device-profiles"},
		{"ListDevicesForWirelessDeviceImportTask", "GET", "/wireless_device_import_task"},
		{"ListEventConfigurations", "GET", "/event-configurations"},
		{"ListFuotaTasks", "GET", "/fuota-tasks"},
		{"ListMulticastGroups", "GET", "/multicast-groups"},
		{"ListMulticastGroupsByFuotaTask", "GET", "/fuota-tasks/PLACEHOLDER/multicast-groups"},
		{"ListNetworkAnalyzerConfigurations", "GET", "/network-analyzer-configurations"},
		{"ListPartnerAccounts", "GET", "/partner-accounts"},
		{"ListPositionConfigurations", "GET", "/position-configurations"},
		{"ListQueuedMessages", "GET", "/wireless-devices/PLACEHOLDER/data"},
		{"ListServiceProfiles", "GET", "/service-profiles"},
		{"ListTagsForResource", "GET", "/tags"},
		{"ListWirelessDeviceImportTasks", "GET", "/wireless_device_import_tasks"},
		{"ListWirelessDevices", "GET", "/wireless-devices"},
		{"ListWirelessGatewayTaskDefinitions", "GET", "/wireless-gateway-task-definitions"},
		{"ListWirelessGateways", "GET", "/wireless-gateways"},
		{"PutPositionConfiguration", "PUT", "/position-configurations/PLACEHOLDER"},
		{"PutResourceLogLevel", "PUT", "/log-levels/PLACEHOLDER"},
		{"ResetAllResourceLogLevels", "DELETE", "/log-levels"},
		{"ResetResourceLogLevel", "DELETE", "/log-levels/PLACEHOLDER"},
		{"SendDataToMulticastGroup", "POST", "/multicast-groups/PLACEHOLDER/data"},
		{"SendDataToWirelessDevice", "POST", "/wireless-devices/PLACEHOLDER/data"},
		{"StartBulkAssociateWirelessDeviceWithMulticastGroup", "PATCH", "/multicast-groups/PLACEHOLDER/bulk"},
		{"StartBulkDisassociateWirelessDeviceFromMulticastGroup", "POST", "/multicast-groups/PLACEHOLDER/bulk"},
		{"StartFuotaTask", "PUT", "/fuota-tasks/PLACEHOLDER"},
		{"StartMulticastGroupSession", "PUT", "/multicast-groups/PLACEHOLDER/session"},
		{"StartSingleWirelessDeviceImportTask", "POST", "/wireless_single_device_import_task"},
		{"StartWirelessDeviceImportTask", "POST", "/wireless_device_import_task"},
		{"TagResource", "POST", "/tags"},
		{"TestWirelessDevice", "POST", "/wireless-devices/PLACEHOLDER/test"},
		{"UntagResource", "DELETE", "/tags"},
		{"UpdateDestination", "PATCH", "/destinations/PLACEHOLDER"},
		{"UpdateEventConfigurationByResourceTypes", "PATCH", "/event-configurations-resource-types"},
		{"UpdateFuotaTask", "PATCH", "/fuota-tasks/PLACEHOLDER"},
		{"UpdateLogLevelsByResourceTypes", "POST", "/log-levels"},
		{"UpdateMetricConfiguration", "PUT", "/metric-configuration"},
		{"UpdateMulticastGroup", "PATCH", "/multicast-groups/PLACEHOLDER"},
		{"UpdateNetworkAnalyzerConfiguration", "PATCH", "/network-analyzer-configurations/PLACEHOLDER"},
		{"UpdatePartnerAccount", "PATCH", "/partner-accounts/PLACEHOLDER"},
		{"UpdatePosition", "PATCH", "/positions/PLACEHOLDER"},
		{"UpdateResourceEventConfiguration", "PATCH", "/event-configurations/PLACEHOLDER"},
		{"UpdateResourcePosition", "PATCH", "/resource-positions/PLACEHOLDER"},
		{"UpdateWirelessDevice", "PATCH", "/wireless-devices/PLACEHOLDER"},
		{"UpdateWirelessDeviceImportTask", "PATCH", "/wireless_device_import_task/PLACEHOLDER"},
		{"UpdateWirelessGateway", "PATCH", "/wireless-gateways/PLACEHOLDER"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real iotwireless op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts the route table resolves it to the right op. gopherstack-jqh2.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())

			got := h.ExtractOperation(c)
			if got != tc.op {
				t.Errorf("method=%s path=%s: got op %q, want %q", tc.method, tc.path, got, tc.op)
			}
		})
	}
}
