package iotwireless_test

import (
	"testing"

	iotwirelesssdk "github.com/aws/aws-sdk-go-v2/service/iotwireless"
	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/iotwireless"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// iotwireless client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := iotwireless.NewInMemoryBackend()
	h := iotwireless.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &iotwirelesssdk.Client{}, h.GetSupportedOperations(), []string{
		"CreateMulticastGroup",
		"CreateNetworkAnalyzerConfiguration",
		"CreateWirelessGatewayTask",
		"CreateWirelessGatewayTaskDefinition",
		"DeleteMulticastGroup",
		"DeleteNetworkAnalyzerConfiguration",
		"DeleteQueuedMessages",
		"DeleteWirelessDeviceImportTask",
		"DeleteWirelessGatewayTask",
		"DeleteWirelessGatewayTaskDefinition",
		"DeregisterWirelessDevice",
		"DisassociateAwsAccountFromPartnerAccount",
		"DisassociateMulticastGroupFromFuotaTask",
		"DisassociateWirelessDeviceFromFuotaTask",
		"DisassociateWirelessDeviceFromMulticastGroup",
		"DisassociateWirelessDeviceFromThing",
		"DisassociateWirelessGatewayFromCertificate",
		"DisassociateWirelessGatewayFromThing",
		"GetEventConfigurationByResourceTypes",
		"GetLogLevelsByResourceTypes",
		"GetMetricConfiguration",
		"GetMetrics",
		"GetMulticastGroup",
		"GetMulticastGroupSession",
		"GetNetworkAnalyzerConfiguration",
		"GetPartnerAccount",
		"GetPosition",
		"GetPositionConfiguration",
		"GetPositionEstimate",
		"GetResourceEventConfiguration",
		"GetResourceLogLevel",
		"GetResourcePosition",
		"GetServiceEndpoint",
		"GetWirelessDeviceImportTask",
		"GetWirelessDeviceStatistics",
		"GetWirelessGatewayCertificate",
		"GetWirelessGatewayFirmwareInformation",
		"GetWirelessGatewayStatistics",
		"GetWirelessGatewayTask",
		"GetWirelessGatewayTaskDefinition",
		"ListDevicesForWirelessDeviceImportTask",
		"ListEventConfigurations",
		"ListMulticastGroups",
		"ListMulticastGroupsByFuotaTask",
		"ListNetworkAnalyzerConfigurations",
		"ListPartnerAccounts",
		"ListPositionConfigurations",
		"ListQueuedMessages",
		"ListWirelessDeviceImportTasks",
		"ListWirelessGatewayTaskDefinitions",
		"PutPositionConfiguration",
		"PutResourceLogLevel",
		"ResetAllResourceLogLevels",
		"ResetResourceLogLevel",
		"SendDataToMulticastGroup",
		"SendDataToWirelessDevice",
		"StartBulkAssociateWirelessDeviceWithMulticastGroup",
		"StartBulkDisassociateWirelessDeviceFromMulticastGroup",
		"StartFuotaTask",
		"StartMulticastGroupSession",
		"StartSingleWirelessDeviceImportTask",
		"StartWirelessDeviceImportTask",
		"TestWirelessDevice",
		"UpdateDestination",
		"UpdateEventConfigurationByResourceTypes",
		"UpdateFuotaTask",
		"UpdateLogLevelsByResourceTypes",
		"UpdateMetricConfiguration",
		"UpdateMulticastGroup",
		"UpdateNetworkAnalyzerConfiguration",
		"UpdatePartnerAccount",
		"UpdatePosition",
		"UpdateResourceEventConfiguration",
		"UpdateResourcePosition",
		"UpdateWirelessDevice",
		"UpdateWirelessDeviceImportTask",
		"UpdateWirelessGateway",
	})
}
