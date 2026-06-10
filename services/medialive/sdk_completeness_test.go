package medialive_test

import (
	"testing"

	medialivestk "github.com/aws/aws-sdk-go-v2/service/medialive"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/medialive"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// medialive client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := medialive.NewInMemoryBackend("000000000000", "us-east-1")
	h := medialive.NewHandler(backend)

	notImplemented := []string{
		"BatchDelete",
		"BatchStart",
		"BatchStop",
		"BatchUpdateSchedule",
		"CreateChannelPlacementGroup",
		"CreateCloudWatchAlarmTemplate",
		"CreateCloudWatchAlarmTemplateGroup",
		"CreateCluster",
		"CreateEventBridgeRuleTemplate",
		"CreateEventBridgeRuleTemplateGroup",
		"CreateMultiplex",
		"CreateMultiplexProgram",
		"CreateNetwork",
		"CreateNode",
		"CreateNodeRegistrationScript",
		"CreatePartnerInput",
		"CreateSdiSource",
		"CreateSignalMap",
		"DeleteChannelPlacementGroup",
		"DeleteCloudWatchAlarmTemplate",
		"DeleteCloudWatchAlarmTemplateGroup",
		"DeleteCluster",
		"DeleteEventBridgeRuleTemplate",
		"DeleteEventBridgeRuleTemplateGroup",
		"DeleteMultiplex",
		"DeleteMultiplexProgram",
		"DeleteNetwork",
		"DeleteNode",
		"DeleteReservation",
		"DeleteSchedule",
		"DeleteSdiSource",
		"DeleteSignalMap",
		"DescribeAccountConfiguration",
		"DescribeChannelPlacementGroup",
		"DescribeCluster",
		"DescribeInputDeviceThumbnail",
		"DescribeMultiplex",
		"DescribeMultiplexProgram",
		"DescribeNetwork",
		"DescribeNode",
		"DescribeOffering",
		"DescribeReservation",
		"DescribeSchedule",
		"DescribeSdiSource",
		"DescribeThumbnails",
		"GetCloudWatchAlarmTemplate",
		"GetCloudWatchAlarmTemplateGroup",
		"GetEventBridgeRuleTemplate",
		"GetEventBridgeRuleTemplateGroup",
		"GetSignalMap",
		"ListAlerts",
		"ListChannelPlacementGroups",
		"ListCloudWatchAlarmTemplateGroups",
		"ListCloudWatchAlarmTemplates",
		"ListClusterAlerts",
		"ListClusters",
		"ListEventBridgeRuleTemplateGroups",
		"ListEventBridgeRuleTemplates",
		"ListMultiplexAlerts",
		"ListMultiplexes",
		"ListMultiplexPrograms",
		"ListNetworks",
		"ListNodes",
		"ListOfferings",
		"ListReservations",
		"ListSdiSources",
		"ListSignalMaps",
		"ListVersions",
		"PurchaseOffering",
		"RestartChannelPipelines",
		"StartDeleteMonitorDeployment",
		"StartInputDevice",
		"StartInputDeviceMaintenanceWindow",
		"StartMonitorDeployment",
		"StartMultiplex",
		"StartUpdateSignalMap",
		"StopInputDevice",
		"StopMultiplex",
		"UpdateAccountConfiguration",
		"UpdateChannelClass",
		"UpdateChannelPlacementGroup",
		"UpdateCloudWatchAlarmTemplate",
		"UpdateCloudWatchAlarmTemplateGroup",
		"UpdateCluster",
		"UpdateEventBridgeRuleTemplate",
		"UpdateEventBridgeRuleTemplateGroup",
		"UpdateMultiplex",
		"UpdateMultiplexProgram",
		"UpdateNetwork",
		"UpdateNode",
		"UpdateNodeState",
		"UpdateReservation",
		"UpdateSdiSource",
	}

	sdkcheck.CheckCompleteness(t, &medialivestk.Client{}, h.GetSupportedOperations(), notImplemented)
}
