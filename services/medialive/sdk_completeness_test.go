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
		"CreateEventBridgeRuleTemplate",
		"CreateEventBridgeRuleTemplateGroup",
		"CreateNetwork",
		"CreatePartnerInput",
		"CreateSdiSource",
		"CreateSignalMap",
		"DeleteChannelPlacementGroup",
		"DeleteCloudWatchAlarmTemplate",
		"DeleteCloudWatchAlarmTemplateGroup",
		"DeleteEventBridgeRuleTemplate",
		"DeleteEventBridgeRuleTemplateGroup",
		"DeleteNetwork",
		"DeleteReservation",
		"DeleteSchedule",
		"DeleteSdiSource",
		"DeleteSignalMap",
		"DescribeAccountConfiguration",
		"DescribeChannelPlacementGroup",
		"DescribeInputDeviceThumbnail",
		"DescribeNetwork",
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
		"ListEventBridgeRuleTemplateGroups",
		"ListEventBridgeRuleTemplates",
		"ListMultiplexAlerts",
		"ListNetworks",
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
		"StartUpdateSignalMap",
		"StopInputDevice",
		"UpdateAccountConfiguration",
		"UpdateChannelClass",
		"UpdateChannelPlacementGroup",
		"UpdateCloudWatchAlarmTemplate",
		"UpdateCloudWatchAlarmTemplateGroup",
		"UpdateEventBridgeRuleTemplate",
		"UpdateEventBridgeRuleTemplateGroup",
		"UpdateNetwork",
		"UpdateReservation",
		"UpdateSdiSource",
	}

	sdkcheck.CheckCompleteness(
		t,
		&medialivestk.Client{},
		h.GetSupportedOperations(),
		notImplemented,
	)
}
