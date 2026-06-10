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
		"CreateChannelPlacementGroup",
		"CreateNetwork",
		"CreatePartnerInput",
		"CreateSdiSource",
		"DeleteChannelPlacementGroup",
		"DeleteNetwork",
		"DeleteSchedule",
		"DeleteSdiSource",
		"DescribeAccountConfiguration",
		"DescribeChannelPlacementGroup",
		"DescribeInputDeviceThumbnail",
		"DescribeNetwork",
		"DescribeSchedule",
		"DescribeSdiSource",
		"DescribeThumbnails",
		"ListAlerts",
		"ListChannelPlacementGroups",
		"ListMultiplexAlerts",
		"ListNetworks",
		"ListSdiSources",
		"ListVersions",
		"RestartChannelPipelines",
		"StartDeleteMonitorDeployment",
		"StartInputDevice",
		"StartInputDeviceMaintenanceWindow",
		"StopInputDevice",
		"UpdateAccountConfiguration",
		"UpdateChannelClass",
		"UpdateChannelPlacementGroup",
		"UpdateNetwork",
		"UpdateSdiSource",
	}

	sdkcheck.CheckCompleteness(
		t,
		&medialivestk.Client{},
		h.GetSupportedOperations(),
		notImplemented,
	)
}
