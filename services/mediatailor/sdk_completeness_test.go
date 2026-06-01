package mediatailor_test

import (
	"testing"

	mediatailorsdk "github.com/aws/aws-sdk-go-v2/service/mediatailor"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/mediatailor"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// mediatailor client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := mediatailor.NewInMemoryBackend("000000000000", "us-east-1")
	h := mediatailor.NewHandler(backend)

	notImplemented := []string{
		"ConfigureLogsForChannel",
		"ConfigureLogsForPlaybackConfiguration",
		"CreateLiveSource",
		"CreatePrefetchSchedule",
		"CreateProgram",
		"DeleteChannelPolicy",
		"DeleteFunction",
		"DeleteLiveSource",
		"DeletePrefetchSchedule",
		"DeleteProgram",
		"DescribeLiveSource",
		"DescribeProgram",
		"GetChannelPolicy",
		"GetChannelSchedule",
		"GetFunction",
		"GetPrefetchSchedule",
		"ListAlerts",
		"ListFunctions",
		"ListLiveSources",
		"ListPrefetchSchedules",
		"PutChannelPolicy",
		"PutFunction",
		"UpdateLiveSource",
		"UpdateProgram",
	}

	sdkcheck.CheckCompleteness(t, &mediatailorsdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
