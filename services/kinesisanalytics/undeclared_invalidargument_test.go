package kinesisanalytics_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	kasdk "github.com/aws/aws-sdk-go-v2/service/kinesisanalytics"
	katypes "github.com/aws/aws-sdk-go-v2/service/kinesisanalytics/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesisanalytics"
)

// TestEmptyApplicationName_ReportsNotFound_NotInvalidArgument proves that an
// empty-but-present ApplicationName on DeleteApplication, DescribeApplication
// and StopApplication decodes to the real client's typed
// ResourceNotFoundException, not InvalidArgumentException -- none of these
// three operations' deserializers recognize InvalidArgumentException
// (aws-sdk-go-v2/service/kinesisanalytics/deserializers.go), so an emulator
// that pre-checks the empty string and returns that code leaves the client's
// typed InvalidArgumentException branch unreachable and the caller sees only
// a generic *smithy.GenericAPIError. The client-side required-field
// validator only rejects a nil *string, so aws.String("") reaches the
// handler unblocked.
func TestEmptyApplicationName_ReportsNotFound_NotInvalidArgument(t *testing.T) {
	t.Parallel()

	backend := kinesisanalytics.NewInMemoryBackend("us-east-1", "000000000000")
	h := kinesisanalytics.NewHandler(backend)
	h.AccountID = "000000000000"
	h.DefaultRegion = "us-east-1"

	client := newTestKASDKClient(t, h)
	ctx := t.Context()

	t.Run("delete", func(t *testing.T) {
		t.Parallel()

		_, err := client.DeleteApplication(ctx, &kasdk.DeleteApplicationInput{
			ApplicationName: aws.String(""),
			CreateTimestamp: aws.Time(time.Unix(0, 0).UTC()),
		})
		require.Error(t, err)

		var nf *katypes.ResourceNotFoundException
		require.ErrorAs(t, err, &nf, "expected typed ResourceNotFoundException, got %T: %v", err, err)
	})

	t.Run("describe", func(t *testing.T) {
		t.Parallel()

		_, err := client.DescribeApplication(ctx, &kasdk.DescribeApplicationInput{
			ApplicationName: aws.String(""),
		})
		require.Error(t, err)

		var nf *katypes.ResourceNotFoundException
		require.ErrorAs(t, err, &nf, "expected typed ResourceNotFoundException, got %T: %v", err, err)
	})

	t.Run("stop", func(t *testing.T) {
		t.Parallel()

		_, err := client.StopApplication(ctx, &kasdk.StopApplicationInput{
			ApplicationName: aws.String(""),
		})
		require.Error(t, err)

		var nf *katypes.ResourceNotFoundException
		require.ErrorAs(t, err, &nf, "expected typed ResourceNotFoundException, got %T: %v", err, err)
	})
}
