package scheduler_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	schedulersdk "github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/services/scheduler"
)

// TestHandleREST_OversizedBodySurfacesInternalServerException drives a real
// scheduler client's CreateSchedule with a Description large enough to push
// the request body past httputils.MaxRequestBodyBytes (a real client can
// legitimately send this; aws-sdk-go-v2 imposes no client-side cap). Before
// this fix, handleREST's ReadBody-failure branch wrote a bare
// "internal server error" text/plain body -- the restjson1 error decoder
// (aws-sdk-go-v2@v1.43.4 aws/protocol/restjson.GetErrorInfo) cannot parse
// plain text, so the client saw smithy.GenericAPIError{Code:"UnknownError"}
// instead of the typed InternalServerException handleError's default branch
// already produces for every unmatched backend error (gopherstack-o7gx).
func TestHandleREST_OversizedBodySurfacesInternalServerException(t *testing.T) {
	t.Parallel()

	backend := scheduler.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestSchedulerClient(t, scheduler.NewHandler(backend))

	huge := string(bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1)))

	_, err := client.CreateSchedule(t.Context(), &schedulersdk.CreateScheduleInput{
		Name:               aws.String("oversized-body-schedule"),
		ScheduleExpression: aws.String("rate(1 hour)"),
		FlexibleTimeWindow: &types.FlexibleTimeWindow{Mode: types.FlexibleTimeWindowModeOff},
		Description:        aws.String(huge),
		Target: &types.Target{
			Arn:     aws.String("arn:aws:lambda:us-east-1:000000000000:function:test"),
			RoleArn: aws.String("arn:aws:iam::000000000000:role/test"),
		},
	}, func(o *schedulersdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InternalServerException", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}
