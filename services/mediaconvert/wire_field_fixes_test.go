package mediaconvert_test

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	mediaconvertsdk "github.com/aws/aws-sdk-go-v2/service/mediaconvert"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediaconvert"
)

// TestCreateQueue_RealSDKHasNoServiceOverrides covers
// gopherstack-wksweep-mc-1: the real CreateQueueInput and Queue types
// (mediaconvert@v1.97.1 api_op_CreateQueue.go) have no ServiceOverrides
// member on either the request or the response -- a prior version accepted
// and echoed it on both sides. Because the real Go structs never had the
// field, a typed client can't construct or observe it; this proves a real
// client's CreateQueue still works end to end.
func TestCreateQueue_RealSDKHasNoServiceOverrides(t *testing.T) {
	t.Parallel()

	h := mediaconvert.NewHandler(mediaconvert.NewInMemoryBackend(testAccountID, testRegion))
	client := newSDKTestClient(t, h)
	ctx := t.Context()

	out, err := client.CreateQueue(ctx, &mediaconvertsdk.CreateQueueInput{
		Name: aws.String("service-overrides-wire-fix-q"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Queue)
	assert.Equal(t, "service-overrides-wire-fix-q", aws.ToString(out.Queue.Name))

	got, err := client.GetQueue(ctx, &mediaconvertsdk.GetQueueInput{
		Name: aws.String("service-overrides-wire-fix-q"),
	})
	require.NoError(t, err)
	assert.Equal(t, "service-overrides-wire-fix-q", aws.ToString(got.Queue.Name))
}

// TestCreateQueue_RawServiceOverridesFieldIgnored is the raw-body
// fail-before/pass-after proof gopherstack-wksweep-mc-1's typed-client test
// above can't provide: before the fix, gopherstack's createQueueInput read a
// "serviceOverrides" key no real client can send (and echoed it back on the
// response), but a raw HTTP body could still set it. Sending it directly
// must have no effect.
func TestCreateQueue_RawServiceOverridesFieldIgnored(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/queues", map[string]any{
		"name": "raw-service-overrides-q",
		"serviceOverrides": map[string]any{
			"feature_x": true,
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	assert.NotContains(t, rec.Body.String(), "feature_x",
		"CreateQueue must not accept serviceOverrides; the real CreateQueueInput has no such member")

	getRec := doRequest(t, h, http.MethodGet, "/2017-08-29/queues/raw-service-overrides-q", nil)
	require.Equal(t, http.StatusOK, getRec.Code)
	assert.NotContains(t, getRec.Body.String(), "feature_x")
	assert.NotContains(t, getRec.Body.String(), "serviceOverrides")
}
