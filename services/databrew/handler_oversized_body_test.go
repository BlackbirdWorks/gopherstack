package databrew_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	databrewsdk "github.com/aws/aws-sdk-go-v2/service/databrew"
	"github.com/aws/aws-sdk-go-v2/service/databrew/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/services/databrew"
)

// TestHandler_OversizedBodySurfacesInternalFailure drives a real DataBrew
// client's CreateRuleset with a Description large enough to push the request
// body past httputils.MaxRequestBodyBytes (a real client can legitimately
// send this; aws-sdk-go-v2 imposes no client-side cap). Before this fix,
// Handler()'s ReadBody-failure branch wrote a bare "internal server error"
// text/plain body -- the restJson1 error decoder
// (aws-sdk-go-v2@v1.43.4 aws/protocol/restjson.GetErrorInfo) cannot parse
// plain text, so the client saw smithy.GenericAPIError{Code:"UnknownError"}
// instead of the typed InternalFailure handleError's default branch already
// produces for every unmatched backend error (gopherstack-o7gx).
func TestHandler_OversizedBodySurfacesInternalFailure(t *testing.T) {
	t.Parallel()

	backend := databrew.NewInMemoryBackend("123456789012", "us-east-1")
	client := newRoundTripClient(t, databrew.NewHandler(backend))

	huge := aws.String(string(bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1))))

	_, err := client.CreateRuleset(t.Context(), &databrewsdk.CreateRulesetInput{
		Name:      aws.String("oversized-body-ruleset"),
		TargetArn: aws.String("arn:aws:databrew:us-east-1:123456789012:dataset/x"),
		Rules: []types.Rule{
			{Name: aws.String("r1"), CheckExpression: aws.String(":col1 starts_with :prefix1")},
		},
		Description: huge,
	}, func(o *databrewsdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InternalFailure", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}
