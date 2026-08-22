package resourcegroups_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	resourcegroupssdk "github.com/aws/aws-sdk-go-v2/service/resourcegroups"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/services/resourcegroups"
)

// TestHandleREST_OversizedBodySurfacesInternalServerErrorException drives a
// real resourcegroups client's CreateGroup (a static REST path routed
// through handleREST, not the JSON-RPC service.HandleTarget branch that
// c6554e9f8 already fixed) with a Description large enough to push the
// request body past httputils.MaxRequestBodyBytes (a real client can
// legitimately send this; aws-sdk-go-v2 imposes no client-side cap). Before
// this fix, handleREST's ReadBody-failure branch wrote a bare
// "internal server error" text/plain body -- the restjson1 error decoder
// (aws-sdk-go-v2@v1.43.4 aws/protocol/restjson.GetErrorInfo) cannot parse
// plain text, so the client saw smithy.GenericAPIError{Code:"UnknownError"}
// instead of the typed InternalServerErrorException handleError's default
// branch already produces for every unmatched backend error
// (gopherstack-o7gx). The same fix landed at 4 other identical call sites in
// this package (handler_tags.go's Tag/Untag/PATCH-Untag paths).
func TestHandleREST_OversizedBodySurfacesInternalServerErrorException(t *testing.T) {
	t.Parallel()

	backend := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestResourceGroupsClient(t, resourcegroups.NewHandler(backend))

	huge := string(bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1)))

	_, err := client.CreateGroup(t.Context(), &resourcegroupssdk.CreateGroupInput{
		Name:        aws.String("oversized-body-group"),
		Description: aws.String(huge),
	}, func(o *resourcegroupssdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InternalServerErrorException", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}
