package iam_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/services/iam"
)

// TestHandler_OversizedBodySurfacesInternalFailure drives a real IAM
// client's ListUsers with a Marker large enough to push the request body
// (form-urlencoded, Query/XML protocol) past httputils.MaxRequestBodyBytes,
// through the same registry/router used in production
// (service.NewRegistry + service.NewServiceRouter via newTestIAMClient).
//
// Before this fix, RouteMatcher's own httputils.ReadBody call swallowed this
// same read failure as a plain "false", so the router found no owner and
// answered a generic 404 -- masking Handler()'s already-typed ServiceFailure.
// RouteMatcher now falls back to the api/iam User-Agent marker (set by
// every aws-sdk-go-v2 iam client) when the body can't be read, claims the
// request, and lets Handler() produce the typed error.
func TestHandler_OversizedBodySurfacesInternalFailure(t *testing.T) {
	t.Parallel()

	h := iam.NewHandler(iam.NewInMemoryBackend())
	client := newTestIAMClient(t, h)

	huge := aws.String(string(bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1))))

	_, err := client.ListUsers(t.Context(), &iamsdk.ListUsersInput{
		Marker: huge,
	}, func(o *iamsdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "ServiceFailure", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}

// TestHandler_NormalSizedBodyStillRoutes is the regression guard: a normal
// request must still reach Handler() and succeed now that RouteMatcher's
// read-failure branch has changed.
func TestHandler_NormalSizedBodyStillRoutes(t *testing.T) {
	t.Parallel()

	h := iam.NewHandler(iam.NewInMemoryBackend())
	client := newTestIAMClient(t, h)

	out, err := client.ListUsers(t.Context(), &iamsdk.ListUsersInput{})
	require.NoError(t, err)
	assert.NotNil(t, out)
}
