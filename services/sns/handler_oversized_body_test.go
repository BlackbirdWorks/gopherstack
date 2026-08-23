package sns_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	snssdk "github.com/aws/aws-sdk-go-v2/service/sns"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/services/sns"
)

// TestHandler_OversizedBodySurfacesTypedError drives a real SNS client's
// ListTopics with a NextToken large enough to push the request body
// (form-urlencoded, Query/XML protocol) past httputils.MaxRequestBodyBytes,
// through the same registry/router used in production (service.NewRegistry
// + service.NewServiceRouter via newTestSNSClient).
//
// Before this fix, RouteMatcher's own httputils.ReadBody call swallowed this
// same read failure as a plain "false", so the router found no owner and
// answered a generic 404. RouteMatcher now falls back to the api/sns
// User-Agent marker (set by every aws-sdk-go-v2 sns client) when the body
// can't be read, claims the request, and lets Handler() run.
//
// Handler() itself still calls c.Request().ParseForm() (not migrated): every
// action handler in this package (handleCreateTopic and friends) reads
// parameters via c.Request().FormValue(), which depends on r.Form already
// being populated by that one ParseForm() call -- migrating it to
// httputils.ReadBody would mean threading parsed url.Values through every
// action handler in the package, well beyond this bug's blast radius.
// ExtractOperation/ExtractResource already use httputils.ReadBody rather
// than ParseForm, so Handler()'s is the ONLY ParseForm call for a given
// request -- the docdb/neptune double-call landmine (a second ParseForm
// call silently seeing a cached-empty, non-nil PostForm) does not apply.
//
// Fixed (gopherstack-ioww): Handler() now calls httputils.ReadBody itself
// before ParseForm() and answers a read failure with SNS's own modelled
// "InternalError"/500 (types/errors.go, wired into every op's error switch in
// deserializers.go) instead of misclassifying it as "InvalidParameter"/400.
// ReadBody's cache (pkgs/httputils: bodyReadCloser/bodyReadErrCloser) makes
// this pre-check free of a second real read, and rewinds so ParseForm's own
// body read still succeeds on the non-failure path -- the fifty FormValue
// call sites are unaffected.
func TestHandler_OversizedBodySurfacesTypedError(t *testing.T) {
	t.Parallel()

	h := sns.NewHandler(sns.NewInMemoryBackend())
	client := newTestSNSClient(t, h)

	huge := aws.String(string(bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1))))

	_, err := client.ListTopics(t.Context(), &snssdk.ListTopicsInput{
		NextToken: huge,
	}, func(o *snssdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InternalError", apiErr.ErrorCode(),
		"a body-read failure is a server-side condition, not a client parameter error")
	assert.Equal(t, smithy.FaultServer, apiErr.ErrorFault())
}

// TestHandler_NormalSizedBodyStillRoutes is the regression guard: a normal
// request must still reach Handler() and succeed now that RouteMatcher's
// read-failure branch has changed.
func TestHandler_NormalSizedBodyStillRoutes(t *testing.T) {
	t.Parallel()

	h := sns.NewHandler(sns.NewInMemoryBackend())
	client := newTestSNSClient(t, h)

	out, err := client.ListTopics(t.Context(), &snssdk.ListTopicsInput{})
	require.NoError(t, err)
	assert.NotNil(t, out)
}
