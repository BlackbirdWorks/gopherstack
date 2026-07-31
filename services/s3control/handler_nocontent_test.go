package s3control_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	s3control "github.com/blackbirdworks/gopherstack/services/s3control"
)

// This file adds the first handler-level (real h.Handler() dispatch, through
// httptest.NewRecorder(), with the returned error checked) tests for eight
// operations that previously used c.String(http.StatusNoContent, "") instead
// of c.NoContent(http.StatusNoContent). This is a hygiene/testability change,
// NOT a bug fix: a real net/http server no-ops an empty Write after a 204
// WriteHeader (see net/http's (*response).write, which returns early on
// zero-length data before it ever reaches the body-allowed check), so
// c.String(204, "") never actually failed against a real client. Only
// httptest.ResponseRecorder.Write checks bodyAllowedForStatus unconditionally
// (no exemption for zero-length writes), which is exactly why no test
// dispatching through h.Handler() and asserting a nil error could previously
// exist for these eight operations -- they had only ever been exercised at
// the backend (Go method) level. doS3ControlNewOpRequest (handler_test.go)
// already asserts require.NoError(t, err) on the dispatch, so simply routing
// through it here is what makes each of the following tests fail against the
// pre-conversion c.String(204, "") handlers and pass once they use
// c.NoContent.

func TestHTTP_DeleteAccessGrantsInstanceResourcePolicy_NoContent(t *testing.T) {
	t.Parallel()

	h := newTestS3ControlHandler(t)
	h.Backend.PutAccessGrantsInstanceResourcePolicy("acct1", `{"Version":"2012-10-17"}`)

	rec := doS3ControlNewOpRequest(
		t, h, http.MethodDelete, "/v20180820/accessgrantsinstance/resourcepolicy", "acct1", "",
	)

	assert.Equal(t, http.StatusNoContent, rec.Code)
	assert.Empty(t, rec.Body.String())
}

func TestHTTP_DissociateAccessGrantsIdentityCenter_NoContent(t *testing.T) {
	t.Parallel()

	h := newTestS3ControlHandler(t)
	h.Backend.AssociateAccessGrantsIdentityCenter("acct1", "arn:aws:sso:::instance/ssoins-test")

	rec := doS3ControlNewOpRequest(
		t, h, http.MethodDelete, "/v20180820/accessgrantsinstance/identitycenter", "acct1", "",
	)

	assert.Equal(t, http.StatusNoContent, rec.Code)
	assert.Empty(t, rec.Body.String())
}

func TestHTTP_DeleteAccessGrant_NoContent(t *testing.T) {
	t.Parallel()

	h := newTestS3ControlHandler(t)
	loc := h.Backend.CreateAccessGrantsLocation("acct1", "s3://bucket/", "arn:aws:iam::acct1:role/r")
	grant, err := h.Backend.CreateAccessGrant(
		"acct1", loc.AccessGrantsLocationID, "IAMUser", "arn:test", "READ", "",
	)
	require.NoError(t, err)

	rec := doS3ControlNewOpRequest(
		t, h, http.MethodDelete,
		"/v20180820/accessgrantsinstance/grant/"+grant.AccessGrantID, "acct1", "",
	)

	assert.Equal(t, http.StatusNoContent, rec.Code)
	assert.Empty(t, rec.Body.String())
}

func TestHTTP_DeleteAccessGrantsLocation_NoContent(t *testing.T) {
	t.Parallel()

	h := newTestS3ControlHandler(t)
	loc := h.Backend.CreateAccessGrantsLocation("acct1", "s3://bucket/", "arn:aws:iam::acct1:role/r")

	rec := doS3ControlNewOpRequest(
		t, h, http.MethodDelete,
		"/v20180820/accessgrantsinstance/location/"+loc.AccessGrantsLocationID, "acct1", "",
	)

	assert.Equal(t, http.StatusNoContent, rec.Code)
	assert.Empty(t, rec.Body.String())
}

func TestHTTP_DeleteAccessPointForObjectLambda_NoContent(t *testing.T) {
	t.Parallel()

	h := newTestS3ControlHandler(t)
	h.Backend.CreateAccessPointForObjectLambda("acct1", "my-ap")

	rec := doS3ControlNewOpRequest(
		t, h, http.MethodDelete, "/v20180820/accesspointforobjectlambda/my-ap", "acct1", "",
	)

	assert.Equal(t, http.StatusNoContent, rec.Code)
	assert.Empty(t, rec.Body.String())
}

func TestHTTP_DeleteAccessPointPolicyForObjectLambda_NoContent(t *testing.T) {
	t.Parallel()

	h := newTestS3ControlHandler(t)
	h.Backend.CreateAccessPointForObjectLambda("acct1", "my-ap")
	require.NoError(
		t, h.Backend.PutAccessPointPolicyForObjectLambda("acct1", "my-ap", `{"Version":"2012-10-17"}`),
	)

	rec := doS3ControlNewOpRequest(
		t, h, http.MethodDelete, "/v20180820/accesspointforobjectlambda/my-ap/policy", "acct1", "",
	)

	assert.Equal(t, http.StatusNoContent, rec.Code)
	assert.Empty(t, rec.Body.String())
}

func TestHTTP_DeleteJobTagging_NoContent(t *testing.T) {
	t.Parallel()

	h := newTestS3ControlHandler(t)
	job, err := h.Backend.CreateJob("acct1", "arn:aws:iam::acct1:role/r", 1)
	require.NoError(t, err)
	require.NoError(t, h.Backend.PutJobTagging("acct1", job.JobID, s3control.TagSet{"env": "prod"}))

	rec := doS3ControlNewOpRequest(
		t, h, http.MethodDelete, "/v20180820/jobs/"+job.JobID+"/tagging", "acct1", "",
	)

	assert.Equal(t, http.StatusNoContent, rec.Code)
	assert.Empty(t, rec.Body.String())
}

func TestHTTP_DeleteAccessPointScope_NoContent(t *testing.T) {
	t.Parallel()

	h := newTestS3ControlHandler(t)
	h.Backend.CreateAccessPoint("acct1", "my-ap", "my-bucket")
	require.NoError(t, h.Backend.PutAccessPointScope("acct1", "my-ap", `{"Permissions":["GetObject"]}`))

	rec := doS3ControlNewOpRequest(
		t, h, http.MethodDelete, "/v20180820/accesspoint/my-ap/scope", "acct1", "",
	)

	assert.Equal(t, http.StatusNoContent, rec.Code)
	assert.Empty(t, rec.Body.String())
}
