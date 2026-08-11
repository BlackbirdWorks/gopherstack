package s3control_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	s3control "github.com/blackbirdworks/gopherstack/services/s3control"
)

// This file adds handler-level (real h.Handler() dispatch, through
// httptest.NewRecorder(), with the returned error checked) tests for eight
// operations that use c.NoContent(http.StatusNoContent) rather than
// c.String(http.StatusNoContent, ""): only httptest.ResponseRecorder.Write
// (unlike real net/http) rejects an empty write after a 204 WriteHeader, so
// c.String(204, "") only fails in tests dispatched through h.Handler(),
// never against a real client. doS3ControlNewOpRequest (handler_test.go)
// asserts require.NoError(t, err) on the dispatch, so routing through it
// here is what catches the regression.

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
