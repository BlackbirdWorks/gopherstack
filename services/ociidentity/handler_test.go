package ociidentity_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/identity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	oci "github.com/blackbirdworks/gopherstack/services/ociidentity"
)

func rawDo(t *testing.T, method, url string, body []byte) (*http.Response, []byte) {
	t.Helper()

	req, err := http.NewRequestWithContext(context.Background(), method, url, bytes.NewReader(body))
	require.NoError(t, err)

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	var buf bytes.Buffer
	_, _ = buf.ReadFrom(resp.Body)

	return resp, buf.Bytes()
}

func TestRawErrorEnvelopes(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)

	tests := []struct {
		name     string
		method   string
		path     string
		body     string
		wantCode string
		status   int
	}{
		{"unknown path", http.MethodGet, "/20160918/nothing", "", "NotFound", http.StatusNotFound},
		{"bad json", http.MethodPost, "/20160918/groups", "{", "CannotParseRequest", http.StatusBadRequest},
		{"no body", http.MethodPost, "/20160918/groups", "", "MissingParameter", http.StatusBadRequest},
		{
			"bad access level",
			http.MethodGet,
			"/20160918/compartments?compartmentId=" + testTenancy + "&accessLevel=SOME",
			"",
			"InvalidParameter", http.StatusBadRequest},
		{"bad sort order", http.MethodGet, "/20160918/users?compartmentId=" + testTenancy + "&sortOrder=UP", "",
			"InvalidParameter", http.StatusBadRequest},
		{"missing compartment id", http.MethodGet, "/20160918/policies", "", "MissingParameter", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp, body := rawDo(t, tt.method, e.base+tt.path, []byte(tt.body))
			assert.Equal(t, tt.status, resp.StatusCode)
			assert.NotEmpty(t, resp.Header.Get("Opc-Request-Id"))

			var eb struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			}
			require.NoError(t, json.Unmarshal(body, &eb))
			assert.Equal(t, tt.wantCode, eb.Code)
			assert.NotEmpty(t, eb.Message)
		})
	}
}

func TestEnforcedAuthAndAdminEndpoint(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)

	_, err := e.client.GetTenancy(t.Context(), identity.GetTenancyRequest{TenancyId: new(testTenancy)})
	assert.Equal(t, http.StatusUnauthorized, svcErr(t, err).GetHTTPStatusCode())

	body, err := json.Marshal(
		map[string]string{"tenancy": testTenancy, "user": "ocid1.user.oc1..bootstrap", "publicKey": e.kp.pubPEM},
	)
	require.NoError(t, err)

	resp, _ := rawDo(t, http.MethodPost, e.base+"/_gopherstack/oci/keys", body)
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	r, err := e.client.GetTenancy(t.Context(), identity.GetTenancyRequest{TenancyId: new(testTenancy)})
	require.NoError(t, err)
	assert.Equal(t, testTenancy, *r.Id)

	resp, lst := rawDo(t, http.MethodGet, e.base+"/_gopherstack/oci/keys", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(lst), "ocid1.user.oc1..bootstrap")

	resp, _ = rawDo(
		t,
		http.MethodDelete,
		e.base+"/_gopherstack/oci/keys?keyId="+testTenancy+"/ocid1.user.oc1..bootstrap/"+e.kp.fprint,
		nil,
	)
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	_, err = e.client.GetTenancy(t.Context(), identity.GetTenancyRequest{TenancyId: new(testTenancy)})
	assert.Equal(t, http.StatusUnauthorized, svcErr(t, err).GetHTTPStatusCode())
}

func TestSnapshotRoundTrip(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	u := e.mkUser(t, "persisted")
	g := e.mkGroup(t, "pgroup")
	c := e.mkCompartment(t, testTenancy, "pcomp")

	kp := newKeyPair(t)
	_, err := e.client.UploadApiKey(ctx, identity.UploadApiKeyRequest{
		UserId: u.Id, Key: new(kp.pubPEM),
	})
	require.NoError(t, err)

	_, err = e.client.AddUserToGroup(ctx, identity.AddUserToGroupRequest{
		UserId: u.Id, GroupId: g.Id,
	})
	require.NoError(t, err)

	snap := e.backend.Snapshot(ctx)
	require.NotEmpty(t, snap)

	fresh := oci.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(ctx, snap))

	keys, err := fresh.ListAPIKeys(*u.Id)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	assert.Equal(t, kp.fprint, keys[0].Fingerprint)

	_, _, err = fresh.GetCompartment(*c.Id)
	require.NoError(t, err)

	assert.Len(t, fresh.Auth.Records(), 1)

	e.backend.Reset()
	_, _, err = e.backend.GetUser(*u.Id)
	require.Error(t, err)
	assert.Empty(t, e.backend.Auth.Records())
}
