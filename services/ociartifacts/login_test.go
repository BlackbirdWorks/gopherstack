package ociartifacts_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/artifacts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ociartifacts"
)

const (
	verifiedUserOCID = "ocid1.user.oc1..verified"
	verifiedToken    = "identity-issued-token"
)

func verifier(user, token string) (string, bool) {
	if user == "alice@example.com" && token == verifiedToken {
		return verifiedUserOCID, true
	}

	return "", false
}

func TestRegistryLoginVerifiedAgainstIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		authz  string
		status int
	}{
		{"issued token", basic(loginUser, verifiedToken), http.StatusOK},
		{"other token", basic(loginUser, loginToken), http.StatusUnauthorized},
		{"other user", basic(testNamespace+"/bob", verifiedToken), http.StatusUnauthorized},
		{"wrong namespace", basic("other/alice@example.com", verifiedToken), http.StatusUnauthorized},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			e.backend.SetLoginVerifier(verifier)

			resp := e.anonymous(t).Do(http.MethodGet, "/v2/", nil, map[string]string{"Authorization": tt.authz})
			assert.Equal(t, tt.status, resp.Status)
		})
	}
}

func TestFirstPushRecordsUserOCID(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.backend.SetLoginVerifier(verifier)

	c := e.anonymous(t)
	c.Prepare = func(r *http.Request) { r.Header.Set("Authorization", basic(loginUser, verifiedToken)) }
	c.PushImage(testNamespace+"/team/app", "v1")

	resp, err := e.client.ListContainerRepositories(t.Context(), artifacts.ListContainerRepositoriesRequest{
		CompartmentId: new(ociartifacts.DefaultTenancy),
	})
	require.NoError(t, err)
	require.Len(t, resp.Items, 1)

	full, err := e.client.GetContainerRepository(t.Context(),
		artifacts.GetContainerRepositoryRequest{RepositoryId: resp.Items[0].Id})
	require.NoError(t, err)
	assert.Equal(t, verifiedUserOCID, *full.CreatedBy)
}
