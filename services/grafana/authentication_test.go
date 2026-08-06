package grafana_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	grafanasdk "github.com/aws/aws-sdk-go-v2/service/grafana"
	"github.com/aws/aws-sdk-go-v2/service/grafana/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/grafana"
)

func createActiveWorkspace(t *testing.T, client *grafanasdk.Client, in *grafanasdk.CreateWorkspaceInput) string {
	t.Helper()

	out, err := client.CreateWorkspace(t.Context(), in)
	require.NoError(t, err)
	id := aws.ToString(out.Workspace.Id)
	waitForWorkspaceActive(t, client, id)

	return id
}

func TestDescribeWorkspaceAuthentication_AwsSso(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	id := createActiveWorkspace(t, client, minimalCreateWorkspaceInput())

	out, err := client.DescribeWorkspaceAuthentication(t.Context(), &grafanasdk.DescribeWorkspaceAuthenticationInput{
		WorkspaceId: aws.String(id),
	})
	require.NoError(t, err)
	require.Equal(t,
		[]types.AuthenticationProviderTypes{types.AuthenticationProviderTypesAwsSso},
		out.Authentication.Providers)
	require.NotNil(t, out.Authentication.AwsSso)
	require.NotEmpty(t, aws.ToString(out.Authentication.AwsSso.SsoClientId))
	require.Nil(t, out.Authentication.Saml)
}

func TestUpdateWorkspaceAuthentication_SAML(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	id := createActiveWorkspace(t, client, minimalCreateWorkspaceInput())

	out, err := client.UpdateWorkspaceAuthentication(t.Context(), &grafanasdk.UpdateWorkspaceAuthenticationInput{
		WorkspaceId:             aws.String(id),
		AuthenticationProviders: []types.AuthenticationProviderTypes{types.AuthenticationProviderTypesSaml},
		SamlConfiguration: &types.SamlConfiguration{
			IdpMetadata: &types.IdpMetadataMemberUrl{Value: "https://idp.example.com/metadata"},
			RoleValues: &types.RoleValues{
				Admin:  []string{"admins"},
				Editor: []string{"editors"},
			},
			LoginValidityDuration: 60,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out.Authentication.Saml)
	require.Equal(t, types.SamlConfigurationStatusConfigured, out.Authentication.Saml.Status)
	require.NotNil(t, out.Authentication.Saml.Configuration)

	urlMember, ok := out.Authentication.Saml.Configuration.IdpMetadata.(*types.IdpMetadataMemberUrl)
	require.True(t, ok, "expected the url member of the IdpMetadata union to round-trip")
	require.Equal(t, "https://idp.example.com/metadata", urlMember.Value)
	require.Equal(t, []string{"admins"}, out.Authentication.Saml.Configuration.RoleValues.Admin)

	// Re-describe to confirm the state actually persisted, not just echoed
	// back from the update response.
	desc, err := client.DescribeWorkspaceAuthentication(t.Context(), &grafanasdk.DescribeWorkspaceAuthenticationInput{
		WorkspaceId: aws.String(id),
	})
	require.NoError(t, err)
	require.Equal(t, types.SamlConfigurationStatusConfigured, desc.Authentication.Saml.Status)
}

// TestUpdateWorkspaceAuthentication_BothIdpMetadataMembers_Rejected exercises
// the "both url and xml set" validation directly at the HTTP layer: the real
// SDK's IdpMetadata union is a Go interface that can only ever hold one
// concrete member (IdpMetadataMemberUrl XOR IdpMetadataMemberXml), so this
// malformed shape can only be produced by a non-Go client (or a hand-built
// request) -- there is no way to construct it through grafanasdk's typed
// client, so this bypasses the SDK client and posts the raw JSON body
// directly to the handler.
func TestUpdateWorkspaceAuthentication_BothIdpMetadataMembers_Rejected(t *testing.T) {
	t.Parallel()

	backend := grafana.NewInMemoryBackend(t.Context(), "000000000000", rtTestRegion)
	t.Cleanup(backend.Close)
	h := grafana.NewHandler(backend)

	body := []byte(`{
		"authenticationProviders": ["SAML"],
		"samlConfiguration": {
			"idpMetadata": {
				"url": "https://idp.example.com/metadata",
				"xml": "<EntityDescriptor/>"
			}
		}
	}`)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/workspaces/g-whatever/authentication", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	require.NoError(t, h.Handler()(c))
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, "ValidationException", rec.Header().Get("X-Amzn-Errortype"))
}

func TestUpdateWorkspaceAuthentication_SamlConfigWithoutSamlProvider_Rejected(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	id := createActiveWorkspace(t, client, minimalCreateWorkspaceInput())

	_, err := client.UpdateWorkspaceAuthentication(t.Context(), &grafanasdk.UpdateWorkspaceAuthenticationInput{
		WorkspaceId:             aws.String(id),
		AuthenticationProviders: []types.AuthenticationProviderTypes{types.AuthenticationProviderTypesAwsSso},
		SamlConfiguration: &types.SamlConfiguration{
			IdpMetadata: &types.IdpMetadataMemberUrl{Value: "https://idp.example.com/metadata"},
		},
	})
	require.Error(t, err)

	var ve *types.ValidationException
	require.ErrorAs(t, err, &ve)
}
