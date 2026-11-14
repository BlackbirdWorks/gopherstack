package app

import (
	"encoding/base64"
	"encoding/json/v2"
	"net/http"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/appservice/armappservice/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func TestAzureFunctionsARMSDK_ManagedIdentityToken(t *testing.T) {
	t.Parallel()

	s := newFnSDKStack(t)
	plan := s.createPlan(t, "idplan")

	poller, err := s.factory.NewWebAppsClient().BeginCreateOrUpdate(t.Context(), fnSDKRG, "idapp", armappservice.Site{
		Location: new("local"),
		Kind:     new("functionapp,linux"),
		Identity: &armappservice.ManagedServiceIdentity{
			Type: new(armappservice.ManagedServiceIdentityTypeSystemAssigned),
		},
		Properties: &armappservice.SiteProperties{
			ServerFarmID: new(*plan.ID),
			SiteConfig:   &armappservice.SiteConfig{LinuxFxVersion: new("NODE|20")},
		},
	}, nil)
	require.NoError(t, err)

	res, err := poller.PollUntilDone(t.Context(), pollOpts())
	require.NoError(t, err)
	require.NotNil(t, res.Identity)

	principal := *res.Identity.PrincipalID
	require.NotEmpty(t, principal)

	_, err = s.backend.Deploy("idapp", fnZip(t), "t", "m")
	require.NoError(t, err)

	host := "idapp." + fnSDKDomain

	_, header := s.call(t, http.MethodGet, host, "/env/IDENTITY_HEADER", nil, "", "")
	require.NotEmpty(t, header)

	code, endpoint := s.call(t, http.MethodGet, host, "/env/IDENTITY_ENDPOINT", nil, "", "")
	require.Equal(t, http.StatusOK, code)
	assert.True(t, strings.HasSuffix(endpoint, "/msi/token"), endpoint)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
		s.fnSrv.URL+"/MSI/token?resource=https://vault.azure.net&api-version=2019-08-01", nil)
	require.NoError(t, err)
	req.Header.Set("X-IDENTITY-HEADER", header)

	resp, err := s.fnSrv.Client().Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var tok map[string]string
	require.NoError(t, json.UnmarshalRead(resp.Body, &tok, jsonx.V1()))

	parts := strings.Split(tok["access_token"], ".")
	require.Len(t, parts, 3)

	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	require.NoError(t, err)

	var claims map[string]any
	require.NoError(t, json.Unmarshal(raw, &claims, jsonx.V1()))
	assert.Equal(t, principal, claims["oid"])
}

func TestAzureFunctionsARMSDK_ManagedIdentityRemovedRestartsApp(t *testing.T) {
	t.Parallel()

	s := newFnSDKStack(t)
	plan := s.createPlan(t, "idplan2")

	put := func(typ armappservice.ManagedServiceIdentityType) {
		poller, err := s.factory.NewWebAppsClient().
			BeginCreateOrUpdate(t.Context(), fnSDKRG, "idapp2", armappservice.Site{
				Location: new("local"), Kind: new("functionapp,linux"),
				Identity: &armappservice.ManagedServiceIdentity{Type: new(typ)},
				Properties: &armappservice.SiteProperties{
					ServerFarmID: new(*plan.ID),
					SiteConfig:   &armappservice.SiteConfig{LinuxFxVersion: new("NODE|20")},
				},
			}, nil)
		require.NoError(t, err)

		_, err = poller.PollUntilDone(t.Context(), pollOpts())
		require.NoError(t, err)
	}

	put(armappservice.ManagedServiceIdentityTypeSystemAssigned)

	_, err := s.backend.Deploy("idapp2", fnZip(t), "t", "m")
	require.NoError(t, err)

	host := "idapp2." + fnSDKDomain

	_, header := s.call(t, http.MethodGet, host, "/env/IDENTITY_HEADER", nil, "", "")
	require.NotEmpty(t, header)

	put(armappservice.ManagedServiceIdentityTypeNone)

	_, header = s.call(t, http.MethodGet, host, "/env/IDENTITY_HEADER", nil, "", "")
	assert.Empty(t, header, "the restarted host no longer has a token service")
}
