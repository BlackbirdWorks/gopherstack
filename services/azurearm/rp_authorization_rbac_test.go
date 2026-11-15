package azurearm_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/aadauth"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

const (
	rbacSuper       = "99999999-9999-9999-9999-999999999999"
	rbacUser        = "11111111-2222-3333-4444-555555555555"
	rbacReaderRole  = "acdd72a7-3385-48ef-bd42-f606fba81ae7"
	rbacContribRole = "b24988ac-6180-42a0-ab88-20f7382dd24c"
)

func TestEnforceRBAC(t *testing.T) {
	t.Parallel()

	svc, err := (&azurearm.Provider{}).Init(&service.AppContext{})
	require.NoError(t, err)

	h, ok := svc.(*azurearm.Handler)
	require.True(t, ok)

	t.Cleanup(func() { h.Shutdown(context.Background()) })

	h.Settings.EnforceRBAC = true
	h.Settings.RBACSuperusers = rbacSuper
	issuer := h.Issuer
	sub := "/subscriptions/" + h.Settings.SubscriptionID

	token := func(oid string) string {
		tok, terr := issuer.IssueClientCredentialsToken(aadauth.ClientCredentialsClaims{
			Issuer: "https://host:10006/" + h.Settings.TenantID + "/v2.0", Audience: "https://management.azure.com/",
			TenantID: h.Settings.TenantID, ClientID: oid, ObjectID: oid,
		}, aadauth.DefaultTokenExpirySeconds)
		require.NoError(t, terr)

		return "Bearer " + tok
	}

	do := func(auth, method, path, body string) (int, string) {
		req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
		req.Host = "host:10006"
		req.Header.Set("Content-Type", "application/json")

		if auth != "" {
			req.Header.Set("Authorization", auth)
		}

		rec := httptest.NewRecorder()
		newEchoServer(h).ServeHTTP(rec, req)

		return rec.Code, rec.Body.String()
	}

	rg := sub + "/resourcegroups/rbac-rg?api-version=2021-04-01"
	assign := func(guid, role string) {
		code, out := do(token(rbacSuper), http.MethodPut,
			sub+"/providers/Microsoft.Authorization/roleAssignments/"+guid+"?api-version=2022-04-01",
			`{"properties":{"roleDefinitionId":"`+sub+`/providers/Microsoft.Authorization/roleDefinitions/`+role+
				`","principalId":"`+rbacUser+`"}}`)
		require.Equal(t, http.StatusCreated, code, out)
	}

	code, _ := do("", http.MethodGet, rg, "")
	assert.Equal(t, http.StatusUnauthorized, code, "no token")

	code, _ = do(token(rbacSuper), http.MethodPut, rg, `{"location":"westus"}`)
	require.Equal(t, http.StatusCreated, code, "superuser is exempt")

	code, body := do(token(rbacUser), http.MethodGet, rg, "")
	assert.Equal(t, http.StatusForbidden, code)
	assert.Contains(t, body, "AuthorizationFailed")
	assert.Contains(t, body, "Microsoft.Resources/subscriptions/resourceGroups/read")

	assign("aaaaaaaa-0000-0000-0000-000000000001", rbacReaderRole)

	code, _ = do(token(rbacUser), http.MethodGet, rg, "")
	assert.Equal(t, http.StatusOK, code, "reader reads")

	code, _ = do(
		token(rbacUser),
		http.MethodPut,
		sub+"/resourcegroups/other?api-version=2021-04-01",
		`{"location":"westus"}`,
	)
	assert.Equal(t, http.StatusForbidden, code, "reader cannot write")

	code, body = do(token(rbacUser), http.MethodGet,
		sub+"/resourcegroups/rbac-rg/providers/Microsoft.Authorization/permissions?api-version=2022-04-01", "")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, `"*/read"`)
	assert.NotContains(t, body, `"*"`+`,`)

	assign("aaaaaaaa-0000-0000-0000-000000000002", rbacContribRole)

	code, _ = do(
		token(rbacUser),
		http.MethodPut,
		sub+"/resourcegroups/other?api-version=2021-04-01",
		`{"location":"westus"}`,
	)
	assert.Equal(t, http.StatusCreated, code, "contributor writes")
}

func TestEnforceRBACOffByDefault(t *testing.T) {
	t.Parallel()

	svc, err := (&azurearm.Provider{}).Init(&service.AppContext{})
	require.NoError(t, err)

	h, ok := svc.(*azurearm.Handler)
	require.True(t, ok)

	t.Cleanup(func() { h.Shutdown(context.Background()) })

	status, _ := doRequest(
		t,
		h,
		http.MethodGet,
		"/subscriptions/"+h.Settings.SubscriptionID+"/resourcegroups?api-version=2021-04-01",
		nil,
	)
	assert.Equal(t, http.StatusOK, status)
}
