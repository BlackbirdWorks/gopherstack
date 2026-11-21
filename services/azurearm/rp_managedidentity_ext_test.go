package azurearm_test

import (
	"encoding/json/v2"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	msiBase = "/subscriptions/" + aciSub + "/resourceGroups/" + netRG +
		"/providers/Microsoft.ManagedIdentity/userAssignedIdentities/"
	msiNew = "?api-version=2025-05-31-preview"
)

func (s *cmpStack) msiRaw(t *testing.T, method, path, body string) (int, map[string]any) {
	t.Helper()

	resp := s.rawDo(t, method, path, body)

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var out map[string]any
	if len(raw) > 0 {
		require.NoError(t, json.Unmarshal(raw, &out, jsonx.V1()))
	}

	return resp.StatusCode, out
}

func restrictedIdentity(providers string) string {
	return `{"location":"westus","properties":{"assignmentRestrictions":{"providers":` + providers + `}}}`
}

func TestManagedIdentity_AssignmentRestrictions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		providers string
		wantVM    bool
	}{
		{name: "namespace match", providers: `["Microsoft.Compute"]`, wantVM: true},
		{name: "type match any case", providers: `["microsoft.compute/VirtualMachines"]`, wantVM: true},
		{name: "other namespace", providers: `["Microsoft.Storage"]`},
		{name: "other type", providers: `["Microsoft.Compute/virtualMachineScaleSets"]`},
		{name: "list with match", providers: `["Microsoft.Web","Microsoft.Compute"]`, wantVM: true},
		{name: "empty list restricts nothing", providers: `[]`, wantVM: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newCmpStack(t)

			status, body := s.msiRaw(t, http.MethodPut, msiBase+"restricted"+msiNew, restrictedIdentity(tt.providers))
			require.Equal(t, http.StatusCreated, status, "%v", body)

			ar := body["properties"].(map[string]any)["assignmentRestrictions"].(map[string]any)

			var wantProviders []string
			require.NoError(t, json.Unmarshal([]byte(tt.providers), &wantProviders, jsonx.V1()))

			gotProviders, _ := ar["providers"].([]any)
			require.Len(t, gotProviders, len(wantProviders))

			for i, w := range wantProviders {
				assert.Equal(t, w, gotProviders[i])
			}

			status, got := s.msiRaw(t, http.MethodGet, msiBase+"restricted"+msiNew, "")
			require.Equal(t, http.StatusOK, status)
			assert.Equal(t, body["properties"], got["properties"])

			id := body["id"].(string)
			vm := linuxVM(s.newNIC(t, "nic"))
			vm.Identity = vmIdentity(armcompute.ResourceIdentityTypeUserAssigned, id)

			_, err := s.putVM(t, netRG, "vm", vm)
			if tt.wantVM {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			assert.Equal(t, "FailedIdentityOperation", sdkErrCode(err))
		})
	}
}

func TestManagedIdentity_AssignmentRestrictionsValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{name: "not an object", body: `{"location":"westus","properties":{"assignmentRestrictions":"x"}}`},
		{name: "providers not array", body: restrictedIdentity(`"Microsoft.Compute"`)},
		{name: "bad entry", body: restrictedIdentity(`["not a provider"]`)},
		{name: "non string entry", body: restrictedIdentity(`[1]`)},
		{name: "three segments", body: restrictedIdentity(`["Microsoft.Compute/a/b"]`)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newCmpStack(t)
			status, body := s.msiRaw(t, http.MethodPut, msiBase+"bad"+msiNew, tt.body)
			require.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidRequestContent", body["error"].(map[string]any)["code"])
		})
	}
}

func TestManagedIdentity_PatchProperties(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)

	status, _ := s.msiRaw(t, http.MethodPut, msiBase+"patched"+msiNew, `{"location":"westus"}`)
	require.Equal(t, http.StatusCreated, status)

	status, body := s.msiRaw(
		t,
		http.MethodPatch,
		msiBase+"patched"+msiNew,
		`{"tags":{"a":"b"},"properties":{"isolationScope":"Regional",`+
			`"assignmentRestrictions":{"providers":["Microsoft.Web"]}}}`,
	)
	require.Equal(t, http.StatusOK, status, "%v", body)

	props := body["properties"].(map[string]any)
	assert.Equal(t, "Regional", props["isolationScope"])
	assert.Equal(t, map[string]any{"providers": []any{"Microsoft.Web"}}, props["assignmentRestrictions"])
	assert.Equal(t, map[string]any{"a": "b"}, body["tags"])

	status, body = s.msiRaw(t, http.MethodPatch, msiBase+"patched"+msiNew, `{"tags":{"c":"d"}}`)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "Regional", body["properties"].(map[string]any)["isolationScope"], "absent properties are kept")
	assert.Contains(t, body["properties"], "assignmentRestrictions")

	status, body = s.msiRaw(t, http.MethodPatch, msiBase+"patched"+msiNew,
		`{"properties":{"assignmentRestrictions":null,"isolationScope":"None"}}`)
	require.Equal(t, http.StatusOK, status)
	assert.NotContains(t, body["properties"], "assignmentRestrictions")

	status, _ = s.msiRaw(t, http.MethodPatch, msiBase+"patched"+msiNew, `{"properties":{"isolationScope":"Bad"}}`)
	assert.Equal(t, http.StatusBadRequest, status)

	snap := s.handler.Snapshot(t.Context())
	dst := newCmpStack(t)
	require.NoError(t, dst.handler.Restore(t.Context(), snap))
}

func TestManagedIdentity_FlexibleFederatedCredential(t *testing.T) {
	t.Parallel()

	const (
		gh   = "https://token.actions.githubusercontent.com"
		expr = `claims['sub'] matches 'repo:contoso/app:ref:refs/heads/*' and claims['repository_id'] eq '456789'`
	)

	body := func(issuer, extra string) string {
		return `{"properties":{"issuer":"` + issuer + `","audiences":["api://AzureADTokenExchange"],` + extra + `}}`
	}

	cme := func(value string, lang int) string {
		b, err := json.Marshal(map[string]any{"value": value, "languageVersion": lang}, jsonx.V1())
		require.NoError(t, err)

		return `"claimsMatchingExpression":` + string(b)
	}

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{name: "github expression", body: body(gh, cme(expr, 1)), wantCode: http.StatusOK},
		{name: "gitlab expression", body: body("https://gitlab.com",
			cme(`claims['sub'] eq 'project_path:g/p:ref_type:branch:ref:main' and claims['project_id'] eq '12'`, 1)),
			wantCode: http.StatusOK},
		{name: "terraform expression", body: body("https://app.terraform.io",
			cme(`claims['sub'] matches 'organization:o:project:*'`, 1)), wantCode: http.StatusOK},
		{
			name:     "subject and expression",
			body:     body(gh, `"subject":"s",`+cme(expr, 1)),
			wantCode: http.StatusBadRequest,
		},
		{name: "neither", body: body(gh, `"x":1`), wantCode: http.StatusBadRequest},
		{name: "language version", body: body(gh, cme(expr, 2)), wantCode: http.StatusBadRequest},
		{
			name:     "unsupported issuer",
			body:     body("https://issuer.example", cme(expr, 1)),
			wantCode: http.StatusBadRequest,
		},
		{name: "bad term", body: body(gh, cme(`sub matches 'x'`, 1)), wantCode: http.StatusBadRequest},
		{name: "operator not allowed", body: body(gh,
			cme(`claims['sub'] eq 'a' and claims['repository_id'] matches '4*'`, 1)), wantCode: http.StatusBadRequest},
		{
			name:     "missing immutable claim",
			body:     body(gh, cme(`claims['sub'] eq 'a'`, 1)),
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing sub",
			body:     body(gh, cme(`claims['repository_id'] eq '1'`, 1)),
			wantCode: http.StatusBadRequest,
		},
		{name: "empty value", body: body(gh, cme("", 1)), wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newCmpStack(t)

			status, _ := s.msiRaw(t, http.MethodPut, msiBase+"fic-owner"+msiNew, `{"location":"westus"}`)
			require.Equal(t, http.StatusCreated, status)

			path := msiBase + "fic-owner/federatedIdentityCredentials/cred1" + msiNew
			status, got := s.msiRaw(t, http.MethodPut, path, tt.body)
			if tt.wantCode == http.StatusBadRequest {
				require.Equal(t, tt.wantCode, status, "%v", got)

				return
			}

			require.Contains(t, []int{http.StatusOK, http.StatusCreated}, status, "%v", got)

			props := got["properties"].(map[string]any)
			assert.NotContains(t, props, "subject")
			assert.InDelta(t, 1.0, props["claimsMatchingExpression"].(map[string]any)["languageVersion"], 0)

			snap := s.handler.Snapshot(t.Context())
			dst := newCmpStack(t)
			require.NoError(t, dst.handler.Restore(t.Context(), snap))

			status, again := dst.msiRaw(t, http.MethodGet, path, "")
			require.Equal(t, http.StatusOK, status)
			assert.Equal(t, got["properties"], again["properties"])
		})
	}
}

func TestManagedIdentity_RestrictionsApplyToEveryPath(t *testing.T) {
	t.Parallel()

	const (
		envPath = "/subscriptions/" + aciSub + "/resourceGroups/" + netRG +
			"/providers/Microsoft.App/managedEnvironments/env1?api-version=2024-03-01"
		aciPath = "/subscriptions/" + aciSub + "/resourceGroups/" + netRG +
			"/providers/Microsoft.ContainerInstance/containerGroups/grp1?api-version=2023-05-01"
		aciBody = `"properties":{"osType":"Linux","containers":[{"name":"c","properties":{"image":"img",` +
			`"resources":{"requests":{"cpu":1,"memoryInGB":1}}}}]}`
	)

	tests := []struct {
		body func(id string) string
		name string
		aci  bool
	}{
		{name: "container apps environment", body: func(id string) string {
			return `{"location":"westus","identity":{"type":"UserAssigned","userAssignedIdentities":{"` + id +
				`":{}}},"properties":{}}`
		}},
		{name: "container group", aci: true, body: func(id string) string {
			return `{"location":"westus","identity":{"type":"UserAssigned","userAssignedIdentities":{"` + id +
				`":{}}},` + aciBody + `}`
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newCmpStack(t)

			status, body := s.msiRaw(t, http.MethodPut, msiBase+"restricted"+msiNew,
				restrictedIdentity(`["Microsoft.Storage"]`))
			require.Equal(t, http.StatusCreated, status, "%v", body)

			id, _ := body["id"].(string)

			path := envPath
			if tt.aci {
				path = aciPath
			}

			status, got := s.msiRaw(t, http.MethodPut, path, tt.body(id))
			assert.Equal(t, http.StatusBadRequest, status, "%v", got)

			errBody, _ := got["error"].(map[string]any)
			assert.Equal(t, "FailedIdentityOperation", errBody["code"])
			assert.Contains(t, errBody["message"], "assignmentRestrictions")
		})
	}
}

func TestManagedIdentity_RestrictionsSurviveIDSpelling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		id   func(string) string
		name string
	}{
		{name: "upper case", id: strings.ToUpper},
		{name: "lower case", id: strings.ToLower},
		{name: "trailing slash", id: func(s string) string { return s + "/" }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newCmpStack(t)

			status, body := s.msiRaw(t, http.MethodPut, msiBase+"restricted"+msiNew,
				restrictedIdentity(`["Microsoft.Storage"]`))
			require.Equal(t, http.StatusCreated, status, "%v", body)

			id, _ := body["id"].(string)
			vm := linuxVM(s.newNIC(t, "nic"))
			vm.Identity = vmIdentity(armcompute.ResourceIdentityTypeUserAssigned, tt.id(id))

			_, err := s.putVM(t, netRG, "vm", vm)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "assignmentRestrictions")
		})
	}
}
