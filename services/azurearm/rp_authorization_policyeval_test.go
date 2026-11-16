package azurearm_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

func newPolicyHandler(t *testing.T) (*azurearm.Handler, func(method, path, body string) (int, string)) {
	t.Helper()

	svc, err := (&azurearm.Provider{}).Init(&service.AppContext{})
	require.NoError(t, err)

	h, ok := svc.(*azurearm.Handler)
	require.True(t, ok)

	t.Cleanup(func() { h.Shutdown(context.Background()) })

	return h, func(method, path, body string) (int, string) {
		req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
		req.Host = "host:10006"
		req.Header.Set("Content-Type", "application/json")

		rec := httptest.NewRecorder()
		newEchoServer(h).ServeHTTP(rec, req)

		return rec.Code, rec.Body.String()
	}
}

func TestPolicyEffectsOnWrite(t *testing.T) {
	t.Parallel()

	const (
		requireTag = "/providers/Microsoft.Authorization/policyDefinitions/871b6d14-10aa-478d-b590-94f262ecfa99"
		api        = "?api-version=2021-06-01"
	)

	requireEnv := func(extra string) string {
		return `{"properties":{` + extra + `"policyDefinitionId":"` + requireTag +
			`","parameters":{"tagName":{"value":"env"}}}}`
	}

	tests := []struct {
		name       string
		defBody    string
		assignBody string
		rgBody     string
		wantBody   string
		wantCode   int
	}{
		{
			name:       "deny without the tag",
			assignBody: requireEnv(""),
			rgBody:     `{"location":"westus"}`,
			wantCode:   http.StatusForbidden,
			wantBody:   "RequestDisallowedByPolicy",
		},
		{
			name:       "allow with the tag",
			assignBody: requireEnv(""),
			rgBody:     `{"location":"westus","tags":{"Env":"x"}}`,
			wantCode:   http.StatusCreated,
		},
		{
			name:       "do not enforce",
			assignBody: requireEnv(`"enforcementMode":"DoNotEnforce",`),
			rgBody:     `{"location":"westus"}`,
			wantCode:   http.StatusCreated,
		},
		{
			name: "append a tag",
			defBody: `{"properties":{"policyType":"Custom","mode":"All","policyRule":{"if":{"field":"type",` +
				`"equals":"Microsoft.Resources/subscriptions/resourceGroups"},"then":{"effect":"append",` +
				`"details":[{"field":"tags['owner']","value":"team"}]}}}}`,
			rgBody: `{"location":"westus"}`, wantCode: http.StatusCreated, wantBody: `"owner":"team"`,
		},
		{
			name: "modify replaces a tag",
			defBody: `{"properties":{"policyType":"Custom","mode":"All","policyRule":{"if":{"field":"location",` +
				`"equals":"westus"},"then":{"effect":"modify","details":{"roleDefinitionIds":["/providers/` +
				`microsoft.authorization/roleDefinitions/b24988ac-6180-42a0-ab88-20f7382dd24c"],` +
				`"operations":[{"operation":"addOrReplace","field":"tags['cost']","value":"[concat('c', '1')]"}]}}}}}`,
			rgBody:   `{"location":"westus","tags":{"cost":"old"}}`,
			wantCode: http.StatusCreated,
			wantBody: `"cost":"c1"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, do := newPolicyHandler(t)
			sub := "/subscriptions/" + h.Settings.SubscriptionID

			assignBody := tc.assignBody

			if tc.defBody != "" {
				code, out := do(
					http.MethodPut,
					sub+"/providers/Microsoft.Authorization/policyDefinitions/custom1"+api,
					tc.defBody,
				)
				require.Equal(t, http.StatusCreated, code, out)

				assignBody = `{"properties":{"policyDefinitionId":"` + sub +
					`/providers/Microsoft.Authorization/policyDefinitions/custom1"}}`
			}

			code, out := do(
				http.MethodPut,
				sub+"/providers/Microsoft.Authorization/policyAssignments/a1"+api,
				assignBody,
			)
			require.Equal(t, http.StatusCreated, code, out)

			code, out = do(http.MethodPut, sub+"/resourcegroups/pol-rg?api-version=2021-04-01", tc.rgBody)
			assert.Equal(t, tc.wantCode, code, out)
			assert.Contains(t, out, tc.wantBody)
		})
	}
}
