package apigateway_test

import (
	"encoding/json"
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

// TestRestAPIPolicy_WireReEscaping verifies UpdateRestApi's "/policy" PATCH
// round-trips through the wire re-escaping real API Gateway applies
// (services/apigateway/models.go's wireRestAPI): the response's "policy"
// value is the plain policy JSON's escaped content with the wrapping quotes
// stripped, exactly what terraform-provider-aws's flattenAPIPolicy
// (rest_api.go) expects to wrap-and-unquote back into the original text.
// Internal storage (InMemoryBackend.GetRestAPI) stays plain.
func TestRestAPIPolicy_WireReEscaping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		plainPolicy string
	}{
		{
			name: "simple_policy",
			plainPolicy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
				`"Principal":"*","Action":"execute-api:Invoke","Resource":"execute-api:/*"}]}`,
		},
		{
			// AWS-provider doc comment (flattenAPIPolicy) calls out CIDR
			// forward slashes specifically as a case strconv.Unquote could
			// break on if the content weren't escaped correctly first.
			name: "policy_with_cidr_forward_slash",
			plainPolicy: `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*",` +
				`"Action":"execute-api:Invoke","Resource":"execute-api:/*",` +
				`"Condition":{"NotIpAddress":{"aws:SourceIp":"10.0.0.0/24"}}}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			h := apigateway.NewHandler(b)

			createRec := restRequest(t, h, http.MethodPost, "/restapis", `{"name":"policy-wire-api"}`)
			require.Equal(t, http.StatusCreated, createRec.Code)

			var created struct {
				ID string `json:"id"`
			}
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

			patchValue, err := json.Marshal(tt.plainPolicy)
			require.NoError(t, err)
			patchBody := `{"patchOperations":[{"op":"replace","path":"/policy","value":` +
				string(patchValue) + `}]}`

			patchRec := restRequest(t, h, http.MethodPatch, "/restapis/"+created.ID, patchBody)
			require.Equal(t, http.StatusOK, patchRec.Code, "body: %s", patchRec.Body.String())

			var resp struct {
				Policy string `json:"policy"`
			}
			require.NoError(t, json.Unmarshal(patchRec.Body.Bytes(), &resp))

			// Mirror terraform-provider-aws's flattenAPIPolicy: wrap in
			// literal quotes, then unquote -- must recover the plain policy.
			recovered, err := strconv.Unquote(`"` + resp.Policy + `"`)
			require.NoError(t, err, "provider-side unquote should succeed on the wire value")
			assert.JSONEq(t, tt.plainPolicy, recovered)

			stored, err := b.GetRestAPI(created.ID)
			require.NoError(t, err)
			assert.JSONEq(t, tt.plainPolicy, stored.Policy)
		})
	}
}
