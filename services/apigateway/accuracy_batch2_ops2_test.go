package apigateway_test

// accuracy_batch2_ops2_test.go — AWS-accuracy fixes for ApiGateway batch-2 ops audit (go-sud8):
//   - PutIntegration: timeoutInMillis defaults to 29000ms when not specified
//   - GetRestApis: position field omitted from response when on last page
//   - GetResources: position field omitted from response when on last page

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

// ---------------------------------------------------------------------------
// PutIntegration: timeoutInMillis defaults to 29000 when not specified
// ---------------------------------------------------------------------------

// TestBatch2Ops2_PutIntegration_DefaultTimeout verifies that PutIntegration
// returns timeoutInMillis=29000 when the caller does not specify it, matching
// AWS behaviour. Previously the field was absent (serialised as 0 with omitempty).
func TestBatch2Ops2_PutIntegration_DefaultTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		body          string
		wantTimeoutMs float64
	}{
		{
			name:          "no_timeout_defaults_to_29000",
			body:          `{"type":"AWS_PROXY","httpMethod":"POST","uri":"arn:aws:lambda:::function:fn"}`,
			wantTimeoutMs: 29000,
		},
		{
			name: "explicit_timeout_preserved",
			body: `{"type":"AWS_PROXY","httpMethod":"POST","uri":"arn:aws:lambda:::function:fn",` +
				`"timeoutInMillis":5000}`,
			wantTimeoutMs: 5000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			h := apigateway.NewHandler(b)

			createRec := restRequest(t, h, http.MethodPost, "/restapis", `{"name":"timeout-api"}`)
			require.True(t, createRec.Code >= 200 && createRec.Code < 300)

			var apiResp map[string]any
			require.NoError(t, json.NewDecoder(createRec.Body).Decode(&apiResp))
			apiID := apiResp["id"].(string)
			require.NotEmpty(t, apiID)

			resRec := restRequest(t, h, http.MethodGet, "/restapis/"+apiID+"/resources", "")
			require.True(t, resRec.Code >= 200 && resRec.Code < 300)

			var resResp map[string]any
			require.NoError(t, json.NewDecoder(resRec.Body).Decode(&resResp))
			items := resResp["item"].([]any)
			rootID := items[0].(map[string]any)["id"].(string)

			putMethodRec := restRequest(t, h, http.MethodPut,
				"/restapis/"+apiID+"/resources/"+rootID+"/methods/POST",
				`{"httpMethod":"POST","authorizationType":"NONE"}`)
			require.True(t, putMethodRec.Code >= 200 && putMethodRec.Code < 300)

			intRec := restRequest(t, h, http.MethodPut,
				"/restapis/"+apiID+"/resources/"+rootID+"/methods/POST/integration",
				tt.body)
			require.True(t, intRec.Code >= 200 && intRec.Code < 300)

			var intResp map[string]any
			require.NoError(t, json.NewDecoder(intRec.Body).Decode(&intResp))

			got, ok := intResp["timeoutInMillis"].(float64)
			require.True(t, ok, "timeoutInMillis must be present in PutIntegration response")
			assert.InDelta(t, tt.wantTimeoutMs, got, 0.1,
				"timeoutInMillis must equal %v", tt.wantTimeoutMs)
		})
	}
}

// ---------------------------------------------------------------------------
// GetRestApis: position omitted from response when on last page
// ---------------------------------------------------------------------------

// TestBatch2Ops2_GetRestApis_NoPositionOnLastPage verifies that GetRestApis
// omits the position field from the response when all results fit in one page,
// matching AWS behaviour. Previously position was always serialised as "" which
// the AWS SDK v2 treats as a valid cursor indicating more pages.
func TestBatch2Ops2_GetRestApis_NoPositionOnLastPage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		apiNames     []string
		wantPosition bool
	}{
		{
			name:         "empty_list_omits_position",
			apiNames:     nil,
			wantPosition: false,
		},
		{
			name:         "multiple_apis_on_single_page_omits_position",
			apiNames:     []string{"api-one", "api-two", "api-three"},
			wantPosition: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			h := apigateway.NewHandler(b)

			for _, name := range tt.apiNames {
				rec := restRequest(t, h, http.MethodPost, "/restapis",
					`{"name":"`+name+`"}`)
				require.True(t, rec.Code >= 200 && rec.Code < 300)
			}

			rec := restRequest(t, h, http.MethodGet, "/restapis", "")
			require.True(t, rec.Code >= 200 && rec.Code < 300)

			var resp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

			_, hasPosition := resp["position"]
			assert.False(t, hasPosition,
				"position must be absent when all results fit on one page")
		})
	}
}

// TestBatch2Ops2_GetRestApis_PositionPresentWhenPaginating verifies that
// GetRestApis includes position when there are more pages (backend-level test).
func TestBatch2Ops2_GetRestApis_PositionPresentWhenPaginating(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	for _, name := range []string{"alpha", "beta", "gamma"} {
		_, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: name})
		require.NoError(t, err)
	}

	// Fetch only 1 of 3 — backend should return a non-empty position cursor.
	_, position, err := b.GetRestAPIs(1, "")
	require.NoError(t, err)
	assert.NotEmpty(t, position, "position must be non-empty when more results remain")
}

// ---------------------------------------------------------------------------
// GetResources: position omitted from response when on last page
// ---------------------------------------------------------------------------

// TestBatch2Ops2_GetResources_NoPositionOnLastPage verifies that GetResources
// omits the position field from the response when all resources fit in one page,
// matching AWS behaviour. Previously position was always serialised as "".
func TestBatch2Ops2_GetResources_NoPositionOnLastPage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		addChild     bool
		wantPosition bool
	}{
		{
			name:         "root_only_omits_position",
			addChild:     false,
			wantPosition: false,
		},
		{
			name:         "root_and_child_omits_position",
			addChild:     true,
			wantPosition: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			h := apigateway.NewHandler(b)

			apiRec := restRequest(t, h, http.MethodPost, "/restapis", `{"name":"res-pag-api"}`)
			require.True(t, apiRec.Code >= 200 && apiRec.Code < 300)

			var apiResp map[string]any
			require.NoError(t, json.NewDecoder(apiRec.Body).Decode(&apiResp))
			apiID := apiResp["id"].(string)
			require.NotEmpty(t, apiID)

			if tt.addChild {
				resRec := restRequest(t, h, http.MethodGet, "/restapis/"+apiID+"/resources", "")
				require.True(t, resRec.Code >= 200 && resRec.Code < 300)

				var resResp map[string]any
				require.NoError(t, json.NewDecoder(resRec.Body).Decode(&resResp))
				items := resResp["item"].([]any)
				rootID := items[0].(map[string]any)["id"].(string)

				childRec := restRequest(t, h, http.MethodPost,
					"/restapis/"+apiID+"/resources/"+rootID,
					`{"pathPart":"items"}`)
				require.True(t, childRec.Code >= 200 && childRec.Code < 300)
			}

			rec := restRequest(t, h, http.MethodGet, "/restapis/"+apiID+"/resources", "")
			require.True(t, rec.Code >= 200 && rec.Code < 300)

			var resp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

			_, hasPosition := resp["position"]
			assert.False(t, hasPosition,
				"position must be absent when all resources fit on one page")
		})
	}
}

// TestBatch2Ops2_GetResources_PositionPresentWhenPaginating verifies that
// GetResources includes position when there are more pages (backend-level test).
func TestBatch2Ops2_GetResources_PositionPresentWhenPaginating(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "pag-api"})
	require.NoError(t, err)

	// Get root resource.
	resources, _, err := b.GetResources(api.ID, "", 0)
	require.NoError(t, err)
	rootID := resources[0].ID

	// Add two children so there are 3 resources total.
	for _, part := range []string{"items", "users"} {
		_, err = b.CreateResource(api.ID, rootID, part)
		require.NoError(t, err)
	}

	// Fetch limit=1 → should get non-empty position.
	_, position, err := b.GetResources(api.ID, "", 1)
	require.NoError(t, err)
	assert.NotEmpty(t, position, "position must be non-empty when more resources remain")
}
