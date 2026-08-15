package apigateway_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigatewaysdk "github.com/aws/aws-sdk-go-v2/service/apigateway"
	apigatewaytypes "github.com/aws/aws-sdk-go-v2/service/apigateway/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

// TestAPIGW_UsagePlans covers CreateUsagePlan, GetUsagePlan, GetUsagePlans, UpdateUsagePlan,
// DeleteUsagePlan, CreateUsagePlanKey, GetUsagePlanKey, GetUsagePlanKeys, DeleteUsagePlanKey.
func TestAPIGW_UsagePlans(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()

	// CreateApiKey first for key association.
	rec := restRequest(t, h, http.MethodPost, "/apikeys", `{"name":"plan-key","enabled":true}`)
	require.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	var keyResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&keyResp))
	keyID, _ := keyResp["id"].(string)
	require.NotEmpty(t, keyID)

	// CreateUsagePlan via X-Amz-Target (using same handler h).
	rec = postWithHandler(
		t,
		h,
		nil,
		"CreateUsagePlan",
		`{"name":"test-plan","throttle":{"burstLimit":100,"rateLimit":50}}`,
	)
	require.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	var planResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&planResp))
	planID, _ := planResp["id"].(string)
	require.NotEmpty(t, planID)

	// GetUsagePlan via REST.
	rec = restRequest(t, h, http.MethodGet, "/usageplans/"+planID, "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	// GetUsagePlans via REST.
	rec = restRequest(t, h, http.MethodGet, "/usageplans", "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	// UpdateUsagePlan.
	rec = restRequest(t, h, http.MethodPatch, "/usageplans/"+planID,
		`[{"op":"replace","path":"/name","value":"updated-plan"}]`)
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	// CreateUsagePlanKey.
	rec = restRequest(t, h, http.MethodPost, "/usageplans/"+planID+"/keys",
		`{"keyId":"`+keyID+`","keyType":"API_KEY"}`)
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	// GetUsagePlanKeys.
	rec = restRequest(t, h, http.MethodGet, "/usageplans/"+planID+"/keys", "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	// GetUsagePlanKey.
	rec = restRequest(t, h, http.MethodGet, "/usageplans/"+planID+"/keys/"+keyID, "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	// DeleteUsagePlanKey.
	rec = restRequest(t, h, http.MethodDelete, "/usageplans/"+planID+"/keys/"+keyID, "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	// DeleteUsagePlan.
	rec = restRequest(t, h, http.MethodDelete, "/usageplans/"+planID, "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")
}

// TestPatchOps_UpdateUsagePlan verifies that JSON patch operations
// are applied when PATCH /usageplans/{id} is called. Previously patch ops were
// silently dropped, leaving the plan name unchanged.
func TestPatchOps_UpdateUsagePlan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		patchBody string
		wantName  string
	}{
		{
			name:      "replace_name_via_patch_op",
			patchBody: `[{"op":"replace","path":"/name","value":"renamed-plan"}]`,
			wantName:  "renamed-plan",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			h := apigateway.NewHandler(b)

			plan, err := b.CreateUsagePlan(apigateway.CreateUsagePlanInput{Name: "original-plan"})
			require.NoError(t, err)

			patchRec := restRequest(t, h, http.MethodPatch, "/usageplans/"+plan.ID, tt.patchBody)
			require.True(t, patchRec.Code >= 200 && patchRec.Code < 300)

			updated, err := b.GetUsagePlan(plan.ID)
			require.NoError(t, err)
			assert.Equal(t, tt.wantName, updated.Name,
				"patch op must update the usage plan name")
		})
	}
}

func TestBackend_EnforceUsagePlan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		quota    *apigateway.QuotaSettings
		throttle *apigateway.ThrottleSettings
		wantErr  error
		name     string
		calls    int
	}{
		{
			name:    "quota_allows_up_to_limit",
			quota:   &apigateway.QuotaSettings{Limit: 3, Period: "DAY"},
			calls:   3,
			wantErr: nil,
		},
		{
			name:    "quota_rejects_over_limit",
			quota:   &apigateway.QuotaSettings{Limit: 3, Period: "DAY"},
			calls:   4,
			wantErr: apigateway.ErrQuotaExceeded,
		},
		{
			name:     "throttle_allows_within_burst",
			throttle: &apigateway.ThrottleSettings{RateLimit: 1, BurstLimit: 3},
			calls:    3,
			wantErr:  nil,
		},
		{
			name:     "throttle_rejects_over_burst",
			throttle: &apigateway.ThrottleSettings{RateLimit: 1, BurstLimit: 3},
			calls:    4,
			wantErr:  apigateway.ErrThrottled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "a"})
			require.NoError(t, err)
			key, err := b.CreateAPIKey(apigateway.CreateAPIKeyInput{Name: "k", Enabled: true})
			require.NoError(t, err)
			associateKeyToPlan(t, b, api.ID, key.ID, tt.quota, tt.throttle)

			var lastErr error
			for range tt.calls {
				lastErr = b.EnforceUsagePlan(api.ID, "prod", key.ID)
			}
			require.ErrorIs(t, lastErr, tt.wantErr)
		})
	}
}

func TestUsagePlan_ApiStages_Create(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "stage-plan-api"})
	_, _ = b.CreateDeployment(api.ID, "prod", "v1")

	plan, err := b.CreateUsagePlan(apigateway.CreateUsagePlanInput{
		Name: "throttle-plan",
		APIStages: []apigateway.APIStageAssociation{
			{RestAPIID: api.ID, Stage: "prod"},
		},
		Throttle: &apigateway.ThrottleSettings{RateLimit: 1000, BurstLimit: 500},
	})
	require.NoError(t, err)
	require.Len(t, plan.APIStages, 1)
	assert.Equal(t, api.ID, plan.APIStages[0].RestAPIID)
	assert.Equal(t, "prod", plan.APIStages[0].Stage)

	got, err := b.GetUsagePlan(plan.ID)
	require.NoError(t, err)
	require.Len(t, got.APIStages, 1)
}

func TestUsagePlan_ApiStages_Update(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "upd-plan-api"})
	_, _ = b.CreateDeployment(api.ID, "prod", "v1")
	api2, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "upd-plan-api2"})
	_, _ = b.CreateDeployment(api2.ID, "v1", "v1")

	plan, _ := b.CreateUsagePlan(apigateway.CreateUsagePlanInput{
		Name: "update-plan",
		APIStages: []apigateway.APIStageAssociation{
			{RestAPIID: api.ID, Stage: "prod"},
		},
	})

	updated, err := b.UpdateUsagePlan(apigateway.UpdateUsagePlanInput{
		UsagePlanID: plan.ID,
		APIStages: []apigateway.APIStageAssociation{
			{RestAPIID: api.ID, Stage: "prod"},
			{RestAPIID: api2.ID, Stage: "v1"},
		},
	})
	require.NoError(t, err)
	assert.Len(t, updated.APIStages, 2)
}

func TestUsagePlan_ApiStages_PerStageThrottle(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "throttle-stage-api"})
	_, _ = b.CreateDeployment(api.ID, "prod", "v1")

	methodThrottle := map[string]*apigateway.ThrottleSettings{
		"GET /items": {RateLimit: 100, BurstLimit: 50},
	}
	plan, err := b.CreateUsagePlan(apigateway.CreateUsagePlanInput{
		Name: "per-method-plan",
		APIStages: []apigateway.APIStageAssociation{
			{
				RestAPIID: api.ID,
				Stage:     "prod",
				Throttle:  methodThrottle,
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, plan.APIStages, 1)
	require.NotNil(t, plan.APIStages[0].Throttle)
	assert.InDelta(t, 100.0, plan.APIStages[0].Throttle["GET /items"].RateLimit, 0.001)
}

func TestHandlerUsagePlan_ApiStages(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()

	rec := restRequest(t, h, http.MethodPost, "/restapis", `{"name":"plan-stage-api"}`)
	require.True(t, rec.Code >= 200 && rec.Code < 300)

	var apiResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&apiResp))
	apiID := apiResp["id"].(string)

	rec = restRequest(t, h, http.MethodPost, "/restapis/"+apiID+"/deployments",
		`{"stageName":"prod","description":"v1"}`)
	require.True(t, rec.Code >= 200 && rec.Code < 300)

	body := `{"name":"stages-plan",` +
		`"apiStages":[{"apiId":"` + apiID + `","stage":"prod"}],` +
		`"throttle":{"rateLimit":500,"burstLimit":200}}`
	rec = restRequest(t, h, http.MethodPost, "/usageplans", body)
	require.True(t, rec.Code >= 200 && rec.Code < 300)

	var planResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&planResp))
	apiStages, ok := planResp["apiStages"].([]any)
	require.True(t, ok)
	require.Len(t, apiStages, 1)
	firstStage := apiStages[0].(map[string]any)
	assert.Equal(t, apiID, firstStage["apiId"])
	assert.Equal(t, "prod", firstStage["stage"])
}

// TestUpdateUsagePlan tests UpdateUsagePlan.
func TestUpdateUsagePlan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		newName  string
		wantCode int
		useValid bool
	}{
		{
			name:     "update_name",
			newName:  "renamed-plan",
			wantCode: http.StatusOK,
			useValid: true,
		},
		{
			name:     "plan_not_found",
			wantCode: http.StatusNotFound,
			useValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()

			createRec := postWithHandler(t, handler, e, "CreateUsagePlan", `{"name":"orig-plan"}`)
			require.Equal(t, http.StatusCreated, createRec.Code)
			var createResp map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
			planID := createResp["id"].(string)

			lookupID := planID
			if !tt.useValid {
				lookupID = "notexist"
			}

			rec := postWithHandler(t, handler, e, "UpdateUsagePlan",
				fmt.Sprintf(`{"usagePlanId":%q,"name":%q}`, lookupID, tt.newName))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestBackend_UsagePlan_ApiStages_CRUD(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()

	api1, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "plan-api-1"})
	_, _ = b.CreateDeployment(api1.ID, "prod", "v1")
	api2, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "plan-api-2"})
	_, _ = b.CreateDeployment(api2.ID, "staging", "v1")

	tests := []struct {
		check func(t *testing.T, plan *apigateway.UsagePlan)
		name  string
		input apigateway.CreateUsagePlanInput
	}{
		{
			name: "no_api_stages",
			input: apigateway.CreateUsagePlanInput{
				Name: "plain-plan",
			},
			check: func(t *testing.T, plan *apigateway.UsagePlan) {
				t.Helper()
				assert.Empty(t, plan.APIStages)
			},
		},
		{
			name: "single_api_stage",
			input: apigateway.CreateUsagePlanInput{
				Name: "single-stage-plan",
				APIStages: []apigateway.APIStageAssociation{
					{RestAPIID: api1.ID, Stage: "prod"},
				},
			},
			check: func(t *testing.T, plan *apigateway.UsagePlan) {
				t.Helper()
				require.Len(t, plan.APIStages, 1)
				assert.Equal(t, api1.ID, plan.APIStages[0].RestAPIID)
				assert.Equal(t, "prod", plan.APIStages[0].Stage)
			},
		},
		{
			name: "multiple_api_stages",
			input: apigateway.CreateUsagePlanInput{
				Name: "multi-stage-plan",
				APIStages: []apigateway.APIStageAssociation{
					{RestAPIID: api1.ID, Stage: "prod"},
					{RestAPIID: api2.ID, Stage: "staging"},
				},
			},
			check: func(t *testing.T, plan *apigateway.UsagePlan) {
				t.Helper()
				assert.Len(t, plan.APIStages, 2)
			},
		},
		{
			name: "api_stage_with_per_method_throttle",
			input: apigateway.CreateUsagePlanInput{
				Name: "throttle-method-plan",
				APIStages: []apigateway.APIStageAssociation{
					{
						RestAPIID: api1.ID,
						Stage:     "prod",
						Throttle: map[string]*apigateway.ThrottleSettings{
							"GET /items":  {RateLimit: 100, BurstLimit: 50},
							"POST /items": {RateLimit: 50, BurstLimit: 25},
						},
					},
				},
			},
			check: func(t *testing.T, plan *apigateway.UsagePlan) {
				t.Helper()
				require.Len(t, plan.APIStages, 1)
				require.NotNil(t, plan.APIStages[0].Throttle)
				assert.Contains(t, plan.APIStages[0].Throttle, "GET /items")
				assert.Contains(t, plan.APIStages[0].Throttle, "POST /items")
				assert.InDelta(t, 100.0, plan.APIStages[0].Throttle["GET /items"].RateLimit, 0.001)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			plan, err := b.CreateUsagePlan(tt.input)
			require.NoError(t, err)
			tt.check(t, plan)

			got, err := b.GetUsagePlan(plan.ID)
			require.NoError(t, err)
			tt.check(t, got)
		})
	}
}

// Test_SDKRoundTrip_UsagePlanApiStages proves the real wire field for an
// apiStages entry is "apiId" (types.ApiStage in the pinned SDK), not
// "restApiId". Before the fix, gopherstack emitted "restApiId" -- a real
// client's deserializer only reads ApiId, so it decoded as nil/empty for
// every usage plan's apiStages, even though the two sides of a hand-built
// raw-JSON test (which controls both request and response encoding) agreed
// with each other and thus could not catch the mismatch.
func Test_SDKRoundTrip_UsagePlanApiStages(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()
	client := newTestAPIGatewayClient(t, h)

	api, err := client.CreateRestApi(t.Context(), &apigatewaysdk.CreateRestApiInput{
		Name: aws.String("sdk-usageplan-api"),
	})
	require.NoError(t, err)

	_, err = client.CreateDeployment(t.Context(), &apigatewaysdk.CreateDeploymentInput{
		RestApiId: api.Id,
		StageName: aws.String("prod"),
	})
	require.NoError(t, err)

	created, err := client.CreateUsagePlan(t.Context(), &apigatewaysdk.CreateUsagePlanInput{
		Name: aws.String("sdk-stages-plan"),
		ApiStages: []apigatewaytypes.ApiStage{
			{ApiId: api.Id, Stage: aws.String("prod")},
		},
	})
	require.NoError(t, err)
	require.Len(t, created.ApiStages, 1)
	assert.Equal(t, *api.Id, *created.ApiStages[0].ApiId)
	assert.Equal(t, "prod", *created.ApiStages[0].Stage)

	got, err := client.GetUsagePlan(t.Context(), &apigatewaysdk.GetUsagePlanInput{
		UsagePlanId: created.Id,
	})
	require.NoError(t, err)
	require.Len(t, got.ApiStages, 1)
	assert.Equal(t, *api.Id, *got.ApiStages[0].ApiId)
}
