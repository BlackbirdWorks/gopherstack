package apigateway_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

// deepAPIOpts configures the method created by buildDeepAPI.
type deepAPIOpts struct {
	requestParams  map[string]bool
	requestModels  map[string]string
	modelName      string
	modelSchema    string
	httpMethod     string
	validateBody   bool
	validateParams bool
	hasValidator   bool
	apiKeyRequired bool
}

// buildDeepAPI provisions a deployed REST API with a single MOCK-backed method on
// /items, configured per opts, and returns the handler, echo instance, backend, and
// API ID. The MOCK integration returns HTTP 200 for allowed requests.
func buildDeepAPI(
	t *testing.T,
	opts deepAPIOpts,
) (*apigateway.Handler, *echo.Echo, *apigateway.InMemoryBackend, string) {
	t.Helper()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	e := echo.New()

	api, err := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "deep-api"})
	require.NoError(t, err)

	resources, _, err := backend.GetResources(api.ID, "", 0)
	require.NoError(t, err)
	rootID := resources[0].ID

	child, err := backend.CreateResource(api.ID, rootID, "items")
	require.NoError(t, err)

	if opts.modelName != "" {
		_, err = backend.CreateModel(apigateway.CreateModelInput{
			RestAPIID:   api.ID,
			Name:        opts.modelName,
			ContentType: "application/json",
			Schema:      opts.modelSchema,
		})
		require.NoError(t, err)
	}

	validatorID := ""
	if opts.hasValidator {
		rv, verr := backend.CreateRequestValidator(api.ID, apigateway.CreateRequestValidatorInput{
			Name:                      "validator",
			ValidateRequestBody:       opts.validateBody,
			ValidateRequestParameters: opts.validateParams,
		})
		require.NoError(t, verr)
		validatorID = rv.ID
	}

	method := opts.httpMethod
	if method == "" {
		method = http.MethodGet
	}

	_, err = backend.PutMethod(apigateway.PutMethodInput{
		RestAPIID:          api.ID,
		ResourceID:         child.ID,
		HTTPMethod:         method,
		AuthorizationType:  "NONE",
		APIKeyRequired:     opts.apiKeyRequired,
		RequestValidatorID: validatorID,
		RequestParameters:  opts.requestParams,
		RequestModels:      opts.requestModels,
	})
	require.NoError(t, err)

	_, err = backend.PutIntegration(api.ID, child.ID, method, apigateway.PutIntegrationInput{Type: "MOCK"})
	require.NoError(t, err)
	_, err = backend.PutIntegrationResponse(api.ID, child.ID, method, "200", apigateway.PutIntegrationResponseInput{})
	require.NoError(t, err)

	// CreateDeployment with a stage name also creates the "prod" stage.
	_, err = backend.CreateDeployment(api.ID, "prod", "v1")
	require.NoError(t, err)

	return h, e, backend, api.ID
}

// deepReq issues a data-plane request through the handler and returns the recorder.
func deepReq(
	t *testing.T, h *apigateway.Handler, e *echo.Echo, method, apiID, query, body string, headers map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()

	url := "/restapis/" + apiID + "/prod/_user_request_/items" + query
	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(method, url, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, url, nil)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

const personSchema = `{
	"$schema": "http://json-schema.org/draft-04/schema#",
	"type": "object",
	"required": ["name", "age"],
	"properties": {
		"name": {"type": "string", "minLength": 1},
		"age": {"type": "integer", "minimum": 0}
	},
	"additionalProperties": false
}`

func TestProxy_RequestValidation_Body(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{name: "valid_body", body: `{"name":"ada","age":36}`, wantStatus: http.StatusOK},
		{name: "malformed_json", body: `{"name":`, wantStatus: http.StatusBadRequest},
		{name: "missing_required_property", body: `{"name":"ada"}`, wantStatus: http.StatusBadRequest},
		{name: "wrong_type", body: `{"name":"ada","age":"old"}`, wantStatus: http.StatusBadRequest},
		{name: "additional_property", body: `{"name":"ada","age":36,"x":1}`, wantStatus: http.StatusBadRequest},
		{name: "non_integer_age", body: `{"name":"ada","age":1.5}`, wantStatus: http.StatusBadRequest},
		{name: "empty_body", body: `{}`, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, e, _, apiID := buildDeepAPI(t, deepAPIOpts{
				httpMethod:    http.MethodPost,
				hasValidator:  true,
				validateBody:  true,
				modelName:     "Person",
				modelSchema:   personSchema,
				requestModels: map[string]string{"application/json": "Person"},
			})

			rec := deepReq(t, h, e, http.MethodPost, apiID, "", tt.body, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantStatus == http.StatusBadRequest {
				assert.Contains(t, rec.Body.String(), "Invalid request body")
			}
		})
	}
}

func TestProxy_RequestValidation_Parameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		headers    map[string]string
		name       string
		query      string
		wantStatus int
	}{
		{
			name:       "all_present",
			query:      "?q=hello",
			headers:    map[string]string{"X-Trace": "abc"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_query",
			headers:    map[string]string{"X-Trace": "abc"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_header",
			query:      "?q=hello",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, e, _, apiID := buildDeepAPI(t, deepAPIOpts{
				httpMethod:     http.MethodGet,
				hasValidator:   true,
				validateParams: true,
				requestParams: map[string]bool{
					"method.request.querystring.q":  true,
					"method.request.header.X-Trace": true,
				},
			})

			rec := deepReq(t, h, e, http.MethodGet, apiID, tt.query, "", tt.headers)
			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantStatus == http.StatusBadRequest {
				assert.Contains(t, rec.Body.String(), "Missing required request parameters")
			}
		})
	}
}

// associateKeyToPlan wires an API key into a usage plan attached to the prod stage.
func associateKeyToPlan(
	t *testing.T, b *apigateway.InMemoryBackend, apiID, keyID string,
	quota *apigateway.QuotaSettings, throttle *apigateway.ThrottleSettings,
) {
	t.Helper()

	plan, err := b.CreateUsagePlan(apigateway.CreateUsagePlanInput{
		Name:     "plan",
		Quota:    quota,
		Throttle: throttle,
		APIStages: []apigateway.APIStageAssociation{
			{RestAPIID: apiID, Stage: "prod"},
		},
	})
	require.NoError(t, err)

	_, err = b.CreateUsagePlanKey(apigateway.CreateUsagePlanKeyInput{
		UsagePlanID: plan.ID,
		KeyID:       keyID,
		KeyType:     "API_KEY",
	})
	require.NoError(t, err)
}

func TestProxy_UsagePlan_QuotaEnforced(t *testing.T) {
	t.Parallel()

	h, e, backend, apiID := buildDeepAPI(t, deepAPIOpts{httpMethod: http.MethodGet, apiKeyRequired: true})

	key, err := backend.CreateAPIKey(apigateway.CreateAPIKeyInput{Name: "k", Enabled: true})
	require.NoError(t, err)

	associateKeyToPlan(t, backend, apiID, key.ID,
		&apigateway.QuotaSettings{Limit: 2, Period: "DAY"}, nil)

	headers := map[string]string{"X-Api-Key": key.Value}

	// Under the quota (2) → allowed.
	assert.Equal(t, http.StatusOK, deepReq(t, h, e, http.MethodGet, apiID, "", "", headers).Code)
	assert.Equal(t, http.StatusOK, deepReq(t, h, e, http.MethodGet, apiID, "", "", headers).Code)

	// Third request exceeds the quota → 429 LimitExceededException.
	over := deepReq(t, h, e, http.MethodGet, apiID, "", "", headers)
	assert.Equal(t, http.StatusTooManyRequests, over.Code)
	assert.Equal(t, "LimitExceededException", over.Header().Get("X-Amzn-Errortype"))
	assert.Contains(t, over.Body.String(), "Limit Exceeded")
}

func TestProxy_UsagePlan_ThrottleEnforced(t *testing.T) {
	t.Parallel()

	h, e, backend, apiID := buildDeepAPI(t, deepAPIOpts{httpMethod: http.MethodGet, apiKeyRequired: true})

	key, err := backend.CreateAPIKey(apigateway.CreateAPIKeyInput{Name: "k", Enabled: true})
	require.NoError(t, err)

	// Rate 1/s, burst 2: two immediate requests allowed, the third throttled.
	associateKeyToPlan(t, backend, apiID, key.ID, nil,
		&apigateway.ThrottleSettings{RateLimit: 1, BurstLimit: 2})

	headers := map[string]string{"X-Api-Key": key.Value}

	assert.Equal(t, http.StatusOK, deepReq(t, h, e, http.MethodGet, apiID, "", "", headers).Code)
	assert.Equal(t, http.StatusOK, deepReq(t, h, e, http.MethodGet, apiID, "", "", headers).Code)

	over := deepReq(t, h, e, http.MethodGet, apiID, "", "", headers)
	assert.Equal(t, http.StatusTooManyRequests, over.Code)
	assert.Equal(t, "TooManyRequestsException", over.Header().Get("X-Amzn-Errortype"))
	assert.Contains(t, over.Body.String(), "Too Many Requests")
}

func TestProxy_UsagePlan_UnassociatedKeyNotThrottled(t *testing.T) {
	t.Parallel()

	h, e, backend, apiID := buildDeepAPI(t, deepAPIOpts{httpMethod: http.MethodGet, apiKeyRequired: true})

	key, err := backend.CreateAPIKey(apigateway.CreateAPIKeyInput{Name: "k", Enabled: true})
	require.NoError(t, err)

	headers := map[string]string{"X-Api-Key": key.Value}
	// No usage plan association → the key authenticates but is never throttled.
	for range 5 {
		assert.Equal(t, http.StatusOK, deepReq(t, h, e, http.MethodGet, apiID, "", "", headers).Code)
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

func TestBackend_GetUsage_RealItems(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "a"})
	require.NoError(t, err)
	key, err := b.CreateAPIKey(apigateway.CreateAPIKeyInput{Name: "k", Enabled: true})
	require.NoError(t, err)

	plan, err := b.CreateUsagePlan(apigateway.CreateUsagePlanInput{
		Name:      "plan",
		Quota:     &apigateway.QuotaSettings{Limit: 100, Period: "DAY"},
		APIStages: []apigateway.APIStageAssociation{{RestAPIID: api.ID, Stage: "prod"}},
	})
	require.NoError(t, err)
	_, err = b.CreateUsagePlanKey(apigateway.CreateUsagePlanKeyInput{
		UsagePlanID: plan.ID, KeyID: key.ID, KeyType: "API_KEY",
	})
	require.NoError(t, err)

	// Meter three requests.
	for range 3 {
		require.NoError(t, b.EnforceUsagePlan(api.ID, "prod", key.ID))
	}

	usage, err := b.GetUsage(apigateway.GetUsageInput{UsagePlanID: plan.ID})
	require.NoError(t, err)
	require.Contains(t, usage.Items, key.ID)

	pair, ok := usage.Items[key.ID][0].([]int)
	require.True(t, ok, "usage item should be a [used, remaining] pair")
	assert.Equal(t, 3, pair[0], "used count should reflect metered requests")
	assert.Equal(t, 97, pair[1], "remaining should be limit minus used")
}

func TestBackend_GetAPIKeyByValue_IndexConsistency(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	key, err := b.CreateAPIKey(apigateway.CreateAPIKeyInput{Name: "k", Value: "secret-value-1", Enabled: true})
	require.NoError(t, err)

	got, err := b.GetAPIKeyByValue("secret-value-1")
	require.NoError(t, err)
	assert.Equal(t, key.ID, got.ID)

	// After deletion the value must no longer resolve.
	require.NoError(t, b.DeleteAPIKey(key.ID))
	_, err = b.GetAPIKeyByValue("secret-value-1")
	require.Error(t, err)
}

func TestBackend_GetResources_DefaultPageSizeIs25(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "a"})
	require.NoError(t, err)
	resources, _, err := b.GetResources(api.ID, "", 0)
	require.NoError(t, err)
	rootID := resources[0].ID

	// Root + 30 children = 31 resources.
	for i := range 30 {
		_, cerr := b.CreateResource(api.ID, rootID, "p"+string(rune('a'+i%26))+string(rune('0'+i/26)))
		require.NoError(t, cerr)
	}

	page, token, err := b.GetResources(api.ID, "", 0)
	require.NoError(t, err)
	assert.Len(t, page, 25, "default page size must be AWS's 25, not 500")
	assert.NotEmpty(t, token, "a next-page cursor must be returned when more resources remain")
}

func TestProxy_TrieCache_InvalidatesOnNewResource(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	e := echo.New()

	api, err := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "trie-api"})
	require.NoError(t, err)
	resources, _, err := backend.GetResources(api.ID, "", 0)
	require.NoError(t, err)
	rootID := resources[0].ID

	first, err := backend.CreateResource(api.ID, rootID, "first")
	require.NoError(t, err)
	wireMock(t, backend, api.ID, first.ID)
	_, err = backend.CreateDeployment(api.ID, "prod", "v1")
	require.NoError(t, err)

	// Prime the trie cache.
	assert.Equal(t, http.StatusOK, rawProxyGet(t, h, e, api.ID, "/first").Code)
	assert.Equal(t, http.StatusNotFound, rawProxyGet(t, h, e, api.ID, "/second").Code)

	// Add a new resource; the cached trie must be invalidated by the version bump.
	second, err := backend.CreateResource(api.ID, rootID, "second")
	require.NoError(t, err)
	wireMock(t, backend, api.ID, second.ID)

	assert.Equal(t, http.StatusOK, rawProxyGet(t, h, e, api.ID, "/second").Code)
}

func wireMock(t *testing.T, b *apigateway.InMemoryBackend, apiID, resourceID string) {
	t.Helper()
	_, err := b.PutMethod(apigateway.PutMethodInput{
		RestAPIID: apiID, ResourceID: resourceID, HTTPMethod: http.MethodGet, AuthorizationType: "NONE",
	})
	require.NoError(t, err)
	_, err = b.PutIntegration(apiID, resourceID, http.MethodGet, apigateway.PutIntegrationInput{Type: "MOCK"})
	require.NoError(t, err)
	_, err = b.PutIntegrationResponse(
		apiID,
		resourceID,
		http.MethodGet,
		"200",
		apigateway.PutIntegrationResponseInput{},
	)
	require.NoError(t, err)
}

func rawProxyGet(t *testing.T, h *apigateway.Handler, e *echo.Echo, apiID, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/restapis/"+apiID+"/prod/_user_request_"+path, nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}
