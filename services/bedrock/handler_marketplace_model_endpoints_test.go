package bedrock_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAccuracy_MarketplaceEndpoint_CreateStartsAsCreating(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		endpointName  string
		modelSourceID string
		wantStatus    int
	}{
		{
			name:          "valid endpoint starts Creating",
			endpointName:  "my-endpoint",
			modelSourceID: "arn:aws:sagemaker:us-east-1:000000000000:endpoint/my-ep",
			wantStatus:    http.StatusCreated,
		},
		{
			name:         "missing endpoint name returns 400",
			endpointName: "",
			wantStatus:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(
				t, h, http.MethodPost, "/marketplace-model/endpoints",
				map[string]any{
					"endpointName":          tt.endpointName,
					"modelSourceIdentifier": tt.modelSourceID,
				},
			)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusCreated {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				ep := out["marketplaceModelEndpoint"].(map[string]any)
				assert.Equal(t, "Creating", ep["status"])
				assert.NotEmpty(t, ep["endpointArn"])
				assert.Equal(t, tt.endpointName, ep["endpointName"])
				assert.Equal(t, tt.modelSourceID, ep["modelSourceIdentifier"])
				assert.NotEmpty(t, ep["createdAt"])
				assert.NotEmpty(t, ep["updatedAt"])
			}
		})
	}
}

func TestAccuracy_MarketplaceEndpoint_RegisterTransitionsToActive(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)
	ep, err := b.CreateMarketplaceModelEndpoint("activate-ep", "src-id", nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "Creating", ep.Status)

	rec := doRequest(t, h, http.MethodPost,
		"/marketplace-model/endpoints/"+url.PathEscape(ep.EndpointArn)+"/registration", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	got, err := b.GetMarketplaceModelEndpoint(ep.EndpointArn)
	require.NoError(t, err)
	assert.Equal(t, "Active", got.Status)
}

func TestAccuracy_MarketplaceEndpoint_DeregisterTransitionsToDeregistered(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)
	ep, err := b.CreateMarketplaceModelEndpoint("deactivate-ep", "src-id", nil, nil)
	require.NoError(t, err)

	// Register first.
	require.NoError(t, b.RegisterMarketplaceModelEndpoint(ep.EndpointArn))

	rec := doRequest(t, h, http.MethodDelete,
		"/marketplace-model/endpoints/"+url.PathEscape(ep.EndpointArn)+"/registration", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	got, err := b.GetMarketplaceModelEndpoint(ep.EndpointArn)
	require.NoError(t, err)
	assert.Equal(t, "Deregistered", got.Status)
}

func TestAccuracy_MarketplaceEndpoint_GetNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/marketplace-model/endpoints/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAccuracy_MarketplaceEndpoint_DeleteNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodDelete, "/marketplace-model/endpoints/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAccuracy_MarketplaceEndpoint_DuplicateNameConflict(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)

	rec1 := doRequest(t, h, http.MethodPost, "/marketplace-model/endpoints",
		map[string]any{"endpointName": "dup-ep", "modelSourceIdentifier": "src"})
	require.Equal(t, http.StatusCreated, rec1.Code)

	rec2 := doRequest(t, h, http.MethodPost, "/marketplace-model/endpoints",
		map[string]any{"endpointName": "dup-ep", "modelSourceIdentifier": "src"})
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

func TestAccuracy_MarketplaceEndpoint_ListResponseShape(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)

	names := []string{"ep-one", "ep-two", "ep-three"}
	for _, n := range names {
		_, err := b.CreateMarketplaceModelEndpoint(n, "src", nil, nil)
		require.NoError(t, err)
	}

	rec := doRequest(t, h, http.MethodGet, "/marketplace-model/endpoints", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	endpoints := out["marketplaceModelEndpoints"].([]any)
	assert.Len(t, endpoints, 3)

	for _, raw := range endpoints {
		ep := raw.(map[string]any)
		assert.NotEmpty(t, ep["endpointArn"])
		assert.NotEmpty(t, ep["endpointName"])
		assert.Equal(t, "Creating", ep["status"])
		assert.NotEmpty(t, ep["createdAt"])
		assert.NotEmpty(t, ep["updatedAt"])
	}
}

func TestAccuracy_MarketplaceEndpoint_DeleteRemovesFromList(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)
	ep, err := b.CreateMarketplaceModelEndpoint("del-ep", "src", nil, nil)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodDelete, "/marketplace-model/endpoints/"+url.PathEscape(ep.EndpointArn), nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	recList := doRequest(t, h, http.MethodGet, "/marketplace-model/endpoints", nil)
	require.Equal(t, http.StatusOK, recList.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &out))
	assert.Empty(t, out["marketplaceModelEndpoints"].([]any))
}

func TestAccuracy_MarketplaceEndpoint_UpdateReturnsEndpoint(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)
	ep, err := b.CreateMarketplaceModelEndpoint("update-ep", "src", nil, nil)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodPatch, "/marketplace-model/endpoints/"+url.PathEscape(ep.EndpointArn), nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, ep.EndpointArn, out["endpointArn"])
	assert.NotEmpty(t, out["updatedAt"])
}

// TestAccuracy_MarketplaceEndpoint_UpdateAppliesEndpointConfig locks in the
// parity fix for UpdateMarketplaceModelEndpoint: the real API's required
// EndpointConfig request field must actually be parsed and stored, not
// silently discarded (gopherstack previously only bumped UpdatedAt).
func TestAccuracy_MarketplaceEndpoint_UpdateAppliesEndpointConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, http.MethodPost, "/marketplace-model/endpoints", map[string]any{
		"endpointName":          "cfg-ep",
		"modelSourceIdentifier": "src-id",
		"endpointConfig": map[string]any{
			"sageMaker": map[string]any{
				"executionRole":        "arn:aws:iam::000000000000:role/exec",
				"instanceType":         "ml.m5.xlarge",
				"initialInstanceCount": 1,
			},
		},
	})
	require.Equal(t, http.StatusCreated, createRec.Code)

	var createOut map[string]any
	mustUnmarshal(t, createRec, &createOut)
	ep := createOut["marketplaceModelEndpoint"].(map[string]any)
	endpointARN := ep["endpointArn"].(string)

	createdCfg := ep["endpointConfig"].(map[string]any)["sageMaker"].(map[string]any)
	assert.Equal(t, "arn:aws:iam::000000000000:role/exec", createdCfg["executionRole"])
	assert.Equal(t, "ml.m5.xlarge", createdCfg["instanceType"])
	assert.InEpsilon(t, float64(1), createdCfg["initialInstanceCount"], 0)

	// Update with a DIFFERENT config -- must actually apply, not no-op.
	updateRec := doRequest(t, h, http.MethodPatch, "/marketplace-model/endpoints/"+url.PathEscape(endpointARN),
		map[string]any{
			"endpointConfig": map[string]any{
				"sageMaker": map[string]any{
					"executionRole":        "arn:aws:iam::000000000000:role/exec2",
					"instanceType":         "ml.m5.2xlarge",
					"initialInstanceCount": 3,
				},
			},
		})
	require.Equal(t, http.StatusOK, updateRec.Code)

	var updateOut map[string]any
	mustUnmarshal(t, updateRec, &updateOut)
	updatedCfg := updateOut["endpointConfig"].(map[string]any)["sageMaker"].(map[string]any)
	assert.Equal(t, "arn:aws:iam::000000000000:role/exec2", updatedCfg["executionRole"])
	assert.Equal(t, "ml.m5.2xlarge", updatedCfg["instanceType"])
	assert.InEpsilon(t, float64(3), updatedCfg["initialInstanceCount"], 0)

	// Get must reflect the applied update too, not just the Update response.
	getRec := doRequest(t, h, http.MethodGet, "/marketplace-model/endpoints/"+url.PathEscape(endpointARN), nil)
	var getOut map[string]any
	mustUnmarshal(t, getRec, &getOut)
	getCfg := getOut["endpointConfig"].(map[string]any)["sageMaker"].(map[string]any)
	assert.Equal(t, "ml.m5.2xlarge", getCfg["instanceType"])
}

// TestAccuracy_MarketplaceEndpoint_UpdateWithoutEndpointConfigPreservesExisting
// verifies a PATCH with no endpointConfig field leaves the previously-stored
// config untouched instead of clearing it.
func TestAccuracy_MarketplaceEndpoint_UpdateWithoutEndpointConfigPreservesExisting(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)

	ep, err := b.CreateMarketplaceModelEndpoint("preserve-ep", "src", &bedrock.SageMakerEndpointConfig{
		ExecutionRole: "arn:aws:iam::000000000000:role/original",
		InstanceType:  "ml.m5.large",
	}, nil)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodPatch, "/marketplace-model/endpoints/"+url.PathEscape(ep.EndpointArn), nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	cfg := out["endpointConfig"].(map[string]any)["sageMaker"].(map[string]any)
	assert.Equal(t, "arn:aws:iam::000000000000:role/original", cfg["executionRole"])
}

func TestHandler_MarketplaceModelEndpointLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create marketplace endpoint.
	rec := doRequest(t, h, http.MethodPost, "/marketplace-model/endpoints", map[string]any{
		"endpointName":          "my-market-endpoint",
		"modelSourceIdentifier": "arn:aws:sagemaker:us-east-1:000000000000:hub-content/my-hub/Model/my-model",
		"endpointConfig":        map[string]any{},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	endpointARN := createOut["marketplaceModelEndpoint"].(map[string]any)["endpointArn"].(string)

	// List endpoints.
	rec2 := doRequest(t, h, http.MethodGet, "/marketplace-model/endpoints", nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	// Get endpoint.
	rec3 := doRequest(t, h, http.MethodGet, "/marketplace-model/endpoints/"+url.PathEscape(endpointARN), nil)
	assert.Equal(t, http.StatusOK, rec3.Code)

	// Update endpoint.
	rec4 := doRequest(
		t, h, http.MethodPatch,
		"/marketplace-model/endpoints/"+url.PathEscape(endpointARN),
		map[string]any{"endpointConfig": map[string]any{}},
	)
	assert.Equal(t, http.StatusOK, rec4.Code)

	// Register endpoint.
	regPath := "/marketplace-model/endpoints/" + url.PathEscape(endpointARN) + "/registration"
	rec5 := doRequest(t, h, http.MethodPost, regPath, nil)
	assert.Equal(t, http.StatusOK, rec5.Code)

	// Deregister endpoint (same "/registration" path, DELETE method).
	rec6 := doRequest(t, h, http.MethodDelete, regPath, nil)
	assert.Equal(t, http.StatusOK, rec6.Code)

	// Delete endpoint.
	rec7 := doRequest(t, h, http.MethodDelete, "/marketplace-model/endpoints/"+url.PathEscape(endpointARN), nil)
	assert.Equal(t, http.StatusOK, rec7.Code)
}
