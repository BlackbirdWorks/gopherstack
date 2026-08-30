package bedrock_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

func TestHandler_CreateCustomModelDeployment(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		setup      func(*testing.T, *bedrock.Handler)
		input      map[string]any
		name       string
		wantStatus int
		wantARN    bool
	}{
		{
			name: "valid deployment",
			input: map[string]any{
				"modelArn":            "arn:aws:bedrock:us-east-1:000000000000:custom-model/cm-0000001",
				"modelDeploymentName": "my-deployment",
			},
			wantStatus: http.StatusCreated,
			wantARN:    true,
		},
		{
			name: "missing deployment name",
			input: map[string]any{
				"modelArn":            "arn:aws:bedrock:us-east-1:000000000000:custom-model/cm-0000001",
				"modelDeploymentName": "",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "duplicate deployment name",
			setup: func(t *testing.T, h *bedrock.Handler) {
				t.Helper()
				_, err := h.Backend.CreateCustomModelDeployment("some-arn", "dup-deploy", nil)
				require.NoError(t, err)
			},
			input: map[string]any{
				"modelArn":            "arn:aws:bedrock:us-east-1:000000000000:custom-model/cm-0000001",
				"modelDeploymentName": "dup-deploy",
			},
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests { //nolint:paralleltest // existing issue.
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doRequest(t, h, http.MethodPost, "/model-customization/custom-model-deployments", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantARN {
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				assert.NotEmpty(t, out["customModelDeploymentArn"])
			}
		})
	}
}

func TestHandler_TagsOnCustomModelDeployment(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/model-customization/custom-model-deployments", map[string]any{
		"modelArn":            "arn:aws:bedrock:us-east-1:000000000000:custom-model/cm-0000001",
		"modelDeploymentName": "tagged-deploy",
		"tags": []map[string]string{
			{"key": "stage", "value": "beta"},
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	deployARN := out["customModelDeploymentArn"].(string)

	rec2 := doRequest(t, h, http.MethodPost, "/listTagsForResource", map[string]any{
		"resourceARN": deployARN,
	})
	assert.Equal(t, http.StatusOK, rec2.Code)

	var tagsOut map[string]any
	mustUnmarshal(t, rec2, &tagsOut)
	assert.Len(t, tagsOut["tags"].([]any), 1)
}

func TestHandler_CreateCustomModelDeploymentMissingModelARN( //nolint:paralleltest // existing issue.
	t *testing.T,
) {
	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model-customization/custom-model-deployments", map[string]any{
		"modelArn":            "",
		"modelDeploymentName": "my-deploy",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAccuracy_CustomModelDeployment_StatusIsActive(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)

	// Create a custom model first
	modelRec := doRequest(t, h, http.MethodPost, "/custom-models/create-custom-model", map[string]any{
		"modelName":           "my-fine-tuned-model",
		"baseModelIdentifier": "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-text-express-v1",
		"roleArn":             "arn:aws:iam::000000000000:role/bedrock-custom-role",
		"outputDataConfig":    map[string]any{"s3Uri": "s3://my-bucket/output/"},
	})
	require.Equal(t, http.StatusCreated, modelRec.Code, modelRec.Body.String())

	var modelOut map[string]any
	require.NoError(t, json.Unmarshal(modelRec.Body.Bytes(), &modelOut))
	modelARN := modelOut["modelArn"].(string)

	// Create deployment for the model
	depRec := doRequest(t, h, http.MethodPost, "/model-customization/custom-model-deployments", map[string]any{
		"modelDeploymentName": "my-deployment",
		"modelArn":            modelARN,
	})
	require.Equal(t, http.StatusCreated, depRec.Code)

	var depOut map[string]any
	require.NoError(t, json.Unmarshal(depRec.Body.Bytes(), &depOut))
	depARN := depOut["customModelDeploymentArn"].(string)
	assert.NotEmpty(t, depARN)

	getRec := doRequest(t, h, http.MethodGet, "/model-customization/custom-model-deployments/"+depARN, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	assert.Equal(t, "my-deployment", getOut["modelDeploymentName"])
	// Creating is right immediately after create, but this test's own name
	// promises "StatusIsActive" -- an assertion that only checks the first
	// moment cannot catch a machine that never advances. Confirm it actually
	// reaches Active via the janitor's advancer, not just its initial stamp.
	assert.Equal(t, "Creating", getOut["status"])

	b.AdvanceCustomModelDeploymentStatuses()

	getRec2 := doRequest(t, h, http.MethodGet, "/model-customization/custom-model-deployments/"+depARN, nil)
	require.Equal(t, http.StatusOK, getRec2.Code)

	var getOut2 map[string]any
	require.NoError(t, json.Unmarshal(getRec2.Body.Bytes(), &getOut2))
	assert.Equal(t, "Active", getOut2["status"])
}

func TestAccuracy_CustomModelDeployment_ListAfterMultipleCreates(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)

	// Create 3 deployments
	for i := range 3 {
		doRequest(t, h, http.MethodPost, "/model-customization/custom-model-deployments", map[string]any{
			"modelDeploymentName": fmt.Sprintf("deploy-list-test-%d", i),
			"modelArn":            "arn:aws:bedrock:us-east-1:000000000000:custom-model/test-model",
		})
	}

	listRec := doRequest(t, h, http.MethodGet, "/model-customization/custom-model-deployments", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	deployments := listOut["modelDeploymentSummaries"].([]any)
	assert.GreaterOrEqual(t, len(deployments), 3)
}

func TestAccuracy_CustomModelDeployment_DeleteRemovesFromList(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)

	// Create a deployment
	depRec := doRequest(t, h, http.MethodPost, "/model-customization/custom-model-deployments", map[string]any{
		"modelDeploymentName": "disposable-deploy",
		"modelArn":            "arn:aws:bedrock:us-east-1:000000000000:custom-model/x",
	})
	require.Equal(t, http.StatusCreated, depRec.Code)

	var depOut map[string]any
	require.NoError(t, json.Unmarshal(depRec.Body.Bytes(), &depOut))
	depARN := depOut["customModelDeploymentArn"].(string)

	// Delete it
	deleteRec := doRequest(t, h, http.MethodDelete, "/model-customization/custom-model-deployments/"+depARN, nil)
	assert.Equal(t, http.StatusNoContent, deleteRec.Code)

	// Get should return 404
	getRec := doRequest(t, h, http.MethodGet, "/model-customization/custom-model-deployments/"+depARN, nil)
	assert.Equal(t, http.StatusNotFound, getRec.Code)
}

func TestHandler_CustomModelDeployment_GetListUpdateDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create via the existing path
	rec := doRequest(t, h, http.MethodPost, "/model-customization/custom-model-deployments",
		map[string]any{"modelArn": "arn:aws:bedrock:us-east-1::custom-model/m1", "modelDeploymentName": "deploy-1"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	mustUnmarshal(t, rec, &created)
	deployARN := created["customModelDeploymentArn"].(string)
	assert.NotEmpty(t, deployARN)

	// List
	rec2 := doRequest(t, h, http.MethodGet, "/model-customization/custom-model-deployments", nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var listOut map[string]any
	mustUnmarshal(t, rec2, &listOut)
	assert.Len(t, listOut["modelDeploymentSummaries"], 1)

	deployPath := "/model-customization/custom-model-deployments/" + url.PathEscape(deployARN)

	// Get
	rec3 := doRequest(t, h, http.MethodGet, deployPath, nil)
	require.Equal(t, http.StatusOK, rec3.Code)

	var getOut map[string]any
	mustUnmarshal(t, rec3, &getOut)
	assert.Equal(t, deployARN, getOut["customModelDeploymentArn"])

	// Update
	rec4 := doRequest(t, h, http.MethodPatch, deployPath, nil)
	assert.Equal(t, http.StatusOK, rec4.Code)

	// Delete
	rec5 := doRequest(t, h, http.MethodDelete, deployPath, nil)
	assert.Equal(t, http.StatusNoContent, rec5.Code)

	// Get after delete
	rec6 := doRequest(t, h, http.MethodGet, deployPath, nil)
	assert.Equal(t, http.StatusNotFound, rec6.Code)
}

// TestParity_ListCustomModelDeployments_NameContainsFilter locks in the
// nameContains query filter (bedrock@v1.66.4
// api_op_ListCustomModelDeployments.go's NameContains) -- ListCustomModelDeployments
// previously took no arguments at all, so no filter, sort, or maxResults
// query parameter reached the backend regardless of what a real client sent.
func TestParity_ListCustomModelDeployments_NameContainsFilter(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("123456789012", "us-east-1")
	h := bedrock.NewHandler(b)

	_, err := b.CreateCustomModelDeployment(
		"arn:aws:bedrock:us-east-1:123456789012:custom-model/other-model", "other-deployment", nil,
	)
	require.NoError(t, err)

	wantDeploy, err := b.CreateCustomModelDeployment(
		"arn:aws:bedrock:us-east-1:123456789012:custom-model/target-model", "target-deployment", nil,
	)
	require.NoError(t, err)

	rec := doRequest(
		t, h, http.MethodGet, "/model-customization/custom-model-deployments?nameContains=target", nil,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	summaries, ok := out["modelDeploymentSummaries"].([]any)
	require.True(t, ok)
	require.Len(t, summaries, 1)

	summary, ok := summaries[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, wantDeploy.CustomModelDeploymentArn, summary["customModelDeploymentArn"])
}
