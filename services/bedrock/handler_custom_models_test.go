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

func TestHandler_CreateCustomModel(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		setup      func(*testing.T, *bedrock.Handler)
		input      map[string]any
		name       string
		wantStatus int
		wantARN    bool
	}{
		{
			name:       "valid custom model",
			input:      map[string]any{"modelName": "my-custom-model"},
			wantStatus: http.StatusCreated,
			wantARN:    true,
		},
		{
			name:       "missing model name",
			input:      map[string]any{"modelName": ""},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "duplicate name",
			setup: func(t *testing.T, h *bedrock.Handler) {
				t.Helper()
				_, err := h.Backend.CreateCustomModel("dup-model", nil)
				require.NoError(t, err)
			},
			input:      map[string]any{"modelName": "dup-model"},
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests { //nolint:paralleltest // existing issue.
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doRequest(t, h, http.MethodPost, "/custom-models/create-custom-model", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantARN {
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				assert.NotEmpty(t, out["modelArn"])
			}
		})
	}
}

func TestHandler_TagsOnCustomModel(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/custom-models/create-custom-model", map[string]any{
		"modelName": "tagged-model",
		"modelTags": []map[string]string{
			{"key": "version", "value": "1.0"},
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	modelARN := out["modelArn"].(string)

	rec2 := doRequest(t, h, http.MethodPost, "/listTagsForResource", map[string]any{
		"resourceARN": modelARN,
	})
	assert.Equal(t, http.StatusOK, rec2.Code)

	var tagsOut map[string]any
	mustUnmarshal(t, rec2, &tagsOut)
	assert.Len(t, tagsOut["tags"].([]any), 1)
}

func TestAccuracy_CustomModel_GetViaHTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		modelName string
	}{
		{name: "get existing model", modelName: "my-custom-model"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)
			m, err := b.CreateCustomModel(tt.modelName, nil)
			require.NoError(t, err)

			rec := doRequest(t, h, http.MethodGet, "/custom-models/"+url.PathEscape(m.ModelArn), nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, m.ModelArn, out["modelArn"])
			assert.Equal(t, tt.modelName, out["modelName"])
			assert.NotEmpty(t, out["creationTime"])
		})
	}
}

func TestAccuracy_CustomModel_GetNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/custom-models/nonexistent-model-arn", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAccuracy_CustomModel_ListViaHTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		modelNames []string
		wantLen    int
	}{
		{name: "empty list", modelNames: nil, wantLen: 0},
		{name: "single model", modelNames: []string{"model-1"}, wantLen: 1},
		{name: "three models", modelNames: []string{"m-a", "m-b", "m-c"}, wantLen: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)

			for _, name := range tt.modelNames {
				_, err := b.CreateCustomModel(name, nil)
				require.NoError(t, err)
			}

			rec := doRequest(t, h, http.MethodGet, "/custom-models", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			models := out["modelSummaries"].([]any)
			assert.Len(t, models, tt.wantLen)
		})
	}
}

// TestAccuracy_CustomModel_ListFilters locks in that ListCustomModels' real
// query params (nameContains, modelStatus) are parsed and applied, not
// silently ignored (aws-sdk-go-v2 serializers.go:6152-6202).
func TestAccuracy_CustomModel_ListFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		query     string
		wantNames []string
	}{
		{name: "namecontains matches one", query: "?nameContains=alpha", wantNames: []string{"alpha-model"}},
		{name: "namecontains matches none", query: "?nameContains=zzz", wantNames: nil},
		{
			name:      "modelstatus active matches all",
			query:     "?modelStatus=Active",
			wantNames: []string{"alpha-model", "beta-model"},
		},
		{name: "modelstatus failed matches none", query: "?modelStatus=Failed", wantNames: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)

			_, err := b.CreateCustomModel("alpha-model", nil)
			require.NoError(t, err)
			_, err = b.CreateCustomModel("beta-model", nil)
			require.NoError(t, err)

			rec := doRequest(t, h, http.MethodGet, "/custom-models"+tt.query, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			models := out["modelSummaries"].([]any)

			gotNames := make([]string, 0, len(models))
			for _, m := range models {
				gotNames = append(gotNames, m.(map[string]any)["modelName"].(string))
			}

			assert.ElementsMatch(t, tt.wantNames, gotNames)
		})
	}
}

func TestAccuracy_CustomModel_DeleteViaHTTP(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)
	m, err := b.CreateCustomModel("to-delete-model", nil)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodDelete, "/custom-models/"+url.PathEscape(m.ModelArn), nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	recGet := doRequest(t, h, http.MethodGet, "/custom-models/"+url.PathEscape(m.ModelArn), nil)
	assert.Equal(t, http.StatusNotFound, recGet.Code)
}

func TestAccuracy_CustomModel_DeleteNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodDelete,
		"/custom-models/arn:aws:bedrock:us-east-1:000000000000:custom-model/no-such-model", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAccuracy_CustomModel_TagsOnCreate(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	tags := []bedrock.Tag{{Key: "project", Value: "prod"}}
	m, err := b.CreateCustomModel("tagged-model", tags)
	require.NoError(t, err)
	assert.Len(t, m.Tags, 1)
	assert.Equal(t, "project", m.Tags[0].Key)
	assert.Equal(t, "prod", m.Tags[0].Value)

	got, err := b.GetCustomModel(m.ModelArn)
	require.NoError(t, err)
	assert.Len(t, got.Tags, 1)
	assert.Equal(t, "project", got.Tags[0].Key)
}

func TestHandler_CustomModelLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create via handler.
	rec := doRequest(t, h, http.MethodPost, "/custom-models/create-custom-model", map[string]any{
		"modelName": "lifecycle-model",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	modelARN := createOut["modelArn"].(string)

	// List custom models.
	rec2 := doRequest(t, h, http.MethodGet, "/custom-models", nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &listOut))
	assert.NotEmpty(t, listOut["modelSummaries"])

	// Get custom model by ARN.
	rec3 := doRequest(t, h, http.MethodGet, "/custom-models/"+url.PathEscape(modelARN), nil)
	assert.Equal(t, http.StatusOK, rec3.Code)

	// Delete custom model.
	rec4 := doRequest(t, h, http.MethodDelete, "/custom-models/"+url.PathEscape(modelARN), nil)
	assert.Equal(t, http.StatusOK, rec4.Code)
}
