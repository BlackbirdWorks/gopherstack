package apigatewayv2_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateModel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		apiID      string
		wantName   string
		wantStatus int
	}{
		{
			name:       "success",
			body:       map[string]any{"name": "UserModel", "schema": `{"type":"object"}`},
			wantName:   "UserModel",
			wantStatus: http.StatusCreated,
		},
		{
			name:       "with_content_type",
			body:       map[string]any{"name": "OrderModel", "schema": `{}`, "contentType": "application/json"},
			wantName:   "OrderModel",
			wantStatus: http.StatusCreated,
		},
		{
			name:       "api_not_found",
			apiID:      "nonexistent",
			body:       map[string]any{"name": "M", "schema": "{}"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "invalid_body",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			apiID := tt.apiID
			if apiID == "" {
				apiID = createAPI(t, h, "model-api")
			}

			path := fmt.Sprintf("/v2/apis/%s/models", apiID)

			var rr *httptest.ResponseRecorder

			if s, ok := tt.body.(string); ok {
				rr = doRequestRaw(t, h, path, s)
			} else {
				rr = doRequest(t, h, http.MethodPost, path, tt.body)
			}

			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantName != "" {
				var model apigatewayv2.Model
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &model))
				assert.Equal(t, tt.wantName, model.Name)
				assert.NotEmpty(t, model.ModelID)
			}
		})
	}
}

// TestHandler_GetModelTemplate verifies that GetModelTemplate returns the model's
// schema as the template value, falling back to an empty object only when the model
// has no schema defined.
func TestHandler_GetModelTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		schema    string
		wantValue string
	}{
		{
			name:      "returns_schema",
			schema:    `{"type":"object","properties":{"id":{"type":"string"}}}`,
			wantValue: `{"type":"object","properties":{"id":{"type":"string"}}}`,
		},
		{
			name:      "empty_schema_falls_back_to_object",
			schema:    "",
			wantValue: "{}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "tmpl-api")

			body := map[string]any{"name": "TmplModel", "contentType": "application/json"}
			if tt.schema != "" {
				body["schema"] = tt.schema
			}

			createRec := doRequest(t, h, http.MethodPost,
				fmt.Sprintf("/v2/apis/%s/models", apiID), body)
			require.Equal(t, http.StatusCreated, createRec.Code)

			var model apigatewayv2.Model
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &model))

			rec := doRequest(t, h, http.MethodGet,
				fmt.Sprintf("/v2/apis/%s/models/%s/template", apiID, model.ModelID), nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Value string `json:"value"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, tt.wantValue, out.Value)
		})
	}
}

func TestHandler_DuplicateModelName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "duplicate_model_name_returns_409",
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/models", apiID), map[string]any{
				"name": "MyModel",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			rr = doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/models", apiID), map[string]any{
				"name": "MyModel",
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_GetModels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		apiExists  bool
		modelCnt   int
		wantStatus int
	}{
		{
			name:       "empty",
			apiExists:  true,
			modelCnt:   0,
			wantStatus: http.StatusOK,
		},
		{
			name:       "multiple",
			apiExists:  true,
			modelCnt:   2,
			wantStatus: http.StatusOK,
		},
		{
			name:       "api_not_found",
			apiExists:  false,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			apiID := "nonexistent"
			if tt.apiExists {
				apiID = createAPI(t, h, "test-api")
			}

			for i := range tt.modelCnt {
				rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/models", apiID), map[string]any{
					"name":        fmt.Sprintf("Model%d", i),
					"schema":      `{}`,
					"contentType": "application/json",
				})
				require.Equal(t, http.StatusCreated, rr.Code)
			}

			rr := doRequest(t, h, http.MethodGet, fmt.Sprintf("/v2/apis/%s/models", apiID), nil)
			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantStatus == http.StatusOK {
				var out struct {
					Items []apigatewayv2.Model `json:"items"`
				}
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &out))
				assert.Len(t, out.Items, tt.modelCnt)
			}
		})
	}
}

func TestHandler_GetModel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		useWrongID bool
		wantStatus int
	}{
		{
			name:       "found",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			useWrongID: true,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/models", apiID), map[string]any{
				"name":        "MyModel",
				"schema":      `{}`,
				"contentType": "application/json",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			var model apigatewayv2.Model
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &model))

			modelID := model.ModelID
			if tt.useWrongID {
				modelID = "nonexistent"
			}

			rr = doRequest(t, h, http.MethodGet, fmt.Sprintf("/v2/apis/%s/models/%s", apiID, modelID), nil)
			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantStatus == http.StatusOK {
				var got apigatewayv2.Model
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &got))
				assert.Equal(t, "MyModel", got.Name)
			}
		})
	}
}

func TestHandler_DeleteModel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		useWrongID bool
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "not_found",
			useWrongID: true,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/models", apiID), map[string]any{
				"name":        "MyModel",
				"schema":      `{}`,
				"contentType": "application/json",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			var model apigatewayv2.Model
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &model))

			modelID := model.ModelID
			if tt.useWrongID {
				modelID = "nonexistent"
			}

			rr = doRequest(t, h, http.MethodDelete, fmt.Sprintf("/v2/apis/%s/models/%s", apiID, modelID), nil)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_UpdateModel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *apigatewayv2.Handler) (apiID, modelID string)
		body       any
		name       string
		wantName   string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *apigatewayv2.Handler) (string, string) {
				apiID := createAPI(t, h, "test-api")
				rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/models", apiID), map[string]any{
					"name": "MyModel",
				})
				require.Equal(t, http.StatusCreated, rr.Code)
				var m apigatewayv2.Model
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &m))

				return apiID, m.ModelID
			},
			body:       map[string]any{"name": "UpdatedModel"},
			wantStatus: http.StatusOK,
			wantName:   "UpdatedModel",
		},
		{
			name: "api_not_found",
			setup: func(_ *apigatewayv2.Handler) (string, string) {
				return "nonexistent", "model123"
			},
			body:       map[string]any{"name": "x"},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "model_not_found",
			setup: func(h *apigatewayv2.Handler) (string, string) {
				return createAPI(t, h, "test-api"), "nonexistent"
			},
			body:       map[string]any{"name": "x"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID, modelID := tt.setup(h)

			rr := doRequest(t, h, http.MethodPatch,
				fmt.Sprintf("/v2/apis/%s/models/%s", apiID, modelID), tt.body)

			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantName != "" {
				var m apigatewayv2.Model
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &m))
				assert.Equal(t, tt.wantName, m.Name)
			}
		})
	}
}

func TestGetModels_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	apiID := createAPI(t, h, "model-api")

	for i := range 3 {
		rr := doRequest(t, h, http.MethodPost,
			fmt.Sprintf("/v2/apis/%s/models", apiID),
			map[string]any{
				"name":        fmt.Sprintf("Model%02d", i),
				"contentType": "application/json",
				"schema":      `{"type":"object"}`,
			},
		)
		require.Equal(t, http.StatusCreated, rr.Code)
	}

	seen := map[string]int{}
	nextToken := ""
	pages := 0

	for {
		path := fmt.Sprintf("/v2/apis/%s/models?maxResults=2", apiID)
		if nextToken != "" {
			path += "&nextToken=" + nextToken
		}

		rr := doRequest(t, h, http.MethodGet, path, nil)
		require.Equal(t, http.StatusOK, rr.Code)

		var resp struct {
			NextToken string           `json:"nextToken"`
			Items     []map[string]any `json:"items"`
		}
		require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
		require.LessOrEqual(t, len(resp.Items), 2)

		for _, m := range resp.Items {
			id, _ := m["modelId"].(string)
			seen[id]++
		}

		pages++
		nextToken = resp.NextToken

		if nextToken == "" {
			break
		}

		require.Less(t, pages, 20)
	}

	assert.Equal(t, 2, pages)
	assert.Len(t, seen, 3)

	for id, count := range seen {
		assert.Equalf(t, 1, count, "model %s duplicated", id)
	}
}
