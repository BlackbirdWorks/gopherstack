package bedrock_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

func newTestHandler(t *testing.T) *bedrock.Handler {
	t.Helper()

	return bedrock.NewHandler(bedrock.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doRequest(
	t *testing.T,
	h *bedrock.Handler,
	method, path string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func mustUnmarshal(t *testing.T, rec *httptest.ResponseRecorder, v any) {
	t.Helper()
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), v))
}

// --- Handler metadata tests ---

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "Bedrock", h.Name())
}

func TestHandler_ChaosServiceName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "bedrock", h.ChaosServiceName())
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, service.PriorityPathVersioned, h.MatchPriority())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	for _, op := range []string{
		"CreateGuardrail",
		"GetGuardrail",
		"ListGuardrails",
		"UpdateGuardrail",
		"DeleteGuardrail",
		"ListFoundationModels",
		"GetFoundationModel",
		"CreateProvisionedModelThroughput",
		"GetProvisionedModelThroughput",
		"ListProvisionedModelThroughputs",
		"UpdateProvisionedModelThroughput",
		"DeleteProvisionedModelThroughput",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
	} {
		assert.Contains(t, ops, op)
	}
}

func TestHandler_ChaosOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
}

func TestHandler_ChaosRegions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, []string{"us-east-1"}, h.ChaosRegions())
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name string
		path string
		want bool
	}{
		{"guardrails path", "/guardrails", true},
		{"guardrail with id", "/guardrails/abc123", true},
		{"foundation models", "/foundation-models", true},
		{"foundation model with id", "/foundation-models/amazon.titan-text-express-v1", true},
		{"provisioned throughput", "/provisioned-model-throughput", true},
		{"provisioned throughputs list", "/provisioned-model-throughputs", true},
		{"list tags", "/listTagsForResource", true},
		{"tag resource", "/tagResource", true},
		{"untag resource", "/untagResource", true},
		{"unmatched path", "/something-else", false},
		{"s3 path", "/s3/bucket", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			matcher := h.RouteMatcher()
			assert.Equal(t, tt.want, matcher(c))
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name   string
		method string
		path   string
		want   string
	}{
		{"CreateGuardrail", http.MethodPost, "/guardrails", "CreateGuardrail"},
		{"ListGuardrails", http.MethodGet, "/guardrails", "ListGuardrails"},
		{"GetGuardrail", http.MethodGet, "/guardrails/abc123", "GetGuardrail"},
		{"UpdateGuardrail", http.MethodPut, "/guardrails/abc123", "UpdateGuardrail"},
		{"DeleteGuardrail", http.MethodDelete, "/guardrails/abc123", "DeleteGuardrail"},
		{"ListFoundationModels", http.MethodGet, "/foundation-models", "ListFoundationModels"},
		{
			"GetFoundationModel",
			http.MethodGet,
			"/foundation-models/amazon.titan-text-express-v1",
			"GetFoundationModel",
		},
		{
			"CreateProvisionedModelThroughput",
			http.MethodPost,
			"/provisioned-model-throughput",
			"CreateProvisionedModelThroughput",
		},
		{
			"ListProvisionedModelThroughputs",
			http.MethodGet,
			"/provisioned-model-throughputs",
			"ListProvisionedModelThroughputs",
		},
		{
			"GetProvisionedModelThroughput",
			http.MethodGet,
			"/provisioned-model-throughput/pmt-123",
			"GetProvisionedModelThroughput",
		},
		{
			"UpdateProvisionedModelThroughput",
			http.MethodPut,
			"/provisioned-model-throughput/pmt-123",
			"UpdateProvisionedModelThroughput",
		},
		{
			"DeleteProvisionedModelThroughput",
			http.MethodDelete,
			"/provisioned-model-throughput/pmt-123",
			"DeleteProvisionedModelThroughput",
		},
		{"ListTagsForResource", http.MethodPost, "/listTagsForResource", "ListTagsForResource"},
		{"TagResource", http.MethodPost, "/tagResource", "TagResource"},
		{"UntagResource", http.MethodPost, "/untagResource", "UntagResource"},
		{"Unknown", http.MethodGet, "/unknown", "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			assert.Equal(t, tt.want, h.ExtractOperation(c))
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name string
		path string
		want string
	}{
		{"guardrail id", "/guardrails/abc123", "abc123"},
		{"foundation model id", "/foundation-models/amazon.titan-text-express-v1", "amazon.titan-text-express-v1"},
		{"provisioned throughput id", "/provisioned-model-throughput/pmt-001", "pmt-001"},
		{"no resource", "/guardrails", ""},
		{"tags path", "/listTagsForResource", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			assert.Equal(t, tt.want, h.ExtractResource(c))
		})
	}
}

// --- Guardrail CRUD tests ---

func TestHandler_CreateGuardrail(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input      map[string]any
		wantFields map[string]string
		name       string
		wantStatus int
	}{
		{
			name: "valid guardrail",
			input: map[string]any{
				"name":                    "test-guardrail",
				"description":             "A test guardrail",
				"blockedInputMessaging":   "blocked input",
				"blockedOutputsMessaging": "blocked output",
			},
			wantStatus: http.StatusOK,
			wantFields: map[string]string{
				"guardrailId":  "",
				"guardrailArn": "",
				"version":      "DRAFT",
			},
		},
		{
			name:       "empty body",
			input:      nil,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.input == nil {
				e := echo.New()
				req := httptest.NewRequest(http.MethodPost, "/guardrails", bytes.NewReader([]byte("invalid json")))
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()
				c := e.NewContext(req, rec)
				err := h.Handler()(c)
				require.NoError(t, err)
				assert.Equal(t, tt.wantStatus, rec.Code)

				return
			}

			rec := doRequest(t, h, http.MethodPost, "/guardrails", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				assert.NotEmpty(t, out["guardrailId"])
				assert.NotEmpty(t, out["guardrailArn"])
				assert.Equal(t, "DRAFT", out["version"])
			}
		})
	}
}

func TestHandler_GetGuardrail(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*bedrock.Handler) string
		name       string
		id         string
		wantStatus int
	}{
		{
			name: "existing guardrail",
			setup: func(h *bedrock.Handler) string {
				g, err := h.Backend.CreateGuardrail("test", "desc", "blocked-in", "blocked-out", nil)
				if err != nil {
					panic(err)
				}

				return g.GuardrailID
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "non-existent guardrail",
			id:         "nonexistent",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			id := tt.id
			if tt.setup != nil {
				id = tt.setup(h)
			}

			rec := doRequest(t, h, http.MethodGet, "/guardrails/"+id, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				assert.Equal(t, id, out["guardrailId"])
				assert.Equal(t, "READY", out["status"])
			}
		})
	}
}

func TestHandler_ListGuardrails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(*bedrock.Handler)
		name  string
		want  int
	}{
		{
			name:  "empty",
			setup: func(*bedrock.Handler) {},
			want:  0,
		},
		{
			name: "two guardrails",
			setup: func(h *bedrock.Handler) {
				_, err := h.Backend.CreateGuardrail("g1", "", "", "", nil)
				require.NoError(t, err)
				_, err = h.Backend.CreateGuardrail("g2", "", "", "", nil)
				require.NoError(t, err)
			},
			want: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(h)

			rec := doRequest(t, h, http.MethodGet, "/guardrails", nil)
			assert.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			mustUnmarshal(t, rec, &out)
			guardrails := out["guardrails"].([]any)
			assert.Len(t, guardrails, tt.want)
		})
	}
}

func TestHandler_UpdateGuardrail(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*bedrock.Handler) string
		input      map[string]any
		name       string
		id         string
		wantStatus int
	}{
		{
			name: "update existing",
			setup: func(h *bedrock.Handler) string {
				g, err := h.Backend.CreateGuardrail("test", "old desc", "", "", nil)
				if err != nil {
					panic(err)
				}

				return g.GuardrailID
			},
			input: map[string]any{
				"name":        "test",
				"description": "new desc",
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "update non-existent",
			id:         "nonexistent",
			input:      map[string]any{"name": "test"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			id := tt.id
			if tt.setup != nil {
				id = tt.setup(h)
			}

			rec := doRequest(t, h, http.MethodPut, "/guardrails/"+id, tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DeleteGuardrail(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*bedrock.Handler) string
		name       string
		id         string
		wantStatus int
	}{
		{
			name: "delete existing",
			setup: func(h *bedrock.Handler) string {
				g, err := h.Backend.CreateGuardrail("test", "", "", "", nil)
				if err != nil {
					panic(err)
				}

				return g.GuardrailID
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete non-existent",
			id:         "nonexistent",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			id := tt.id
			if tt.setup != nil {
				id = tt.setup(h)
			}

			rec := doRequest(t, h, http.MethodDelete, "/guardrails/"+id, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- Foundation model tests ---

func TestHandler_ListFoundationModels(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/foundation-models", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	summaries := out["modelSummaries"].([]any)
	assert.NotEmpty(t, summaries)
}

func TestHandler_GetFoundationModel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		modelID    string
		wantStatus int
	}{
		{
			name:       "existing model",
			modelID:    "amazon.titan-text-express-v1",
			wantStatus: http.StatusOK,
		},
		{
			name:       "non-existent model",
			modelID:    "nonexistent-model",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodGet, "/foundation-models/"+tt.modelID, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				assert.NotNil(t, out["modelDetails"])
			}
		})
	}
}

// --- Provisioned model throughput tests ---

func TestHandler_CreateProvisionedModelThroughput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *bedrock.Handler)
		input      map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "valid provisioned throughput",
			input: map[string]any{
				"provisionedModelName": "my-throughput",
				"modelId":              "amazon.titan-text-express-v1",
				"modelUnits":           1,
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "duplicate name",
			setup: func(t *testing.T, h *bedrock.Handler) {
				t.Helper()

				_, err := h.Backend.CreateProvisionedModelThroughput(
					"dup-throughput", "amazon.titan-text-express-v1", 1, "", nil,
				)
				require.NoError(t, err)
			},
			input: map[string]any{
				"provisionedModelName": "dup-throughput",
				"modelId":              "amazon.titan-text-express-v1",
				"modelUnits":           1,
			},
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doRequest(t, h, http.MethodPost, "/provisioned-model-throughput", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				assert.NotEmpty(t, out["provisionedModelArn"])
			}
		})
	}
}

func TestHandler_GetProvisionedModelThroughput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*bedrock.Handler) string
		id         string
		wantStatus int
	}{
		{
			name: "existing pmt by arn",
			setup: func(h *bedrock.Handler) string {
				pmt, err := h.Backend.CreateProvisionedModelThroughput(
					"my-pmt",
					"amazon.titan-text-express-v1",
					1,
					"",
					nil,
				)
				if err != nil {
					panic(err)
				}

				return pmt.ProvisionedModelArn
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "existing pmt by name",
			setup: func(h *bedrock.Handler) string {
				_, err := h.Backend.CreateProvisionedModelThroughput(
					"named-pmt",
					"amazon.titan-text-express-v1",
					1,
					"",
					nil,
				)
				if err != nil {
					panic(err)
				}

				return "named-pmt"
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "non-existent pmt",
			id:         "nonexistent",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			id := tt.id
			if tt.setup != nil {
				id = tt.setup(h)
			}

			encodedID := url.PathEscape(id)
			rec := doRequest(t, h, http.MethodGet, "/provisioned-model-throughput/"+encodedID, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListProvisionedModelThroughputs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateProvisionedModelThroughput("pmt1", "amazon.titan-text-express-v1", 1, "", nil)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodGet, "/provisioned-model-throughputs", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	summaries := out["provisionedModelSummaries"].([]any)
	assert.Len(t, summaries, 1)
}

func TestHandler_DeleteProvisionedModelThroughput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*bedrock.Handler) string
		id         string
		wantStatus int
	}{
		{
			name: "delete existing",
			setup: func(h *bedrock.Handler) string {
				pmt, err := h.Backend.CreateProvisionedModelThroughput(
					"my-pmt",
					"amazon.titan-text-express-v1",
					1,
					"",
					nil,
				)
				if err != nil {
					panic(err)
				}

				return pmt.ProvisionedModelArn
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete non-existent",
			id:         "nonexistent",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			id := tt.id
			if tt.setup != nil {
				id = tt.setup(h)
			}

			encodedID := url.PathEscape(id)
			rec := doRequest(t, h, http.MethodDelete, "/provisioned-model-throughput/"+encodedID, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_Tags tests tag operations in sequence using a single shared handler state.
//
//nolint:tparallel,paralleltest // subtests are intentionally sequential — each depends on the previous state
func TestHandler_Tags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	g, err := h.Backend.CreateGuardrail("tagged-guardrail", "", "", "", nil)
	require.NoError(t, err)

	arn := g.GuardrailArn

	t.Run("list empty tags", func(t *testing.T) {
		rec := doRequest(t, h, http.MethodPost, "/listTagsForResource", map[string]any{
			"resourceARN": arn,
		})
		assert.Equal(t, http.StatusOK, rec.Code)

		var out map[string]any
		mustUnmarshal(t, rec, &out)
		tags := out["tags"].([]any)
		assert.Empty(t, tags)
	})

	t.Run("tag resource", func(t *testing.T) {
		rec := doRequest(t, h, http.MethodPost, "/tagResource", map[string]any{
			"resourceARN": arn,
			"tags": []map[string]string{
				{"key": "env", "value": "test"},
			},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("list tags after tagging", func(t *testing.T) {
		rec := doRequest(t, h, http.MethodPost, "/listTagsForResource", map[string]any{
			"resourceARN": arn,
		})
		assert.Equal(t, http.StatusOK, rec.Code)

		var out map[string]any
		mustUnmarshal(t, rec, &out)
		tags := out["tags"].([]any)
		assert.Len(t, tags, 1)
	})

	t.Run("untag resource", func(t *testing.T) {
		rec := doRequest(t, h, http.MethodPost, "/untagResource", map[string]any{
			"resourceARN": arn,
			"tagKeys":     []string{"env"},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("list tags after untagging", func(t *testing.T) {
		rec := doRequest(t, h, http.MethodPost, "/listTagsForResource", map[string]any{
			"resourceARN": arn,
		})
		assert.Equal(t, http.StatusOK, rec.Code)

		var out map[string]any
		mustUnmarshal(t, rec, &out)
		tags := out["tags"].([]any)
		assert.Empty(t, tags)
	})
}

func TestHandler_Tags_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	t.Run("list tags for nonexistent resource", func(t *testing.T) {
		t.Parallel()

		rec := doRequest(t, h, http.MethodPost, "/listTagsForResource", map[string]any{
			"resourceARN": "arn:aws:bedrock:us-east-1:000000000000:guardrail/nonexistent",
		})
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("tag nonexistent resource", func(t *testing.T) {
		t.Parallel()

		rec := doRequest(t, h, http.MethodPost, "/tagResource", map[string]any{
			"resourceARN": "arn:aws:bedrock:us-east-1:000000000000:guardrail/nonexistent",
			"tags": []map[string]string{
				{"key": "k", "value": "v"},
			},
		})
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}

func TestHandler_UnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/unknown-path", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_UpdateProvisionedModelThroughput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*bedrock.Handler) string
		input      map[string]any
		name       string
		id         string
		wantStatus int
	}{
		{
			name: "update existing",
			setup: func(h *bedrock.Handler) string {
				pmt, err := h.Backend.CreateProvisionedModelThroughput(
					"upd-pmt",
					"amazon.titan-text-express-v1",
					1,
					"",
					nil,
				)
				if err != nil {
					panic(err)
				}

				return pmt.ProvisionedModelArn
			},
			input:      map[string]any{"modelId": "anthropic.claude-v2"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "update non-existent",
			id:         "nonexistent",
			input:      map[string]any{"modelId": "anthropic.claude-v2"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "invalid json",
			id:         "nonexistent",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			id := tt.id
			if tt.setup != nil {
				id = tt.setup(h)
			}

			var rec *httptest.ResponseRecorder

			if tt.input == nil {
				e := echo.New()
				req := httptest.NewRequest(
					http.MethodPut,
					"/provisioned-model-throughput/"+url.PathEscape(id),
					bytes.NewReader([]byte("bad json")),
				)
				req.Header.Set("Content-Type", "application/json")
				recRaw := httptest.NewRecorder()
				c := e.NewContext(req, recRaw)
				err := h.Handler()(c)
				require.NoError(t, err)
				assert.Equal(t, tt.wantStatus, recRaw.Code)

				return
			}

			rec = doRequest(t, h, http.MethodPut, "/provisioned-model-throughput/"+url.PathEscape(id), tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetGuardrailByARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	g, err := h.Backend.CreateGuardrail("arn-test", "", "", "", nil)
	require.NoError(t, err)

	// Look up by ARN
	encodedARN := url.PathEscape(g.GuardrailArn)
	rec := doRequest(t, h, http.MethodGet, "/guardrails/"+encodedARN, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_InvalidJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{"untagResource bad json", http.MethodPost, "/untagResource"},
		{"tagResource bad json", http.MethodPost, "/tagResource"},
		{"listTagsForResource bad json", http.MethodPost, "/listTagsForResource"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, bytes.NewReader([]byte("bad json")))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_ListGuardrailsPagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create more than one page of guardrails (bedrockDefaultPageSize=100).
	for i := range 105 {
		_, err := h.Backend.CreateGuardrail(
			fmt.Sprintf("guardrail-%04d", i),
			"", "", "", nil,
		)
		require.NoError(t, err)
	}

	// First page.
	rec := doRequest(t, h, http.MethodGet, "/guardrails", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)

	guardrails := out["guardrails"].([]any)
	assert.Len(t, guardrails, 100)
	nextToken, ok := out["nextToken"].(string)
	require.True(t, ok, "nextToken should be present")
	assert.NotEmpty(t, nextToken)

	// Second page using the token.
	rec2 := doRequest(t, h, http.MethodGet, "/guardrails?nextToken="+url.QueryEscape(nextToken), nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	mustUnmarshal(t, rec2, &out2)

	guardrails2 := out2["guardrails"].([]any)
	assert.Len(t, guardrails2, 5)
	assert.Empty(t, out2["nextToken"])
}

func TestHandler_ListFoundationModelsPagination(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")

	// Append enough models to exceed the page size (bedrockDefaultPageSize=100).
	extra := make([]*bedrock.FoundationModelSummary, 100)
	for i := range 100 {
		extra[i] = &bedrock.FoundationModelSummary{
			ModelID:      fmt.Sprintf("test.model-%04d", i),
			ModelName:    fmt.Sprintf("Test Model %04d", i),
			ProviderName: "TestProvider",
		}
	}
	b.AppendFoundationModelsForTest(extra)

	h := bedrock.NewHandler(b)

	// First page.
	rec := doRequest(t, h, http.MethodGet, "/foundation-models", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)

	models := out["modelSummaries"].([]any)
	assert.Len(t, models, 100)
	nextToken, ok := out["nextToken"].(string)
	require.True(t, ok, "nextToken should be present")
	assert.NotEmpty(t, nextToken)

	// Second page.
	rec2 := doRequest(t, h, http.MethodGet, "/foundation-models?nextToken="+url.QueryEscape(nextToken), nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	mustUnmarshal(t, rec2, &out2)

	// Default backend seeds 5 models; we added 100, so total is 105.
	models2 := out2["modelSummaries"].([]any)
	assert.Len(t, models2, 5)
	assert.Empty(t, out2["nextToken"])
}

func TestHandler_ListProvisionedModelThroughputsPagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 105 {
		_, err := h.Backend.CreateProvisionedModelThroughput(
			fmt.Sprintf("pmt-%04d", i),
			"amazon.titan-text-express-v1",
			1, "", nil,
		)
		require.NoError(t, err)
	}

	// First page.
	rec := doRequest(t, h, http.MethodGet, "/provisioned-model-throughputs", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)

	summaries := out["provisionedModelSummaries"].([]any)
	assert.Len(t, summaries, 100)
	nextToken, ok := out["nextToken"].(string)
	require.True(t, ok, "nextToken should be present")
	assert.NotEmpty(t, nextToken)

	// Second page.
	rec2 := doRequest(t, h, http.MethodGet, "/provisioned-model-throughputs?nextToken="+url.QueryEscape(nextToken), nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	mustUnmarshal(t, rec2, &out2)

	summaries2 := out2["provisionedModelSummaries"].([]any)
	assert.Len(t, summaries2, 5)
	assert.Empty(t, out2["nextToken"])
}

// --- New operation tests ---

func TestHandler_CreateEvaluationJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input      map[string]any
		name       string
		wantStatus int
		wantJobArn bool
	}{
		{
			name:       "valid job",
			input:      map[string]any{"jobName": "my-eval-job"},
			wantStatus: http.StatusCreated,
			wantJobArn: true,
		},
		{
			name:       "missing job name",
			input:      map[string]any{"jobName": ""},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantJobArn {
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				assert.NotEmpty(t, out["jobArn"])
			}
		})
	}
}

func TestHandler_BatchDeleteEvaluationJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *bedrock.Handler) []string
		name       string
		wantStatus int
		wantErrs   int
		wantJobs   int
	}{
		{
			name: "delete existing jobs",
			setup: func(t *testing.T, h *bedrock.Handler) []string {
				t.Helper()

				job, err := h.Backend.CreateEvaluationJob("job-1", nil)
				require.NoError(t, err)
				// AWS only allows batch-delete on terminal-state jobs; stop first.
				require.NoError(t, h.Backend.StopEvaluationJob(job.JobArn))

				return []string{job.JobArn}
			},
			wantStatus: http.StatusOK,
			wantErrs:   0,
			wantJobs:   1,
		},
		{
			name: "delete non-existent job",
			setup: func(_ *testing.T, _ *bedrock.Handler) []string {
				return []string{"arn:aws:bedrock:us-east-1:000000000000:evaluation-job/nonexistent"}
			},
			wantStatus: http.StatusOK,
			wantErrs:   1,
			wantJobs:   0,
		},
		{
			name: "mixed existing and non-existent",
			setup: func(t *testing.T, h *bedrock.Handler) []string {
				t.Helper()

				job, err := h.Backend.CreateEvaluationJob("job-mixed", nil)
				require.NoError(t, err)
				// Stop the job so it's in a terminal state before batch-delete.
				require.NoError(t, h.Backend.StopEvaluationJob(job.JobArn))

				return []string{
					job.JobArn,
					"arn:aws:bedrock:us-east-1:000000000000:evaluation-job/nonexistent",
				}
			},
			wantStatus: http.StatusOK,
			wantErrs:   1,
			wantJobs:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			jobARNs := tt.setup(t, h)

			rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs/batch-delete", map[string]any{
				"jobIdentifiers": jobARNs,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			var out map[string]any
			mustUnmarshal(t, rec, &out)
			assert.Len(t, out["errors"].([]any), tt.wantErrs)
			assert.Len(t, out["evaluationJobs"].([]any), tt.wantJobs)
		})
	}
}

func TestHandler_CreateAutomatedReasoningPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *bedrock.Handler)
		input      map[string]any
		name       string
		wantStatus int
		wantFields bool
	}{
		{
			name:       "valid policy",
			input:      map[string]any{"name": "my-policy", "description": "test"},
			wantStatus: http.StatusCreated,
			wantFields: true,
		},
		{
			name:       "missing name",
			input:      map[string]any{"name": ""},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "duplicate name",
			setup: func(t *testing.T, h *bedrock.Handler) {
				t.Helper()
				_, err := h.Backend.CreateAutomatedReasoningPolicy("dup-policy", "", nil)
				require.NoError(t, err)
			},
			input:      map[string]any{"name": "dup-policy"},
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doRequest(t, h, http.MethodPost, "/automated-reasoning-policies", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantFields {
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				assert.NotEmpty(t, out["policyArn"])
				assert.Equal(t, "my-policy", out["name"])
				assert.Equal(t, "ACTIVE", out["status"])
			}
		})
	}
}

func TestHandler_CancelAutomatedReasoningPolicyBuildWorkflow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		policyARN   string
		workflowID  string
		setupPolicy bool
		setupWF     bool
		wantStatus  int
	}{
		{
			name:        "cancel existing workflow",
			setupPolicy: true,
			setupWF:     true,
			wantStatus:  http.StatusOK,
		},
		{
			name:        "policy not found",
			policyARN:   "arn:aws:bedrock:us-east-1:000000000000:automated-reasoning-policy/nonexistent",
			workflowID:  "wf-001",
			setupPolicy: false,
			setupWF:     false,
			wantStatus:  http.StatusNotFound,
		},
		{
			name:        "workflow not found",
			setupPolicy: true,
			setupWF:     false,
			workflowID:  "nonexistent-wf",
			wantStatus:  http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			policyARN := tt.policyARN
			workflowID := tt.workflowID

			if tt.setupPolicy {
				policy, err := h.Backend.CreateAutomatedReasoningPolicy("test-cancel-policy", "", nil)
				require.NoError(t, err)
				policyARN = policy.PolicyArn
			}

			if tt.setupWF {
				wf := h.Backend.AddBuildWorkflowForTest(policyARN)
				workflowID = wf.BuildWorkflowID
			}

			path := fmt.Sprintf("/automated-reasoning-policies/%s/build-workflows/%s/cancel",
				url.PathEscape(policyARN), workflowID)
			rec := doRequest(t, h, http.MethodPost, path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateAutomatedReasoningPolicyTestCase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		policyARN   string
		setupPolicy bool
		wantStatus  int
	}{
		{
			name:        "valid test case",
			setupPolicy: true,
			wantStatus:  http.StatusCreated,
		},
		{
			name:        "policy not found",
			policyARN:   "arn:aws:bedrock:us-east-1:000000000000:automated-reasoning-policy/nonexistent",
			setupPolicy: false,
			wantStatus:  http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			policyARN := tt.policyARN
			if tt.setupPolicy {
				policy, err := h.Backend.CreateAutomatedReasoningPolicy("tc-policy", "", nil)
				require.NoError(t, err)
				policyARN = policy.PolicyArn
			}

			path := fmt.Sprintf("/automated-reasoning-policies/%s/test-cases", url.PathEscape(policyARN))
			rec := doRequest(t, h, http.MethodPost, path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusCreated {
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				assert.NotEmpty(t, out["testCaseId"])
				assert.Equal(t, policyARN, out["policyArn"])
			}
		})
	}
}

func TestHandler_CreateAutomatedReasoningPolicyVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		policyARN   string
		setupPolicy bool
		wantStatus  int
	}{
		{
			name:        "valid version",
			setupPolicy: true,
			wantStatus:  http.StatusCreated,
		},
		{
			name:        "policy not found",
			policyARN:   "arn:aws:bedrock:us-east-1:000000000000:automated-reasoning-policy/nonexistent",
			setupPolicy: false,
			wantStatus:  http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			policyARN := tt.policyARN
			if tt.setupPolicy {
				policy, err := h.Backend.CreateAutomatedReasoningPolicy("ver-policy", "", nil)
				require.NoError(t, err)
				policyARN = policy.PolicyArn
			}

			path := fmt.Sprintf("/automated-reasoning-policies/%s/versions", url.PathEscape(policyARN))
			rec := doRequest(t, h, http.MethodPost, path, map[string]any{
				"lastUpdatedDefinitionHash": "abc123hash",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusCreated {
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				assert.NotEmpty(t, out["policyArn"])
				assert.NotEmpty(t, out["version"])
				assert.Equal(t, "abc123hash", out["definitionHash"])
			}
		})
	}
}

func TestHandler_CreateCustomModel(t *testing.T) {
	t.Parallel()

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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

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

func TestHandler_CreateCustomModelDeployment(t *testing.T) {
	t.Parallel()

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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

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

func TestHandler_CreateFoundationModelAgreement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input      map[string]any
		name       string
		wantStatus int
		wantModel  bool
	}{
		{
			name: "valid agreement",
			input: map[string]any{
				"modelId":    "anthropic.claude-v2",
				"offerToken": "token-abc123",
			},
			wantStatus: http.StatusOK,
			wantModel:  true,
		},
		{
			name: "missing model id",
			input: map[string]any{
				"modelId":    "",
				"offerToken": "token",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/create-foundation-model-agreement", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantModel {
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				assert.Equal(t, "anthropic.claude-v2", out["modelId"])
			}
		})
	}
}

func TestHandler_CreateGuardrailVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *bedrock.Handler) string
		input      map[string]any
		name       string
		id         string
		wantStatus int
		wantFields bool
	}{
		{
			name: "create version for existing guardrail",
			setup: func(t *testing.T, h *bedrock.Handler) string {
				t.Helper()

				g, err := h.Backend.CreateGuardrail("ver-guardrail", "desc", "", "", nil)
				require.NoError(t, err)

				return g.GuardrailID
			},
			input:      map[string]any{"description": "v1 snapshot"},
			wantStatus: http.StatusOK,
			wantFields: true,
		},
		{
			name:       "guardrail not found",
			id:         "nonexistent",
			input:      map[string]any{},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			id := tt.id
			if tt.setup != nil {
				id = tt.setup(t, h)
			}

			rec := doRequest(t, h, http.MethodPost, "/guardrails/"+id, tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantFields {
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				assert.NotEmpty(t, out["guardrailId"])
				assert.NotEmpty(t, out["version"])
			}
		})
	}
}

func TestHandler_NewOperationsRouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name string
		path string
		want bool
	}{
		{"evaluation jobs", "/evaluation-jobs", true},
		{"evaluation jobs batch-delete", "/evaluation-jobs/batch-delete", true},
		{"automated reasoning policies", "/automated-reasoning-policies", true},
		{"arp sub-path", "/automated-reasoning-policies/arn:aws:bedrock::/test-cases", true},
		{"custom models create", "/custom-models/create-custom-model", true},
		{"custom model deployments", "/model-customization/custom-model-deployments", true},
		{"foundation model agreement", "/create-foundation-model-agreement", true},
		{"unmatched", "/other-path", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			matcher := h.RouteMatcher()
			assert.Equal(t, tt.want, matcher(c))
		})
	}
}

func TestHandler_NewOperationsExtractOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name   string
		method string
		path   string
		want   string
	}{
		{"CreateEvaluationJob", http.MethodPost, "/evaluation-jobs", "CreateEvaluationJob"},
		{"BatchDeleteEvaluationJob", http.MethodPost, "/evaluation-jobs/batch-delete", "BatchDeleteEvaluationJob"},
		{
			"CreateAutomatedReasoningPolicy",
			http.MethodPost,
			"/automated-reasoning-policies",
			"CreateAutomatedReasoningPolicy",
		},
		{
			"CreateAutomatedReasoningPolicyTestCase",
			http.MethodPost,
			"/automated-reasoning-policies/arn/test-cases",
			"CreateAutomatedReasoningPolicyTestCase",
		},
		{
			"CreateAutomatedReasoningPolicyVersion",
			http.MethodPost,
			"/automated-reasoning-policies/arn/versions",
			"CreateAutomatedReasoningPolicyVersion",
		},
		{
			"CancelAutomatedReasoningPolicyBuildWorkflow",
			http.MethodPost,
			"/automated-reasoning-policies/arn/build-workflows/wf-001/cancel",
			"CancelAutomatedReasoningPolicyBuildWorkflow",
		},
		{"CreateCustomModel", http.MethodPost, "/custom-models/create-custom-model", "CreateCustomModel"},
		{
			"CreateCustomModelDeployment",
			http.MethodPost,
			"/model-customization/custom-model-deployments",
			"CreateCustomModelDeployment",
		},
		{
			"CreateFoundationModelAgreement",
			http.MethodPost,
			"/create-foundation-model-agreement",
			"CreateFoundationModelAgreement",
		},
		{"CreateGuardrailVersion", http.MethodPost, "/guardrails/my-guardrail-id", "CreateGuardrailVersion"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			assert.Equal(t, tt.want, h.ExtractOperation(c))
		})
	}
}

func TestHandler_NewOperationsBadJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	paths := []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/evaluation-jobs"},
		{http.MethodPost, "/evaluation-jobs/batch-delete"},
		{http.MethodPost, "/automated-reasoning-policies"},
		{http.MethodPost, "/custom-models/create-custom-model"},
		{http.MethodPost, "/model-customization/custom-model-deployments"},
		{http.MethodPost, "/create-foundation-model-agreement"},
		{http.MethodPost, "/guardrails/some-id"},
	}

	for _, p := range paths {
		t.Run(p.method+" "+p.path, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(p.method, p.path, bytes.NewReader([]byte("bad json")))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_GetSupportedOperationsIncludes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	for _, op := range []string{
		"BatchDeleteEvaluationJob",
		"CancelAutomatedReasoningPolicyBuildWorkflow",
		"CreateAutomatedReasoningPolicy",
		"CreateAutomatedReasoningPolicyTestCase",
		"CreateAutomatedReasoningPolicyVersion",
		"CreateCustomModel",
		"CreateCustomModelDeployment",
		"CreateEvaluationJob",
		"CreateFoundationModelAgreement",
		"CreateGuardrailVersion",
	} {
		assert.Contains(t, ops, op)
	}
}

func TestHandler_BackendReset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create resources.
	_, err := h.Backend.CreateGuardrail("reset-guardrail", "", "", "", nil)
	require.NoError(t, err)

	_, err = h.Backend.CreateEvaluationJob("reset-job", nil)
	require.NoError(t, err)

	_, err = h.Backend.CreateAutomatedReasoningPolicy("reset-policy", "", nil)
	require.NoError(t, err)

	// Verify resources exist.
	rec := doRequest(t, h, http.MethodGet, "/guardrails", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var listOut map[string]any
	mustUnmarshal(t, rec, &listOut)
	assert.Len(t, listOut["guardrails"].([]any), 1)

	// Reset.
	h.Backend.Reset()

	// Verify resources are gone.
	rec2 := doRequest(t, h, http.MethodGet, "/guardrails", nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var listOut2 map[string]any
	mustUnmarshal(t, rec2, &listOut2)
	assert.Empty(t, listOut2["guardrails"].([]any))

	// Verify foundation models still seeded after reset.
	rec3 := doRequest(t, h, http.MethodGet, "/foundation-models", nil)
	assert.Equal(t, http.StatusOK, rec3.Code)

	var fmOut map[string]any
	mustUnmarshal(t, rec3, &fmOut)
	assert.NotEmpty(t, fmOut["modelSummaries"].([]any))
}

func TestHandler_Provider(t *testing.T) {
	t.Parallel()

	p := &bedrock.Provider{}
	assert.Equal(t, "Bedrock", p.Name())

	h, err := p.Init(&service.AppContext{})
	require.NoError(t, err)
	require.NotNil(t, h)
}

func TestHandler_CreateGuardrailNameRequired(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/guardrails", map[string]any{"name": ""})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_CreateGuardrailTagsReturned(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/guardrails", map[string]any{
		"name": "tagged-guardrail",
		"tags": []map[string]string{
			{"key": "env", "value": "test"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]any
	mustUnmarshal(t, rec, &createOut)
	id := createOut["guardrailId"].(string)

	// Tags should be present in GET response.
	rec2 := doRequest(t, h, http.MethodGet, "/guardrails/"+id, nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var getOut map[string]any
	mustUnmarshal(t, rec2, &getOut)
	require.NotEmpty(t, getOut["tags"])
	tags := getOut["tags"].([]any)
	assert.Len(t, tags, 1)
	tag := tags[0].(map[string]any)
	assert.Equal(t, "env", tag["key"])
	assert.Equal(t, "test", tag["value"])
}

func TestHandler_UpdateGuardrailNameChange(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/guardrails", map[string]any{"name": "original-name"})
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]any
	mustUnmarshal(t, rec, &createOut)
	id := createOut["guardrailId"].(string)

	// Rename the guardrail.
	rec2 := doRequest(t, h, http.MethodPut, "/guardrails/"+id, map[string]any{
		"name": "renamed-guardrail",
	})
	assert.Equal(t, http.StatusOK, rec2.Code)

	// Old name should be available again.
	rec3 := doRequest(t, h, http.MethodPost, "/guardrails", map[string]any{"name": "original-name"})
	assert.Equal(t, http.StatusOK, rec3.Code)

	// New name should conflict if created again.
	rec4 := doRequest(t, h, http.MethodPost, "/guardrails", map[string]any{"name": "renamed-guardrail"})
	assert.Equal(t, http.StatusConflict, rec4.Code)
}

func TestHandler_ListGuardrailsFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	g, err := h.Backend.CreateGuardrail("filter-target", "", "", "", nil)
	require.NoError(t, err)

	_, err = h.Backend.CreateGuardrail("other-guardrail", "", "", "", nil)
	require.NoError(t, err)

	tests := []struct {
		name       string
		identifier string
		wantCount  int
	}{
		{"filter by id", g.GuardrailID, 1},
		{"filter by arn", g.GuardrailArn, 1},
		{"filter by name", "filter-target", 1},
		{"filter nonexistent", "does-not-exist", 0},
		{"no filter", "", 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := "/guardrails"
			if tt.identifier != "" {
				path += "?guardrailIdentifier=" + url.QueryEscape(tt.identifier)
			}

			rec := doRequest(t, h, http.MethodGet, path, nil)
			assert.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			mustUnmarshal(t, rec, &out)
			assert.Len(t, out["guardrails"].([]any), tt.wantCount)
		})
	}
}

func TestHandler_CreateProvisionedModelThroughputValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input      map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "missing name",
			input: map[string]any{
				"provisionedModelName": "",
				"modelId":              "amazon.titan-text-express-v1",
				"modelUnits":           1,
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "zero model units",
			input: map[string]any{
				"provisionedModelName": "my-pmt",
				"modelId":              "amazon.titan-text-express-v1",
				"modelUnits":           0,
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "negative model units",
			input: map[string]any{
				"provisionedModelName": "my-pmt",
				"modelId":              "amazon.titan-text-express-v1",
				"modelUnits":           -1,
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/provisioned-model-throughput", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_BatchDeleteEvaluationJobEmptyList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs/batch-delete", map[string]any{
		"jobIdentifiers": []string{},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_CreateEvaluationJobNameUniqueness(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{"jobName": "unique-job"})
	require.Equal(t, http.StatusCreated, rec.Code)

	rec2 := doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{"jobName": "unique-job"})
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

func TestHandler_CancelWorkflowWrongPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	policy1, err := h.Backend.CreateAutomatedReasoningPolicy("policy-1", "", nil)
	require.NoError(t, err)

	policy2, err := h.Backend.CreateAutomatedReasoningPolicy("policy-2", "", nil)
	require.NoError(t, err)

	wf := h.Backend.AddBuildWorkflowForTest(policy1.PolicyArn)

	// Cancel using wrong policy ARN should return 404.
	path := fmt.Sprintf("/automated-reasoning-policies/%s/build-workflows/%s/cancel",
		url.PathEscape(policy2.PolicyArn), wf.BuildWorkflowID)
	rec := doRequest(t, h, http.MethodPost, path, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_TagsOnEvaluationJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a job with tags.
	rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{
		"jobName": "tagged-job",
		"tags": []map[string]string{
			{"key": "project", "value": "alpha"},
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createOut map[string]any
	mustUnmarshal(t, rec, &createOut)
	jobARN := createOut["jobArn"].(string)

	// List tags via ListTagsForResource.
	rec2 := doRequest(t, h, http.MethodPost, "/listTagsForResource", map[string]any{
		"resourceARN": jobARN,
	})
	assert.Equal(t, http.StatusOK, rec2.Code)

	var tagsOut map[string]any
	mustUnmarshal(t, rec2, &tagsOut)
	tags := tagsOut["tags"].([]any)
	assert.Len(t, tags, 1)
	assert.Equal(t, "project", tags[0].(map[string]any)["key"])

	// Add more tags.
	rec3 := doRequest(t, h, http.MethodPost, "/tagResource", map[string]any{
		"resourceARN": jobARN,
		"tags": []map[string]string{
			{"key": "env", "value": "prod"},
		},
	})
	assert.Equal(t, http.StatusOK, rec3.Code)

	// Verify new tag added.
	rec4 := doRequest(t, h, http.MethodPost, "/listTagsForResource", map[string]any{
		"resourceARN": jobARN,
	})
	assert.Equal(t, http.StatusOK, rec4.Code)

	var tagsOut2 map[string]any
	mustUnmarshal(t, rec4, &tagsOut2)
	assert.Len(t, tagsOut2["tags"].([]any), 2)
}

func TestHandler_TagsOnAutomatedReasoningPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/automated-reasoning-policies", map[string]any{
		"name": "tagged-policy",
		"tags": []map[string]string{
			{"key": "team", "value": "platform"},
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	policyARN := out["policyArn"].(string)

	rec2 := doRequest(t, h, http.MethodPost, "/listTagsForResource", map[string]any{
		"resourceARN": policyARN,
	})
	assert.Equal(t, http.StatusOK, rec2.Code)

	var tagsOut map[string]any
	mustUnmarshal(t, rec2, &tagsOut)
	tags := tagsOut["tags"].([]any)
	assert.Len(t, tags, 1)
	assert.Equal(t, "team", tags[0].(map[string]any)["key"])
}

func TestHandler_TagsOnCustomModel(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/custom-models/create-custom-model", map[string]any{
		"modelName": "tagged-model",
		"tags": []map[string]string{
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

func TestHandler_TagsOnCustomModelDeployment(t *testing.T) {
	t.Parallel()

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

func TestHandler_UntagResource_AllTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(*bedrock.Handler) string
		name   string
		tagKey string
	}{
		{
			name:   "untag evaluation job",
			tagKey: "project",
			setup: func(h *bedrock.Handler) string {
				job, err := h.Backend.CreateEvaluationJob("untag-job",
					[]bedrock.Tag{{Key: "project", Value: "x"}})
				require.NoError(t, err)

				return job.JobArn
			},
		},
		{
			name:   "untag automated reasoning policy",
			tagKey: "team",
			setup: func(h *bedrock.Handler) string {
				p, err := h.Backend.CreateAutomatedReasoningPolicy("untag-policy", "",
					[]bedrock.Tag{{Key: "team", Value: "x"}})
				require.NoError(t, err)

				return p.PolicyArn
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			resourceARN := tt.setup(h)

			rec := doRequest(t, h, http.MethodPost, "/untagResource", map[string]any{
				"resourceARN": resourceARN,
				"tagKeys":     []string{tt.tagKey},
			})
			assert.Equal(t, http.StatusOK, rec.Code)

			rec2 := doRequest(t, h, http.MethodPost, "/listTagsForResource", map[string]any{
				"resourceARN": resourceARN,
			})
			assert.Equal(t, http.StatusOK, rec2.Code)

			var tagsOut map[string]any
			mustUnmarshal(t, rec2, &tagsOut)
			assert.Empty(t, tagsOut["tags"].([]any))
		})
	}
}

func TestHandler_TagsMergedSorted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	g, err := h.Backend.CreateGuardrail("sorted-tags-guardrail", "", "", "", nil)
	require.NoError(t, err)

	// Add tags in non-alphabetical order.
	rec := doRequest(t, h, http.MethodPost, "/tagResource", map[string]any{
		"resourceARN": g.GuardrailArn,
		"tags": []map[string]string{
			{"key": "z-last", "value": "3"},
			{"key": "a-first", "value": "1"},
			{"key": "m-middle", "value": "2"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doRequest(t, h, http.MethodPost, "/listTagsForResource", map[string]any{
		"resourceARN": g.GuardrailArn,
	})
	assert.Equal(t, http.StatusOK, rec2.Code)

	var tagsOut map[string]any
	mustUnmarshal(t, rec2, &tagsOut)
	tags := tagsOut["tags"].([]any)
	require.Len(t, tags, 3)

	// Verify sorted by key.
	assert.Equal(t, "a-first", tags[0].(map[string]any)["key"])
	assert.Equal(t, "m-middle", tags[1].(map[string]any)["key"])
	assert.Equal(t, "z-last", tags[2].(map[string]any)["key"])
}

func TestHandler_PerPolicyVersionNumbering(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	p1, err := h.Backend.CreateAutomatedReasoningPolicy("policy-ver-1", "", nil)
	require.NoError(t, err)

	p2, err := h.Backend.CreateAutomatedReasoningPolicy("policy-ver-2", "", nil)
	require.NoError(t, err)

	// Create versions for each policy independently.
	rec1 := doRequest(t, h, http.MethodPost,
		"/automated-reasoning-policies/"+url.PathEscape(p1.PolicyArn)+"/versions",
		map[string]any{"lastUpdatedDefinitionHash": "hash1"})
	require.Equal(t, http.StatusCreated, rec1.Code)

	rec2 := doRequest(t, h, http.MethodPost,
		"/automated-reasoning-policies/"+url.PathEscape(p2.PolicyArn)+"/versions",
		map[string]any{"lastUpdatedDefinitionHash": "hash2"})
	require.Equal(t, http.StatusCreated, rec2.Code)

	// Both should be version "1" (per-policy counter).
	var v1Out map[string]any
	mustUnmarshal(t, rec1, &v1Out)
	assert.Equal(t, "1", v1Out["version"])

	var v2Out map[string]any
	mustUnmarshal(t, rec2, &v2Out)
	assert.Equal(t, "1", v2Out["version"])

	// Second version for p1 should be "2".
	rec3 := doRequest(t, h, http.MethodPost,
		"/automated-reasoning-policies/"+url.PathEscape(p1.PolicyArn)+"/versions",
		map[string]any{"lastUpdatedDefinitionHash": "hash3"})
	require.Equal(t, http.StatusCreated, rec3.Code)

	var v3Out map[string]any
	mustUnmarshal(t, rec3, &v3Out)
	assert.Equal(t, "2", v3Out["version"])
}

func TestHandler_CreateCustomModelDeploymentMissingModelARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model-customization/custom-model-deployments", map[string]any{
		"modelArn":            "",
		"modelDeploymentName": "my-deploy",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GuardrailVersionPersisted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	g, err := h.Backend.CreateGuardrail("versioned-guardrail", "", "", "", nil)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodPost, "/guardrails/"+g.GuardrailID, map[string]any{
		"description": "first version",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var v1Out map[string]any
	mustUnmarshal(t, rec, &v1Out)
	version1 := v1Out["version"].(string)
	assert.NotEmpty(t, version1)

	// Create a second version.
	rec2 := doRequest(t, h, http.MethodPost, "/guardrails/"+g.GuardrailID, map[string]any{
		"description": "second version",
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var v2Out map[string]any
	mustUnmarshal(t, rec2, &v2Out)
	version2 := v2Out["version"].(string)
	assert.NotEmpty(t, version2)
	assert.NotEqual(t, version1, version2)
}
