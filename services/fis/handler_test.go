package fis_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/fis"
)

func newTestHandler(t *testing.T) *fis.Handler {
	t.Helper()

	backend := fis.NewTestBackend()
	h := fis.NewHandler(backend)
	h.DefaultRegion = "us-east-1"
	h.AccountID = "000000000000"

	return h
}

func doRequest(t *testing.T, h *fis.Handler, method, path string, body any) *httptest.ResponseRecorder {
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

func mustJSON(t *testing.T, rec *httptest.ResponseRecorder, v any) {
	t.Helper()
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), v))
}

// ----------------------------------------
// Handler metadata tests
// ----------------------------------------

// minimalTemplateBody returns a minimal valid create-experiment-template request body.
func minimalTemplateBody() map[string]any {
	return map[string]any{
		"description": "refinement test template",
		"roleArn":     "arn:aws:iam::000000000000:role/TestRole",
		"stopConditions": []map[string]any{
			{"source": "none"},
		},
		"targets": map[string]any{},
		"actions": map[string]any{},
	}
}

// seedTemplate creates a template via HTTP and returns its ID.
func seedTemplate(t *testing.T, h *fis.Handler) string {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", minimalTemplateBody())
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp struct {
		ExperimentTemplate struct {
			ID string `json:"id"`
		} `json:"experimentTemplate"`
	}
	mustJSON(t, rec, &resp)

	return resp.ExperimentTemplate.ID
}

// ----------------------------------------
// Reset tests
// ----------------------------------------

// jsonUnmarshalFIS decodes JSON from a recorder into v.
func jsonUnmarshalFIS(t *testing.T, data []byte, v any) {
	t.Helper()
	require.NoError(t, json.Unmarshal(data, v))
}

func TestFISHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "FIS", h.Name())
}

func TestFISHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	for _, expected := range []string{
		"CreateExperimentTemplate",
		"GetExperimentTemplate",
		"UpdateExperimentTemplate",
		"DeleteExperimentTemplate",
		"ListExperimentTemplates",
		"StartExperiment",
		"GetExperiment",
		"StopExperiment",
		"ListExperiments",
		"GetAction",
		"ListActions",
		"GetTargetResourceType",
		"ListTargetResourceTypes",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
	} {
		assert.Contains(t, ops, expected)
	}
}

func TestFISHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Positive(t, h.MatchPriority())
}

func TestFISHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		path      string
		wantMatch bool
	}{
		{name: "experimentTemplates", path: "/experimentTemplates", wantMatch: true},
		{name: "experimentTemplates_id", path: "/experimentTemplates/EXTabc", wantMatch: true},
		{name: "experiments", path: "/experiments", wantMatch: true},
		{name: "experiments_id", path: "/experiments/EXPabc", wantMatch: true},
		{name: "actions", path: "/actions", wantMatch: true},
		{name: "actions_id", path: "/actions/aws:fis:wait", wantMatch: true},
		{name: "targetResourceTypes", path: "/targetResourceTypes", wantMatch: true},
		{name: "tags", path: "/tags/arn:aws:fis:us-east-1:000:experiment/EXP1", wantMatch: true},
		{name: "other", path: "/tables", wantMatch: false},
		{name: "root", path: "/", wantMatch: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			matcher := h.RouteMatcher()
			assert.Equal(t, tt.wantMatch, matcher(c))
		})
	}
}

// ----------------------------------------
// ExperimentTemplate CRUD tests
// ----------------------------------------

func TestFISHandler_InvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{name: "create_template", method: http.MethodPost, path: "/experimentTemplates"},
		{name: "start_experiment", method: http.MethodPost, path: "/experiments"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, bytes.NewReader([]byte("not-json")))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// ----------------------------------------
// Experiment completion test
// ----------------------------------------

func TestFISProvider_Name(t *testing.T) {
	t.Parallel()

	p := &fis.Provider{}
	assert.Equal(t, "FIS", p.Name())
}

func TestFISProvider_Init(t *testing.T) {
	t.Parallel()

	p := &fis.Provider{}
	reg, err := p.Init(&service.AppContext{})
	require.NoError(t, err)
	require.NotNil(t, reg)
}

// ----------------------------------------
// Tag resource not found tests
// ----------------------------------------

func TestErrorResponse_HasType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/experimentTemplates/EXTnotfound", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	var resp struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}

	mustJSON(t, rec, &resp)
	// FIS's API model defines a single ResourceNotFoundException shape service-wide —
	// there is no per-resource "ExperimentTemplateNotFoundException".
	assert.Equal(t, "ResourceNotFoundException", resp.Type)
	assert.NotEmpty(t, resp.Message)
}

func TestErrorResponse_ValidationException_HasType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", map[string]any{
		"targets": map[string]any{},
		"actions": map[string]any{},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp struct {
		Type string `json:"__type"`
	}

	mustJSON(t, rec, &resp)
	assert.Equal(t, "ValidationException", resp.Type)
}

// ----------------------------------------
// Issue #27 — ListActions deduplication
// ----------------------------------------

func TestReset(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()
	b.AddTemplateInternal(&fis.ExperimentTemplate{
		ID:  "EXT-reset1",
		Arn: "arn:aws:fis:us-east-1:000000000000:experiment-template/EXT-reset1",
	})
	require.Equal(t, 1, b.TemplateCount())

	b.Reset()

	assert.Equal(t, 0, b.TemplateCount())
	assert.Equal(t, 0, b.ExperimentCount())
	assert.Equal(t, 0, b.TargetAccountConfigCount())
}

func TestMultipleResetCycle(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()

	for i := range 3 {
		b.AddTemplateInternal(&fis.ExperimentTemplate{
			ID:  "EXT-cycle1",
			Arn: "arn:aws:fis:us-east-1:000000000000:experiment-template/EXT-cycle1",
		})
		b.Reset()
		assert.Equal(t, 0, b.TemplateCount(), "reset cycle %d", i)
	}
}

func TestHandlerReset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	seedTemplate(t, h)

	b := h.Backend.(*fis.ExportedInMemoryBackend)
	require.Equal(t, 1, b.TemplateCount())

	h.Reset()
	assert.Equal(t, 0, b.TemplateCount())
}

// ----------------------------------------
// Provider tests
// ----------------------------------------

func TestProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &fis.Provider{}
	_, err := p.Init(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, fis.ErrNilAppContext)
}

// ----------------------------------------
// GetSupportedOperations
// ----------------------------------------

func TestGetSupportedOperations_AllOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	expected := []string{
		"CreateExperimentTemplate",
		"GetExperimentTemplate",
		"UpdateExperimentTemplate",
		"DeleteExperimentTemplate",
		"ListExperimentTemplates",
		"StartExperiment",
		"GetExperiment",
		"StopExperiment",
		"ListExperiments",
		"ListExperimentResolvedTargets",
		"GetAction",
		"ListActions",
		"GetTargetResourceType",
		"ListTargetResourceTypes",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
		"GetSafetyLever",
		"UpdateSafetyLeverState",
		"CreateTargetAccountConfiguration",
		"DeleteTargetAccountConfiguration",
		"GetExperimentTargetAccountConfiguration",
		"GetTargetAccountConfiguration",
		"ListExperimentTargetAccountConfigurations",
		"ListTargetAccountConfigurations",
		"UpdateTargetAccountConfiguration",
	}

	for _, op := range expected {
		assert.Contains(t, ops, op, "missing op: %s", op)
	}

	assert.Equal(t, len(expected), h.HandlerOpsLen())
}

// ----------------------------------------
// Seed helpers and count helpers
// ----------------------------------------

func TestSeedHelpers(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()

	tpl := &fis.ExperimentTemplate{
		ID:   "EXT-seed1",
		Arn:  "arn:aws:fis:us-east-1:000000000000:experiment-template/EXT-seed1",
		Tags: map[string]string{"env": "test"},
	}
	b.AddTemplateInternal(tpl)

	require.Equal(t, 1, b.TemplateCount())

	got, err := b.GetExperimentTemplate("EXT-seed1")
	require.NoError(t, err)
	assert.Equal(t, "test", got.Tags["env"])

	cfg := &fis.TargetAccountConfiguration{
		ExperimentTemplateID: "EXT-seed1",
		AccountID:            "111111111111",
		RoleArn:              "arn:aws:iam::111111111111:role/FISRole",
	}
	b.AddTargetAccountConfigInternal(cfg)

	assert.Equal(t, 1, b.TargetAccountConfigCount())
}

func TestExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()
	assert.Equal(t, 0, b.TemplateCount())
	assert.Equal(t, 0, b.ExperimentCount())
	assert.Equal(t, 0, b.TargetAccountConfigCount())

	b.AddTemplateInternal(&fis.ExperimentTemplate{
		ID:  "EXT-count1",
		Arn: "arn:aws:fis:us-east-1:000000000000:experiment-template/EXT-count1",
	})
	assert.Equal(t, 1, b.TemplateCount())

	b.AddTargetAccountConfigInternal(&fis.TargetAccountConfiguration{
		ExperimentTemplateID: "EXT-count1",
		AccountID:            "222222222222",
		RoleArn:              "arn:aws:iam::222222222222:role/FISRole",
	})
	assert.Equal(t, 1, b.TargetAccountConfigCount())
}

// ----------------------------------------
// ErrValidation mapping
// ----------------------------------------

func TestErrValidationMapping(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)

	// CreateTargetAccountConfiguration with empty roleArn → 400
	rec := doRequest(
		t, h, http.MethodPost,
		"/experimentTemplates/"+tplID+"/targetAccountConfigurations/123456789012",
		map[string]any{"roleArn": ""},
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestPersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()

	b.AddTemplateInternal(&fis.ExperimentTemplate{
		ID:   "EXT-persist1",
		Arn:  "arn:aws:fis:us-east-1:000000000000:experiment-template/EXT-persist1",
		Tags: map[string]string{"key": "val"},
	})
	b.AddTargetAccountConfigInternal(&fis.TargetAccountConfiguration{
		ExperimentTemplateID: "EXT-persist1",
		AccountID:            "444444444444",
		RoleArn:              "arn:aws:iam::444444444444:role/FISRole",
		Description:          "persist test",
	})

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := fis.NewTestBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 1, b2.TemplateCount())
	assert.Equal(t, 1, b2.TargetAccountConfigCount())

	cfg, err := b2.GetTargetAccountConfiguration("EXT-persist1", "444444444444")
	require.NoError(t, err)
	assert.Equal(t, "persist test", cfg.Description)
}

func TestPersistenceEmpty(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()
	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := fis.NewTestBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 0, b2.TemplateCount())
	assert.Equal(t, 0, b2.ExperimentCount())
	assert.Equal(t, 0, b2.TargetAccountConfigCount())
}

func TestSeedHelper_DeepCopy(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()
	b.AddTemplateInternal(&fis.ExperimentTemplate{
		ID:  "EXT-deepcopy1",
		Arn: "arn:aws:fis:us-east-1:000000000000:experiment-template/EXT-deepcopy1",
	})

	original := &fis.TargetAccountConfiguration{
		ExperimentTemplateID: "EXT-deepcopy1",
		AccountID:            "999999999999",
		RoleArn:              "arn:aws:iam::999999999999:role/Original",
		Description:          "original",
	}
	b.AddTargetAccountConfigInternal(original)

	// Mutate original after insert — stored copy should not change.
	original.Description = "mutated"

	got, err := b.GetTargetAccountConfiguration("EXT-deepcopy1", "999999999999")
	require.NoError(t, err)
	assert.Equal(t, "original", got.Description)
}

// TestFISPaginateOpaqueToken verifies that nextToken is a base64-encoded integer
// offset (not a raw item ID), so deleted items do not silently rewind the cursor.
func TestFISPaginateOpaqueToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		total      int
		pageSize   int
		wantPage1  int
		wantPage2  int
		wantOpaque bool
	}{
		{
			name:       "three items two per page",
			total:      3,
			pageSize:   2,
			wantPage1:  2,
			wantPage2:  1,
			wantOpaque: true,
		},
		{
			name:      "exact fit no token",
			total:     2,
			pageSize:  2,
			wantPage1: 2,
			wantPage2: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for i := range tc.total {
				rec := doRequest(
					t, h, http.MethodPost, "/experimentTemplates",
					minimalTemplate(fmt.Sprintf("tpl-%d", i), "arn:aws:iam::000000000000:role/R"),
				)
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			// Page 1
			url1 := fmt.Sprintf("/experimentTemplates?maxResults=%d", tc.pageSize)
			rec1 := doRequest(t, h, http.MethodGet, url1, nil)
			require.Equal(t, http.StatusOK, rec1.Code)

			var resp1 struct {
				NextToken           string `json:"nextToken"`
				ExperimentTemplates []any  `json:"experimentTemplates"`
			}

			jsonUnmarshalFIS(t, rec1.Body.Bytes(), &resp1)
			assert.Len(t, resp1.ExperimentTemplates, tc.wantPage1)

			if !tc.wantOpaque {
				assert.Empty(t, resp1.NextToken)

				return
			}

			require.NotEmpty(t, resp1.NextToken)

			// Token must be base64-encoded integer, not a raw item ID.
			b, err := base64.StdEncoding.DecodeString(resp1.NextToken)
			require.NoError(t, err, "nextToken must be valid base64")
			offset, err := strconv.Atoi(string(b))
			require.NoError(t, err, "decoded token must be an integer offset")
			assert.Equal(t, tc.pageSize, offset)

			// Page 2
			url2 := fmt.Sprintf("/experimentTemplates?maxResults=%d&nextToken=%s", tc.pageSize, resp1.NextToken)
			rec2 := doRequest(t, h, http.MethodGet, url2, nil)
			require.Equal(t, http.StatusOK, rec2.Code)

			var resp2 struct {
				NextToken           string `json:"nextToken"`
				ExperimentTemplates []any  `json:"experimentTemplates"`
			}

			jsonUnmarshalFIS(t, rec2.Body.Bytes(), &resp2)
			assert.Len(t, resp2.ExperimentTemplates, tc.wantPage2)
			assert.Empty(t, resp2.NextToken, "last page must have no nextToken")
		})
	}
}
