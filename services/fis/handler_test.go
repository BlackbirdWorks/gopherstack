package fis_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/fis"
)

// ----------------------------------------
// Test helpers
// ----------------------------------------

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

func TestFISHandler_CreateGetExperimentTemplate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"description": "test template",
		"roleArn":     "arn:aws:iam::000000000000:role/TestRole",
		"stopConditions": []map[string]any{
			{"source": "none"},
		},
		"targets": map[string]any{},
		"actions": map[string]any{
			"wait": map[string]any{
				"actionId": "aws:fis:wait",
				"parameters": map[string]string{
					"duration": "PT1S",
				},
			},
		},
	}

	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", body)
	assert.Equal(t, http.StatusCreated, rec.Code)

	var createResp struct {
		ExperimentTemplate struct {
			Tags        map[string]string `json:"tags"`
			ID          string            `json:"id"`
			Description string            `json:"description"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec, &createResp)
	assert.NotEmpty(t, createResp.ExperimentTemplate.ID)
	assert.Equal(t, "test template", createResp.ExperimentTemplate.Description)

	id := createResp.ExperimentTemplate.ID

	// GetExperimentTemplate
	rec2 := doRequest(t, h, http.MethodGet, "/experimentTemplates/"+id, nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var getResp struct {
		ExperimentTemplate struct {
			ID string `json:"id"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec2, &getResp)
	assert.Equal(t, id, getResp.ExperimentTemplate.ID)
}

func TestFISHandler_GetExperimentTemplate_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/experimentTemplates/EXTnonexistent0000000000", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestFISHandler_UpdateExperimentTemplate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create first.
	createBody := map[string]any{
		"description":    "original",
		"stopConditions": []map[string]any{{"source": "none"}},
		"targets":        map[string]any{},
		"actions":        map[string]any{},
	}

	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", createBody)
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp struct {
		ExperimentTemplate struct {
			ID string `json:"id"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec, &createResp)
	id := createResp.ExperimentTemplate.ID

	// Update.
	updateBody := map[string]any{"description": "updated"}
	rec2 := doRequest(t, h, http.MethodPatch, "/experimentTemplates/"+id, updateBody)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var updateResp struct {
		ExperimentTemplate struct {
			Description string `json:"description"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec2, &updateResp)
	assert.Equal(t, "updated", updateResp.ExperimentTemplate.Description)
}

func TestFISHandler_DeleteExperimentTemplate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create first.
	createBody := map[string]any{
		"stopConditions": []map[string]any{{"source": "none"}},
		"targets":        map[string]any{},
		"actions":        map[string]any{},
	}

	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", createBody)
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp struct {
		ExperimentTemplate struct {
			ID string `json:"id"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec, &createResp)
	id := createResp.ExperimentTemplate.ID

	// Delete.
	rec2 := doRequest(t, h, http.MethodDelete, "/experimentTemplates/"+id, nil)
	assert.Equal(t, http.StatusNoContent, rec2.Code)

	// Verify deletion.
	rec3 := doRequest(t, h, http.MethodGet, "/experimentTemplates/"+id, nil)
	assert.Equal(t, http.StatusNotFound, rec3.Code)
}

func TestFISHandler_ListExperimentTemplates(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createBody := func(desc string) map[string]any {
		return map[string]any{
			"description":    desc,
			"stopConditions": []map[string]any{{"source": "none"}},
			"targets":        map[string]any{},
			"actions":        map[string]any{},
		}
	}

	doRequest(t, h, http.MethodPost, "/experimentTemplates", createBody("first"))
	doRequest(t, h, http.MethodPost, "/experimentTemplates", createBody("second"))

	rec := doRequest(t, h, http.MethodGet, "/experimentTemplates", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp struct {
		ExperimentTemplates []struct {
			ID string `json:"id"`
		} `json:"experimentTemplates"`
	}

	mustJSON(t, rec, &listResp)
	assert.Len(t, listResp.ExperimentTemplates, 2)
}

// ----------------------------------------
// Experiment lifecycle tests
// ----------------------------------------

func createTestTemplate(t *testing.T, h *fis.Handler) string {
	t.Helper()

	body := map[string]any{
		"description":    "integration test template",
		"stopConditions": []map[string]any{{"source": "none"}},
		"targets":        map[string]any{},
		"actions": map[string]any{
			"wait": map[string]any{
				"actionId":   "aws:fis:wait",
				"parameters": map[string]string{"duration": "PT1S"},
			},
		},
	}

	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp struct {
		ExperimentTemplate struct {
			ID string `json:"id"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec, &resp)
	require.NotEmpty(t, resp.ExperimentTemplate.ID)

	return resp.ExperimentTemplate.ID
}

func TestFISHandler_StartGetExperiment(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	templateID := createTestTemplate(t, h)

	// StartExperiment
	startBody := map[string]any{
		"experimentTemplateId": templateID,
		"tags":                 map[string]string{"env": "test"},
	}

	rec := doRequest(t, h, http.MethodPost, "/experiments", startBody)
	assert.Equal(t, http.StatusCreated, rec.Code)

	var startResp struct {
		Experiment struct {
			ID                   string `json:"id"`
			ExperimentTemplateID string `json:"experimentTemplateId"`
			Status               struct {
				Status string `json:"status"`
			} `json:"status"`
		} `json:"experiment"`
	}

	mustJSON(t, rec, &startResp)
	assert.NotEmpty(t, startResp.Experiment.ID)
	assert.Equal(t, templateID, startResp.Experiment.ExperimentTemplateID)
	assert.NotEmpty(t, startResp.Experiment.Status.Status)

	expID := startResp.Experiment.ID

	// GetExperiment
	rec2 := doRequest(t, h, http.MethodGet, "/experiments/"+expID, nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var getResp struct {
		Experiment struct {
			ID string `json:"id"`
		} `json:"experiment"`
	}

	mustJSON(t, rec2, &getResp)
	assert.Equal(t, expID, getResp.Experiment.ID)
}

func TestFISHandler_StartExperiment_TemplateNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	startBody := map[string]any{
		"experimentTemplateId": "EXTnonexistent0000000000",
	}

	rec := doRequest(t, h, http.MethodPost, "/experiments", startBody)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestFISHandler_StopExperiment(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Use a long-duration experiment so it's still running when we stop it.
	body := map[string]any{
		"stopConditions": []map[string]any{{"source": "none"}},
		"targets":        map[string]any{},
		"actions": map[string]any{
			"wait": map[string]any{
				"actionId":   "aws:fis:wait",
				"parameters": map[string]string{"duration": "PT1H"},
			},
		},
	}

	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var tplResp struct {
		ExperimentTemplate struct {
			ID string `json:"id"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec, &tplResp)

	rec2 := doRequest(t, h, http.MethodPost, "/experiments", map[string]any{
		"experimentTemplateId": tplResp.ExperimentTemplate.ID,
	})
	require.Equal(t, http.StatusCreated, rec2.Code)

	var expResp struct {
		Experiment struct {
			ID string `json:"id"`
		} `json:"experiment"`
	}

	mustJSON(t, rec2, &expResp)
	expID := expResp.Experiment.ID

	// Stop the experiment.
	rec3 := doRequest(t, h, http.MethodDelete, "/experiments/"+expID, nil)
	assert.Equal(t, http.StatusOK, rec3.Code)
}

func TestFISHandler_GetExperiment_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/experiments/EXPnonexistent0000000000", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestFISHandler_ListExperiments(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	templateID := createTestTemplate(t, h)

	doRequest(t, h, http.MethodPost, "/experiments", map[string]any{"experimentTemplateId": templateID})
	doRequest(t, h, http.MethodPost, "/experiments", map[string]any{"experimentTemplateId": templateID})

	rec := doRequest(t, h, http.MethodGet, "/experiments", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp struct {
		Experiments []struct {
			ID string `json:"id"`
		} `json:"experiments"`
	}

	mustJSON(t, rec, &listResp)
	assert.Len(t, listResp.Experiments, 2)
}

// ----------------------------------------
// Action discovery tests
// ----------------------------------------

func TestFISHandler_ListActions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/actions", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Actions []struct {
			ID string `json:"id"`
		} `json:"actions"`
	}

	mustJSON(t, rec, &resp)
	assert.NotEmpty(t, resp.Actions)

	ids := make([]string, len(resp.Actions))
	for i, a := range resp.Actions {
		ids[i] = a.ID
	}

	assert.Contains(t, ids, "aws:fis:wait")
	assert.Contains(t, ids, "aws:fis:inject-api-internal-error")
	assert.Contains(t, ids, "aws:fis:inject-api-throttle-error")
	assert.Contains(t, ids, "aws:fis:inject-api-unavailable-error")
}

func TestFISHandler_GetAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/actions/aws:fis:wait", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Action struct {
			ID string `json:"id"`
		} `json:"action"`
	}

	mustJSON(t, rec, &resp)
	assert.Equal(t, "aws:fis:wait", resp.Action.ID)
}

func TestFISHandler_GetAction_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/actions/aws:fis:nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ----------------------------------------
// Target resource type tests
// ----------------------------------------

func TestFISHandler_ListTargetResourceTypes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/targetResourceTypes", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		TargetResourceTypes []struct {
			ResourceType string `json:"resourceType"`
		} `json:"targetResourceTypes"`
	}

	mustJSON(t, rec, &resp)
	assert.NotEmpty(t, resp.TargetResourceTypes)

	types := make([]string, len(resp.TargetResourceTypes))
	for i, rt := range resp.TargetResourceTypes {
		types[i] = rt.ResourceType
	}

	assert.Contains(t, types, "aws:ec2:instance")
	assert.Contains(t, types, "aws:lambda:function")
	assert.Contains(t, types, "aws:iam:role")
}

func TestFISHandler_GetTargetResourceType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// URL-encode the resource type.
	rec := doRequest(t, h, http.MethodGet, "/targetResourceTypes/aws%3Aec2%3Ainstance", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		TargetResourceType struct {
			ResourceType string `json:"resourceType"`
		} `json:"targetResourceType"`
	}

	mustJSON(t, rec, &resp)
	assert.Equal(t, "aws:ec2:instance", resp.TargetResourceType.ResourceType)
}

func TestFISHandler_GetTargetResourceType_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/targetResourceTypes/aws%3Anonexistent%3Atype", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ----------------------------------------
// Tag operations tests
// ----------------------------------------

func TestFISHandler_TagResource_ListTags_UntagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a template to get its ARN.
	body := map[string]any{
		"stopConditions": []map[string]any{{"source": "none"}},
		"targets":        map[string]any{},
		"actions":        map[string]any{},
	}

	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp struct {
		ExperimentTemplate struct {
			ID  string `json:"id"`
			Arn string `json:"arn"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec, &createResp)
	arnStr := createResp.ExperimentTemplate.Arn

	// TagResource.
	tagPath := fmt.Sprintf("/tags/%s", arnStr)
	tagBody := map[string]any{"tags": map[string]string{"env": "prod", "owner": "team"}}

	rec2 := doRequest(t, h, http.MethodPost, tagPath, tagBody)
	assert.Equal(t, http.StatusNoContent, rec2.Code)

	// ListTagsForResource.
	rec3 := doRequest(t, h, http.MethodGet, tagPath, nil)
	assert.Equal(t, http.StatusOK, rec3.Code)

	var tagsResp struct {
		Tags map[string]string `json:"tags"`
	}

	mustJSON(t, rec3, &tagsResp)
	assert.Equal(t, "prod", tagsResp.Tags["env"])
	assert.Equal(t, "team", tagsResp.Tags["owner"])

	// UntagResource.
	rec4 := doRequest(t, h, http.MethodDelete, tagPath+"?tagKeys=env", nil)
	assert.Equal(t, http.StatusNoContent, rec4.Code)

	// Verify tag removed.
	rec5 := doRequest(t, h, http.MethodGet, tagPath, nil)
	var tagsResp2 struct {
		Tags map[string]string `json:"tags"`
	}

	mustJSON(t, rec5, &tagsResp2)
	assert.NotContains(t, tagsResp2.Tags, "env")
	assert.Equal(t, "team", tagsResp2.Tags["owner"])
}

// ----------------------------------------
// Invalid request tests
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

func TestFISHandler_ExperimentCompletesAfterDuration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Template with a very short wait action.
	body := map[string]any{
		"stopConditions": []map[string]any{{"source": "none"}},
		"targets":        map[string]any{},
		"actions": map[string]any{
			"wait": map[string]any{
				"actionId":   "aws:fis:wait",
				"parameters": map[string]string{"duration": "PT0.1S"},
			},
		},
	}

	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var tplResp struct {
		ExperimentTemplate struct {
			ID string `json:"id"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec, &tplResp)

	rec2 := doRequest(t, h, http.MethodPost, "/experiments", map[string]any{
		"experimentTemplateId": tplResp.ExperimentTemplate.ID,
	})
	require.Equal(t, http.StatusCreated, rec2.Code)

	var expResp struct {
		Experiment struct {
			ID string `json:"id"`
		} `json:"experiment"`
	}

	mustJSON(t, rec2, &expResp)
	expID := expResp.Experiment.ID

	// Wait for the experiment to complete.
	require.Eventually(t, func() bool {
		rec3 := doRequest(t, h, http.MethodGet, "/experiments/"+expID, nil)
		if rec3.Code != http.StatusOK {
			return false
		}

		var resp struct {
			Experiment struct {
				Status struct {
					Status string `json:"status"`
				} `json:"status"`
			} `json:"experiment"`
		}

		if err := json.Unmarshal(rec3.Body.Bytes(), &resp); err != nil {
			return false
		}

		return resp.Experiment.Status.Status == "completed"
	}, 5*time.Second, 100*time.Millisecond)
}

// ----------------------------------------
// ChaosProvider interface tests
// ----------------------------------------

func TestFISHandler_ChaosProvider(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "fis", h.ChaosServiceName())
	assert.NotEmpty(t, h.ChaosOperations())
	assert.Equal(t, []string{"us-east-1"}, h.ChaosRegions())
}

// ----------------------------------------
// SetFaultStore / SetActionProviders on handler
// ----------------------------------------

func TestFISHandler_SetFaultStore(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	// SetFaultStore with nil should not panic.
	h.SetFaultStore(nil)
}

func TestFISHandler_SetActionProviders(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	h.SetActionProviders(nil)
}

// ----------------------------------------
// UpdateExperimentTemplate invalid JSON
// ----------------------------------------

func TestFISHandler_UpdateTemplate_InvalidJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createBody := map[string]any{
		"stopConditions": []map[string]any{{"source": "none"}},
		"targets":        map[string]any{},
		"actions":        map[string]any{},
	}

	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", createBody)
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp struct {
		ExperimentTemplate struct {
			ID string `json:"id"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec, &createResp)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPatch, "/experimentTemplates/"+createResp.ExperimentTemplate.ID, bytes.NewReader([]byte("not-json")))
	req.Header.Set("Content-Type", "application/json")
	rec2 := httptest.NewRecorder()
	c := e.NewContext(req, rec2)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// ----------------------------------------
// Tag resource with invalid JSON
// ----------------------------------------

func TestFISHandler_TagResource_InvalidJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/tags/arn:aws:fis:us-east-1:000:experiment-template/EXTabc", bytes.NewReader([]byte("not-json")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ----------------------------------------
// List actions with action providers
// ----------------------------------------

func TestFISHandler_ListActions_WithActionProvider(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Setting a nil slice of providers should still work.
	h.SetActionProviders(nil)

	rec := doRequest(t, h, http.MethodGet, "/actions", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Actions []struct {
			ID string `json:"id"`
		} `json:"actions"`
	}

	mustJSON(t, rec, &resp)
	assert.NotEmpty(t, resp.Actions)
}

// ----------------------------------------
// Handler read body error
// ----------------------------------------

func TestFISHandler_EmptyBody_Actions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// GET /actions with empty body should still work.
	rec := doRequest(t, h, http.MethodGet, "/actions", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ----------------------------------------
// List target resource types
// ----------------------------------------

func TestFISHandler_ListTargetResourceTypes_WithFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/targetResourceTypes", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		TargetResourceTypes []struct {
			ResourceType string `json:"resourceType"`
		} `json:"targetResourceTypes"`
	}

	mustJSON(t, rec, &resp)

	// Verify various resource types are present.
	types := make(map[string]bool, len(resp.TargetResourceTypes))
	for _, rt := range resp.TargetResourceTypes {
		types[rt.ResourceType] = true
	}

	assert.True(t, types["aws:ec2:instance"])
	assert.True(t, types["aws:lambda:function"])
	assert.True(t, types["aws:iam:role"])
}

// ----------------------------------------
// Tag experiments tests
// ----------------------------------------

func TestFISHandler_TagExperiment(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	templateID := createTestTemplate(t, h)

	// Start experiment to get ARN.
	rec := doRequest(t, h, http.MethodPost, "/experiments", map[string]any{
		"experimentTemplateId": templateID,
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var expResp struct {
		Experiment struct {
			ID  string `json:"id"`
			Arn string `json:"arn"`
		} `json:"experiment"`
	}

	mustJSON(t, rec, &expResp)
	arnStr := expResp.Experiment.Arn
	require.NotEmpty(t, arnStr)

	// TagResource on experiment.
	tagPath := fmt.Sprintf("/tags/%s", arnStr)
	rec2 := doRequest(t, h, http.MethodPost, tagPath, map[string]any{
		"tags": map[string]string{"phase": "test"},
	})
	assert.Equal(t, http.StatusNoContent, rec2.Code)

	// ListTagsForResource on experiment.
	rec3 := doRequest(t, h, http.MethodGet, tagPath, nil)
	assert.Equal(t, http.StatusOK, rec3.Code)

	var tagsResp struct {
		Tags map[string]string `json:"tags"`
	}

	mustJSON(t, rec3, &tagsResp)
	assert.Equal(t, "test", tagsResp.Tags["phase"])

	// UntagResource on experiment.
	rec4 := doRequest(t, h, http.MethodDelete, tagPath+"?tagKeys=phase", nil)
	assert.Equal(t, http.StatusNoContent, rec4.Code)
}

// ----------------------------------------
// Provider tests
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
