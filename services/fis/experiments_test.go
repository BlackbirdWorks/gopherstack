package fis_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fis"
)

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
		"roleArn":        "arn:aws:iam::000000000000:role/FISRole",
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

func TestExperimentARN_Shape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)

	rec := doRequest(t, h, http.MethodPost, "/experiments", map[string]any{
		"experimentTemplateId": tplID,
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp struct {
		Experiment struct {
			ID  string `json:"id"`
			Arn string `json:"arn"`
		} `json:"experiment"`
	}

	mustJSON(t, rec, &resp)
	id := resp.Experiment.ID
	arn := resp.Experiment.Arn

	// arn:aws:fis:{region}:{account}:experiment/{id}
	assert.NotEmpty(t, arn, "experiment.arn must be set")
	assert.True(t, strings.HasPrefix(arn, "arn:aws:fis:"), "ARN must start with arn:aws:fis:, got %q", arn)
	assert.True(t, strings.HasSuffix(arn, "experiment/"+id),
		"ARN must end with experiment/{id}, got %q", arn)
	assert.Contains(t, arn, "000000000000", "ARN must contain account ID")
	assert.Contains(t, arn, "us-east-1", "ARN must contain region")
}

// ----------------------------------------
// Template lastUpdateTime changes after PATCH
// ----------------------------------------

func TestStartExperiment_Tags_InResponse(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)

	rec := doRequest(t, h, http.MethodPost, "/experiments", map[string]any{
		"experimentTemplateId": tplID,
		"tags":                 map[string]string{"run": "batch2", "triggered-by": "ci"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp struct {
		Experiment struct {
			Tags map[string]string `json:"tags"`
		} `json:"experiment"`
	}

	mustJSON(t, rec, &resp)
	assert.Equal(t, "batch2", resp.Experiment.Tags["run"])
	assert.Equal(t, "ci", resp.Experiment.Tags["triggered-by"])
}

// ----------------------------------------
// TargetAccountConfiguration: description round-trip
// ----------------------------------------

func TestGetExperiment_NotFound_Type(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/experiments/EXPnotfound0123456", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	var resp struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}

	mustJSON(t, rec, &resp)
	// FIS's API model defines a single ResourceNotFoundException shape service-wide —
	// there is no per-resource "ExperimentNotFoundException".
	assert.Equal(t, "ResourceNotFoundException", resp.Type)
	assert.NotEmpty(t, resp.Message)
}

// ----------------------------------------
// Experiment: roleArn carried from template
// ----------------------------------------

func TestExperiment_RoleArn_FromTemplate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := minimalTemplateBody()
	body["roleArn"] = "arn:aws:iam::000000000000:role/SpecificFISRole"

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
			RoleArn              string `json:"roleArn"`
			ExperimentTemplateID string `json:"experimentTemplateId"`
		} `json:"experiment"`
	}

	mustJSON(t, rec2, &expResp)
	assert.Equal(t, "arn:aws:iam::000000000000:role/SpecificFISRole", expResp.Experiment.RoleArn)
	assert.Equal(t, tplResp.ExperimentTemplate.ID, expResp.Experiment.ExperimentTemplateID)
}

func TestExperimentIDLength(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)

	rec := doRequest(t, h, http.MethodPost, "/experiments", map[string]any{
		"experimentTemplateId": tplID,
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp struct {
		Experiment struct {
			ID string `json:"id"`
		} `json:"experiment"`
	}

	mustJSON(t, rec, &resp)
	id := resp.Experiment.ID

	assert.True(t, strings.HasPrefix(id, "EXP"), "expected EXP prefix, got %q", id)
	assert.Len(t, id, 16, "expected 16-char total ID, got %q", id)
}

// ----------------------------------------
// Issue #23 — Experiment creationTime field
// ----------------------------------------

func TestExperimentCreationTime(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)

	before := time.Now().Add(-time.Second)

	rec := doRequest(t, h, http.MethodPost, "/experiments", map[string]any{
		"experimentTemplateId": tplID,
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	after := time.Now().Add(time.Second)

	type expTimes struct {
		ID           string  `json:"id"`
		CreationTime float64 `json:"creationTime"`
		StartTime    float64 `json:"startTime"`
	}

	var resp struct {
		Experiment expTimes `json:"experiment"`
	}

	mustJSON(t, rec, &resp)

	assert.Greater(t, resp.Experiment.CreationTime, 0.0, "creationTime must be set")
	assert.Greater(t, resp.Experiment.StartTime, 0.0, "startTime must be set")

	createdAt := time.Unix(int64(resp.Experiment.CreationTime), 0)
	assert.True(t, createdAt.After(before) && createdAt.Before(after),
		"creationTime %v not in expected range", createdAt)
}

// ----------------------------------------
// Issue #2, #1 — Status lifecycle: initiating, completing
// ----------------------------------------

func TestStopExperiment_POSTRoute(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"roleArn":        "arn:aws:iam::000000000000:role/FISRole",
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

	// Use the canonical AWS POST /experiments/{id}/stop route.
	rec3 := doRequest(t, h, http.MethodPost, "/experiments/"+expID+"/stop", nil)
	assert.Equal(t, http.StatusOK, rec3.Code)
}

func TestStopExperiment_DELETERoute_StillWorks(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)

	rec := doRequest(t, h, http.MethodPost, "/experiments", map[string]any{
		"experimentTemplateId": tplID,
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var expResp struct {
		Experiment struct {
			ID string `json:"id"`
		} `json:"experiment"`
	}

	mustJSON(t, rec, &expResp)

	// DELETE still works for backwards compatibility.
	rec2 := doRequest(t, h, http.MethodDelete, "/experiments/"+expResp.Experiment.ID, nil)
	assert.Equal(t, http.StatusOK, rec2.Code)
}

// ----------------------------------------
// Issue #7 — clientToken idempotency
// ----------------------------------------

func TestStartExperiment_ClientToken_Idempotency(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)

	body := map[string]any{
		"experimentTemplateId": tplID,
		"clientToken":          "exp-token-xyz",
	}

	rec1 := doRequest(t, h, http.MethodPost, "/experiments", body)
	require.Equal(t, http.StatusCreated, rec1.Code)

	var r1 struct {
		Experiment struct {
			ID string `json:"id"`
		} `json:"experiment"`
	}

	mustJSON(t, rec1, &r1)
	id1 := r1.Experiment.ID

	rec2 := doRequest(t, h, http.MethodPost, "/experiments", body)
	assert.Equal(t, http.StatusCreated, rec2.Code)

	var r2 struct {
		Experiment struct {
			ID string `json:"id"`
		} `json:"experiment"`
	}

	mustJSON(t, rec2, &r2)
	assert.Equal(t, id1, r2.Experiment.ID, "same clientToken must return same experiment ID")
}

// ----------------------------------------
// Issue #24 — roleArn validation
// ----------------------------------------

func TestListExperiments_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)

	// Create 5 experiments.
	for range 5 {
		doRequest(t, h, http.MethodPost, "/experiments", map[string]any{
			"experimentTemplateId": tplID,
		})
	}

	// Get first 3.
	rec := doRequest(t, h, http.MethodGet, "/experiments?maxResults=3", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		NextToken   string `json:"nextToken,omitempty"`
		Experiments []struct {
			ID string `json:"id"`
		} `json:"experiments"`
	}

	mustJSON(t, rec, &resp)
	assert.Len(t, resp.Experiments, 3)
	assert.NotEmpty(t, resp.NextToken)
}

func TestListExperiments_FilterByStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)

	// Create one experiment that immediately completes (no timed actions).
	body := map[string]any{
		"roleArn":        "arn:aws:iam::000000000000:role/FISRole",
		"stopConditions": []map[string]any{{"source": "none"}},
		"targets":        map[string]any{},
		"actions":        map[string]any{},
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

	_ = tplID // used above for experiment

	// Filter by status=pending or status=initiating (experiment is in early lifecycle).
	require.Eventually(t, func() bool {
		r := doRequest(t, h, http.MethodGet, "/experiments?status=completed", nil)
		if r.Code != http.StatusOK {
			return false
		}

		var gr struct {
			Experiments []struct {
				ID string `json:"id"`
			} `json:"experiments"`
		}

		if err := json.Unmarshal(r.Body.Bytes(), &gr); err != nil {
			return false
		}

		return len(gr.Experiments) > 0
	}, 5*time.Second, 50*time.Millisecond)
}

func TestListExperiments_FilterByTemplateID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tpl1 := seedTemplate(t, h)
	tpl2 := seedTemplate(t, h)

	doRequest(t, h, http.MethodPost, "/experiments", map[string]any{"experimentTemplateId": tpl1})
	doRequest(t, h, http.MethodPost, "/experiments", map[string]any{"experimentTemplateId": tpl2})

	rec := doRequest(t, h, http.MethodGet, "/experiments?experimentTemplateId="+tpl1, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Experiments []struct {
			ExperimentTemplateID string `json:"experimentTemplateId"`
		} `json:"experiments"`
	}

	mustJSON(t, rec, &resp)
	assert.NotEmpty(t, resp.Experiments)

	for _, e := range resp.Experiments {
		assert.Equal(t, tpl1, e.ExperimentTemplateID)
	}
}

func TestNonNilTags_Experiment(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()
	b.InjectExperiment(&fis.Experiment{
		ID:     "EXP-notags1",
		Arn:    "arn:aws:fis:us-east-1:000000000000:experiment/EXP-notags1",
		Status: fis.ExperimentStatus{Status: "completed"},
		// Tags intentionally nil
	})

	exp, err := b.GetExperiment("EXP-notags1")
	require.NoError(t, err)
	assert.NotNil(t, exp.Tags, "tags should never be nil on returned experiment")
}

// ----------------------------------------
// ListTagsForResource returns non-nil map
// ----------------------------------------
