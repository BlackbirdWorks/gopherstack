package fis_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fis"
)

func TestExperiment_EndTime_AbsentBeforeComplete(t *testing.T) {
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

	// Immediately after start, endTime should be absent (omitempty).
	var raw map[string]json.RawMessage

	mustJSON(t, rec2, &raw)

	var expRaw map[string]json.RawMessage

	require.NoError(t, json.Unmarshal(raw["experiment"], &expRaw))
	_, hasEndTime := expRaw["endTime"]
	assert.False(t, hasEndTime, "endTime must be absent on a running experiment")
}

func TestExperiment_EndTime_PresentAfterComplete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"roleArn":        "arn:aws:iam::000000000000:role/FISRole",
		"stopConditions": []map[string]any{{"source": "none"}},
		"targets":        map[string]any{},
		"actions": map[string]any{
			"wait": map[string]any{
				"actionId":   "aws:fis:wait",
				"parameters": map[string]string{"duration": "PT0.05S"},
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

	// Poll until completed.
	require.Eventually(t, func() bool {
		r := doRequest(t, h, http.MethodGet, "/experiments/"+expID, nil)
		if r.Code != http.StatusOK {
			return false
		}

		var gr struct {
			Experiment struct {
				Status struct {
					Status string `json:"status"`
				} `json:"status"`
			} `json:"experiment"`
		}

		if err := json.Unmarshal(r.Body.Bytes(), &gr); err != nil {
			return false
		}

		return gr.Experiment.Status.Status == "completed"
	}, 5*time.Second, 20*time.Millisecond)

	rec3 := doRequest(t, h, http.MethodGet, "/experiments/"+expID, nil)
	require.Equal(t, http.StatusOK, rec3.Code)

	var raw map[string]json.RawMessage

	mustJSON(t, rec3, &raw)

	var expRaw map[string]json.RawMessage

	require.NoError(t, json.Unmarshal(raw["experiment"], &expRaw))

	endTimeRaw, hasEndTime := expRaw["endTime"]
	require.True(t, hasEndTime, "endTime must be present after completion")

	var endTime float64

	require.NoError(t, json.Unmarshal(endTimeRaw, &endTime))
	assert.Greater(t, endTime, 0.0, "endTime must be a positive Unix timestamp")
}

// ----------------------------------------
// StopExperiment on already-stopped experiment → 409
// ----------------------------------------

func TestStopExperiment_AlreadyStopped_Returns409(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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

	var expResp struct {
		Experiment struct {
			ID string `json:"id"`
		} `json:"experiment"`
	}

	mustJSON(t, rec2, &expResp)
	expID := expResp.Experiment.ID

	// Poll until terminal.
	require.Eventually(t, func() bool {
		r := doRequest(t, h, http.MethodGet, "/experiments/"+expID, nil)
		if r.Code != http.StatusOK {
			return false
		}

		var gr struct {
			Experiment struct {
				Status struct {
					Status string `json:"status"`
				} `json:"status"`
			} `json:"experiment"`
		}

		if err := json.Unmarshal(r.Body.Bytes(), &gr); err != nil {
			return false
		}

		s := gr.Experiment.Status.Status

		return s == "completed" || s == "failed" || s == "stopped"
	}, 5*time.Second, 20*time.Millisecond)

	// Stop an already-terminal experiment → 400 ValidationException. StopExperiment's
	// generated deserializer in aws-sdk-go-v2/service/fis only recognizes
	// ResourceNotFoundException and ValidationException — it has no ConflictException
	// case — so this must not be reported as a conflict.
	rec3 := doRequest(t, h, http.MethodPost, "/experiments/"+expID+"/stop", nil)
	assert.Equal(t, http.StatusBadRequest, rec3.Code)

	var errResp struct {
		Type string `json:"__type"`
	}

	mustJSON(t, rec3, &errResp)
	assert.Equal(t, "ValidationException", errResp.Type)
}

// ----------------------------------------
// ExperimentOptions round-trip: template → experiment
// ----------------------------------------

func TestExperimentOptions_PassThrough(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"roleArn":        "arn:aws:iam::000000000000:role/FISRole",
		"stopConditions": []map[string]any{{"source": "none"}},
		"targets":        map[string]any{},
		"actions":        map[string]any{},
		"experimentOptions": map[string]any{
			"accountTargeting":          "multi-account",
			"emptyTargetResolutionMode": "fail",
		},
	}

	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var tplResp struct {
		ExperimentTemplate struct {
			ID                string `json:"id"`
			ExperimentOptions struct {
				AccountTargeting          string `json:"accountTargeting"`
				EmptyTargetResolutionMode string `json:"emptyTargetResolutionMode"`
			} `json:"experimentOptions"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec, &tplResp)
	assert.Equal(t, "multi-account", tplResp.ExperimentTemplate.ExperimentOptions.AccountTargeting)
	assert.Equal(t, "fail", tplResp.ExperimentTemplate.ExperimentOptions.EmptyTargetResolutionMode)

	// Experiment inherits options.
	rec2 := doRequest(t, h, http.MethodPost, "/experiments", map[string]any{
		"experimentTemplateId": tplResp.ExperimentTemplate.ID,
	})
	require.Equal(t, http.StatusCreated, rec2.Code)

	var expResp struct {
		Experiment struct {
			ExperimentOptions struct {
				AccountTargeting          string `json:"accountTargeting"`
				EmptyTargetResolutionMode string `json:"emptyTargetResolutionMode"`
			} `json:"experimentOptions"`
		} `json:"experiment"`
	}

	mustJSON(t, rec2, &expResp)
	assert.Equal(t, "multi-account", expResp.Experiment.ExperimentOptions.AccountTargeting)
	assert.Equal(t, "fail", expResp.Experiment.ExperimentOptions.EmptyTargetResolutionMode)
}

// ----------------------------------------
// LogConfiguration (CloudWatch) round-trip
// ----------------------------------------

func TestExperiment_ActionStatus_AfterComplete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"roleArn":        "arn:aws:iam::000000000000:role/FISRole",
		"stopConditions": []map[string]any{{"source": "none"}},
		"targets":        map[string]any{},
		"actions": map[string]any{
			"myWait": map[string]any{
				"actionId":   "aws:fis:wait",
				"parameters": map[string]string{"duration": "PT0.05S"},
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

	// Poll until completed.
	require.Eventually(t, func() bool {
		r := doRequest(t, h, http.MethodGet, "/experiments/"+expID, nil)
		if r.Code != http.StatusOK {
			return false
		}

		var gr struct {
			Experiment struct {
				Status struct {
					Status string `json:"status"`
				} `json:"status"`
			} `json:"experiment"`
		}

		if err := json.Unmarshal(r.Body.Bytes(), &gr); err != nil {
			return false
		}

		return gr.Experiment.Status.Status == "completed"
	}, 5*time.Second, 20*time.Millisecond)

	rec3 := doRequest(t, h, http.MethodGet, "/experiments/"+expID, nil)
	require.Equal(t, http.StatusOK, rec3.Code)

	var resp struct {
		Experiment struct {
			Actions map[string]struct {
				Status *struct {
					Status string `json:"status"`
				} `json:"status"`
				State *struct {
					Status string `json:"status"`
				} `json:"state"`
				ActionID string `json:"actionId"`
			} `json:"actions"`
		} `json:"experiment"`
	}

	mustJSON(t, rec3, &resp)
	action, ok := resp.Experiment.Actions["myWait"]
	require.True(t, ok, "myWait action must be in experiment response")
	assert.Equal(t, "aws:fis:wait", action.ActionID)
	require.NotNil(t, action.Status, "action.status must not be nil")
	assert.NotEmpty(t, action.Status.Status, "action.status.status must be set")
	// Both status and state aliases must be present.
	require.NotNil(t, action.State, "action.state must not be nil")
	assert.Equal(t, action.Status.Status, action.State.Status, "action.status and action.state must agree")
}

// ----------------------------------------
// Experiment: both status and state fields present
// ----------------------------------------

func TestExperiment_StatusAndState_BothPresent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)

	rec := doRequest(t, h, http.MethodPost, "/experiments", map[string]any{
		"experimentTemplateId": tplID,
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var raw map[string]json.RawMessage

	mustJSON(t, rec, &raw)

	var expRaw map[string]json.RawMessage

	require.NoError(t, json.Unmarshal(raw["experiment"], &expRaw))
	_, hasStatus := expRaw["status"]
	_, hasState := expRaw["state"]
	assert.True(t, hasStatus, "experiment must have 'status' field")
	assert.True(t, hasState, "experiment must have 'state' field")
}

// ----------------------------------------
// Template tags: included in create response and GET
// ----------------------------------------

func TestExperimentStatusLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Template with a very short wait to observe lifecycle transitions.
	body := map[string]any{
		"roleArn":        "arn:aws:iam::000000000000:role/FISRole",
		"stopConditions": []map[string]any{{"source": "none"}},
		"targets":        map[string]any{},
		"actions": map[string]any{
			"wait": map[string]any{
				"actionId":   "aws:fis:wait",
				"parameters": map[string]string{"duration": "PT0.05S"},
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

	// Poll for completed status — lifecycle goes pending→initiating→running→completing→completed.
	require.Eventually(t, func() bool {
		r := doRequest(t, h, http.MethodGet, "/experiments/"+expID, nil)
		if r.Code != http.StatusOK {
			return false
		}

		var gr struct {
			Experiment struct {
				Status struct {
					Status string `json:"status"`
				} `json:"status"`
			} `json:"experiment"`
		}

		if err := json.Unmarshal(r.Body.Bytes(), &gr); err != nil {
			return false
		}

		return gr.Experiment.Status.Status == "completed"
	}, 5*time.Second, 20*time.Millisecond)
}

// ----------------------------------------
// Issue #20 — POST /experiments/{id}/stop
// ----------------------------------------

func TestRestore_MarksRunningExperimentsFailed(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)

	// Start an experiment that would run for a long time.
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
	_ = tplID

	// Take a snapshot and restore — should mark running experiment as failed.
	mem, ok := h.Backend.(*fis.ExportedInMemoryBackend)
	require.True(t, ok)

	snap := mem.Snapshot(t.Context())
	require.NotNil(t, snap)

	// Restore into a fresh backend.
	fresh := fis.NewTestBackend()
	err := fresh.Restore(t.Context(), snap)
	require.NoError(t, err)

	exp, err := fresh.GetExperiment(expID)
	require.NoError(t, err)
	assert.Equal(t, "failed", exp.Status.Status,
		"restored non-terminal experiment should be marked failed")
}

// ----------------------------------------
// Issue #4 — state.error struct on experimentStatus
// ----------------------------------------

func TestExperimentStatus_ErrorField_Present(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)

	rec := doRequest(t, h, http.MethodPost, "/experiments", map[string]any{
		"experimentTemplateId": tplID,
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]json.RawMessage

	mustJSON(t, rec, &resp)

	expRaw := resp["experiment"]

	var exp map[string]json.RawMessage

	require.NoError(t, json.Unmarshal(expRaw, &exp))

	statusRaw, ok := exp["status"]
	require.True(t, ok, "experiment should have status field")

	var status map[string]json.RawMessage

	require.NoError(t, json.Unmarshal(statusRaw, &status))

	// state.error should be absent for non-failed experiments (omitempty).
	_, hasError := status["error"]
	assert.False(t, hasError, "non-failed experiment should not have status.error")
}
