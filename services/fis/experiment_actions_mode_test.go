package fis_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/fis"
)

// actionsModeTemplateBody returns a template body with a single external action
// wired to actionID "aws:test:mode-action", suitable for exercising
// StartExperiment's experimentOptions.actionsMode.
func actionsModeTemplateBody() map[string]any {
	return map[string]any{
		"roleArn":        "arn:aws:iam::000000000000:role/FISRole",
		"stopConditions": []map[string]any{{"source": "none"}},
		"targets": map[string]any{
			"MyInstances": map[string]any{
				"resourceType":  "aws:ec2:instance",
				"selectionMode": "ALL",
				"resourceArns":  []string{"arn:aws:ec2:us-east-1:000:instance/i-abc123"},
			},
		},
		"actions": map[string]any{
			"modeAction": map[string]any{
				"actionId": "aws:test:mode-action",
				"targets":  map[string]string{"Instances": "MyInstances"},
			},
		},
	}
}

// pollExperimentUntilTerminal polls GET /experiments/{id} until the experiment
// reaches a terminal status and returns the decoded response body.
func pollExperimentUntilTerminal(t *testing.T, h *fis.Handler, expID string) map[string]json.RawMessage {
	t.Helper()

	var result map[string]json.RawMessage

	require.Eventually(t, func() bool {
		r := doRequest(t, h, http.MethodGet, "/experiments/"+expID, nil)
		if r.Code != http.StatusOK {
			return false
		}

		var full struct {
			Experiment map[string]json.RawMessage `json:"experiment"`
		}

		if err := json.Unmarshal(r.Body.Bytes(), &full); err != nil {
			return false
		}

		var status struct {
			Status string `json:"status"`
		}

		if err := json.Unmarshal(full.Experiment["status"], &status); err != nil {
			return false
		}

		switch status.Status {
		case "completed", "failed", "stopped", "cancelled":
			result = full.Experiment

			return true
		default:
			return false
		}
	}, 5*time.Second, 20*time.Millisecond)

	return result
}

func TestStartExperiment_ActionsMode_SkipAll_SkipsActionsAndProvider(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	mock := &fis.MockFISActionProvider{
		Definitions: []service.FISActionDefinition{
			{ActionID: "aws:test:mode-action", TargetType: "aws:ec2:instance"},
		},
	}
	h.SetActionProviders([]service.FISActionProvider{mock})

	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", actionsModeTemplateBody())
	require.Equal(t, http.StatusCreated, rec.Code)

	var tplResp struct {
		ExperimentTemplate struct {
			ID string `json:"id"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec, &tplResp)

	rec2 := doRequest(t, h, http.MethodPost, "/experiments", map[string]any{
		"experimentTemplateId": tplResp.ExperimentTemplate.ID,
		"experimentOptions":    map[string]any{"actionsMode": "skip-all"},
	})
	require.Equal(t, http.StatusCreated, rec2.Code)

	var startResp struct {
		Experiment struct {
			ID                string `json:"id"`
			ExperimentOptions struct {
				ActionsMode string `json:"actionsMode"`
			} `json:"experimentOptions"`
		} `json:"experiment"`
	}

	mustJSON(t, rec2, &startResp)
	assert.Equal(t, "skip-all", startResp.Experiment.ExperimentOptions.ActionsMode)

	exp := pollExperimentUntilTerminal(t, h, startResp.Experiment.ID)

	var status struct {
		Status string `json:"status"`
	}

	require.NoError(t, json.Unmarshal(exp["status"], &status))
	assert.Equal(t, "completed", status.Status)

	var actions map[string]struct {
		Status struct {
			Status string `json:"status"`
		} `json:"status"`
	}

	require.NoError(t, json.Unmarshal(exp["actions"], &actions))
	require.Contains(t, actions, "modeAction")
	assert.Equal(t, "skipped", actions["modeAction"].Status.Status)

	assert.Equal(t, 0, mock.Calls, "skip-all must not invoke the external action provider")
}

func TestStartExperiment_ActionsMode_RunAll_InvokesProvider(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	mock := &fis.MockFISActionProvider{
		Definitions: []service.FISActionDefinition{
			{ActionID: "aws:test:mode-action", TargetType: "aws:ec2:instance"},
		},
	}
	h.SetActionProviders([]service.FISActionProvider{mock})

	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", actionsModeTemplateBody())
	require.Equal(t, http.StatusCreated, rec.Code)

	var tplResp struct {
		ExperimentTemplate struct {
			ID string `json:"id"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec, &tplResp)

	rec2 := doRequest(t, h, http.MethodPost, "/experiments", map[string]any{
		"experimentTemplateId": tplResp.ExperimentTemplate.ID,
		"experimentOptions":    map[string]any{"actionsMode": "run-all"},
	})
	require.Equal(t, http.StatusCreated, rec2.Code)

	var startResp struct {
		Experiment struct {
			ID string `json:"id"`
		} `json:"experiment"`
	}

	mustJSON(t, rec2, &startResp)

	exp := pollExperimentUntilTerminal(t, h, startResp.Experiment.ID)

	var status struct {
		Status string `json:"status"`
	}

	require.NoError(t, json.Unmarshal(exp["status"], &status))
	assert.Equal(t, "completed", status.Status)
	assert.Equal(t, 1, mock.Calls, "run-all must invoke the external action provider exactly once")
}

func TestStartExperiment_ActionsMode_DefaultsToRunAll(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)

	// No experimentOptions at all in the request body.
	rec := doRequest(t, h, http.MethodPost, "/experiments", map[string]any{
		"experimentTemplateId": tplID,
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp struct {
		Experiment struct {
			ExperimentOptions struct {
				ActionsMode string `json:"actionsMode"`
			} `json:"experimentOptions"`
		} `json:"experiment"`
	}

	mustJSON(t, rec, &resp)
	assert.Equal(t, "run-all", resp.Experiment.ExperimentOptions.ActionsMode)
}

func TestStartExperiment_ActionsMode_Invalid_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)

	rec := doRequest(t, h, http.MethodPost, "/experiments", map[string]any{
		"experimentTemplateId": tplID,
		"experimentOptions":    map[string]any{"actionsMode": "bogus-mode"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp struct {
		Type string `json:"__type"`
	}

	mustJSON(t, rec, &errResp)
	assert.Equal(t, "ValidationException", errResp.Type)
}
