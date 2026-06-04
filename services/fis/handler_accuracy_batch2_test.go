package fis_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ----------------------------------------
// ARN shape: experiment template
// ----------------------------------------

func TestAccuracy_Batch2_ExperimentTemplateARN_Shape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", minimalTemplateBody())
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp struct {
		ExperimentTemplate struct {
			ID  string `json:"id"`
			Arn string `json:"arn"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec, &resp)
	id := resp.ExperimentTemplate.ID
	arn := resp.ExperimentTemplate.Arn

	// arn:aws:fis:{region}:{account}:experiment-template/{id}
	assert.NotEmpty(t, arn, "experimentTemplate.arn must be set")
	assert.True(t, strings.HasPrefix(arn, "arn:aws:fis:"), "ARN must start with arn:aws:fis:, got %q", arn)
	assert.True(t, strings.HasSuffix(arn, "experiment-template/"+id),
		"ARN must end with experiment-template/{id}, got %q", arn)
	assert.Contains(t, arn, "000000000000", "ARN must contain account ID")
	assert.Contains(t, arn, "us-east-1", "ARN must contain region")
}

// ----------------------------------------
// ARN shape: experiment
// ----------------------------------------

func TestAccuracy_Batch2_ExperimentARN_Shape(t *testing.T) {
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

func TestAccuracy_Batch2_UpdateTemplate_LastUpdateTime_Changes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)

	rec := doRequest(t, h, http.MethodGet, "/experimentTemplates/"+tplID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var before struct {
		ExperimentTemplate struct {
			CreationTime   float64 `json:"creationTime"`
			LastUpdateTime float64 `json:"lastUpdateTime"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec, &before)

	time.Sleep(5 * time.Millisecond)

	rec2 := doRequest(t, h, http.MethodPatch, "/experimentTemplates/"+tplID, map[string]any{
		"description": "updated description",
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var after struct {
		ExperimentTemplate struct {
			Description    string  `json:"description"`
			LastUpdateTime float64 `json:"lastUpdateTime"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec2, &after)
	assert.Equal(t, "updated description", after.ExperimentTemplate.Description)
	assert.GreaterOrEqual(t, after.ExperimentTemplate.LastUpdateTime, before.ExperimentTemplate.LastUpdateTime,
		"lastUpdateTime must not decrease after PATCH")
}

// ----------------------------------------
// Template creationTime and lastUpdateTime present
// ----------------------------------------

func TestAccuracy_Batch2_Template_TimeFields_Present(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	before := time.Now().Add(-time.Second)
	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", minimalTemplateBody())
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp struct {
		ExperimentTemplate struct {
			CreationTime   float64 `json:"creationTime"`
			LastUpdateTime float64 `json:"lastUpdateTime"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec, &resp)
	assert.Greater(t, resp.ExperimentTemplate.CreationTime, 0.0, "creationTime must be set")
	assert.Greater(t, resp.ExperimentTemplate.LastUpdateTime, 0.0, "lastUpdateTime must be set")

	createdAt := time.Unix(int64(resp.ExperimentTemplate.CreationTime), 0)
	assert.True(t, createdAt.After(before), "creationTime must be after test start")
}

// ----------------------------------------
// UpdateExperimentTemplate: actions and stopConditions updated
// ----------------------------------------

func TestAccuracy_Batch2_UpdateTemplate_Actions_Updated(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)

	// PATCH: add an action.
	rec := doRequest(t, h, http.MethodPatch, "/experimentTemplates/"+tplID, map[string]any{
		"actions": map[string]any{
			"myWait": map[string]any{
				"actionId":   "aws:fis:wait",
				"parameters": map[string]string{"duration": "PT5S"},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		ExperimentTemplate struct {
			Actions map[string]struct {
				ActionID string `json:"actionId"`
			} `json:"actions"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec, &resp)
	action, ok := resp.ExperimentTemplate.Actions["myWait"]
	require.True(t, ok, "myWait action must be present after PATCH")
	assert.Equal(t, "aws:fis:wait", action.ActionID)
}

func TestAccuracy_Batch2_UpdateTemplate_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPatch, "/experimentTemplates/EXTnotexist01234", map[string]any{
		"description": "irrelevant",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)

	var resp struct {
		Type string `json:"__type"`
	}

	mustJSON(t, rec, &resp)
	assert.Equal(t, "ExperimentTemplateNotFoundException", resp.Type)
}

// ----------------------------------------
// Experiment endTime: absent before completion, present after
// ----------------------------------------

func TestAccuracy_Batch2_Experiment_EndTime_AbsentBeforeComplete(t *testing.T) {
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
		ExperimentTemplate struct{ ID string `json:"id"` } `json:"experimentTemplate"`
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

func TestAccuracy_Batch2_Experiment_EndTime_PresentAfterComplete(t *testing.T) {
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
		ExperimentTemplate struct{ ID string `json:"id"` } `json:"experimentTemplate"`
	}

	mustJSON(t, rec, &tplResp)

	rec2 := doRequest(t, h, http.MethodPost, "/experiments", map[string]any{
		"experimentTemplateId": tplResp.ExperimentTemplate.ID,
	})
	require.Equal(t, http.StatusCreated, rec2.Code)

	var expResp struct {
		Experiment struct{ ID string `json:"id"` } `json:"experiment"`
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
				Status struct{ Status string `json:"status"` } `json:"status"`
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

func TestAccuracy_Batch2_StopExperiment_AlreadyStopped_Returns409(t *testing.T) {
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
		ExperimentTemplate struct{ ID string `json:"id"` } `json:"experimentTemplate"`
	}

	mustJSON(t, rec, &tplResp)

	rec2 := doRequest(t, h, http.MethodPost, "/experiments", map[string]any{
		"experimentTemplateId": tplResp.ExperimentTemplate.ID,
	})
	require.Equal(t, http.StatusCreated, rec2.Code)

	var expResp struct {
		Experiment struct{ ID string `json:"id"` } `json:"experiment"`
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
				Status struct{ Status string `json:"status"` } `json:"status"`
			} `json:"experiment"`
		}

		if err := json.Unmarshal(r.Body.Bytes(), &gr); err != nil {
			return false
		}

		s := gr.Experiment.Status.Status
		return s == "completed" || s == "failed" || s == "stopped"
	}, 5*time.Second, 20*time.Millisecond)

	// Stop an already-terminal experiment → 409 ConflictException.
	rec3 := doRequest(t, h, http.MethodPost, "/experiments/"+expID+"/stop", nil)
	assert.Equal(t, http.StatusConflict, rec3.Code)

	var errResp struct {
		Type string `json:"__type"`
	}

	mustJSON(t, rec3, &errResp)
	assert.Equal(t, "ConflictException", errResp.Type)
}

// ----------------------------------------
// ExperimentOptions round-trip: template → experiment
// ----------------------------------------

func TestAccuracy_Batch2_ExperimentOptions_PassThrough(t *testing.T) {
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

func TestAccuracy_Batch2_LogConfiguration_CloudWatch_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"roleArn":        "arn:aws:iam::000000000000:role/FISRole",
		"stopConditions": []map[string]any{{"source": "none"}},
		"targets":        map[string]any{},
		"actions":        map[string]any{},
		"logConfiguration": map[string]any{
			"logSchemaVersion": 2,
			"cloudWatchLogsConfiguration": map[string]any{
				"logGroupArn": "arn:aws:logs:us-east-1:000000000000:log-group:/aws/fis/experiments",
			},
		},
	}

	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp struct {
		ExperimentTemplate struct {
			ID               string `json:"id"`
			LogConfiguration struct {
				LogSchemaVersion            int `json:"logSchemaVersion"`
				CloudWatchLogsConfiguration struct {
					LogGroupArn string `json:"logGroupArn"`
				} `json:"cloudWatchLogsConfiguration"`
			} `json:"logConfiguration"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec, &resp)
	assert.Equal(t, 2, resp.ExperimentTemplate.LogConfiguration.LogSchemaVersion)
	assert.Equal(t, "arn:aws:logs:us-east-1:000000000000:log-group:/aws/fis/experiments",
		resp.ExperimentTemplate.LogConfiguration.CloudWatchLogsConfiguration.LogGroupArn)

	// Verify GET returns same data.
	rec2 := doRequest(t, h, http.MethodGet, "/experimentTemplates/"+resp.ExperimentTemplate.ID, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var getResp struct {
		ExperimentTemplate struct {
			LogConfiguration struct {
				LogSchemaVersion int `json:"logSchemaVersion"`
			} `json:"logConfiguration"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec2, &getResp)
	assert.Equal(t, 2, getResp.ExperimentTemplate.LogConfiguration.LogSchemaVersion)
}

// ----------------------------------------
// Target: resourceArns, resourceTags, filters round-trip
// ----------------------------------------

func TestAccuracy_Batch2_Target_ResourceArns_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	arns := []string{
		"arn:aws:ec2:us-east-1:000000000000:instance/i-0123456789abcdef0",
		"arn:aws:ec2:us-east-1:000000000000:instance/i-fedcba9876543210f",
	}

	body := map[string]any{
		"roleArn":        "arn:aws:iam::000000000000:role/FISRole",
		"stopConditions": []map[string]any{{"source": "none"}},
		"targets": map[string]any{
			"myInstances": map[string]any{
				"resourceType":  "aws:ec2:instance",
				"selectionMode": "ALL",
				"resourceArns":  arns,
			},
		},
		"actions": map[string]any{},
	}

	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp struct {
		ExperimentTemplate struct {
			ID      string `json:"id"`
			Targets map[string]struct {
				ResourceArns []string `json:"resourceArns"`
				ResourceType string   `json:"resourceType"`
			} `json:"targets"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec, &resp)
	target, ok := resp.ExperimentTemplate.Targets["myInstances"]
	require.True(t, ok)
	assert.Equal(t, "aws:ec2:instance", target.ResourceType)
	assert.ElementsMatch(t, arns, target.ResourceArns)
}

func TestAccuracy_Batch2_Target_ResourceTags_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"roleArn":        "arn:aws:iam::000000000000:role/FISRole",
		"stopConditions": []map[string]any{{"source": "none"}},
		"targets": map[string]any{
			"myInstances": map[string]any{
				"resourceType":  "aws:ec2:instance",
				"selectionMode": "ALL",
				"resourceTags":  map[string]string{"env": "prod", "team": "platform"},
			},
		},
		"actions": map[string]any{},
	}

	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp struct {
		ExperimentTemplate struct {
			Targets map[string]struct {
				ResourceTags map[string]string `json:"resourceTags"`
			} `json:"targets"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec, &resp)
	target, ok := resp.ExperimentTemplate.Targets["myInstances"]
	require.True(t, ok)
	assert.Equal(t, "prod", target.ResourceTags["env"])
	assert.Equal(t, "platform", target.ResourceTags["team"])
}

func TestAccuracy_Batch2_Target_Filters_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"roleArn":        "arn:aws:iam::000000000000:role/FISRole",
		"stopConditions": []map[string]any{{"source": "none"}},
		"targets": map[string]any{
			"myInstances": map[string]any{
				"resourceType":  "aws:ec2:instance",
				"selectionMode": "ALL",
				"filters": []map[string]any{
					{"path": "State.Name", "values": []string{"running"}},
					{"path": "Placement.AvailabilityZone", "values": []string{"us-east-1a", "us-east-1b"}},
				},
			},
		},
		"actions": map[string]any{},
	}

	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp struct {
		ExperimentTemplate struct {
			Targets map[string]struct {
				Filters []struct {
					Path   string   `json:"path"`
					Values []string `json:"values"`
				} `json:"filters"`
			} `json:"targets"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec, &resp)
	target, ok := resp.ExperimentTemplate.Targets["myInstances"]
	require.True(t, ok)
	require.Len(t, target.Filters, 2)

	filtersByPath := make(map[string][]string)
	for _, f := range target.Filters {
		filtersByPath[f.Path] = f.Values
	}

	assert.Equal(t, []string{"running"}, filtersByPath["State.Name"])
	assert.ElementsMatch(t, []string{"us-east-1a", "us-east-1b"}, filtersByPath["Placement.AvailabilityZone"])
}

// ----------------------------------------
// Experiment action status fields after completion
// ----------------------------------------

func TestAccuracy_Batch2_Experiment_ActionStatus_AfterComplete(t *testing.T) {
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
		ExperimentTemplate struct{ ID string `json:"id"` } `json:"experimentTemplate"`
	}

	mustJSON(t, rec, &tplResp)

	rec2 := doRequest(t, h, http.MethodPost, "/experiments", map[string]any{
		"experimentTemplateId": tplResp.ExperimentTemplate.ID,
	})
	require.Equal(t, http.StatusCreated, rec2.Code)

	var expResp struct {
		Experiment struct{ ID string `json:"id"` } `json:"experiment"`
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
				Status struct{ Status string `json:"status"` } `json:"status"`
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
				ActionID string `json:"actionId"`
				Status   *struct {
					Status string `json:"status"`
				} `json:"status"`
				State *struct {
					Status string `json:"status"`
				} `json:"state"`
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

func TestAccuracy_Batch2_Experiment_StatusAndState_BothPresent(t *testing.T) {
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

func TestAccuracy_Batch2_Template_Tags_InCreateResponse(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := minimalTemplateBody()
	body["tags"] = map[string]string{"project": "chaos", "env": "test"}

	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp struct {
		ExperimentTemplate struct {
			ID   string            `json:"id"`
			Tags map[string]string `json:"tags"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec, &resp)
	assert.Equal(t, "chaos", resp.ExperimentTemplate.Tags["project"])
	assert.Equal(t, "test", resp.ExperimentTemplate.Tags["env"])

	// GET must also return them.
	rec2 := doRequest(t, h, http.MethodGet, "/experimentTemplates/"+resp.ExperimentTemplate.ID, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var getResp struct {
		ExperimentTemplate struct {
			Tags map[string]string `json:"tags"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec2, &getResp)
	assert.Equal(t, "chaos", getResp.ExperimentTemplate.Tags["project"])
}

// ----------------------------------------
// StartExperiment with tags → experiment response includes tags
// ----------------------------------------

func TestAccuracy_Batch2_StartExperiment_Tags_InResponse(t *testing.T) {
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

func TestAccuracy_Batch2_TargetAccountConfig_Description_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)

	path := "/experimentTemplates/" + tplID + "/targetAccountConfigurations/111111111111"
	rec := doRequest(t, h, http.MethodPost, path, map[string]any{
		"roleArn":     "arn:aws:iam::111111111111:role/FISRole",
		"description": "prod account for chaos testing",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp struct {
		TargetAccountConfiguration struct {
			AccountID   string `json:"accountId"`
			RoleArn     string `json:"roleArn"`
			Description string `json:"description"`
		} `json:"targetAccountConfiguration"`
	}

	mustJSON(t, rec, &resp)
	assert.Equal(t, "111111111111", resp.TargetAccountConfiguration.AccountID)
	assert.Equal(t, "prod account for chaos testing", resp.TargetAccountConfiguration.Description)

	// GET must return same description.
	rec2 := doRequest(t, h, http.MethodGet, path, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var getResp struct {
		TargetAccountConfiguration struct {
			Description string `json:"description"`
		} `json:"targetAccountConfiguration"`
	}

	mustJSON(t, rec2, &getResp)
	assert.Equal(t, "prod account for chaos testing", getResp.TargetAccountConfiguration.Description)
}

// ----------------------------------------
// ListTargetAccountConfigurations: empty list not null
// ----------------------------------------

func TestAccuracy_Batch2_ListTargetAccountConfigs_EmptyList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)

	rec := doRequest(t, h, http.MethodGet, "/experimentTemplates/"+tplID+"/targetAccountConfigurations", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		TargetAccountConfigurations []any `json:"targetAccountConfigurations"`
	}

	mustJSON(t, rec, &resp)
	assert.NotNil(t, resp.TargetAccountConfigurations, "empty list must not be null")
	assert.Empty(t, resp.TargetAccountConfigurations)
}

// ----------------------------------------
// GetTargetAccountConfiguration: not found → 404
// ----------------------------------------

func TestAccuracy_Batch2_GetTargetAccountConfig_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)

	rec := doRequest(t, h, http.MethodGet,
		"/experimentTemplates/"+tplID+"/targetAccountConfigurations/999999999999", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	var resp struct {
		Type string `json:"__type"`
	}

	mustJSON(t, rec, &resp)
	assert.Equal(t, "TargetAccountConfigurationNotFoundException", resp.Type)
}

// ----------------------------------------
// DeleteTargetAccountConfiguration: returns deleted config body
// ----------------------------------------

func TestAccuracy_Batch2_DeleteTargetAccountConfig_ReturnsBody(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)

	accountID := "222222222222"
	path := fmt.Sprintf("/experimentTemplates/%s/targetAccountConfigurations/%s", tplID, accountID)

	rec := doRequest(t, h, http.MethodPost, path, map[string]any{
		"roleArn": "arn:aws:iam::" + accountID + ":role/FISRole",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	// Delete returns the deleted config (200 with body, not 204).
	rec2 := doRequest(t, h, http.MethodDelete, path, nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var resp struct {
		TargetAccountConfiguration struct {
			AccountID string `json:"accountId"`
			RoleArn   string `json:"roleArn"`
		} `json:"targetAccountConfiguration"`
	}

	mustJSON(t, rec2, &resp)
	assert.Equal(t, accountID, resp.TargetAccountConfiguration.AccountID)
	assert.Equal(t, "arn:aws:iam::"+accountID+":role/FISRole", resp.TargetAccountConfiguration.RoleArn)

	// Confirm it's gone.
	rec3 := doRequest(t, h, http.MethodGet, path, nil)
	assert.Equal(t, http.StatusNotFound, rec3.Code)
}

// ----------------------------------------
// GetAction: response shape (id, arn, description, parameters, targets)
// ----------------------------------------

func TestAccuracy_Batch2_GetAction_ResponseShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/actions/aws:fis:wait", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Action struct {
			ID          string            `json:"id"`
			Arn         string            `json:"arn"`
			Description string            `json:"description"`
			Parameters  map[string]struct {
				Description string `json:"description"`
				Required    bool   `json:"required"`
			} `json:"parameters"`
		} `json:"action"`
	}

	mustJSON(t, rec, &resp)
	assert.Equal(t, "aws:fis:wait", resp.Action.ID)
	assert.NotEmpty(t, resp.Action.Arn, "action.arn must be set")
	assert.NotEmpty(t, resp.Action.Description, "action.description must be set")
	assert.NotNil(t, resp.Action.Parameters, "action.parameters must not be nil")

	durParam, ok := resp.Action.Parameters["duration"]
	require.True(t, ok, "aws:fis:wait must have 'duration' parameter")
	assert.True(t, durParam.Required, "duration parameter must be required")
}

// ----------------------------------------
// GetTargetResourceType: response shape
// ----------------------------------------

func TestAccuracy_Batch2_GetTargetResourceType_ResponseShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/targetResourceTypes/aws:ec2:instance", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		TargetResourceType struct {
			ResourceType string `json:"resourceType"`
			Description  string `json:"description"`
		} `json:"targetResourceType"`
	}

	mustJSON(t, rec, &resp)
	assert.Equal(t, "aws:ec2:instance", resp.TargetResourceType.ResourceType)
	assert.NotEmpty(t, resp.TargetResourceType.Description)

	// Verify an action-based type with parameters (aws:fis:wait has a 'duration' parameter).
	rec2 := doRequest(t, h, http.MethodGet, "/actions/aws:fis:wait", nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var actionResp struct {
		Action struct {
			ID         string `json:"id"`
			Parameters map[string]struct {
				Required bool `json:"required"`
			} `json:"parameters"`
		} `json:"action"`
	}

	mustJSON(t, rec2, &actionResp)
	assert.Equal(t, "aws:fis:wait", actionResp.Action.ID)
	_, hasDuration := actionResp.Action.Parameters["duration"]
	assert.True(t, hasDuration, "aws:fis:wait must have 'duration' parameter")
}

// ----------------------------------------
// ListExperimentTemplates: nextToken present when results truncated
// ----------------------------------------

func TestAccuracy_Batch2_ListExperimentTemplates_NextTokenPresent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create 4 templates.
	for range 4 {
		doRequest(t, h, http.MethodPost, "/experimentTemplates", minimalTemplateBody())
	}

	// Page 1: maxResults=2 — expect exactly 2 results and a nextToken.
	rec := doRequest(t, h, http.MethodGet, "/experimentTemplates?maxResults=2", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var page1 struct {
		NextToken           string `json:"nextToken"`
		ExperimentTemplates []struct {
			ID string `json:"id"`
		} `json:"experimentTemplates"`
	}

	mustJSON(t, rec, &page1)
	assert.Len(t, page1.ExperimentTemplates, 2, "maxResults=2 must return exactly 2 templates")
	assert.NotEmpty(t, page1.NextToken, "nextToken must be set when more results remain")

	// Requesting all templates without limit returns all 4.
	rec2 := doRequest(t, h, http.MethodGet, "/experimentTemplates?maxResults=100", nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var allResp struct {
		NextToken           string `json:"nextToken"`
		ExperimentTemplates []struct {
			ID string `json:"id"`
		} `json:"experimentTemplates"`
	}

	mustJSON(t, rec2, &allResp)
	assert.GreaterOrEqual(t, len(allResp.ExperimentTemplates), 4, "full page must return all 4 templates")
	assert.Empty(t, allResp.NextToken, "nextToken must be absent when all results fit on one page")
}

// ----------------------------------------
// DeleteExperimentTemplate not found → 404
// ----------------------------------------

func TestAccuracy_Batch2_DeleteTemplate_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodDelete, "/experimentTemplates/EXTnotexist01234", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	var resp struct {
		Type string `json:"__type"`
	}

	mustJSON(t, rec, &resp)
	assert.Equal(t, "ExperimentTemplateNotFoundException", resp.Type)
}

// ----------------------------------------
// GetExperiment not found → ExperimentNotFoundException
// ----------------------------------------

func TestAccuracy_Batch2_GetExperiment_NotFound_Type(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/experiments/EXPnotfound0123456", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	var resp struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}

	mustJSON(t, rec, &resp)
	assert.Equal(t, "ExperimentNotFoundException", resp.Type)
	assert.NotEmpty(t, resp.Message)
}

// ----------------------------------------
// Experiment: roleArn carried from template
// ----------------------------------------

func TestAccuracy_Batch2_Experiment_RoleArn_FromTemplate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := minimalTemplateBody()
	body["roleArn"] = "arn:aws:iam::000000000000:role/SpecificFISRole"

	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var tplResp struct {
		ExperimentTemplate struct{ ID string `json:"id"` } `json:"experimentTemplate"`
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
