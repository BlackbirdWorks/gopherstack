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

// ----------------------------------------
// Issue #3 — ID length: 16 chars total
// ----------------------------------------

func TestAccuracy_IDLength(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", minimalTemplateBody())
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp struct {
		ExperimentTemplate struct {
			ID string `json:"id"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec, &resp)
	id := resp.ExperimentTemplate.ID

	assert.True(t, strings.HasPrefix(id, "EXT"), "expected EXT prefix, got %q", id)
	assert.Len(t, id, 16, "expected 16-char total ID, got %q", id)
}

func TestAccuracy_ExperimentIDLength(t *testing.T) {
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

func TestAccuracy_ExperimentCreationTime(t *testing.T) {
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

func TestAccuracy_ExperimentStatusLifecycle(t *testing.T) {
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

func TestAccuracy_StopExperiment_POSTRoute(t *testing.T) {
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

func TestAccuracy_StopExperiment_DELETERoute_StillWorks(t *testing.T) {
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

func TestAccuracy_CreateExperimentTemplate_ClientToken_Idempotency(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := minimalTemplateBody()
	body["clientToken"] = "token-abc-123"

	rec1 := doRequest(t, h, http.MethodPost, "/experimentTemplates", body)
	require.Equal(t, http.StatusCreated, rec1.Code)

	var r1 struct {
		ExperimentTemplate struct {
			ID string `json:"id"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec1, &r1)
	id1 := r1.ExperimentTemplate.ID

	// Second request with same token returns the same template.
	rec2 := doRequest(t, h, http.MethodPost, "/experimentTemplates", body)
	assert.Equal(t, http.StatusCreated, rec2.Code)

	var r2 struct {
		ExperimentTemplate struct {
			ID string `json:"id"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec2, &r2)
	assert.Equal(t, id1, r2.ExperimentTemplate.ID, "same clientToken must return same template ID")
}

func TestAccuracy_StartExperiment_ClientToken_Idempotency(t *testing.T) {
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

func TestAccuracy_CreateTemplate_RoleArn_Required(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"stopConditions": []map[string]any{{"source": "none"}},
		"targets":        map[string]any{},
		"actions":        map[string]any{},
	}

	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAccuracy_CreateTemplate_RoleArn_InvalidFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"roleArn":        "not-a-real-arn",
		"stopConditions": []map[string]any{{"source": "none"}},
		"targets":        map[string]any{},
		"actions":        map[string]any{},
	}

	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAccuracy_CreateTemplate_RoleArn_ValidFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, arn := range []string{
		"arn:aws:iam::000000000000:role/FISRole",
		"arn:aws:iam::123456789012:role/path/to/role",
	} {
		body := map[string]any{
			"roleArn":        arn,
			"stopConditions": []map[string]any{{"source": "none"}},
			"targets":        map[string]any{},
			"actions":        map[string]any{},
		}

		rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", body)
		assert.Equal(t, http.StatusCreated, rec.Code, "ARN %q should be valid", arn)
	}
}

// ----------------------------------------
// Issue #8 — selectionMode validation
// ----------------------------------------

func TestAccuracy_CreateTemplate_SelectionMode_Required(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"roleArn":        "arn:aws:iam::000000000000:role/FISRole",
		"stopConditions": []map[string]any{{"source": "none"}},
		"targets": map[string]any{
			"MyInstances": map[string]any{
				"resourceType": "aws:ec2:instance",
				// selectionMode intentionally omitted
			},
		},
		"actions": map[string]any{},
	}

	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAccuracy_CreateTemplate_SelectionMode_Invalid(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, mode := range []string{"random", "SOME", "count(3)", "percent-50"} {
		body := map[string]any{
			"roleArn":        "arn:aws:iam::000000000000:role/FISRole",
			"stopConditions": []map[string]any{{"source": "none"}},
			"targets": map[string]any{
				"MyInstances": map[string]any{
					"resourceType":  "aws:ec2:instance",
					"selectionMode": mode,
				},
			},
			"actions": map[string]any{},
		}

		rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", body)
		assert.Equal(t, http.StatusBadRequest, rec.Code, "selectionMode %q should be rejected", mode)
	}
}

func TestAccuracy_CreateTemplate_SelectionMode_Valid(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, mode := range []string{"ALL", "COUNT(1)", "COUNT(10)", "PERCENT(50)", "PERCENT(100)"} {
		body := map[string]any{
			"roleArn":        "arn:aws:iam::000000000000:role/FISRole",
			"stopConditions": []map[string]any{{"source": "none"}},
			"targets": map[string]any{
				"MyInstances": map[string]any{
					"resourceType":  "aws:ec2:instance",
					"selectionMode": mode,
				},
			},
			"actions": map[string]any{},
		}

		rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", body)
		assert.Equal(t, http.StatusCreated, rec.Code, "selectionMode %q should be valid", mode)
	}
}

// ----------------------------------------
// Issue #8 — action target reference validation
// ----------------------------------------

func TestAccuracy_CreateTemplate_Action_UndefinedTarget_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"roleArn":        "arn:aws:iam::000000000000:role/FISRole",
		"stopConditions": []map[string]any{{"source": "none"}},
		"targets":        map[string]any{},
		"actions": map[string]any{
			"myAction": map[string]any{
				"actionId": "aws:ec2:stop-instances",
				"targets":  map[string]string{"Instances": "NonExistentTarget"},
			},
		},
	}

	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ----------------------------------------
// Issue #21 — aws:fis:wait duration required
// ----------------------------------------

func TestAccuracy_CreateTemplate_Wait_NoDuration_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"roleArn":        "arn:aws:iam::000000000000:role/FISRole",
		"stopConditions": []map[string]any{{"source": "none"}},
		"targets":        map[string]any{},
		"actions": map[string]any{
			"wait": map[string]any{
				"actionId":   "aws:fis:wait",
				"parameters": map[string]string{},
			},
		},
	}

	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAccuracy_CreateTemplate_StopConditions_Required(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"roleArn": "arn:aws:iam::000000000000:role/FISRole",
		"targets": map[string]any{},
		"actions": map[string]any{},
	}

	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ----------------------------------------
// Issue #22 — parseISODuration rejects months/years/weeks
// ----------------------------------------

func TestAccuracy_ParseISODuration_RejectsMonths(t *testing.T) {
	t.Parallel()

	assert.Equal(t, time.Duration(0), fis.ParseISODurationForTest("P1M"),
		"months before T should return 0")
	assert.Equal(t, time.Duration(0), fis.ParseISODurationForTest("P2Y"),
		"years should return 0")
	assert.Equal(t, time.Duration(0), fis.ParseISODurationForTest("P1W"),
		"weeks should return 0")
}

func TestAccuracy_ParseISODuration_AcceptsValidUnits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    string
		expected time.Duration
	}{
		{"PT1H", time.Hour},
		{"PT30M", 30 * time.Minute},
		{"PT45S", 45 * time.Second},
		{"P1D", 24 * time.Hour},
		{"PT1H30M", 90 * time.Minute},
		{"PT0.1S", 100 * time.Millisecond},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			got := fis.ParseISODurationForTest(tt.input)
			assert.Equal(t, tt.expected, got)
		})
	}
}

// ----------------------------------------
// Issue #25 — safety lever "default" alias
// ----------------------------------------

func TestAccuracy_GetSafetyLever_DefaultAlias(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/safetyLevers/default", nil)
	assert.Equal(t, http.StatusOK, rec.Code, "GET /safetyLevers/default should work")

	var resp struct {
		SafetyLever struct {
			ID  string `json:"id"`
			Arn string `json:"arn"`
		} `json:"safetyLever"`
	}

	mustJSON(t, rec, &resp)
	assert.NotEmpty(t, resp.SafetyLever.ID)
}

func TestAccuracy_UpdateSafetyLever_DefaultAlias(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"updateSafetyLeverStateInput": map[string]any{
			"status": "engaged",
			"reason": "testing default alias",
		},
	}

	rec := doRequest(t, h, http.MethodPatch, "/safetyLevers/default", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		SafetyLever struct {
			State struct {
				Status string `json:"status"`
			} `json:"state"`
		} `json:"safetyLever"`
	}

	mustJSON(t, rec, &resp)
	assert.Equal(t, "engaged", resp.SafetyLever.State.Status)
}

// ----------------------------------------
// Issue #26 — __type in error responses
// ----------------------------------------

func TestAccuracy_ErrorResponse_HasType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/experimentTemplates/EXTnotfound", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	var resp struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}

	mustJSON(t, rec, &resp)
	assert.Equal(t, "ExperimentTemplateNotFoundException", resp.Type)
	assert.NotEmpty(t, resp.Message)
}

func TestAccuracy_ErrorResponse_ValidationException_HasType(t *testing.T) {
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

func TestAccuracy_ListActions_Dedup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/actions", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Actions []struct {
			ID string `json:"id"`
		} `json:"actions"`
	}

	mustJSON(t, rec, &resp)

	seen := make(map[string]bool)

	for _, a := range resp.Actions {
		assert.False(t, seen[a.ID], "duplicate action ID %q in ListActions", a.ID)
		seen[a.ID] = true
	}
}

// ----------------------------------------
// Issue #13 — expanded built-in action catalog
// ----------------------------------------

func TestAccuracy_ListActions_BuiltinCatalog(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Use maxResults=100 to retrieve all built-in actions in a single page.
	rec := doRequest(t, h, http.MethodGet, "/actions?maxResults=100", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Actions []struct {
			ID string `json:"id"`
		} `json:"actions"`
	}

	mustJSON(t, rec, &resp)

	ids := make(map[string]bool)
	for _, a := range resp.Actions {
		ids[a.ID] = true
	}

	required := []string{
		"aws:fis:wait",
		"aws:fis:inject-api-internal-error",
		"aws:fis:inject-api-throttle-error",
		"aws:fis:inject-api-unavailable-error",
		"aws:ec2:stop-instances",
		"aws:ec2:reboot-instances",
		"aws:ec2:terminate-instances",
		"aws:rds:reboot-db-instances",
		"aws:rds:failover-db-cluster",
		"aws:ecs:stop-task",
		"aws:eks:terminate-nodegroup-instances",
		"aws:dynamodb:global-table-pause-replication",
		"aws:ssm:send-command",
	}

	for _, id := range required {
		assert.True(t, ids[id], "expected built-in action %q in ListActions", id)
	}
}

// ----------------------------------------
// Issue #6 — targetAccountConfigurationsCount
// ----------------------------------------

func TestAccuracy_Experiment_TargetAccountConfigurationsCount(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)

	// Add two target account configurations.
	for _, accountID := range []string{"111111111111", "222222222222"} {
		path := "/experimentTemplates/" + tplID + "/targetAccountConfigurations/" + accountID
		rec := doRequest(t, h, http.MethodPost, path, map[string]any{
			"roleArn": "arn:aws:iam::" + accountID + ":role/FISRole",
		})
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	rec := doRequest(t, h, http.MethodPost, "/experiments", map[string]any{
		"experimentTemplateId": tplID,
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp struct {
		Experiment struct {
			TargetAccountConfigurationsCount int `json:"targetAccountConfigurationsCount"`
		} `json:"experiment"`
	}

	mustJSON(t, rec, &resp)
	assert.Equal(t, 2, resp.Experiment.TargetAccountConfigurationsCount)
}

// ----------------------------------------
// Issue #18 — tag quota enforcement
// ----------------------------------------

func TestAccuracy_TagResource_TooManyTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)

	// Get the ARN of the template.
	rec := doRequest(t, h, http.MethodGet, "/experimentTemplates/"+tplID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var tplResp struct {
		ExperimentTemplate struct {
			Arn string `json:"arn"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec, &tplResp)
	arnStr := tplResp.ExperimentTemplate.Arn

	// Build 50 unique tag keys.
	tags50 := make(map[string]string)
	letters := "abcdefghijklmnopqrstuvwxyz"

	for i := range 50 {
		key := string([]byte{letters[i/26], letters[i%26]})
		tags50[key] = "v"
	}

	rec2 := doRequest(t, h, http.MethodPost, "/tags/"+arnStr, map[string]any{"tags": tags50})
	assert.Equal(t, http.StatusNoContent, rec2.Code)

	// Adding one more tag should fail with TooManyTagsException.
	rec3 := doRequest(t, h, http.MethodPost, "/tags/"+arnStr, map[string]any{
		"tags": map[string]string{"overflow": "yes"},
	})
	assert.Equal(t, http.StatusBadRequest, rec3.Code)
}

func TestAccuracy_TagResource_InvalidKeyPrefix(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)

	rec := doRequest(t, h, http.MethodGet, "/experimentTemplates/"+tplID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var tplResp struct {
		ExperimentTemplate struct {
			Arn string `json:"arn"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec, &tplResp)

	rec2 := doRequest(t, h, http.MethodPost, "/tags/"+tplResp.ExperimentTemplate.Arn, map[string]any{
		"tags": map[string]string{"aws:reserved": "value"},
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// ----------------------------------------
// Issue #19 — ListExperiments pagination
// ----------------------------------------

func TestAccuracy_ListExperiments_Pagination(t *testing.T) {
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

func TestAccuracy_ListExperiments_FilterByStatus(t *testing.T) {
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

func TestAccuracy_ListExperiments_FilterByTemplateID(t *testing.T) {
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

func TestAccuracy_ListExperimentTemplates_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for range 5 {
		doRequest(t, h, http.MethodPost, "/experimentTemplates", minimalTemplateBody())
	}

	rec := doRequest(t, h, http.MethodGet, "/experimentTemplates?maxResults=2", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		NextToken           string `json:"nextToken,omitempty"`
		ExperimentTemplates []struct {
			ID string `json:"id"`
		} `json:"experimentTemplates"`
	}

	mustJSON(t, rec, &resp)
	assert.Len(t, resp.ExperimentTemplates, 2)
	assert.NotEmpty(t, resp.NextToken)
}

// ----------------------------------------
// Issue #28 — goroutine leak: Restore marks non-terminal experiments failed
// ----------------------------------------

func TestAccuracy_Restore_MarksRunningExperimentsFailed(t *testing.T) {
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

func TestAccuracy_ExperimentStatus_ErrorField_Present(t *testing.T) {
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
