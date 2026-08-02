package fis_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fis"
)

func TestFISHandler_ExperimentCompletesAfterDuration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Template with a very short wait action.
	body := map[string]any{
		"roleArn":        "arn:aws:iam::000000000000:role/FISRole",
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

func TestFISHandler_ExperimentCompletes_NoTimedActions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Template with no actions → maxDuration is 0, should complete immediately.
	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", map[string]any{
		"roleArn":        "arn:aws:iam::000000000000:role/FISRole",
		"stopConditions": []map[string]any{{"source": "none"}},
		"targets":        map[string]any{},
		"actions":        map[string]any{},
	})
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
	}, 5*time.Second, 50*time.Millisecond)
}

// ----------------------------------------
// Phase 3 — Safety Lever tests
// ----------------------------------------

func TestFISHandler_StartExperiment_SafetyLeverEngaged(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a template first.
	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", map[string]any{
		"description":    "test",
		"stopConditions": []map[string]any{{"source": "none"}},
		"targets":        map[string]any{},
		"actions":        map[string]any{},
		"roleArn":        "arn:aws:iam::000000000000:role/fis-role",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var tplResp struct {
		ExperimentTemplate struct {
			ID string `json:"id"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec, &tplResp)

	// Engage the safety lever.
	rec2 := doRequest(t, h, http.MethodPatch, "/safetyLevers/000000000000", map[string]any{
		"updateSafetyLeverStateInput": map[string]any{
			"status": "engaged",
			"reason": "blocking all experiments",
		},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	// Starting an experiment should now fail.
	rec3 := doRequest(t, h, http.MethodPost, "/experiments", map[string]any{
		"experimentTemplateId": tplResp.ExperimentTemplate.ID,
	})
	assert.Equal(t, http.StatusConflict, rec3.Code)
}

// ----------------------------------------
// Phase 3 — Resolved Targets tests
// ----------------------------------------

func TestFISHandler_ListExperimentResolvedTargets(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a template with targets.
	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", map[string]any{
		"description":    "test",
		"stopConditions": []map[string]any{{"source": "none"}},
		"targets": map[string]any{
			"MyInstances": map[string]any{
				"resourceType":  "aws:ec2:instance",
				"selectionMode": "ALL",
				"resourceArns":  []string{"arn:aws:ec2:us-east-1:000000000000:instance/i-1234"},
			},
		},
		"actions": map[string]any{},
		"roleArn": "arn:aws:iam::000000000000:role/fis-role",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var tplResp struct {
		ExperimentTemplate struct {
			ID string `json:"id"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec, &tplResp)

	// Start an experiment.
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

	// List resolved targets.
	rec3 := doRequest(t, h, http.MethodGet, "/experiments/"+expResp.Experiment.ID+"/resolvedTargets", nil)
	require.Equal(t, http.StatusOK, rec3.Code)

	// The real types.ResolvedTarget wire shape has exactly resourceType, targetName,
	// and targetInformation -- no resolvedArns/targetResourcesCount. Assert against
	// the raw JSON so a regression that reintroduces the invented fields is caught,
	// not silently ignored by an unmarshal target that doesn't declare them.
	var raw struct {
		ResolvedTargets []map[string]any `json:"resolvedTargets"`
	}

	mustJSON(t, rec3, &raw)
	require.Len(t, raw.ResolvedTargets, 1)

	got := raw.ResolvedTargets[0]
	assert.Equal(t, "aws:ec2:instance", got["resourceType"])
	assert.Equal(t, "MyInstances", got["targetName"])
	assert.NotContains(t, got, "resolvedArns", "resolvedArns is not a real FIS ResolvedTarget field")
	assert.NotContains(t, got, "targetResourcesCount", "targetResourcesCount is not a real FIS ResolvedTarget field")
}

func TestFISHandler_ListExperimentResolvedTargets_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/experiments/EXPNOTEXIST/resolvedTargets", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestFISHandler_StartExperiment_TooManyExperiments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		startCount int
		wantStatus int
	}{
		{
			name:       "below_limit_succeeds",
			startCount: 1,
			wantStatus: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			templateID := createTestTemplate(t, h)

			startBody := map[string]any{
				"experimentTemplateId": templateID,
			}

			var rec *httptest.ResponseRecorder
			for range tt.startCount {
				rec = doRequest(t, h, http.MethodPost, "/experiments", startBody)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ----------------------------------------
// Target Account Configuration tests
// ----------------------------------------

func TestErrTooManyExperiments_Returns429(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()
	b.AddTemplateInternal(&fis.ExperimentTemplate{
		ID:             "EXT-quota1",
		Arn:            "arn:aws:fis:us-east-1:000000000000:experiment-template/EXT-quota1",
		RoleArn:        "arn:aws:iam::000000000000:role/FIS",
		Actions:        map[string]fis.ExperimentTemplateAction{},
		Targets:        map[string]fis.ExperimentTemplateTarget{},
		StopConditions: []fis.ExperimentTemplateStopCondition{{Source: "none"}},
	})

	// Fill up the experiment cap with 1000 injected experiments.
	endTime := time.Now()

	for i := range 1000 {
		id := "EXP-fill" + fmt.Sprintf("%04d", i)
		b.InjectExperiment(&fis.Experiment{
			ID:      id,
			Arn:     "arn:aws:fis:us-east-1:000000000000:experiment/" + id,
			Status:  fis.ExperimentStatus{Status: "completed"},
			Tags:    map[string]string{},
			EndTime: &endTime,
		})
	}

	h := fis.NewHandler(b)
	h.DefaultRegion = "us-east-1"
	h.AccountID = "000000000000"

	rec := doRequest(
		t, h, http.MethodPost, "/experiments",
		map[string]any{"experimentTemplateId": "EXT-quota1"},
	)
	// FIS's ServiceQuotaExceededException shape carries HTTP 402 (Payment Required),
	// not the generic 429 throttling status.
	assert.Equal(t, http.StatusPaymentRequired, rec.Code)
}

// ----------------------------------------
// Non-nil tags on templates/experiments
// ----------------------------------------

// TestFISResolvedTargetsWireShape verifies that ListExperimentResolvedTargets emits the
// real types.ResolvedTarget wire shape (resourceType/targetName/targetInformation) and
// never the gopherstack-invented resolvedArns/targetResourcesCount fields, regardless of
// how many ARNs the underlying target resolved to.
func TestFISResolvedTargetsWireShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		resourceArns []string
	}{
		{
			name:         "single ARN",
			resourceArns: []string{"arn:aws:ec2:us-east-1:000000000000:instance/i-aaa"},
		},
		{
			name: "multiple ARNs",
			resourceArns: []string{
				"arn:aws:ec2:us-east-1:000000000000:instance/i-aaa",
				"arn:aws:ec2:us-east-1:000000000000:instance/i-bbb",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := fis.NewTestBackend()
			h := fis.NewHandler(b)
			h.DefaultRegion = "us-east-1"
			h.AccountID = "000000000000"

			// Inject experiment with known ResourceArns.
			injected := &fis.Experiment{
				ID:     "EXPtest000000000000000000001",
				Status: fis.ExperimentStatus{Status: "running"},
				Targets: map[string]fis.ExperimentTarget{
					"Instances": {
						ResourceType: "aws:ec2:instance",
						ResourceArns: tc.resourceArns,
					},
				},
				CreationTime: time.Now(),
			}
			b.InjectExperiment(injected)

			rec := doRequest(t, h, http.MethodGet, "/experiments/"+injected.ID+"/resolvedTargets", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var raw struct {
				ResolvedTargets []map[string]any `json:"resolvedTargets"`
			}

			jsonUnmarshalFIS(t, rec.Body.Bytes(), &raw)
			require.Len(t, raw.ResolvedTargets, 1)

			got := raw.ResolvedTargets[0]
			assert.Equal(t, "Instances", got["targetName"])
			assert.Equal(t, "aws:ec2:instance", got["resourceType"])
			assert.NotContains(t, got, "resolvedArns", "resolvedArns is not a real ResolvedTarget field")
			assert.NotContains(t, got, "targetResourcesCount", "targetResourcesCount is not a real field")
		})
	}
}

// TestListExperimentResolvedTargets_Pagination verifies ListExperimentResolvedTargets
// honors maxResults/nextToken like its ListExperiments/ListActions siblings, instead of
// always returning the full unpaginated list.
func TestListExperimentResolvedTargets_Pagination(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()
	h := fis.NewHandler(b)
	h.DefaultRegion = "us-east-1"
	h.AccountID = "000000000000"

	targets := make(map[string]fis.ExperimentTarget, 5)
	for i := range 5 {
		targets[fmt.Sprintf("Target%d", i)] = fis.ExperimentTarget{
			ResourceType: "aws:ec2:instance",
			ResourceArns: []string{fmt.Sprintf("arn:aws:ec2:us-east-1:000000000000:instance/i-%d", i)},
		}
	}

	injected := &fis.Experiment{
		ID:           "EXPtest000000000000000000003",
		Status:       fis.ExperimentStatus{Status: "running"},
		Targets:      targets,
		CreationTime: time.Now(),
	}
	b.InjectExperiment(injected)

	rec := doRequest(t, h, http.MethodGet, "/experiments/"+injected.ID+"/resolvedTargets?maxResults=3", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		NextToken       string `json:"nextToken,omitempty"`
		ResolvedTargets []struct {
			TargetName string `json:"targetName"`
		} `json:"resolvedTargets"`
	}

	mustJSON(t, rec, &resp)
	assert.Len(t, resp.ResolvedTargets, 3)
	assert.NotEmpty(t, resp.NextToken)
}
