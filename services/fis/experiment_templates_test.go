package fis_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fis"
)

func createTestTemplate(t *testing.T, h *fis.Handler) string {
	t.Helper()

	body := map[string]any{
		"description":    "integration test template",
		"roleArn":        "arn:aws:iam::000000000000:role/FISRole",
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
		"roleArn":        "arn:aws:iam::000000000000:role/FISRole",
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
		"roleArn":        "arn:aws:iam::000000000000:role/FISRole",
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
			"roleArn":        "arn:aws:iam::000000000000:role/FISRole",
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

func TestFISHandler_UpdateTemplate_InvalidJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createBody := map[string]any{
		"roleArn":        "arn:aws:iam::000000000000:role/FISRole",
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
	req := httptest.NewRequest(
		http.MethodPatch,
		"/experimentTemplates/"+createResp.ExperimentTemplate.ID,
		bytes.NewReader([]byte("not-json")),
	)
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

func TestExperimentTemplateARN_Shape(t *testing.T) {
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

func TestUpdateTemplate_LastUpdateTime_Changes(t *testing.T) {
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

func TestTemplate_TimeFields_Present(t *testing.T) {
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

func TestUpdateTemplate_Actions_Updated(t *testing.T) {
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

func TestUpdateTemplate_NotFound(t *testing.T) {
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
	// FIS's API model defines a single ResourceNotFoundException shape service-wide.
	assert.Equal(t, "ResourceNotFoundException", resp.Type)
}

// ----------------------------------------
// Experiment endTime: absent before completion, present after
// ----------------------------------------

func TestLogConfiguration_CloudWatch_RoundTrip(t *testing.T) {
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
				CloudWatchLogsConfiguration struct {
					LogGroupArn string `json:"logGroupArn"`
				} `json:"cloudWatchLogsConfiguration"`
				LogSchemaVersion int `json:"logSchemaVersion"`
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

func TestTarget_ResourceArns_RoundTrip(t *testing.T) {
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
			Targets map[string]struct {
				ResourceType string   `json:"resourceType"`
				ResourceArns []string `json:"resourceArns"`
			} `json:"targets"`
			ID string `json:"id"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec, &resp)
	target, ok := resp.ExperimentTemplate.Targets["myInstances"]
	require.True(t, ok)
	assert.Equal(t, "aws:ec2:instance", target.ResourceType)
	assert.ElementsMatch(t, arns, target.ResourceArns)
}

func TestTarget_ResourceTags_RoundTrip(t *testing.T) {
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

func TestTarget_Filters_RoundTrip(t *testing.T) {
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

func TestTemplate_Tags_InCreateResponse(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := minimalTemplateBody()
	body["tags"] = map[string]string{"project": "chaos", "env": "test"}

	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp struct {
		ExperimentTemplate struct {
			Tags map[string]string `json:"tags"`
			ID   string            `json:"id"`
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

func TestListExperimentTemplates_NextTokenPresent(t *testing.T) {
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

func TestDeleteTemplate_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodDelete, "/experimentTemplates/EXTnotexist01234", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	var resp struct {
		Type string `json:"__type"`
	}

	mustJSON(t, rec, &resp)
	// FIS's API model defines a single ResourceNotFoundException shape service-wide.
	assert.Equal(t, "ResourceNotFoundException", resp.Type)
}

// ----------------------------------------
// GetExperiment not found → ResourceNotFoundException
// ----------------------------------------

func TestCreateExperimentTemplate_ClientToken_Idempotency(t *testing.T) {
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

func TestListExperimentTemplates_Pagination(t *testing.T) {
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

func TestSortedListExperimentTemplates(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()

	for _, id := range []string{"EXT-zzz", "EXT-aaa", "EXT-mmm"} {
		b.AddTemplateInternal(&fis.ExperimentTemplate{
			ID:  id,
			Arn: "arn:aws:fis:us-east-1:000000000000:experiment-template/" + id,
		})
	}

	list, err := b.ListExperimentTemplates()
	require.NoError(t, err)
	require.Len(t, list, 3)

	assert.Equal(t, "EXT-aaa", list[0].ID)
	assert.Equal(t, "EXT-mmm", list[1].ID)
	assert.Equal(t, "EXT-zzz", list[2].ID)
}

func TestNonNilTags_Template(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()
	b.AddTemplateInternal(&fis.ExperimentTemplate{
		ID:  "EXT-notags1",
		Arn: "arn:aws:fis:us-east-1:000000000000:experiment-template/EXT-notags1",
		// Tags intentionally nil
	})

	tpl, err := b.GetExperimentTemplate("EXT-notags1")
	require.NoError(t, err)
	assert.NotNil(t, tpl.Tags, "tags should never be nil on returned template")
}

// minimalTemplate returns a request body that passes CreateExperimentTemplate validation.
func minimalTemplate(desc, roleArn string) map[string]any {
	return map[string]any{
		"description": desc,
		"roleArn":     roleArn,
		"stopConditions": []map[string]any{
			{"source": "none"},
		},
		"targets": map[string]any{},
		"actions": map[string]any{
			"wait": map[string]any{
				"actionId":   "aws:fis:wait",
				"parameters": map[string]string{"duration": "PT1S"},
			},
		},
	}
}
