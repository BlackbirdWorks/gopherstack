package fis_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fis"
)

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

func TestFISHandler_SetActionProviders(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	h.SetActionProviders(nil)
}

// ----------------------------------------
// UpdateExperimentTemplate invalid JSON
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

func TestGetAction_ResponseShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/actions/aws:fis:wait", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Action struct {
			Parameters map[string]struct {
				Description string `json:"description"`
				Required    bool   `json:"required"`
			} `json:"parameters"`
			ID          string `json:"id"`
			Arn         string `json:"arn"`
			Description string `json:"description"`
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

func TestGetTargetResourceType_ResponseShape(t *testing.T) {
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
			Parameters map[string]struct {
				Required bool `json:"required"`
			} `json:"parameters"`
			ID string `json:"id"`
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

func TestParseISODuration_RejectsMonths(t *testing.T) {
	t.Parallel()

	assert.Equal(t, time.Duration(0), fis.ParseISODurationForTest("P1M"),
		"months before T should return 0")
	assert.Equal(t, time.Duration(0), fis.ParseISODurationForTest("P2Y"),
		"years should return 0")
	assert.Equal(t, time.Duration(0), fis.ParseISODurationForTest("P1W"),
		"weeks should return 0")
}

func TestParseISODuration_AcceptsValidUnits(t *testing.T) {
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

func TestListActions_Dedup(t *testing.T) {
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

func TestListActions_BuiltinCatalog(t *testing.T) {
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

func TestSortedListActions(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()
	actions := b.ListActions()
	require.NotEmpty(t, actions)

	for i := 1; i < len(actions); i++ {
		assert.LessOrEqual(t, actions[i-1].ID, actions[i].ID, "actions not sorted at index %d", i)
	}
}

func TestSortedListTargetResourceTypes(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()
	types := b.ListTargetResourceTypes()
	require.NotEmpty(t, types)

	for i := 1; i < len(types); i++ {
		assert.LessOrEqual(t, types[i-1].ResourceType, types[i].ResourceType,
			"resource types not sorted at index %d", i)
	}
}

// ----------------------------------------
// Persistence round-trip
// ----------------------------------------
