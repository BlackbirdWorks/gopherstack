package xray_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/xray"
)

func TestHandler_CreateGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*xray.Handler)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "creates group",
			body:       map[string]any{"GroupName": "my-group"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing GroupName returns 400",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "duplicate group returns 400",
			setup: func(h *xray.Handler) {
				_, _ = h.Backend.CreateGroup("dup-group", "")
			},
			body:       map[string]any{"GroupName": "dup-group"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doXrayRequest(t, h, "/CreateGroup", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		groupsToSeed []string
		wantStatus   int
		wantCount    int
	}{
		{
			name:       "returns empty list",
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name:         "returns seeded groups",
			groupsToSeed: []string{"group-a", "group-b"},
			wantStatus:   http.StatusOK,
			wantCount:    2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for _, name := range tt.groupsToSeed {
				rec := doXrayRequest(t, h, "/CreateGroup", map[string]any{"GroupName": name})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doXrayRequest(t, h, "/Groups", nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			groups, ok := resp["Groups"].([]any)
			require.True(t, ok)
			assert.Len(t, groups, tt.wantCount)
		})
	}
}

func TestHandler_DeleteGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*xray.Handler)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "deletes existing group",
			setup: func(h *xray.Handler) {
				_, _ = h.Backend.CreateGroup("my-group", "")
			},
			body:       map[string]any{"GroupName": "my-group"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing GroupName returns 400",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not found returns 400",
			body:       map[string]any{"GroupName": "no-such-group"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doXrayRequest(t, h, "/DeleteGroup", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*xray.Handler)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "gets existing group",
			setup: func(h *xray.Handler) {
				_, _ = h.Backend.CreateGroup("my-group", "")
			},
			body:       map[string]any{"GroupName": "my-group"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing GroupName returns 400",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not found returns 400",
			body:       map[string]any{"GroupName": "missing-group"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doXrayRequest(t, h, "/GetGroup", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UpdateGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*xray.Handler)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "updates existing group",
			setup: func(h *xray.Handler) {
				_, _ = h.Backend.CreateGroup("my-group", "")
			},
			body:       map[string]any{"GroupName": "my-group", "FilterExpression": `service("updated")`},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing GroupName returns 400",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not found returns 400",
			body:       map[string]any{"GroupName": "missing-group"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doXrayRequest(t, h, "/UpdateGroup", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_UpdateGroup_AppliesInsightsConfiguration guards against a real bug found
// during parity audit: UpdateGroup parsed InsightsConfiguration from the request body
// but never passed it to the backend, so a caller could never actually enable/disable
// insights via UpdateGroup even though the real API supports it.
func TestHandler_UpdateGroup_AppliesInsightsConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateGroup("insights-group", "")
	require.NoError(t, err)

	rec := doXrayRequest(t, h, "/UpdateGroup", map[string]any{
		"GroupName": "insights-group",
		"InsightsConfiguration": map[string]any{
			"InsightsEnabled":      true,
			"NotificationsEnabled": true,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	group, ok := resp["Group"].(map[string]any)
	require.True(t, ok)

	ic, ok := group["InsightsConfiguration"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, true, ic["InsightsEnabled"])
	assert.Equal(t, true, ic["NotificationsEnabled"])
}

// TestHandler_UpdateGroup_OmittedFieldsAreNotWiped guards against a real bug found
// during parity audit: UpdateGroup unconditionally overwrote FilterExpression (even
// with an empty string) regardless of whether the caller provided it, and discarded
// InsightsConfiguration entirely. Both are independently optional in the real
// UpdateGroupInput -- updating one must not silently reset the other.
func TestHandler_UpdateGroup_OmittedFieldsAreNotWiped(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doXrayRequest(t, h, "/CreateGroup", map[string]any{
		"GroupName":        "partial-update-group",
		"FilterExpression": `service("original")`,
		"InsightsConfiguration": map[string]any{
			"InsightsEnabled": true,
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	// Update only InsightsConfiguration; FilterExpression must survive unchanged.
	updRec := doXrayRequest(t, h, "/UpdateGroup", map[string]any{
		"GroupName": "partial-update-group",
		"InsightsConfiguration": map[string]any{
			"InsightsEnabled":      true,
			"NotificationsEnabled": true,
		},
	})
	require.Equal(t, http.StatusOK, updRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(updRec.Body.Bytes(), &resp))
	group, ok := resp["Group"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, `service("original")`, group["FilterExpression"],
		"FilterExpression must survive an update that only touches InsightsConfiguration")

	// Update only FilterExpression; InsightsConfiguration must survive unchanged.
	updRec2 := doXrayRequest(t, h, "/UpdateGroup", map[string]any{
		"GroupName":        "partial-update-group",
		"FilterExpression": `service("changed")`,
	})
	require.Equal(t, http.StatusOK, updRec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(updRec2.Body.Bytes(), &resp2))
	group2, ok := resp2["Group"].(map[string]any)
	require.True(t, ok)
	ic, ok := group2["InsightsConfiguration"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, true, ic["InsightsEnabled"],
		"InsightsConfiguration must survive an update that only touches FilterExpression")
	assert.Equal(t, true, ic["NotificationsEnabled"])
}

// TestHandler_UpdateGroup_NotificationsRequireInsightsEnabled mirrors CreateGroup's
// validation: NotificationsEnabled=true with InsightsEnabled=false must be rejected.
func TestHandler_UpdateGroup_NotificationsRequireInsightsEnabled(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateGroup("bad-notify-group", "")
	require.NoError(t, err)

	rec := doXrayRequest(t, h, "/UpdateGroup", map[string]any{
		"GroupName": "bad-notify-group",
		"InsightsConfiguration": map[string]any{
			"InsightsEnabled":      false,
			"NotificationsEnabled": true,
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestGroup_ARNBasedLookup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doXrayRequest(t, h, "/CreateGroup", map[string]any{
		"GroupName": "arn-lookup-group",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	group, ok := createResp["Group"].(map[string]any)
	require.True(t, ok)

	groupARN, ok := group["GroupARN"].(string)
	require.True(t, ok)
	require.NotEmpty(t, groupARN)

	// Get by ARN.
	getRec := doXrayRequest(t, h, "/GetGroup", map[string]any{
		"GroupARN": groupARN,
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))

	g2, ok := getResp["Group"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "arn-lookup-group", g2["GroupName"])
}

func TestHandler_GetGroups_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        map[string]any
		name        string
		groupCount  int
		wantCount   int
		wantHasNext bool
		wantStatus  int
	}{
		{
			name:        "no groups returns empty list",
			groupCount:  0,
			wantCount:   0,
			wantStatus:  http.StatusOK,
			wantHasNext: false,
		},
		{
			name:        "returns all groups when under default page size",
			groupCount:  5,
			wantCount:   5,
			wantStatus:  http.StatusOK,
			wantHasNext: false,
		},
		{
			name:        "MaxResults limits results and sets NextToken",
			groupCount:  5,
			body:        map[string]any{"MaxResults": 2},
			wantCount:   2,
			wantStatus:  http.StatusOK,
			wantHasNext: true,
		},
		{
			name:       "zero MaxResults uses default page size",
			groupCount: 3,
			body:       map[string]any{"MaxResults": 0},
			wantCount:  3,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)

			for i := range tt.groupCount {
				_, err := b.CreateGroup(fmt.Sprintf("group-%d", i), "")
				require.NoError(t, err)
			}

			rec := doXrayRequest(t, h, "/Groups", tt.body)
			require.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			groups, ok := resp["Groups"].([]any)
			require.True(t, ok, "Groups field must be present and array")
			assert.Len(t, groups, tt.wantCount)

			nextToken, _ := resp["NextToken"].(string)
			if tt.wantHasNext {
				assert.NotEmpty(t, nextToken, "expected NextToken when paginating")
			} else {
				assert.Empty(t, nextToken, "expected empty NextToken when no more pages")
			}
		})
	}
}

func TestHandler_GetGroups_NextTokenContinuation(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)

	for i := range 5 {
		_, err := b.CreateGroup(fmt.Sprintf("pg-group-%d", i), "")
		require.NoError(t, err)
	}

	// First page: 3 groups
	rec1 := doXrayRequest(t, h, "/Groups", map[string]any{"MaxResults": 3})
	require.Equal(t, http.StatusOK, rec1.Code)

	var resp1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &resp1))

	groups1 := resp1["Groups"].([]any)
	assert.Len(t, groups1, 3)
	nextToken := resp1["NextToken"].(string)
	require.NotEmpty(t, nextToken)

	// Second page: remaining 2 groups
	rec2 := doXrayRequest(t, h, "/Groups", map[string]any{"MaxResults": 3, "NextToken": nextToken})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))

	groups2 := resp2["Groups"].([]any)
	assert.Len(t, groups2, 2)
	assert.Empty(t, resp2["NextToken"])
}

func TestHandler_GetGroups_EmptyBodyAccepted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/Groups", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp, "Groups")
	assert.Contains(t, resp, "NextToken")
}

func TestGroups_ARNBasedLookup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doXrayRequest(t, h, "/CreateGroup", map[string]any{"GroupName": "arn-group"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	group, ok := createResp["Group"].(map[string]any)
	require.True(t, ok)

	groupARN, ok := group["GroupARN"].(string)
	require.True(t, ok)
	require.NotEmpty(t, groupARN)

	getRec := doXrayRequest(t, h, "/GetGroup", map[string]any{"GroupARN": groupARN})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))

	g2, ok := getResp["Group"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "arn-group", g2["GroupName"])
}

func TestGroups_UpdateByARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doXrayRequest(t, h, "/CreateGroup", map[string]any{"GroupName": "upd-arn-group"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	group, _ := createResp["Group"].(map[string]any)
	groupARN, _ := group["GroupARN"].(string)

	updRec := doXrayRequest(t, h, "/UpdateGroup", map[string]any{
		"GroupARN":         groupARN,
		"FilterExpression": "fault",
	})
	require.Equal(t, http.StatusOK, updRec.Code)

	var updResp map[string]any
	require.NoError(t, json.Unmarshal(updRec.Body.Bytes(), &updResp))

	g2, ok := updResp["Group"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "fault", g2["FilterExpression"])
}

func TestGroups_DeleteByARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doXrayRequest(t, h, "/CreateGroup", map[string]any{"GroupName": "del-arn-group"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	group, _ := createResp["Group"].(map[string]any)
	groupARN, _ := group["GroupARN"].(string)

	delRec := doXrayRequest(t, h, "/DeleteGroup", map[string]any{"GroupARN": groupARN})
	require.Equal(t, http.StatusOK, delRec.Code)

	getRec := doXrayRequest(t, h, "/GetGroup", map[string]any{"GroupARN": groupARN})
	assert.Equal(t, http.StatusBadRequest, getRec.Code, "deleted group must not be found")
}

func TestGroups_FilterExpressionStoredAndReturned(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		filterExpression string
	}{
		{name: "fault filter", filterExpression: "fault"},
		{name: "error filter", filterExpression: "error"},
		{name: "response time filter", filterExpression: "responsetime > 1"},
		{name: "empty filter", filterExpression: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			groupName := fmt.Sprintf("flt-grp-%s", tt.name)

			createRec := doXrayRequest(t, h, "/CreateGroup", map[string]any{
				"GroupName":        groupName,
				"FilterExpression": tt.filterExpression,
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			getRec := doXrayRequest(t, h, "/GetGroup", map[string]any{"GroupName": groupName})
			require.Equal(t, http.StatusOK, getRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))

			g, ok := resp["Group"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.filterExpression, g["FilterExpression"])
		})
	}
}

func TestGroups_ARNPresentInCreateResponse(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/CreateGroup", map[string]any{"GroupName": "arn-check-group"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	group, ok := resp["Group"].(map[string]any)
	require.True(t, ok)

	arn, ok := group["GroupARN"].(string)
	require.True(t, ok)
	assert.Contains(t, arn, "arn:aws:xray:", "GroupARN must be a proper ARN")
}

func TestGroups_ListGroups(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := 1; i <= 3; i++ {
		rec := doXrayRequest(t, h, "/CreateGroup", map[string]any{
			"GroupName": fmt.Sprintf("list-group-%d", i),
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	listRec := doXrayRequest(t, h, "/Groups", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))

	groups, ok := resp["Groups"].([]any)
	require.True(t, ok)
	assert.Len(t, groups, 3)
}
