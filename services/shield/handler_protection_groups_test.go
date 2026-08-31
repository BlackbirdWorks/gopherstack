package shield_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/shield"
)

// TestHandler_CreateProtectionGroup tests CreateProtectionGroup.
func TestHandler_CreateProtectionGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*shield.Handler)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success all fields",
			setup: func(h *shield.Handler) {
				require.NoError(t, h.Backend.CreateSubscription())
			},
			body: map[string]any{
				"ProtectionGroupId": "group-1",
				"Aggregation":       "MAX",
				"Pattern":           "ALL",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "success with members and resource type",
			setup: func(h *shield.Handler) {
				require.NoError(t, h.Backend.CreateSubscription())
			},
			body: map[string]any{
				"ProtectionGroupId": "group-2",
				"Aggregation":       "SUM",
				"Pattern":           "ARBITRARY",
				"ResourceType":      "ELASTIC_IP_ALLOCATION",
				"Members":           []string{"arn:aws:ec2:us-east-1:123:eip/eipalloc-1"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "duplicate group",
			setup: func(h *shield.Handler) {
				require.NoError(t, h.Backend.CreateSubscription())
				_, err := h.Backend.CreateProtectionGroup("group-dup", "MAX", "ALL", "", nil)
				require.NoError(t, err)
			},
			body: map[string]any{
				"ProtectionGroupId": "group-dup",
				"Aggregation":       "MAX",
				"Pattern":           "ALL",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:  "missing protection group id",
			setup: func(_ *shield.Handler) {},
			body: map[string]any{
				"Aggregation": "MAX",
				"Pattern":     "ALL",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:  "missing aggregation",
			setup: func(_ *shield.Handler) {},
			body: map[string]any{
				"ProtectionGroupId": "group-3",
				"Pattern":           "ALL",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:  "missing pattern",
			setup: func(_ *shield.Handler) {},
			body: map[string]any{
				"ProtectionGroupId": "group-4",
				"Aggregation":       "MAX",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(h)
			rec := doShieldRequest(t, h, "CreateProtectionGroup", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_DeleteProtectionGroup tests DeleteProtectionGroup.
func TestHandler_DeleteProtectionGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*shield.Handler) string
		body       func(id string) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *shield.Handler) string {
				require.NoError(t, h.Backend.CreateSubscription())
				_, err := h.Backend.CreateProtectionGroup("group-1", "MAX", "ALL", "", nil)
				require.NoError(t, err)

				return "group-1"
			},
			body: func(id string) map[string]any {
				return map[string]any{"ProtectionGroupId": id}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not found",
			setup: func(_ *shield.Handler) string {
				return "nonexistent"
			},
			body: func(id string) map[string]any {
				return map[string]any{"ProtectionGroupId": id}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing protection group id",
			setup: func(_ *shield.Handler) string {
				return ""
			},
			body: func(_ string) map[string]any {
				return map[string]any{}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.setup(h)
			rec := doShieldRequest(t, h, "DeleteProtectionGroup", tt.body(id))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestParity_OpaquePageToken_ListProtectionGroups verifies group pagination tokens are opaque.
func TestHandler_ListProtectionGroupsOpaquePageToken(t *testing.T) {
	t.Parallel()

	h, b := newSubscribedHandler(t)

	for i := range 5 {
		_, err := b.CreateProtectionGroup(
			"group-"+string(rune('a'+i)),
			"SUM", "ALL", "", nil,
		)
		require.NoError(t, err)
	}

	rec := doShieldRequest(t, h, "ListProtectionGroups", map[string]any{"MaxResults": 3})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		NextToken string `json:"NextToken"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotEmpty(t, out.NextToken)

	offset := opaqueOffset(t, out.NextToken)
	assert.Equal(t, 3, offset)
}

// TestAudit_Gap7_ListProtectionGroupsPagination verifies pagination for protection groups.
func TestHandler_ListProtectionGroupsPagination(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())

	for i := range 3 {
		_, err := b.CreateProtectionGroup(
			"grp-"+string(rune('a'+i)),
			shield.AggregationSum,
			shield.PatternAll,
			"",
			nil,
		)
		require.NoError(t, err)
	}

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "ListProtectionGroups", map[string]any{
		"MaxResults": 2,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	groups := resp["ProtectionGroups"].([]any)
	assert.Len(t, groups, 2)
	assert.NotEmpty(t, resp["NextToken"])
}

// TestHandler_ListProtectionGroupsDefaultMaxResults verifies that omitting
// MaxResults pages at the documented default of 20
// (api_op_ListProtectionGroups.go: "The default setting is 20."), not at the
// handler's internal cap.
func TestHandler_ListProtectionGroupsDefaultMaxResults(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())

	const numGroups = 25
	for i := range numGroups {
		_, err := b.CreateProtectionGroup(
			fmt.Sprintf("grp-%02d", i),
			shield.AggregationSum,
			shield.PatternAll,
			"",
			nil,
		)
		require.NoError(t, err)
	}

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "ListProtectionGroups", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	groups := resp["ProtectionGroups"].([]any)
	assert.Len(t, groups, 20, "omitted MaxResults must default to 20 per the documented default")
	assert.NotEmpty(t, resp["NextToken"], "25 protection groups at a default page size of 20 must continue")
}

// TestAudit_Gap9_ListProtectionGroupsInclusionFilterByPattern verifies Patterns filter.
func TestHandler_ListProtectionGroupsInclusionFilterByPattern(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())

	_, err := b.CreateProtectionGroup("grp-all", shield.AggregationSum, shield.PatternAll, "", nil)
	require.NoError(t, err)
	_, err = b.CreateProtectionGroup(
		"grp-arb",
		shield.AggregationMax,
		shield.PatternArbitrary,
		"",
		[]string{eipARN("1")},
	)
	require.NoError(t, err)

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "ListProtectionGroups", map[string]any{
		"InclusionFilters": map[string]any{
			"Patterns": []string{shield.PatternAll},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	groups := resp["ProtectionGroups"].([]any)
	assert.Len(t, groups, 1)

	g := groups[0].(map[string]any)
	assert.Equal(t, "grp-all", g["ProtectionGroupId"])
}

// TestAudit_Gap9_ListProtectionGroupsInclusionFilterByAggregation verifies Aggregations filter.
func TestHandler_ListProtectionGroupsInclusionFilterByAggregation(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())

	_, err := b.CreateProtectionGroup("grp-sum", shield.AggregationSum, shield.PatternAll, "", nil)
	require.NoError(t, err)
	_, err = b.CreateProtectionGroup("grp-max", shield.AggregationMax, shield.PatternAll, "", nil)
	require.NoError(t, err)

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "ListProtectionGroups", map[string]any{
		"InclusionFilters": map[string]any{
			"Aggregations": []string{shield.AggregationMax},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	groups := resp["ProtectionGroups"].([]any)
	assert.Len(t, groups, 1)
}

// TestHandler_ListResourcesInProtectionGroupPagination verifies MaxResults/
// NextToken are honored (api_op_ListResourcesInProtectionGroup.go documents
// the same "default setting is 20" and NextToken continuation as the other
// three Shield Advanced List operations), not silently ignored in favor of
// returning every member ARN in one unpaginated response.
func TestHandler_ListResourcesInProtectionGroupPagination(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())

	const numProtections = 25
	for i := range numProtections {
		_, err := b.CreateProtection(fmt.Sprintf("prot-%02d", i), eipARN(fmt.Sprintf("%02d", i)), nil)
		require.NoError(t, err)
	}

	_, err := b.CreateProtectionGroup("grp-all", shield.AggregationSum, shield.PatternAll, "", nil)
	require.NoError(t, err)

	h := shield.NewHandler(b)

	// Omitted MaxResults must default to 20, not return all 25 in one page.
	rec := doShieldRequest(t, h, "ListResourcesInProtectionGroup", map[string]any{
		"ProtectionGroupId": "grp-all",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	arns := resp["ResourceArns"].([]any)
	assert.Len(t, arns, 20, "omitted MaxResults must default to 20 per the documented default")

	nextToken, hasNext := resp["NextToken"]
	require.True(t, hasNext, "NextToken must be present when more members remain")
	require.NotEmpty(t, nextToken)

	// The continuation token must retrieve the remaining members.
	rec = doShieldRequest(t, h, "ListResourcesInProtectionGroup", map[string]any{
		"ProtectionGroupId": "grp-all",
		"NextToken":         nextToken,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp2))
	assert.Len(t, resp2["ResourceArns"].([]any), 5, "the remaining 5 of 25 members must be on the next page")
	assert.NotContains(t, resp2, "NextToken", "no further page should be signaled once all members are returned")
}

// TestRefinement1_HTTPDescribeProtectionGroup tests via HTTP.
func TestHandler_DescribeProtectionGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			body:       map[string]any{"ProtectionGroupId": "grp-1"},
			wantStatus: 200,
		},
		{
			name:       "not_found",
			body:       map[string]any{"ProtectionGroupId": "no-such"},
			wantStatus: 400,
		},
		{
			name:       "missing_id",
			body:       map[string]any{},
			wantStatus: 400,
		},
	}

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddProtectionGroupInternal("grp-1", shield.AggregationMax, shield.PatternAll)
	h := shield.NewHandler(b)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doShieldRequest(t, h, "DescribeProtectionGroup", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement1_HTTPListProtectionGroups tests via HTTP.
func TestHandler_ListProtectionGroupsBasic(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddProtectionGroupInternal("grp-1", shield.AggregationMax, shield.PatternAll)
	b.AddProtectionGroupInternal("grp-2", shield.AggregationSum, shield.PatternAll)

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "ListProtectionGroups", nil)
	require.Equal(t, 200, rec.Code)
}

// TestRefinement1_HTTPUpdateProtectionGroup tests via HTTP.
func TestHandler_UpdateProtectionGroup(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddProtectionGroupInternal("grp-1", shield.AggregationMax, shield.PatternAll)
	h := shield.NewHandler(b)

	rec := doShieldRequest(t, h, "UpdateProtectionGroup", map[string]any{
		"ProtectionGroupId": "grp-1",
		"Aggregation":       shield.AggregationSum,
		"Pattern":           shield.PatternAll,
	})
	assert.Equal(t, 200, rec.Code)
}
