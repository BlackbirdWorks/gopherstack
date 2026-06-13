package memorydb_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
)

// TestHandler_DescribeReservedNodes verifies DescribeReservedNodes returns an empty list by default.
func TestHandler_DescribeReservedNodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name:       "empty list",
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name:       "filter by node type - no match",
			body:       map[string]any{"NodeType": "db.r6g.xlarge"},
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "DescribeReservedNodes", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			nodes := resp["ReservedNodes"].([]any)
			assert.Len(t, nodes, tt.wantCount)
		})
	}
}

// TestHandler_DescribeReservedNodesOfferings verifies the built-in offerings are returned.
func TestHandler_DescribeReservedNodesOfferings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
		wantMin    int
	}{
		{
			name:       "all offerings",
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			wantMin:    1,
		},
		{
			name:       "filter by node type",
			body:       map[string]any{"NodeType": "db.r6g.large"},
			wantStatus: http.StatusOK,
			wantMin:    1,
		},
		{
			name:       "filter by unknown node type",
			body:       map[string]any{"NodeType": "db.r99.unknown"},
			wantStatus: http.StatusOK,
			wantMin:    0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "DescribeReservedNodesOfferings", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			offerings := resp["ReservedNodesOfferings"].([]any)
			assert.GreaterOrEqual(t, len(offerings), tt.wantMin)
		})
	}
}

// TestHandler_PurchaseReservedNodesOffering verifies purchasing a reservation.
func TestHandler_PurchaseReservedNodesOffering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "purchase valid offering",
			body: map[string]any{
				"ReservedNodesOfferingId": "aaa00000-1111-2222-3333-444444444444",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "purchase with custom reservation id",
			body: map[string]any{
				"ReservedNodesOfferingId": "bbb00000-1111-2222-3333-444444444444",
				"ReservationId":           "my-reservation",
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing offering id",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "unknown offering id",
			body: map[string]any{
				"ReservedNodesOfferingId": "zzz-does-not-exist",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "PurchaseReservedNodesOffering", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				rn, ok := resp["ReservedNode"].(map[string]any)
				require.True(t, ok)
				assert.NotEmpty(t, rn["NodeType"])
				assert.NotEmpty(t, rn["State"])
			}
		})
	}
}

// TestHandler_PurchaseReservedNodesOffering_ThenDescribe verifies a purchased node is returned
// by DescribeReservedNodes.
func TestHandler_PurchaseReservedNodesOffering_ThenDescribe(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Purchase a reservation.
	recPurchase := doRequest(t, h, "PurchaseReservedNodesOffering", map[string]any{
		"ReservedNodesOfferingId": "aaa00000-1111-2222-3333-444444444444",
		"ReservationId":           "test-reservation",
	})
	require.Equal(t, http.StatusOK, recPurchase.Code)

	// Describe should now return the purchased node.
	recDescribe := doRequest(t, h, "DescribeReservedNodes", map[string]any{})
	require.Equal(t, http.StatusOK, recDescribe.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(recDescribe.Body.Bytes(), &resp))

	nodes := resp["ReservedNodes"].([]any)
	assert.Len(t, nodes, 1)
}

// TestHandler_DescribeMultiRegionParameters verifies DescribeMultiRegionParameters.
func TestHandler_DescribeMultiRegionParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "missing parameter group name",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "non-existent parameter group",
			body:       map[string]any{"ParameterGroupName": "no-such"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "DescribeMultiRegionParameters", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_DescribeMultiRegionParameters_WithGroup verifies parameters are returned for existing group.
func TestHandler_DescribeMultiRegionParameters_WithGroup(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
	b.AddMultiRegionParameterGroupInternal("my-mr-pg", "memorydb_redis7")
	h := memorydb.NewHandler(b)

	rec := doRequest(t, h, "DescribeMultiRegionParameters", map[string]any{
		"ParameterGroupName": "my-mr-pg",
	})

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["Parameters"])
}
