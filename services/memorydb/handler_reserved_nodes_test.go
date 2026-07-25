package memorydb_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestWireEpoch_ReservedNode_StartTimeIsNumber(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "PurchaseReservedNodesOffering", map[string]any{
		"ReservedNodesOfferingId": "aaa00000-1111-2222-3333-444444444444",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	rn, _ := resp["ReservedNode"].(map[string]any)
	require.NotNil(t, rn)

	_, isNumber := rn["StartTime"].(float64)
	assert.True(t, isNumber,
		"ReservedNode.StartTime must be a JSON number, got %T: %v", rn["StartTime"], rn["StartTime"])
}

func TestHandler_ReservedNodes_PurchaseAndDescribe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		offeringID string
		wantStatus int
	}{
		{
			name:       "purchase valid offering",
			offeringID: "aaa00000-1111-2222-3333-444444444444",
			wantStatus: http.StatusOK,
		},
		{
			name:       "purchase nonexistent offering",
			offeringID: "nonexistent-offering-id",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			rec := doRequest(t, h, "PurchaseReservedNodesOffering", map[string]any{
				"ReservedNodesOfferingId": tt.offeringID,
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				rn, _ := resp["ReservedNode"].(map[string]any)
				assert.NotNil(t, rn)
				assert.NotEmpty(t, rn["ReservationId"])
				assert.Equal(t, tt.offeringID, rn["ReservedNodesOfferingId"])
			}
		})
	}
}

func TestHandler_ReservedNodesOfferings_Seeded(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "DescribeReservedNodesOfferings", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	offerings, _ := resp["ReservedNodesOfferings"].([]any)
	assert.GreaterOrEqual(t, len(offerings), 3, "at least 3 reserved node offerings should be seeded")
}

func TestHandler_ReservedNodes_DescribeAfterPurchase(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Purchase a node.
	purchaseRec := doRequest(t, h, "PurchaseReservedNodesOffering", map[string]any{
		"ReservedNodesOfferingId": "aaa00000-1111-2222-3333-444444444444",
		"ReservationId":           "my-reservation-123",
	})
	require.Equal(t, http.StatusOK, purchaseRec.Code, "purchase: %s", purchaseRec.Body)

	// Describe with filter.
	descRec := doRequest(t, h, "DescribeReservedNodes", map[string]any{
		"ReservationId": "my-reservation-123",
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))
	nodes, _ := resp["ReservedNodes"].([]any)
	assert.Len(t, nodes, 1)

	node, _ := nodes[0].(map[string]any)
	assert.Equal(t, "my-reservation-123", node["ReservationId"])
	assert.Equal(t, "aaa00000-1111-2222-3333-444444444444", node["ReservedNodesOfferingId"])
	assert.Equal(t, "active", node["State"])
}

// -- Snapshot filter by type (finding 19) ----------------------------------------

// TestHandler_ReservedNodes_WithPurchase tests reserved node operations after purchase.
func TestHandler_ReservedNodes_WithPurchase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		descBody   map[string]any
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name:       "describe reserved nodes after purchase",
			descBody:   map[string]any{},
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
		{
			name:       "filter reserved node by offering type",
			descBody:   map[string]any{"OfferingType": "No Upfront"},
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
		{
			name:       "filter reserved node by node type",
			descBody:   map[string]any{"NodeType": "db.r6g.large"},
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
		{
			name:       "filter reserved node - no match",
			descBody:   map[string]any{"NodeType": "db.r6g.4xlarge"},
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Purchase a reserved node first
			purchaseRec := doRequest(t, h, "PurchaseReservedNodesOffering", map[string]any{
				"ReservedNodesOfferingId": "aaa00000-1111-2222-3333-444444444444",
			})
			require.Equal(t, http.StatusOK, purchaseRec.Code)

			rec := doRequest(t, h, "DescribeReservedNodes", tt.descBody)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			nodes := resp["ReservedNodes"].([]any)
			assert.Len(t, nodes, tt.wantCount)
		})
	}
}

// TestHandler_ReservedNodesOfferings_Filtered tests reserved nodes offerings filtering.
func TestHandler_ReservedNodesOfferings_Filtered(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body      map[string]any
		name      string
		wantCount int
	}{
		{name: "all offerings", body: map[string]any{}, wantCount: 3},
		{name: "filter by node type", body: map[string]any{"NodeType": "db.r6g.large"}, wantCount: 2},
		{name: "filter by offering type", body: map[string]any{"OfferingType": "All Upfront"}, wantCount: 1},
		{name: "filter by duration 1y", body: map[string]any{"Duration": "1"}, wantCount: 2},
		{name: "filter by duration 3y", body: map[string]any{"Duration": "3"}, wantCount: 1},
		{
			name:      "filter by specific offering ID",
			body:      map[string]any{"ReservedNodesOfferingId": "aaa00000-1111-2222-3333-444444444444"},
			wantCount: 1,
		},
		{name: "unknown duration returns all", body: map[string]any{"Duration": "99"}, wantCount: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "DescribeReservedNodesOfferings", tt.body)
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			offerings := resp["ReservedNodesOfferings"].([]any)
			assert.Len(t, offerings, tt.wantCount)
		})
	}
}

func TestHandler_DescribeReservedNodesOfferings_DurationFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         map[string]any
		name         string
		wantMinCount int
		wantDuration int
	}{
		{
			name:         "no filter returns all offerings",
			body:         map[string]any{},
			wantMinCount: 3,
		},
		{
			name:         "filter by duration 1 year (string)",
			body:         map[string]any{"Duration": "1"},
			wantMinCount: 1,
			wantDuration: 31536000,
		},
		{
			name:         "filter by duration 3 years (string)",
			body:         map[string]any{"Duration": "3"},
			wantMinCount: 1,
			wantDuration: 94608000,
		},
		{
			name:         "filter by duration in seconds (1 year)",
			body:         map[string]any{"Duration": "31536000"},
			wantMinCount: 1,
			wantDuration: 31536000,
		},
		{
			name:         "filter by duration in seconds (3 years)",
			body:         map[string]any{"Duration": "94608000"},
			wantMinCount: 1,
			wantDuration: 94608000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "DescribeReservedNodesOfferings", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			offerings := resp["ReservedNodesOfferings"].([]any)
			assert.GreaterOrEqual(t, len(offerings), tt.wantMinCount)

			if tt.wantDuration > 0 {
				for _, o := range offerings {
					off := o.(map[string]any)
					assert.InDelta(t, float64(tt.wantDuration), off["Duration"], 0)
				}
			}
		})
	}
}

// -- ReservedNodes -------------------------------------------------------------

func TestHandler_ReservedNodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*memorydb.Handler)
		body       map[string]any
		name       string
		op         string
		wantStatus int
	}{
		{
			name:       "describe reserved nodes empty",
			op:         "DescribeReservedNodes",
			body:       map[string]any{},
			wantStatus: http.StatusOK,
		},
		{
			name: "purchase reserved nodes offering",
			op:   "PurchaseReservedNodesOffering",
			body: map[string]any{
				"ReservedNodesOfferingId": "aaa00000-1111-2222-3333-444444444444",
				"ReservationId":           "my-reservation",
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "purchase reserved nodes missing offering id",
			op:         "PurchaseReservedNodesOffering",
			body:       map[string]any{},
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

			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// -- ListAllowedNodeTypeUpdates ------------------------------------------------
