package memorydb_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_DescribeSubnetGroups_All tests DescribeSubnetGroups with no filter.
func TestHandler_DescribeSubnetGroups_All(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateSubnetGroup", map[string]any{
		"SubnetGroupName": "sg-1",
		"SubnetIds":       []string{"subnet-1"},
	})

	rec := doRequest(t, h, "DescribeSubnetGroups", map[string]any{})
	assert.Equal(t, 200, rec.Code)
}

// TestHandler_SubnetGroup_CRUD tests full SubnetGroup lifecycle through the handler.
func TestHandler_SubnetGroup_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		setup      func(*memorydb.Handler)
		name       string
		op         string
		wantStatus int
	}{
		{
			name: "create subnet group",
			op:   "CreateSubnetGroup",
			body: map[string]any{
				"SubnetGroupName": "my-sg",
				"SubnetIds":       []string{"subnet-1"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "create subnet group missing name",
			op:         "CreateSubnetGroup",
			body:       map[string]any{"SubnetIds": []string{"subnet-1"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "describe subnet groups",
			op:   "DescribeSubnetGroups",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateSubnetGroup", map[string]any{
					"SubnetGroupName": "sg-x",
					"SubnetIds":       []string{"subnet-1"},
				})
			},
			body:       map[string]any{"SubnetGroupName": "sg-x"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "describe subnet group not found",
			op:         "DescribeSubnetGroups",
			body:       map[string]any{"SubnetGroupName": "no-such"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "delete subnet group",
			op:   "DeleteSubnetGroup",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateSubnetGroup", map[string]any{
					"SubnetGroupName": "del-sg",
					"SubnetIds":       []string{"subnet-1"},
				})
			},
			body:       map[string]any{"SubnetGroupName": "del-sg"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete subnet group missing name",
			op:         "DeleteSubnetGroup",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "delete subnet group not found",
			op:         "DeleteSubnetGroup",
			body:       map[string]any{"SubnetGroupName": "no-such"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "update subnet group",
			op:   "UpdateSubnetGroup",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateSubnetGroup", map[string]any{
					"SubnetGroupName": "upd-sg",
					"SubnetIds":       []string{"subnet-1"},
				})
			},
			body: map[string]any{
				"SubnetGroupName": "upd-sg",
				"Description":     "new desc",
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "update subnet group missing name",
			op:         "UpdateSubnetGroup",
			body:       map[string]any{"Description": "new"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "update subnet group not found",
			op:         "UpdateSubnetGroup",
			body:       map[string]any{"SubnetGroupName": "no-such"},
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

// TestHandler_UpdateSubnetGroup_Fields tests UpdateSubnetGroup field updates.
func TestHandler_UpdateSubnetGroup_Fields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		updateBody map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "update description",
			updateBody: map[string]any{
				"SubnetGroupName": "upd-sg",
				"Description":     "new description",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "update subnet IDs",
			updateBody: map[string]any{
				"SubnetGroupName": "upd-sg",
				"SubnetIds":       []string{"subnet-new-1"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "update nonexistent subnet group returns 400",
			updateBody: map[string]any{
				"SubnetGroupName": "no-such-sg",
				"Description":     "desc",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name != "update nonexistent subnet group returns 400" {
				doRequest(t, h, "CreateSubnetGroup", map[string]any{
					"SubnetGroupName": "upd-sg",
					"SubnetIds":       []string{"subnet-1"},
				})
			}

			rec := doRequest(t, h, "UpdateSubnetGroup", tt.updateBody)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_SubnetGroupCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*memorydb.Handler)
		body       map[string]any
		name       string
		op         string
		wantStatus int
	}{
		{
			name:       "create subnet group",
			op:         "CreateSubnetGroup",
			body:       map[string]any{"SubnetGroupName": "my-sg", "SubnetIds": []string{"subnet-1", "subnet-2"}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "create subnet group missing name",
			op:         "CreateSubnetGroup",
			body:       map[string]any{"SubnetIds": []string{"subnet-1"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "describe subnet groups",
			op:   "DescribeSubnetGroups",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateSubnetGroup", map[string]any{"SubnetGroupName": "my-sg"})
			},
			body:       map[string]any{},
			wantStatus: http.StatusOK,
		},
		{
			name:       "describe subnet group not found",
			op:         "DescribeSubnetGroups",
			body:       map[string]any{"SubnetGroupName": "no-such"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "update subnet group",
			op:   "UpdateSubnetGroup",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateSubnetGroup", map[string]any{"SubnetGroupName": "my-sg"})
			},
			body:       map[string]any{"SubnetGroupName": "my-sg", "Description": "updated"},
			wantStatus: http.StatusOK,
		},
		{
			name: "delete subnet group",
			op:   "DeleteSubnetGroup",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateSubnetGroup", map[string]any{"SubnetGroupName": "my-sg"})
			},
			body:       map[string]any{"SubnetGroupName": "my-sg"},
			wantStatus: http.StatusOK,
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

// TestHandler_SubnetGroup_SupportedNetworkTypesAndAvailabilityZone verifies
// the SubnetGroup response carries SupportedNetworkTypes (both group- and
// subnet-level) and each Subnet's AvailabilityZone -- fields confirmed
// present on the real SDK's types.SubnetGroup/types.Subnet
// (deserializers.go's awsAwsjson11_deserializeDocumentSubnetGroup/...Subnet)
// but missing from a prior pass's wire shape.
func TestHandler_SubnetGroup_SupportedNetworkTypesAndAvailabilityZone(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateSubnetGroup", map[string]any{
		"SubnetGroupName": "az-sg",
		"SubnetIds":       []string{"subnet-1", "subnet-2"},
	})
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	sg, _ := resp["SubnetGroup"].(map[string]any)
	require.NotNil(t, sg)

	groupTypes, _ := sg["SupportedNetworkTypes"].([]any)
	assert.NotEmpty(t, groupTypes, "SubnetGroup.SupportedNetworkTypes must be present")

	subnets, _ := sg["Subnets"].([]any)
	require.Len(t, subnets, 2)

	for _, s := range subnets {
		subnet, _ := s.(map[string]any)
		require.NotNil(t, subnet)
		assert.NotEmpty(t, subnet["Identifier"])

		subnetTypes, _ := subnet["SupportedNetworkTypes"].([]any)
		assert.NotEmpty(t, subnetTypes, "Subnet.SupportedNetworkTypes must be present")

		az, _ := subnet["AvailabilityZone"].(map[string]any)
		require.NotNil(t, az, "Subnet.AvailabilityZone must be present")
		assert.NotEmpty(t, az["Name"])
	}
}

// -- ParameterGroup CRUD -------------------------------------------------------
