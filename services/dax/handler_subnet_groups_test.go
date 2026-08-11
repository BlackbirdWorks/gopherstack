package dax_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dax"
)

// ---- Subnet Groups ----

func TestHandlerSubnetGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *dax.Handler)
		body       map[string]any
		check      func(t *testing.T, resp map[string]any)
		name       string
		operation  string
		wantStatus int
	}{
		{
			name:      "create",
			operation: "CreateSubnetGroup",
			setup:     func(_ *testing.T, _ *dax.Handler) {},
			body: map[string]any{
				"SubnetGroupName": "my-sg",
				"Description":     "My subnet group",
				"SubnetIds":       []string{"subnet-abc12345"},
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()
				sg := resp["SubnetGroup"].(map[string]any)
				assert.Equal(t, "my-sg", sg["SubnetGroupName"])
				subnets := sg["Subnets"].([]any)
				require.Len(t, subnets, 1)
				subnet := subnets[0].(map[string]any)
				assert.Equal(t, "subnet-abc12345", subnet["SubnetIdentifier"])
				assert.Equal(t, "us-east-1a", subnet["SubnetAvailabilityZone"])
			},
		},
		{
			name:       "describe all",
			operation:  "DescribeSubnetGroups",
			setup:      func(_ *testing.T, _ *dax.Handler) {},
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()
				groups := resp["SubnetGroups"].([]any)
				assert.NotEmpty(t, groups)
			},
		},
		{
			name:      "update",
			operation: "UpdateSubnetGroup",
			setup: func(t *testing.T, h *dax.Handler) {
				t.Helper()
				daxRequest(t, h, "CreateSubnetGroup", map[string]any{
					"SubnetGroupName": "upd-sg",
					"SubnetIds":       []string{"subnet-11111111"},
				})
			},
			body: map[string]any{
				"SubnetGroupName": "upd-sg",
				"Description":     "Updated description",
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()
				sg := resp["SubnetGroup"].(map[string]any)
				assert.Equal(t, "Updated description", sg["Description"])
			},
		},
		{
			name:      "delete",
			operation: "DeleteSubnetGroup",
			setup: func(t *testing.T, h *dax.Handler) {
				t.Helper()
				daxRequest(t, h, "CreateSubnetGroup", map[string]any{
					"SubnetGroupName": "sg-del",
					"SubnetIds":       []string{"subnet-11111111"},
				})
			},
			body:       map[string]any{"SubnetGroupName": "sg-del"},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			tt.setup(t, h)

			rec := daxRequest(t, h, tt.operation, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.check != nil && rec.Code == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				tt.check(t, resp)
			}
		})
	}
}

// TestHandlerSubnetGroupSubnetHasSupportedNetworkTypes verifies that each Subnet in the
// response carries its own SupportedNetworkTypes field -- botocore's dax service-2.json
// (2017-04-19) Subnet shape has a per-subnet SupportedNetworkTypes member (NetworkTypeList),
// distinct from the SubnetGroup-level field of the same name.
func TestHandlerSubnetGroupSubnetHasSupportedNetworkTypes(t *testing.T) {
	t.Parallel()
	h := newTestHandler()

	rec := daxRequest(t, h, "CreateSubnetGroup", map[string]any{
		"SubnetGroupName": "per-subnet-nt",
		"SubnetIds":       []string{"subnet-abc12345"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	sg := resp["SubnetGroup"].(map[string]any)
	subnets := sg["Subnets"].([]any)
	require.Len(t, subnets, 1)

	subnet := subnets[0].(map[string]any)
	nt, ok := subnet["SupportedNetworkTypes"].([]any)
	require.True(t, ok, "Subnet must carry its own SupportedNetworkTypes field")
	require.Len(t, nt, 1)
	assert.Equal(t, dax.NetworkTypeIPv4, nt[0])
}
