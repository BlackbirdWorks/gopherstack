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
