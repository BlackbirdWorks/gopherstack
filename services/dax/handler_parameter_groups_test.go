package dax_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dax"
)

// ---- Parameter Groups ----

func TestHandlerParameterGroups(t *testing.T) {
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
			name:       "create",
			operation:  "CreateParameterGroup",
			setup:      func(_ *testing.T, _ *dax.Handler) {},
			body:       map[string]any{"ParameterGroupName": "my-pg", "Description": "My parameter group"},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()
				pg := resp["ParameterGroup"].(map[string]any)
				assert.Equal(t, "my-pg", pg["ParameterGroupName"])
			},
		},
		{
			name:       "describe all",
			operation:  "DescribeParameterGroups",
			setup:      func(_ *testing.T, _ *dax.Handler) {},
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()
				groups := resp["ParameterGroups"].([]any)
				assert.NotEmpty(t, groups)
			},
		},
		{
			name:      "update",
			operation: "UpdateParameterGroup",
			setup: func(t *testing.T, h *dax.Handler) {
				t.Helper()
				daxRequest(t, h, "CreateParameterGroup", map[string]any{"ParameterGroupName": "upd-pg"})
			},
			body: map[string]any{
				"ParameterGroupName": "upd-pg",
				"ParameterNameValues": []map[string]string{
					{"ParameterName": "query-ttl-millis", "ParameterValue": "60000"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:      "delete",
			operation: "DeleteParameterGroup",
			setup: func(t *testing.T, h *dax.Handler) {
				t.Helper()
				daxRequest(t, h, "CreateParameterGroup", map[string]any{"ParameterGroupName": "pg-del"})
			},
			body:       map[string]any{"ParameterGroupName": "pg-del"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "describe parameters",
			operation:  "DescribeParameters",
			setup:      func(_ *testing.T, _ *dax.Handler) {},
			body:       map[string]any{"ParameterGroupName": dax.DefaultParameterGroupName},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()
				params := resp["Parameters"].([]any)
				assert.NotEmpty(t, params)
			},
		},
		{
			name:       "describe default parameters",
			operation:  "DescribeDefaultParameters",
			setup:      func(_ *testing.T, _ *dax.Handler) {},
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()
				params := resp["Parameters"].([]any)
				assert.Len(t, params, 2)
			},
		},
		{
			name:      "reset parameter group",
			operation: "ResetParameterGroup",
			setup: func(t *testing.T, h *dax.Handler) {
				t.Helper()
				daxRequest(t, h, "CreateParameterGroup", map[string]any{"ParameterGroupName": "reset-pg"})
			},
			body:       map[string]any{"ParameterGroupName": "reset-pg"},
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

// TestHandlerUpdateParameterGroupRequiresParameterNameValues verifies that omitting the
// @required ParameterNameValues field (validators.go:654, validateOpUpdateParameterGroupInput)
// is rejected rather than silently treated as a successful no-op update.
func TestHandlerUpdateParameterGroupRequiresParameterNameValues(t *testing.T) {
	t.Parallel()
	h := newTestHandler()

	createRec := daxRequest(t, h, "CreateParameterGroup", map[string]any{"ParameterGroupName": "pnv-required"})
	require.Equal(t, http.StatusOK, createRec.Code)

	rec := daxRequest(t, h, "UpdateParameterGroup", map[string]any{"ParameterGroupName": "pnv-required"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "InvalidParameterValueException", errResp["__type"])
}
