package timestreamquery_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimestreamQueryHandler_DescribeAccountSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		wantPricingModel string
		wantCode         int
	}{
		{
			name:             "returns default pricing model",
			wantCode:         http.StatusOK,
			wantPricingModel: "COMPUTE_UNITS",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doRequest(t, h, "DescribeAccountSettings", nil)

			assert.Equal(t, tt.wantCode, rec.Code)

			resp := parseResponse(t, rec)
			assert.Equal(t, tt.wantPricingModel, resp["QueryPricingModel"])
		})
	}
}

func TestTimestreamQueryHandler_UpdateAccountSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body             map[string]any
		wantMaxQueryTCU  *float64
		name             string
		wantPricingModel string
		wantCode         int
	}{
		{
			name:             "update pricing model to COMPUTE_UNITS",
			body:             map[string]any{"QueryPricingModel": "COMPUTE_UNITS"},
			wantCode:         http.StatusOK,
			wantPricingModel: "COMPUTE_UNITS",
		},
		{
			name:             "update with MaxQueryTCU",
			body:             map[string]any{"QueryPricingModel": "BYTES_SCANNED", "MaxQueryTCU": 100},
			wantCode:         http.StatusOK,
			wantPricingModel: "BYTES_SCANNED",
			wantMaxQueryTCU: func() *float64 {
				v := float64(100)

				return &v
			}(),
		},
		{
			name:             "empty body uses existing settings",
			body:             map[string]any{},
			wantCode:         http.StatusOK,
			wantPricingModel: "COMPUTE_UNITS",
		},
		{
			name:     "invalid pricing model returns error",
			body:     map[string]any{"QueryPricingModel": "INVALID_MODEL"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doRequest(t, h, "UpdateAccountSettings", tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			resp := parseResponse(t, rec)
			assert.Equal(t, tt.wantPricingModel, resp["QueryPricingModel"])

			if tt.wantMaxQueryTCU != nil {
				assert.InEpsilon(t, *tt.wantMaxQueryTCU, resp["MaxQueryTCU"], 1e-9)
			}
		})
	}
}

func TestTimestreamQueryHandler_UpdateAccountSettings_Persists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		updatePricing    string
		wantPricingModel string
	}{
		{
			name:             "change persists across describe calls",
			updatePricing:    "COMPUTE_UNITS",
			wantPricingModel: "COMPUTE_UNITS",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// Update
			rec := doRequest(t, h, "UpdateAccountSettings", map[string]any{
				"QueryPricingModel": tt.updatePricing,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// Describe should reflect the update
			rec = doRequest(t, h, "DescribeAccountSettings", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResponse(t, rec)
			assert.Equal(t, tt.wantPricingModel, resp["QueryPricingModel"])
		})
	}
}

// TestTimestreamQueryHandler_UpdateAccountSettings_QueryCompute verifies the
// UpdateAccountSettings wire shape for the QueryCompute field: a request
// switching ComputeMode to PROVISIONED with a ProvisionedCapacity.TargetQueryTCU
// must be reflected in the response as
// QueryCompute.ProvisionedCapacity.ActiveQueryTCU (the response-side field
// name, distinct from the request-side TargetQueryTCU), and the change must
// persist across a subsequent DescribeAccountSettings call.
func TestTimestreamQueryHandler_UpdateAccountSettings_QueryCompute(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doRequest(t, h, "UpdateAccountSettings", map[string]any{
		"QueryCompute": map[string]any{
			"ComputeMode": "PROVISIONED",
			"ProvisionedCapacity": map[string]any{
				"TargetQueryTCU": 8,
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResponse(t, rec)
	qc, ok := resp["QueryCompute"].(map[string]any)
	require.True(t, ok, "QueryCompute must be present")
	assert.Equal(t, "PROVISIONED", qc["ComputeMode"])

	pc, ok := qc["ProvisionedCapacity"].(map[string]any)
	require.True(t, ok, "ProvisionedCapacity must be present when ComputeMode is PROVISIONED")
	assert.InEpsilon(t, float64(8), pc["ActiveQueryTCU"], 1e-9)
	_, hasTargetQueryTCU := pc["TargetQueryTCU"]
	assert.False(
		t,
		hasTargetQueryTCU,
		"response ProvisionedCapacity must use ActiveQueryTCU, not the request-side TargetQueryTCU",
	)

	// Persists across Describe.
	rec = doRequest(t, h, "DescribeAccountSettings", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	resp = parseResponse(t, rec)
	qc, ok = resp["QueryCompute"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "PROVISIONED", qc["ComputeMode"])
}

// TestTimestreamQueryHandler_UpdateAccountSettings_QueryCompute_Invalid verifies
// that an invalid or incomplete QueryCompute request is rejected with
// ValidationException rather than silently ignored.
func TestTimestreamQueryHandler_UpdateAccountSettings_QueryCompute_Invalid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "PROVISIONED without TargetQueryTCU",
			body: map[string]any{
				"QueryCompute": map[string]any{"ComputeMode": "PROVISIONED"},
			},
		},
		{
			name: "unrecognised ComputeMode",
			body: map[string]any{
				"QueryCompute": map[string]any{"ComputeMode": "BOGUS"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doRequest(t, h, "UpdateAccountSettings", tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)

			resp := parseResponse(t, rec)
			assert.Equal(t, "ValidationException", resp["__type"])
		})
	}
}

func TestTimestreamQueryHandler_PrepareQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         map[string]any
		name         string
		wantQueryStr string
		wantCode     int
	}{
		{
			name:         "basic prepare",
			body:         map[string]any{"QueryString": "SELECT * FROM my_db.my_table"},
			wantCode:     http.StatusOK,
			wantQueryStr: "SELECT * FROM my_db.my_table",
		},
		{
			name:         "prepare with validate only flag",
			body:         map[string]any{"QueryString": "SELECT 1", "ValidateOnly": true},
			wantCode:     http.StatusOK,
			wantQueryStr: "SELECT 1",
		},
		{
			name:     "missing query string",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doRequest(t, h, "PrepareQuery", tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantQueryStr != "" {
				resp := parseResponse(t, rec)
				assert.Equal(t, tt.wantQueryStr, resp["QueryString"])

				cols, ok := resp["Columns"].([]any)
				require.True(t, ok, "Columns should be a list")
				// PrepareQuery infers columns from the projection; may be non-empty.
				_ = cols

				params, ok := resp["Parameters"].([]any)
				require.True(t, ok, "Parameters should be a list")
				assert.Empty(t, params)
			}
		})
	}
}

func TestTimestreamQueryHandler_PrepareQuery_BackendError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		queryString string
		wantErr     bool
	}{
		{
			name:        "empty query string returns error",
			queryString: "",
			wantErr:     true,
		},
		{
			name:        "non-empty query string succeeds",
			queryString: "SELECT 1",
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestHandler().Backend
			result, err := backend.PrepareQuery(t.Context(), tt.queryString, false)

			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, result)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, result)
			}
		})
	}
}
