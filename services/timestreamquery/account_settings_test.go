package timestreamquery_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/timestreamquery"
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

// TestDescribeAccountSettings_DefaultQueryCompute verifies that
// DescribeAccountSettings always includes QueryCompute even without a prior
// UpdateAccountSettings call. Real AWS includes ComputeMode: "ON_DEMAND" by
// default; the emulator previously omitted the field entirely.
func TestDescribeAccountSettings_DefaultQueryCompute(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doRequest(t, h, "DescribeAccountSettings", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResponse(t, rec)
	qc, ok := resp["QueryCompute"].(map[string]any)
	require.True(t, ok, "QueryCompute must be present in default DescribeAccountSettings response")
	assert.Equal(t, "ON_DEMAND", qc["ComputeMode"])
}

func TestAccountSettings_DefaultComputeUnits(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doRequest(t, h, "DescribeAccountSettings", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	assert.Equal(t, "COMPUTE_UNITS", resp["QueryPricingModel"])
}

func TestAccountSettings_LastUpdatedTimeSetOnUpdate(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doRequest(t, h, "UpdateAccountSettings", map[string]any{
		"QueryPricingModel": "BYTES_SCANNED",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	assert.NotNil(t, resp["LastUpdatedTime"], "LastUpdatedTime must be set after update")
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

// TestUpdateAccountSettings_MaxQueryTCUPositiveValidation verifies MaxQueryTCU must be positive.
func TestUpdateAccountSettings_MaxQueryTCUPositiveValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tcu      int
		wantCode int
	}{
		{name: "positive value is valid", tcu: 100, wantCode: http.StatusOK},
		{name: "zero is invalid", tcu: 0, wantCode: http.StatusBadRequest},
		{name: "negative is invalid", tcu: -1, wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doRequest(t, h, "UpdateAccountSettings", map[string]any{
				"MaxQueryTCU": tt.tcu,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestInMemoryBackend_UpdateAccountSettings_QueryCompute verifies that
// UpdateAccountSettings actually applies the QueryCompute field instead of
// silently discarding it. Before this fix, DescribeAccountSettings always
// echoed ComputeMode: ON_DEMAND regardless of what QueryCompute was sent to
// UpdateAccountSettings, because the field was never read from the request.
func TestInMemoryBackend_UpdateAccountSettings_QueryCompute(t *testing.T) {
	t.Parallel()

	tcu := int32(8)

	tests := []struct {
		queryCompute  *timestreamquery.QueryComputeUpdate
		name          string
		wantMode      string
		wantActiveTCU int32
		wantErr       bool
	}{
		{
			name:          "switch to PROVISIONED applies ActiveQueryTCU",
			queryCompute:  &timestreamquery.QueryComputeUpdate{ComputeMode: "PROVISIONED", TargetQueryTCU: &tcu},
			wantMode:      "PROVISIONED",
			wantActiveTCU: 8,
		},
		{
			name:         "PROVISIONED without TargetQueryTCU is rejected",
			queryCompute: &timestreamquery.QueryComputeUpdate{ComputeMode: "PROVISIONED"},
			wantErr:      true,
		},
		{
			name:         "invalid ComputeMode is rejected",
			queryCompute: &timestreamquery.QueryComputeUpdate{ComputeMode: "BOGUS"},
			wantErr:      true,
		},
		{
			name:         "nil QueryCompute leaves existing settings untouched",
			queryCompute: nil,
			wantMode:     "ON_DEMAND",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := timestreamquery.NewInMemoryBackend("123456789012", "us-east-1")

			settings, err := backend.UpdateAccountSettings(t.Context(), "", nil, tt.queryCompute)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, settings.QueryCompute)
			assert.Equal(t, tt.wantMode, settings.QueryCompute.ComputeMode)

			if tt.wantActiveTCU != 0 {
				require.NotNil(t, settings.QueryCompute.ProvisionedCapacity)
				require.NotNil(t, settings.QueryCompute.ProvisionedCapacity.ActiveQueryTCU)
				assert.Equal(t, tt.wantActiveTCU, *settings.QueryCompute.ProvisionedCapacity.ActiveQueryTCU)
			}
		})
	}
}

// TestInMemoryBackend_UpdateAccountSettings_QueryComputeRoundTrip verifies
// that switching PROVISIONED -> ON_DEMAND clears the stale ProvisionedCapacity
// rather than leaving it dangling, and that the change is visible via
// DescribeAccountSettings.
func TestInMemoryBackend_UpdateAccountSettings_QueryComputeRoundTrip(t *testing.T) {
	t.Parallel()

	backend := timestreamquery.NewInMemoryBackend("123456789012", "us-east-1")
	tcu := int32(4)

	_, err := backend.UpdateAccountSettings(t.Context(), "", nil,
		&timestreamquery.QueryComputeUpdate{ComputeMode: "PROVISIONED", TargetQueryTCU: &tcu})
	require.NoError(t, err)

	settings, err := backend.UpdateAccountSettings(t.Context(), "", nil,
		&timestreamquery.QueryComputeUpdate{ComputeMode: "ON_DEMAND"})
	require.NoError(t, err)
	require.NotNil(t, settings.QueryCompute)
	assert.Equal(t, "ON_DEMAND", settings.QueryCompute.ComputeMode)
	assert.Nil(t, settings.QueryCompute.ProvisionedCapacity)

	described := backend.DescribeAccountSettings(t.Context())
	require.NotNil(t, described.QueryCompute)
	assert.Equal(t, "ON_DEMAND", described.QueryCompute.ComputeMode)
}
