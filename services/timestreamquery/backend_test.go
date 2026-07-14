package timestreamquery_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/timestreamquery"
)

// TestInMemoryBackend_CreateScheduledQuery_ClientTokenIdempotent verifies that
// CreateScheduledQuery replays the original success when called again with the
// same ClientToken, instead of surfacing ConflictException. The aws-sdk-go-v2
// client auto-generates a ClientToken on every CreateScheduledQuery call (see
// idempotencyToken_initializeOpCreateScheduledQuery), so a retried request
// after a lost response (e.g. a network blip) must not error just because the
// resource already exists under that token.
func TestInMemoryBackend_CreateScheduledQuery_ClientTokenIdempotent(t *testing.T) {
	t.Parallel()

	backend := timestreamquery.NewInMemoryBackend("123456789012", "us-east-1")

	first, err := backend.CreateScheduledQuery(
		t.Context(), "idempotent-sq", "SELECT 1", "rate(1 hour)", "arn:aws:iam::123456789012:role/r",
		"", "", "", "", "retry-token-1", nil,
	)
	require.NoError(t, err)

	// A second call with the SAME ClientToken (and, notably, a different
	// QueryString -- simulating a client that doesn't realise the first
	// attempt already succeeded) must replay the original result rather than
	// erroring.
	second, err := backend.CreateScheduledQuery(
		t.Context(), "idempotent-sq", "SELECT 2", "rate(2 hours)", "arn:aws:iam::123456789012:role/other",
		"", "", "", "", "retry-token-1", nil,
	)
	require.NoError(t, err)
	assert.Equal(t, first.Arn, second.Arn)
	assert.Equal(t, "SELECT 1", second.QueryString, "replay must return the ORIGINAL query, not the retry's")
	assert.Equal(t, 1, timestreamquery.ScheduledQueryCount(backend))

	// A different ClientToken for the same Name is a genuine conflict.
	_, err = backend.CreateScheduledQuery(
		t.Context(), "idempotent-sq", "SELECT 3", "rate(1 hour)", "arn:aws:iam::123456789012:role/r",
		"", "", "", "", "different-token", nil,
	)
	require.Error(t, err)

	// No ClientToken at all: repeated calls are NOT deduplicated and hit the
	// normal "already exists" conflict path.
	_, err = backend.CreateScheduledQuery(
		t.Context(), "no-token-sq", "SELECT 1", "rate(1 hour)", "arn:aws:iam::123456789012:role/r",
		"", "", "", "", "", nil,
	)
	require.NoError(t, err)

	_, err = backend.CreateScheduledQuery(
		t.Context(), "no-token-sq", "SELECT 1", "rate(1 hour)", "arn:aws:iam::123456789012:role/r",
		"", "", "", "", "", nil,
	)
	require.Error(t, err)
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
