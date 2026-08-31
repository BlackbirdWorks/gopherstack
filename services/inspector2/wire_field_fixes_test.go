package inspector2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/inspector2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	inspector2sdk "github.com/aws/aws-sdk-go-v2/service/inspector2"
)

// TestGetConfiguration_StatusFields_RealSDKClient proves two
// GetConfigurationOutput enum fields decode as real SDK enum members, not
// the non-member "ENABLED" string the handler previously emitted for both:
// Ec2ScanModeState.ScanModeStatus (inspector2@v1.54.1 types/types.go's
// Ec2ScanModeState, types/enums.go:1191-1207 -- only SUCCESS/PENDING, no
// ENABLED) and EcrRescanDurationState.Status (types/types.go's
// EcrRescanDurationState, types/enums.go:1289-1303 -- only
// SUCCESS/PENDING/FAILED, no ENABLED). A typed client decodes any string
// into these enums without error, so the wrong value produced no decode
// failure.
func TestGetConfiguration_StatusFields_RealSDKClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(*testing.T, *inspector2sdk.GetConfigurationOutput)
		name  string
	}{
		{
			name: "Ec2ScanModeState.ScanModeStatus",
			check: func(t *testing.T, out *inspector2sdk.GetConfigurationOutput) {
				t.Helper()

				require.NotNil(t, out.Ec2Configuration)
				require.NotNil(t, out.Ec2Configuration.ScanModeState)
				assert.Equal(t, types.Ec2ScanModeStatusSuccess, out.Ec2Configuration.ScanModeState.ScanModeStatus)
			},
		},
		{
			name: "EcrRescanDurationState.Status",
			check: func(t *testing.T, out *inspector2sdk.GetConfigurationOutput) {
				t.Helper()

				require.NotNil(t, out.EcrConfiguration)
				require.NotNil(t, out.EcrConfiguration.RescanDurationState)
				assert.Equal(t, types.EcrRescanDurationStatusSuccess, out.EcrConfiguration.RescanDurationState.Status)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newRoundTripTestClient(t)

			out, err := client.GetConfiguration(t.Context(), &inspector2sdk.GetConfigurationInput{})
			require.NoError(t, err)

			tc.check(t, out)
		})
	}
}
