package inspector2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/inspector2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	inspector2sdk "github.com/aws/aws-sdk-go-v2/service/inspector2"
)

// TestGetConfiguration_ScanModeStatus_RealSDKClient proves
// Ec2ScanModeState.ScanModeStatus (inspector2@v1.54.1 types/types.go's
// Ec2ScanModeState, types/enums.go:1191-1207) decodes as the real
// types.Ec2ScanModeStatusSuccess ("SUCCESS") member, not the non-member
// string "ENABLED" the handler previously emitted -- Ec2ScanModeStatus only
// has SUCCESS/PENDING, no ENABLED. A typed client decodes any string into
// ScanModeStatus without error, so the wrong value produced no decode
// failure.
func TestGetConfiguration_ScanModeStatus_RealSDKClient(t *testing.T) {
	t.Parallel()

	client := newRoundTripTestClient(t)
	ctx := t.Context()

	out, err := client.GetConfiguration(ctx, &inspector2sdk.GetConfigurationInput{})
	require.NoError(t, err)

	require.NotNil(t, out.Ec2Configuration)
	require.NotNil(t, out.Ec2Configuration.ScanModeState)
	assert.Equal(t, types.Ec2ScanModeStatusSuccess, out.Ec2Configuration.ScanModeState.ScanModeStatus)
}
