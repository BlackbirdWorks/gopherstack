package inspector2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	inspector2sdk "github.com/aws/aws-sdk-go-v2/service/inspector2"
	"github.com/aws/aws-sdk-go-v2/service/inspector2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_MemberScopedConfiguration drives GetConfiguration/
// UpdateConfiguration's AccountId parameter (and UpdateConfigurationInheritance)
// through a real aws-sdk-go-v2 client: a member account's scan settings
// individually override the delegated admin's own configuration until reset
// to inherit.
func TestRealClient_MemberScopedConfiguration(t *testing.T) {
	t.Parallel()

	backend, client := newRealClient(t)
	ctx := t.Context()

	_, err := client.UpdateConfiguration(ctx, &inspector2sdk.UpdateConfigurationInput{
		Ec2Configuration: &types.Ec2Configuration{ScanMode: types.Ec2ScanModeEc2Hybrid},
		EcrConfiguration: &types.EcrConfiguration{RescanDuration: types.EcrRescanDurationDays30},
	})
	require.NoError(t, err)

	require.NoError(t, backend.AssociateMember("222222222222"))

	_, err = client.GetConfiguration(ctx, &inspector2sdk.GetConfigurationInput{
		AccountId: aws.String("999999999999"),
	})
	require.Error(t, err, "GetConfiguration for an unassociated account must fail")

	memberOut, err := client.GetConfiguration(ctx, &inspector2sdk.GetConfigurationInput{
		AccountId: aws.String("222222222222"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.Ec2ScanModeEc2Hybrid, memberOut.Ec2Configuration.ScanModeState.ScanMode,
		"an unconfigured member inherits the delegated admin's configuration")
	assert.Equal(
		t, types.EcrRescanDurationDays30, memberOut.EcrConfiguration.RescanDurationState.RescanDuration,
	)

	_, err = client.UpdateConfiguration(ctx, &inspector2sdk.UpdateConfigurationInput{
		AccountId:        aws.String("222222222222"),
		Ec2Configuration: &types.Ec2Configuration{ScanMode: types.Ec2ScanModeEc2SsmAgentBased},
	})
	require.NoError(t, err)

	overriddenOut, err := client.GetConfiguration(ctx, &inspector2sdk.GetConfigurationInput{
		AccountId: aws.String("222222222222"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.Ec2ScanModeEc2SsmAgentBased, overriddenOut.Ec2Configuration.ScanModeState.ScanMode,
		"the member's own override must take precedence over the admin's configuration")
	assert.Equal(t, types.EcrRescanDurationDays30, overriddenOut.EcrConfiguration.RescanDurationState.RescanDuration,
		"an untouched scan type still inherits the admin's configuration")

	adminOut, err := client.GetConfiguration(ctx, &inspector2sdk.GetConfigurationInput{})
	require.NoError(t, err)
	assert.Equal(t, types.Ec2ScanModeEc2Hybrid, adminOut.Ec2Configuration.ScanModeState.ScanMode,
		"the admin's own configuration must be untouched by a member-scoped update")

	_, err = client.UpdateConfiguration(ctx, &inspector2sdk.UpdateConfigurationInput{
		AccountId: aws.String("222222222222"),
		UpdateConfigurationInheritance: &types.UpdateConfigurationInheritance{
			Ec2Configuration: types.InheritanceModeInheritFromAdmin,
		},
	})
	require.NoError(t, err)

	revertedOut, err := client.GetConfiguration(ctx, &inspector2sdk.GetConfigurationInput{
		AccountId: aws.String("222222222222"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.Ec2ScanModeEc2Hybrid, revertedOut.Ec2Configuration.ScanModeState.ScanMode,
		"resetting to inherit must restore the admin's configuration")
}
