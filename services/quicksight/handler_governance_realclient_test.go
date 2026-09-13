package quicksight_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	quicksightsdk "github.com/aws/aws-sdk-go-v2/service/quicksight"
	"github.com/aws/aws-sdk-go-v2/service/quicksight/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

// TestApprovalPolicy_RealClient drives a full Create/Describe/Update/List/
// Delete lifecycle through the real aws-sdk-go-v2 quicksight client, proving
// the wire shapes (the non-standard "/governance/approvalworkflows/..." path
// with no AwsAccountId, the "Policy"/"Policies" response envelope, and the
// epoch-seconds CreatedAt/UpdatedAt) against the SDK's own serializer and
// deserializer rather than a hand-built fixture.
func TestApprovalPolicy_RealClient(t *testing.T) {
	t.Parallel()

	backend := quicksight.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestQuickSightClient(t, quicksight.NewHandler(backend))
	ctx := t.Context()

	created, err := client.CreateApprovalPolicy(ctx, &quicksightsdk.CreateApprovalPolicyInput{
		PolicyId:       aws.String("rt-policy"),
		Name:           aws.String("Real Client Policy"),
		Description:    aws.String("created via the real SDK client"),
		Actions:        []types.GovernedAction{types.GovernedActionShare},
		AssetTypes:     []types.AssetType{types.AssetTypeAgent},
		ApprovalGroups: []string{"arn:aws:quicksight:us-east-1:000000000000:group/default/approvers"},
		ApplicableTo: &types.ApplicableTo{
			Type:      types.ApplicableToTypeGroup,
			GroupArns: []string{"arn:aws:quicksight:us-east-1:000000000000:group/default/eng"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created.Policy)
	assert.Equal(t, "rt-policy", aws.ToString(created.Policy.PolicyId))
	assert.Contains(t, aws.ToString(created.Policy.PolicyArn), "approval-policy/rt-policy")
	assert.False(t, aws.ToTime(created.Policy.CreatedAt).IsZero())

	described, err := client.DescribeApprovalPolicy(ctx, &quicksightsdk.DescribeApprovalPolicyInput{
		PolicyId: aws.String("rt-policy"),
	})
	require.NoError(t, err)
	require.NotNil(t, described.Policy)
	assert.Equal(t, "Real Client Policy", aws.ToString(described.Policy.Name))
	require.Len(t, described.Policy.Actions, 1)
	assert.Equal(t, types.GovernedActionShare, described.Policy.Actions[0])
	require.NotNil(t, described.Policy.ApplicableTo)
	assert.Equal(t, types.ApplicableToTypeGroup, described.Policy.ApplicableTo.Type)

	updated, err := client.UpdateApprovalPolicy(ctx, &quicksightsdk.UpdateApprovalPolicyInput{
		PolicyId: aws.String("rt-policy"),
		Name:     aws.String("Renamed via SDK"),
	})
	require.NoError(t, err)
	require.NotNil(t, updated.Policy)
	assert.Equal(t, "Renamed via SDK", aws.ToString(updated.Policy.Name))

	listed, err := client.ListApprovalPolicies(ctx, &quicksightsdk.ListApprovalPoliciesInput{})
	require.NoError(t, err)
	require.Len(t, listed.Policies, 1)
	assert.Equal(t, "rt-policy", aws.ToString(listed.Policies[0].PolicyId))

	_, err = client.DeleteApprovalPolicy(ctx, &quicksightsdk.DeleteApprovalPolicyInput{
		PolicyId: aws.String("rt-policy"),
	})
	require.NoError(t, err)

	_, err = client.DescribeApprovalPolicy(ctx, &quicksightsdk.DescribeApprovalPolicyInput{
		PolicyId: aws.String("rt-policy"),
	})
	require.Error(t, err)
	var rnf *types.ResourceNotFoundException
	require.ErrorAs(t, err, &rnf, "expected a real ResourceNotFoundException from the SDK deserializer")
}

// TestDlpSetting_RealClient drives a full Create/Describe/Update/List/Delete
// lifecycle through the real SDK client, proving the ProviderConfig union
// serialization/deserialization ("MicrosoftPurview" member) and the
// ResourceExistsException duplicate-create error round-trip.
func TestDlpSetting_RealClient(t *testing.T) {
	t.Parallel()

	backend := quicksight.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestQuickSightClient(t, quicksight.NewHandler(backend))
	ctx := t.Context()

	providerConfig := &types.ProviderConfigMemberMicrosoftPurview{
		Value: types.MicrosoftPurviewProviderConfig{
			Credentials: &types.MicrosoftPurviewCredentials{
				SecretArn: aws.String("arn:aws:secretsmanager:us-east-1:000000000000:secret:purview-xyz"),
			},
			LabelActionMappings: []types.LabelActionMapping{
				{Action: types.DlpActionBlock, LabelId: aws.String("lbl1"), LabelName: aws.String("Confidential")},
			},
			UnmappedAction: types.DlpActionWarn,
		},
	}

	created, err := client.CreateDlpSetting(ctx, &quicksightsdk.CreateDlpSettingInput{
		AwsAccountId:         aws.String("000000000000"),
		DlpSettingId:         aws.String("rt-dlp"),
		Name:                 aws.String("Real Client DLP Setting"),
		Enabled:              true,
		ProviderType:         types.DlpProviderTypeMicrosoftPurview,
		ProviderOutageAction: types.DlpActionBlock,
		ProviderConfig:       providerConfig,
	})
	require.NoError(t, err)
	assert.Equal(t, "rt-dlp", aws.ToString(created.DlpSettingId))
	assert.Contains(t, aws.ToString(created.Arn), "dlp-setting/rt-dlp")

	_, err = client.CreateDlpSetting(ctx, &quicksightsdk.CreateDlpSettingInput{
		AwsAccountId:         aws.String("000000000000"),
		DlpSettingId:         aws.String("rt-dlp"),
		Name:                 aws.String("duplicate"),
		Enabled:              true,
		ProviderType:         types.DlpProviderTypeMicrosoftPurview,
		ProviderOutageAction: types.DlpActionBlock,
		ProviderConfig:       providerConfig,
	})
	require.Error(t, err)
	var existsErr *types.ResourceExistsException
	require.ErrorAs(t, err, &existsErr, "expected a real ResourceExistsException from the SDK deserializer")

	described, err := client.DescribeDlpSetting(ctx, &quicksightsdk.DescribeDlpSettingInput{
		AwsAccountId: aws.String("000000000000"),
		DlpSettingId: aws.String("rt-dlp"),
	})
	require.NoError(t, err)
	require.NotNil(t, described.DlpSetting)
	assert.Equal(t, types.DlpSettingStatusActive, described.DlpSetting.Status)
	mp, ok := described.DlpSetting.ProviderConfig.(*types.ProviderConfigMemberMicrosoftPurview)
	require.True(t, ok, "ProviderConfig must round-trip as the MicrosoftPurview union variant")
	assert.Equal(
		t,
		"arn:aws:secretsmanager:us-east-1:000000000000:secret:purview-xyz",
		aws.ToString(mp.Value.Credentials.SecretArn),
	)

	updated, err := client.UpdateDlpSetting(ctx, &quicksightsdk.UpdateDlpSettingInput{
		AwsAccountId: aws.String("000000000000"),
		DlpSettingId: aws.String("rt-dlp"),
		Enabled:      aws.Bool(false),
	})
	require.NoError(t, err)
	assert.Equal(t, "rt-dlp", aws.ToString(updated.DlpSettingId))

	describedAfterUpdate, err := client.DescribeDlpSetting(ctx, &quicksightsdk.DescribeDlpSettingInput{
		AwsAccountId: aws.String("000000000000"),
		DlpSettingId: aws.String("rt-dlp"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.DlpSettingStatusInactive, describedAfterUpdate.DlpSetting.Status)

	listed, err := client.ListDlpSettings(ctx, &quicksightsdk.ListDlpSettingsInput{
		AwsAccountId: aws.String("000000000000"),
	})
	require.NoError(t, err)
	require.Len(t, listed.DlpSettingSummaries, 1)
	assert.Equal(t, "rt-dlp", aws.ToString(listed.DlpSettingSummaries[0].DlpSettingId))

	_, err = client.DeleteDlpSetting(ctx, &quicksightsdk.DeleteDlpSettingInput{
		AwsAccountId: aws.String("000000000000"),
		DlpSettingId: aws.String("rt-dlp"),
	})
	require.NoError(t, err)

	_, err = client.DescribeDlpSetting(ctx, &quicksightsdk.DescribeDlpSettingInput{
		AwsAccountId: aws.String("000000000000"),
		DlpSettingId: aws.String("rt-dlp"),
	})
	require.Error(t, err)
	var rnf *types.ResourceNotFoundException
	require.ErrorAs(t, err, &rnf, "expected a real ResourceNotFoundException from the SDK deserializer")
}

// TestLimitsProfile_RealClient drives a full Create/Describe/Update/List/
// Delete lifecycle through the real SDK client, proving the
// "/governance/limits/accounts/{accountId}/profiles" path, the server-
// generated ProfileId, and the lowercase-keyed response envelope
// ("profile"/"profiles"/"arn"/"profileId", unlike ApprovalPolicy and
// DlpSetting's PascalCase).
func TestLimitsProfile_RealClient(t *testing.T) {
	t.Parallel()

	backend := quicksight.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestQuickSightClient(t, quicksight.NewHandler(backend))
	ctx := t.Context()

	created, err := client.CreateLimitsProfile(ctx, &quicksightsdk.CreateLimitsProfileInput{
		AccountId:   aws.String("000000000000"),
		ClientToken: aws.String("rt-token"),
		ProfileName: aws.String("Real Client Limits Profile"),
		ResourceLimits: map[string]types.ProfileLimitValue{
			"INDEX_STORAGE": {MaxValue: aws.Int64(100), Unit: types.LimitUnitGb},
		},
	})
	require.NoError(t, err)
	profileID := aws.ToString(created.ProfileId)
	require.NotEmpty(t, profileID)
	assert.Contains(t, aws.ToString(created.Arn), "limits-profile/"+profileID)

	described, err := client.DescribeLimitsProfile(ctx, &quicksightsdk.DescribeLimitsProfileInput{
		AccountId: aws.String("000000000000"),
		ProfileId: aws.String(profileID),
	})
	require.NoError(t, err)
	require.NotNil(t, described.Profile)
	assert.Equal(t, "Real Client Limits Profile", aws.ToString(described.Profile.ProfileName))
	require.Contains(t, described.Profile.ResourceLimits, "INDEX_STORAGE")
	assert.Equal(t, int64(100), aws.ToInt64(described.Profile.ResourceLimits["INDEX_STORAGE"].MaxValue))
	assert.Equal(t, types.LimitUnitGb, described.Profile.ResourceLimits["INDEX_STORAGE"].Unit)

	updated, err := client.UpdateLimitsProfile(ctx, &quicksightsdk.UpdateLimitsProfileInput{
		AccountId:   aws.String("000000000000"),
		ProfileId:   aws.String(profileID),
		ProfileName: aws.String("Renamed Limits Profile"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.Arn), aws.ToString(updated.Arn))

	describedAfterUpdate, err := client.DescribeLimitsProfile(ctx, &quicksightsdk.DescribeLimitsProfileInput{
		AccountId: aws.String("000000000000"),
		ProfileId: aws.String(profileID),
	})
	require.NoError(t, err)
	assert.Equal(t, "Renamed Limits Profile", aws.ToString(describedAfterUpdate.Profile.ProfileName))

	listed, err := client.ListLimitsProfiles(ctx, &quicksightsdk.ListLimitsProfilesInput{
		AccountId: aws.String("000000000000"),
	})
	require.NoError(t, err)
	require.Len(t, listed.Profiles, 1)
	assert.Equal(t, profileID, aws.ToString(listed.Profiles[0].ProfileId))

	_, err = client.DeleteLimitsProfile(ctx, &quicksightsdk.DeleteLimitsProfileInput{
		AccountId: aws.String("000000000000"),
		ProfileId: aws.String(profileID),
	})
	require.NoError(t, err)

	_, err = client.DescribeLimitsProfile(ctx, &quicksightsdk.DescribeLimitsProfileInput{
		AccountId: aws.String("000000000000"),
		ProfileId: aws.String(profileID),
	})
	require.Error(t, err)
	var rnf *types.ResourceNotFoundException
	require.ErrorAs(t, err, &rnf, "expected a real ResourceNotFoundException from the SDK deserializer")
}
