package inspector2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	inspector2sdk "github.com/aws/aws-sdk-go-v2/service/inspector2"
	"github.com/aws/aws-sdk-go-v2/service/inspector2/types"
	"github.com/stretchr/testify/require"
)

// TestListCisScans_Scans proves ListCisScans decodes through the real SDK
// client. The handler wrapped its list under "cisScans" -- the real key
// (inspector2@v1.54.1 deserializers.go
// awsRestjson1_deserializeOpDocumentListCisScansOutput) is "scans". The SDK
// deserializer's switch never matched "cisScans", so Scans silently decoded
// nil with err == nil.
func TestListCisScans_Scans(t *testing.T) {
	t.Parallel()

	client := newRoundTripTestClient(t)
	ctx := t.Context()

	_, createErr := client.CreateCisScanConfiguration(ctx, &inspector2sdk.CreateCisScanConfigurationInput{
		ScanName:      aws.String("roundtrip-scan"),
		SecurityLevel: types.CisSecurityLevelLevel1,
		Schedule:      &types.ScheduleMemberOneTime{Value: types.OneTimeSchedule{}},
		Targets: &types.CreateCisTargets{
			AccountIds:         []string{rtTestAccountID},
			TargetResourceTags: map[string][]string{},
		},
	})
	require.NoError(t, createErr)

	listOut, listErr := client.ListCisScans(ctx, &inspector2sdk.ListCisScansInput{})
	require.NoError(t, listErr)
	require.NotEmpty(t, listOut.Scans, "ListCisScansOutput.Scans must decode a non-empty slice")
	require.NotEmpty(t, aws.ToString(listOut.Scans[0].ScanArn))
}

// TestBatchGetMemberEc2DeepInspectionStatus_AccountIds proves the op decodes
// through the real SDK client. The handler wrapped its list under "members"
// -- the real key (inspector2@v1.54.1 deserializers.go
// awsRestjson1_deserializeOpDocumentBatchGetMemberEc2DeepInspectionStatusOutput)
// is "accountIds", despite the field holding per-account status objects, not
// bare account ID strings. The SDK deserializer's switch never matched
// "members", so AccountIds silently decoded nil with err == nil.
func TestBatchGetMemberEc2DeepInspectionStatus_AccountIds(t *testing.T) {
	t.Parallel()

	client := newRoundTripTestClient(t)
	ctx := t.Context()

	out, err := client.BatchGetMemberEc2DeepInspectionStatus(
		ctx,
		&inspector2sdk.BatchGetMemberEc2DeepInspectionStatusInput{
			AccountIds: []string{"555555555555"},
		},
	)
	require.NoError(t, err)
	require.NotEmpty(
		t, out.AccountIds,
		"BatchGetMemberEc2DeepInspectionStatusOutput.AccountIds must decode a non-empty slice",
	)
	require.Equal(t, "555555555555", aws.ToString(out.AccountIds[0].AccountId))
}

// TestBatchUpdateMemberEc2DeepInspectionStatus_AccountIds proves the op
// decodes through the real SDK client. The handler wrapped its list under
// "accounts" -- the real key (inspector2@v1.54.1 deserializers.go
// awsRestjson1_deserializeOpDocumentBatchUpdateMemberEc2DeepInspectionStatusOutput)
// is "accountIds". The SDK deserializer's switch never matched "accounts", so
// AccountIds silently decoded nil with err == nil.
func TestBatchUpdateMemberEc2DeepInspectionStatus_AccountIds(t *testing.T) {
	t.Parallel()

	client := newRoundTripTestClient(t)
	ctx := t.Context()

	out, err := client.BatchUpdateMemberEc2DeepInspectionStatus(
		ctx,
		&inspector2sdk.BatchUpdateMemberEc2DeepInspectionStatusInput{
			AccountIds: []types.MemberAccountEc2DeepInspectionStatus{
				{AccountId: aws.String("555555555555"), ActivateDeepInspection: aws.Bool(true)},
			},
		},
	)
	require.NoError(t, err)
	require.NotEmpty(
		t, out.AccountIds,
		"BatchUpdateMemberEc2DeepInspectionStatusOutput.AccountIds must decode a non-empty slice",
	)
	require.Equal(t, "555555555555", aws.ToString(out.AccountIds[0].AccountId))
}
