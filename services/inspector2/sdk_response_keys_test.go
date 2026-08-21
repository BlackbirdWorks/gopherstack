package inspector2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	inspector2sdk "github.com/aws/aws-sdk-go-v2/service/inspector2"
	"github.com/aws/aws-sdk-go-v2/service/inspector2/types"
	"github.com/stretchr/testify/assert"
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

// TestDeleteCodeSecurityIntegration_IntegrationArn proves DeleteCodeSecurityIntegration
// returns the deleted integration's ARN rather than an empty envelope. Real
// DeleteCodeSecurityIntegrationOutput carries integrationArn
// (inspector2@v1.54.1 deserializers.go
// awsRestjson1_deserializeOpDocumentDeleteCodeSecurityIntegrationOutput) -- the
// same empty-envelope bug class as ec2's DeleteLaunchTemplate.
func TestDeleteCodeSecurityIntegration_IntegrationArn(t *testing.T) {
	t.Parallel()

	client := newRoundTripTestClient(t)
	ctx := t.Context()

	created, createErr := client.CreateCodeSecurityIntegration(
		ctx,
		&inspector2sdk.CreateCodeSecurityIntegrationInput{
			Name: aws.String("roundtrip-integration"),
			Type: types.IntegrationTypeGithub,
		},
	)
	require.NoError(t, createErr)
	require.NotEmpty(t, aws.ToString(created.IntegrationArn))

	out, deleteErr := client.DeleteCodeSecurityIntegration(
		ctx,
		&inspector2sdk.DeleteCodeSecurityIntegrationInput{
			IntegrationArn: created.IntegrationArn,
		},
	)
	require.NoError(t, deleteErr)
	require.NotEmpty(
		t, aws.ToString(out.IntegrationArn),
		"DeleteCodeSecurityIntegrationOutput.IntegrationArn must decode non-empty",
	)
	require.Equal(t, aws.ToString(created.IntegrationArn), aws.ToString(out.IntegrationArn))
}

// TestDeleteCodeSecurityScanConfiguration_ScanConfigurationArn proves
// DeleteCodeSecurityScanConfiguration returns the deleted scan
// configuration's ARN rather than an empty envelope. Real
// DeleteCodeSecurityScanConfigurationOutput carries scanConfigurationArn
// (inspector2@v1.54.1 deserializers.go
// awsRestjson1_deserializeOpDocumentDeleteCodeSecurityScanConfigurationOutput).
func TestDeleteCodeSecurityScanConfiguration_ScanConfigurationArn(t *testing.T) {
	t.Parallel()

	client := newRoundTripTestClient(t)
	ctx := t.Context()

	created, createErr := client.CreateCodeSecurityScanConfiguration(
		ctx,
		&inspector2sdk.CreateCodeSecurityScanConfigurationInput{
			Name:  aws.String("roundtrip-scan-config"),
			Level: types.ConfigurationLevelAccount,
			Configuration: &types.CodeSecurityScanConfiguration{
				RuleSetCategories: []types.RuleSetCategory{types.RuleSetCategorySast},
			},
		},
	)
	require.NoError(t, createErr)
	require.NotEmpty(t, aws.ToString(created.ScanConfigurationArn))

	out, deleteErr := client.DeleteCodeSecurityScanConfiguration(
		ctx,
		&inspector2sdk.DeleteCodeSecurityScanConfigurationInput{
			ScanConfigurationArn: created.ScanConfigurationArn,
		},
	)
	require.NoError(t, deleteErr)
	require.NotEmpty(
		t, aws.ToString(out.ScanConfigurationArn),
		"DeleteCodeSecurityScanConfigurationOutput.ScanConfigurationArn must decode non-empty",
	)
	require.Equal(t, aws.ToString(created.ScanConfigurationArn), aws.ToString(out.ScanConfigurationArn))
}

// TestBatchAssociateCodeSecurityScanConfiguration_SuccessfulAssociations
// proves the op's response carries successfulAssociations, not just
// failedAssociations. Real
// BatchAssociateCodeSecurityScanConfigurationOutput has both members
// (inspector2@v1.54.1 deserializers.go
// awsRestjson1_deserializeOpDocumentBatchAssociateCodeSecurityScanConfigurationOutput)
// -- dropping successfulAssociations left a real client's
// SuccessfulAssociations field always empty even on a clean association.
func TestBatchAssociateCodeSecurityScanConfiguration_SuccessfulAssociations(t *testing.T) {
	t.Parallel()

	client := newRoundTripTestClient(t)
	ctx := t.Context()

	created, createErr := client.CreateCodeSecurityScanConfiguration(
		ctx,
		&inspector2sdk.CreateCodeSecurityScanConfigurationInput{
			Name:  aws.String("roundtrip-assoc-config"),
			Level: types.ConfigurationLevelAccount,
			Configuration: &types.CodeSecurityScanConfiguration{
				RuleSetCategories: []types.RuleSetCategory{types.RuleSetCategorySast},
			},
		},
	)
	require.NoError(t, createErr)

	out, assocErr := client.BatchAssociateCodeSecurityScanConfiguration(
		ctx,
		&inspector2sdk.BatchAssociateCodeSecurityScanConfigurationInput{
			AssociateConfigurationRequests: []types.AssociateConfigurationRequest{
				{
					Resource:             &types.CodeSecurityResourceMemberProjectId{Value: "roundtrip-project"},
					ScanConfigurationArn: created.ScanConfigurationArn,
				},
			},
		},
	)
	require.NoError(t, assocErr)
	require.NotEmpty(
		t, out.SuccessfulAssociations,
		"BatchAssociateCodeSecurityScanConfigurationOutput.SuccessfulAssociations must decode a non-empty slice",
	)
	assert.Equal(
		t, aws.ToString(created.ScanConfigurationArn),
		aws.ToString(out.SuccessfulAssociations[0].ScanConfigurationArn),
	)
}

// TestBatchDisassociateCodeSecurityScanConfiguration_SuccessfulAssociations
// proves the op's response uses the real member names failedAssociations/
// successfulAssociations, not an invented "failedDisassociations" key. Real
// BatchDisassociateCodeSecurityScanConfigurationOutput
// (inspector2@v1.54.1 deserializers.go
// awsRestjson1_deserializeOpDocumentBatchDisassociateCodeSecurityScanConfigurationOutput)
// only recognizes failedAssociations/successfulAssociations -- the invented
// key was silently ignored by a real client.
func TestBatchDisassociateCodeSecurityScanConfiguration_SuccessfulAssociations(t *testing.T) {
	t.Parallel()

	client := newRoundTripTestClient(t)
	ctx := t.Context()

	created, createErr := client.CreateCodeSecurityScanConfiguration(
		ctx,
		&inspector2sdk.CreateCodeSecurityScanConfigurationInput{
			Name:  aws.String("roundtrip-disassoc-config"),
			Level: types.ConfigurationLevelAccount,
			Configuration: &types.CodeSecurityScanConfiguration{
				RuleSetCategories: []types.RuleSetCategory{types.RuleSetCategorySast},
			},
		},
	)
	require.NoError(t, createErr)

	_, assocErr := client.BatchAssociateCodeSecurityScanConfiguration(
		ctx,
		&inspector2sdk.BatchAssociateCodeSecurityScanConfigurationInput{
			AssociateConfigurationRequests: []types.AssociateConfigurationRequest{
				{
					Resource:             &types.CodeSecurityResourceMemberProjectId{Value: "roundtrip-project-2"},
					ScanConfigurationArn: created.ScanConfigurationArn,
				},
			},
		},
	)
	require.NoError(t, assocErr)

	out, disErr := client.BatchDisassociateCodeSecurityScanConfiguration(
		ctx,
		&inspector2sdk.BatchDisassociateCodeSecurityScanConfigurationInput{
			DisassociateConfigurationRequests: []types.DisassociateConfigurationRequest{
				{
					Resource:             &types.CodeSecurityResourceMemberProjectId{Value: "roundtrip-project-2"},
					ScanConfigurationArn: created.ScanConfigurationArn,
				},
			},
		},
	)
	require.NoError(t, disErr)
	require.NotEmpty(
		t, out.SuccessfulAssociations,
		"BatchDisassociateCodeSecurityScanConfigurationOutput.SuccessfulAssociations must decode a non-empty slice",
	)
}

// TestStartCodeSecurityScan_Status proves StartCodeSecurityScan returns a
// status alongside scanId. Real StartCodeSecurityScanOutput carries both
// scanId and status (inspector2@v1.54.1 deserializers.go
// awsRestjson1_deserializeOpDocumentStartCodeSecurityScanOutput) -- omitting
// status left a real client's Status field always empty.
//
// gopherstack-muzq (2026-08-21): IN_PROGRESS is the right status immediately
// after Start, but an assertion that stops there cannot catch a machine that
// never advances -- GetCodeSecurityScan previously never wrote Status again,
// so a client polling for readiness never exited its loop. Confirm it
// actually reaches a terminal status too.
func TestStartCodeSecurityScan_Status(t *testing.T) {
	t.Parallel()

	client := newRoundTripTestClient(t)
	ctx := t.Context()

	out, err := client.StartCodeSecurityScan(ctx, &inspector2sdk.StartCodeSecurityScanInput{
		Resource: &types.CodeSecurityResourceMemberProjectId{Value: "roundtrip-scan-project"},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(out.ScanId))
	assert.Equal(
		t, types.CodeScanStatusInProgress, out.Status,
		"StartCodeSecurityScanOutput.Status must decode non-empty",
	)

	getOut, getErr := client.GetCodeSecurityScan(ctx, &inspector2sdk.GetCodeSecurityScanInput{
		Resource: &types.CodeSecurityResourceMemberProjectId{Value: "roundtrip-scan-project"},
		ScanId:   out.ScanId,
	})
	require.NoError(t, getErr)
	assert.Equal(
		t, types.CodeScanStatusSuccessful, getOut.Status,
		"GetCodeSecurityScan must reap IN_PROGRESS to SUCCESSFUL on poll",
	)
}

// TestUpdateOrganizationConfiguration_AutoEnable proves
// UpdateOrganizationConfiguration returns the resulting autoEnable settings
// rather than an empty envelope. Real UpdateOrganizationConfigurationOutput
// carries autoEnable (inspector2@v1.54.1 deserializers.go
// awsRestjson1_deserializeOpDocumentUpdateOrganizationConfigurationOutput).
func TestUpdateOrganizationConfiguration_AutoEnable(t *testing.T) {
	t.Parallel()

	client := newRoundTripTestClient(t)
	ctx := t.Context()

	out, err := client.UpdateOrganizationConfiguration(ctx, &inspector2sdk.UpdateOrganizationConfigurationInput{
		AutoEnable: &types.AutoEnable{
			Ec2: aws.Bool(true),
			Ecr: aws.Bool(true),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out.AutoEnable, "UpdateOrganizationConfigurationOutput.AutoEnable must decode non-nil")
	assert.True(t, aws.ToBool(out.AutoEnable.Ec2))
}
