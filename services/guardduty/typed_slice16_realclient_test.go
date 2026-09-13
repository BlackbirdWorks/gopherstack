package guardduty_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	guarddutysdk "github.com/aws/aws-sdk-go-v2/service/guardduty"
	"github.com/aws/aws-sdk-go-v2/service/guardduty/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/guardduty"
)

// newSlice16GuardDutyClient stands up a fresh backend/handler/client triple
// for gopherstack-n3zi typed slice 16.
func newSlice16GuardDutyClient(t *testing.T) *guarddutysdk.Client {
	t.Helper()

	h := guardduty.NewHandler(guardduty.NewInMemoryBackend("000000000000", "us-east-1"))

	return newTestGuardDutyClient(t, h)
}

// slice16CreateDetector creates an enabled detector via the real client and
// returns its ID.
func slice16CreateDetector(t *testing.T, client *guarddutysdk.Client) string {
	t.Helper()

	created, err := client.CreateDetector(t.Context(), &guarddutysdk.CreateDetectorInput{
		Enable: aws.Bool(true),
		Features: []types.DetectorFeatureConfiguration{
			{Name: types.DetectorFeatureAiAnalyst, Status: types.FeatureStatusEnabled},
		},
	})
	require.NoError(t, err)
	detectorID := aws.ToString(created.DetectorId)
	require.NotEmpty(t, detectorID)

	return detectorID
}

// TestTypedSlice16GuardDutyRealClient drives every op the census still
// listed as uncovered before this pass (gopherstack-n3zi typed-client
// coverage slice 16).
func TestTypedSlice16GuardDutyRealClient(t *testing.T) {
	t.Parallel()

	t.Run("detector_and_org_family", func(t *testing.T) {
		t.Parallel()

		client := newSlice16GuardDutyClient(t)
		detectorID := slice16CreateDetector(t, client)

		_, err := client.UpdateDetector(t.Context(), &guarddutysdk.UpdateDetectorInput{
			DetectorId: aws.String(detectorID),
			Enable:     aws.Bool(false),
		})
		require.NoError(t, err)

		got, err := client.GetDetector(t.Context(), &guarddutysdk.GetDetectorInput{
			DetectorId: aws.String(detectorID),
		})
		require.NoError(t, err)
		assert.Equal(t, types.DetectorStatusDisabled, got.Status)

		_, err = client.EnableOrganizationAdminAccount(
			t.Context(),
			&guarddutysdk.EnableOrganizationAdminAccountInput{
				AdminAccountId: aws.String("111111111111"),
			},
		)
		require.NoError(t, err)

		_, err = client.DisableOrganizationAdminAccount(
			t.Context(),
			&guarddutysdk.DisableOrganizationAdminAccountInput{
				AdminAccountId: aws.String("111111111111"),
			},
		)
		require.NoError(t, err)

		stats, err := client.GetOrganizationStatistics(
			t.Context(),
			&guarddutysdk.GetOrganizationStatisticsInput{},
		)
		require.NoError(t, err)
		_ = stats
	})

	t.Run("findings_family", func(t *testing.T) {
		t.Parallel()

		client := newSlice16GuardDutyClient(t)
		detectorID := slice16CreateDetector(t, client)

		_, err := client.CreateSampleFindings(t.Context(), &guarddutysdk.CreateSampleFindingsInput{
			DetectorId: aws.String(detectorID),
		})
		require.NoError(t, err)

		listed, err := client.ListFindings(t.Context(), &guarddutysdk.ListFindingsInput{
			DetectorId: aws.String(detectorID),
		})
		require.NoError(t, err)
		require.NotEmpty(t, listed.FindingIds)
		findingID := listed.FindingIds[0]

		got, err := client.GetFindings(t.Context(), &guarddutysdk.GetFindingsInput{
			DetectorId: aws.String(detectorID),
			FindingIds: []string{findingID},
		})
		require.NoError(t, err)
		require.Len(t, got.Findings, 1)
		assert.Equal(t, findingID, aws.ToString(got.Findings[0].Id))

		_, err = client.UpdateFindingsFeedback(t.Context(), &guarddutysdk.UpdateFindingsFeedbackInput{
			DetectorId: aws.String(detectorID),
			FindingIds: []string{findingID},
			Feedback:   types.FeedbackUseful,
		})
		require.NoError(t, err)

		_, err = client.ArchiveFindings(t.Context(), &guarddutysdk.ArchiveFindingsInput{
			DetectorId: aws.String(detectorID),
			FindingIds: []string{findingID},
		})
		require.NoError(t, err)

		_, err = client.UnarchiveFindings(t.Context(), &guarddutysdk.UnarchiveFindingsInput{
			DetectorId: aws.String(detectorID),
			FindingIds: []string{findingID},
		})
		require.NoError(t, err)
	})

	t.Run("ip_threatintel_delete_family", func(t *testing.T) {
		t.Parallel()

		client := newSlice16GuardDutyClient(t)
		detectorID := slice16CreateDetector(t, client)

		ipSet, err := client.CreateIPSet(t.Context(), &guarddutysdk.CreateIPSetInput{
			DetectorId: aws.String(detectorID),
			Name:       aws.String("del-ipset"),
			Format:     types.IpSetFormatTxt,
			Location:   aws.String("s3://bucket/ipset.txt"),
			Activate:   aws.Bool(false),
		})
		require.NoError(t, err)

		_, err = client.DeleteIPSet(t.Context(), &guarddutysdk.DeleteIPSetInput{
			DetectorId: aws.String(detectorID),
			IpSetId:    ipSet.IpSetId,
		})
		require.NoError(t, err)

		tiSet, err := client.CreateThreatIntelSet(t.Context(), &guarddutysdk.CreateThreatIntelSetInput{
			DetectorId: aws.String(detectorID),
			Name:       aws.String("del-tiset"),
			Format:     types.ThreatIntelSetFormatTxt,
			Location:   aws.String("s3://bucket/tiset.txt"),
			Activate:   aws.Bool(false),
		})
		require.NoError(t, err)

		_, err = client.DeleteThreatIntelSet(t.Context(), &guarddutysdk.DeleteThreatIntelSetInput{
			DetectorId:       aws.String(detectorID),
			ThreatIntelSetId: tiSet.ThreatIntelSetId,
		})
		require.NoError(t, err)
	})

	t.Run("entity_sets_family", func(t *testing.T) {
		t.Parallel()

		client := newSlice16GuardDutyClient(t)
		detectorID := slice16CreateDetector(t, client)

		teSet, err := client.CreateThreatEntitySet(t.Context(), &guarddutysdk.CreateThreatEntitySetInput{
			DetectorId: aws.String(detectorID),
			Name:       aws.String("entity-teset"),
			Format:     types.ThreatEntitySetFormatTxt,
			Location:   aws.String("s3://bucket/teset.txt"),
			Activate:   aws.Bool(false),
		})
		require.NoError(t, err)
		teSetID := aws.ToString(teSet.ThreatEntitySetId)

		gotTE, err := client.GetThreatEntitySet(t.Context(), &guarddutysdk.GetThreatEntitySetInput{
			DetectorId:        aws.String(detectorID),
			ThreatEntitySetId: aws.String(teSetID),
		})
		require.NoError(t, err)
		assert.Equal(t, "entity-teset", aws.ToString(gotTE.Name))

		_, err = client.UpdateThreatEntitySet(t.Context(), &guarddutysdk.UpdateThreatEntitySetInput{
			DetectorId:        aws.String(detectorID),
			ThreatEntitySetId: aws.String(teSetID),
			Name:              aws.String("entity-teset-renamed"),
		})
		require.NoError(t, err)

		gotTE, err = client.GetThreatEntitySet(t.Context(), &guarddutysdk.GetThreatEntitySetInput{
			DetectorId:        aws.String(detectorID),
			ThreatEntitySetId: aws.String(teSetID),
		})
		require.NoError(t, err)
		assert.Equal(t, "entity-teset-renamed", aws.ToString(gotTE.Name))

		_, err = client.DeleteThreatEntitySet(t.Context(), &guarddutysdk.DeleteThreatEntitySetInput{
			DetectorId:        aws.String(detectorID),
			ThreatEntitySetId: aws.String(teSetID),
		})
		require.NoError(t, err)

		tseSet, err := client.CreateTrustedEntitySet(t.Context(), &guarddutysdk.CreateTrustedEntitySetInput{
			DetectorId: aws.String(detectorID),
			Name:       aws.String("entity-tseset"),
			Format:     types.TrustedEntitySetFormatTxt,
			Location:   aws.String("s3://bucket/tseset.txt"),
			Activate:   aws.Bool(false),
		})
		require.NoError(t, err)
		tseSetID := aws.ToString(tseSet.TrustedEntitySetId)

		gotTSE, err := client.GetTrustedEntitySet(t.Context(), &guarddutysdk.GetTrustedEntitySetInput{
			DetectorId:         aws.String(detectorID),
			TrustedEntitySetId: aws.String(tseSetID),
		})
		require.NoError(t, err)
		assert.Equal(t, "entity-tseset", aws.ToString(gotTSE.Name))

		_, err = client.UpdateTrustedEntitySet(t.Context(), &guarddutysdk.UpdateTrustedEntitySetInput{
			DetectorId:         aws.String(detectorID),
			TrustedEntitySetId: aws.String(tseSetID),
			Name:               aws.String("entity-tseset-renamed"),
		})
		require.NoError(t, err)

		gotTSE, err = client.GetTrustedEntitySet(t.Context(), &guarddutysdk.GetTrustedEntitySetInput{
			DetectorId:         aws.String(detectorID),
			TrustedEntitySetId: aws.String(tseSetID),
		})
		require.NoError(t, err)
		assert.Equal(t, "entity-tseset-renamed", aws.ToString(gotTSE.Name))

		_, err = client.DeleteTrustedEntitySet(t.Context(), &guarddutysdk.DeleteTrustedEntitySetInput{
			DetectorId:         aws.String(detectorID),
			TrustedEntitySetId: aws.String(tseSetID),
		})
		require.NoError(t, err)
	})

	t.Run("publishing_destination_family", func(t *testing.T) {
		t.Parallel()

		client := newSlice16GuardDutyClient(t)
		detectorID := slice16CreateDetector(t, client)

		created, err := client.CreatePublishingDestination(
			t.Context(),
			&guarddutysdk.CreatePublishingDestinationInput{
				DetectorId:      aws.String(detectorID),
				DestinationType: types.DestinationTypeS3,
				DestinationProperties: &types.DestinationProperties{
					DestinationArn: aws.String("arn:aws:s3:::pubdest-bucket"),
					KmsKeyArn:      aws.String("arn:aws:kms:us-east-1:000000000000:key/pubdest-key"),
				},
			},
		)
		require.NoError(t, err)
		destID := aws.ToString(created.DestinationId)
		require.NotEmpty(t, destID)

		described, err := client.DescribePublishingDestination(
			t.Context(),
			&guarddutysdk.DescribePublishingDestinationInput{
				DetectorId:    aws.String(detectorID),
				DestinationId: aws.String(destID),
			},
		)
		require.NoError(t, err)
		assert.Equal(t, destID, aws.ToString(described.DestinationId))

		_, err = client.UpdatePublishingDestination(
			t.Context(),
			&guarddutysdk.UpdatePublishingDestinationInput{
				DetectorId:    aws.String(detectorID),
				DestinationId: aws.String(destID),
				DestinationProperties: &types.DestinationProperties{
					DestinationArn: aws.String("arn:aws:s3:::pubdest-bucket-2"),
					KmsKeyArn:      aws.String("arn:aws:kms:us-east-1:000000000000:key/pubdest-key"),
				},
			},
		)
		require.NoError(t, err)

		_, err = client.DeletePublishingDestination(
			t.Context(),
			&guarddutysdk.DeletePublishingDestinationInput{
				DetectorId:    aws.String(detectorID),
				DestinationId: aws.String(destID),
			},
		)
		require.NoError(t, err)

		_, err = client.DescribePublishingDestination(
			t.Context(),
			&guarddutysdk.DescribePublishingDestinationInput{
				DetectorId:    aws.String(detectorID),
				DestinationId: aws.String(destID),
			},
		)
		require.Error(t, err)
	})

	t.Run("malware_protection_family", func(t *testing.T) {
		t.Parallel()

		client := newSlice16GuardDutyClient(t)
		detectorID := slice16CreateDetector(t, client)

		started, err := client.StartMalwareScan(t.Context(), &guarddutysdk.StartMalwareScanInput{
			ResourceArn: aws.String("arn:aws:ec2:us-east-1:000000000000:instance/i-0123456789abcdef0"),
		})
		require.NoError(t, err)
		scanID := aws.ToString(started.ScanId)
		require.NotEmpty(t, scanID)

		gotScan, err := client.GetMalwareScan(t.Context(), &guarddutysdk.GetMalwareScanInput{
			ScanId: aws.String(scanID),
		})
		require.NoError(t, err)
		assert.Equal(t,
			"arn:aws:ec2:us-east-1:000000000000:instance/i-0123456789abcdef0",
			aws.ToString(gotScan.ResourceArn),
		)
		assert.Equal(t, types.MalwareProtectionResourceTypeEc2Instance, gotScan.ResourceType)

		listed, err := client.ListMalwareScans(t.Context(), &guarddutysdk.ListMalwareScansInput{})
		require.NoError(t, err)
		assert.NotEmpty(t, listed.Scans)

		settings, err := client.GetMalwareScanSettings(t.Context(), &guarddutysdk.GetMalwareScanSettingsInput{
			DetectorId: aws.String(detectorID),
		})
		require.NoError(t, err)
		assert.Equal(t, types.EbsSnapshotPreservationNoRetention, settings.EbsSnapshotPreservation)

		_, err = client.UpdateMalwareScanSettings(t.Context(), &guarddutysdk.UpdateMalwareScanSettingsInput{
			DetectorId:              aws.String(detectorID),
			EbsSnapshotPreservation: types.EbsSnapshotPreservationRetentionWithFinding,
		})
		require.NoError(t, err)

		settings, err = client.GetMalwareScanSettings(t.Context(), &guarddutysdk.GetMalwareScanSettingsInput{
			DetectorId: aws.String(detectorID),
		})
		require.NoError(t, err)
		assert.Equal(t, types.EbsSnapshotPreservationRetentionWithFinding, settings.EbsSnapshotPreservation)

		_, err = client.SendObjectMalwareScan(t.Context(), &guarddutysdk.SendObjectMalwareScanInput{
			S3Object: &types.S3ObjectForSendObjectMalwareScan{
				Bucket: aws.String("scan-bucket"),
				Key:    aws.String("scan-object-key"),
			},
		})
		require.NoError(t, err)

		plan, err := client.CreateMalwareProtectionPlan(
			t.Context(),
			&guarddutysdk.CreateMalwareProtectionPlanInput{
				Role: aws.String("arn:aws:iam::000000000000:role/malware-role"),
				ProtectedResource: &types.CreateProtectedResource{
					S3Bucket: &types.CreateS3BucketResource{BucketName: aws.String("plan-bucket")},
				},
			},
		)
		require.NoError(t, err)
		planID := aws.ToString(plan.MalwareProtectionPlanId)
		require.NotEmpty(t, planID)

		gotPlan, err := client.GetMalwareProtectionPlan(
			t.Context(),
			&guarddutysdk.GetMalwareProtectionPlanInput{MalwareProtectionPlanId: aws.String(planID)},
		)
		require.NoError(t, err)
		assert.Equal(t, "arn:aws:iam::000000000000:role/malware-role", aws.ToString(gotPlan.Role))

		_, err = client.UpdateMalwareProtectionPlan(
			t.Context(),
			&guarddutysdk.UpdateMalwareProtectionPlanInput{
				MalwareProtectionPlanId: aws.String(planID),
				Actions: &types.MalwareProtectionPlanActions{
					Tagging: &types.MalwareProtectionPlanTaggingAction{
						Status: types.MalwareProtectionPlanTaggingActionStatusEnabled,
					},
				},
			},
		)
		require.NoError(t, err)

		_, err = client.DeleteMalwareProtectionPlan(
			t.Context(),
			&guarddutysdk.DeleteMalwareProtectionPlanInput{MalwareProtectionPlanId: aws.String(planID)},
		)
		require.NoError(t, err)
	})

	t.Run("members_family", func(t *testing.T) {
		t.Parallel()

		client := newSlice16GuardDutyClient(t)
		detectorID := slice16CreateDetector(t, client)

		const memberAccountID = "222222222222"

		_, err := client.CreateMembers(t.Context(), &guarddutysdk.CreateMembersInput{
			DetectorId: aws.String(detectorID),
			AccountDetails: []types.AccountDetail{
				{AccountId: aws.String(memberAccountID), Email: aws.String("member@example.com")},
			},
		})
		require.NoError(t, err)

		invited, err := client.InviteMembers(t.Context(), &guarddutysdk.InviteMembersInput{
			DetectorId: aws.String(detectorID),
			AccountIds: []string{memberAccountID},
		})
		require.NoError(t, err)
		assert.Empty(t, invited.UnprocessedAccounts)

		gotMembers, err := client.GetMembers(t.Context(), &guarddutysdk.GetMembersInput{
			DetectorId: aws.String(detectorID),
			AccountIds: []string{memberAccountID},
		})
		require.NoError(t, err)
		require.Len(t, gotMembers.Members, 1)
		assert.Equal(t, memberAccountID, aws.ToString(gotMembers.Members[0].AccountId))

		_, err = client.StartMonitoringMembers(t.Context(), &guarddutysdk.StartMonitoringMembersInput{
			DetectorId: aws.String(detectorID),
			AccountIds: []string{memberAccountID},
		})
		require.NoError(t, err)

		_, err = client.UpdateMemberDetectors(t.Context(), &guarddutysdk.UpdateMemberDetectorsInput{
			DetectorId: aws.String(detectorID),
			AccountIds: []string{memberAccountID},
		})
		require.NoError(t, err)

		_, err = client.StopMonitoringMembers(t.Context(), &guarddutysdk.StopMonitoringMembersInput{
			DetectorId: aws.String(detectorID),
			AccountIds: []string{memberAccountID},
		})
		require.NoError(t, err)

		_, err = client.DisassociateMembers(t.Context(), &guarddutysdk.DisassociateMembersInput{
			DetectorId: aws.String(detectorID),
			AccountIds: []string{memberAccountID},
		})
		require.NoError(t, err)

		_, err = client.DeleteMembers(t.Context(), &guarddutysdk.DeleteMembersInput{
			DetectorId: aws.String(detectorID),
			AccountIds: []string{memberAccountID},
		})
		require.NoError(t, err)
	})

	t.Run("invitations_admin_family", func(t *testing.T) {
		t.Parallel()

		client := newSlice16GuardDutyClient(t)
		detectorID := slice16CreateDetector(t, client)

		const invitingAccountID = "333333333333"

		_, err := client.InviteMembers(t.Context(), &guarddutysdk.InviteMembersInput{
			DetectorId: aws.String(detectorID),
			AccountIds: []string{invitingAccountID},
		})
		require.NoError(t, err)

		count, err := client.GetInvitationsCount(t.Context(), &guarddutysdk.GetInvitationsCountInput{})
		require.NoError(t, err)
		require.NotNil(t, count.InvitationsCount)
		assert.GreaterOrEqual(t, *count.InvitationsCount, int32(1))

		_, err = client.AcceptAdministratorInvitation(
			t.Context(),
			&guarddutysdk.AcceptAdministratorInvitationInput{
				DetectorId:      aws.String(detectorID),
				AdministratorId: aws.String(invitingAccountID),
				InvitationId:    aws.String("invite-admin-1"),
			},
		)
		require.NoError(t, err)

		gotAdmin, err := client.GetAdministratorAccount(
			t.Context(),
			&guarddutysdk.GetAdministratorAccountInput{DetectorId: aws.String(detectorID)},
		)
		require.NoError(t, err)
		require.NotNil(t, gotAdmin.Administrator)
		assert.Equal(t, invitingAccountID, aws.ToString(gotAdmin.Administrator.AccountId))

		_, err = client.DisassociateFromAdministratorAccount(
			t.Context(),
			&guarddutysdk.DisassociateFromAdministratorAccountInput{DetectorId: aws.String(detectorID)},
		)
		require.NoError(t, err)

		//nolint:staticcheck // legacy op still in the uncovered census; must be driven, not skipped
		_, err = client.AcceptInvitation(t.Context(), &guarddutysdk.AcceptInvitationInput{
			DetectorId:   aws.String(detectorID),
			MasterId:     aws.String(invitingAccountID),
			InvitationId: aws.String("invite-master-1"),
		})
		require.NoError(t, err)

		//nolint:staticcheck // legacy op still in the uncovered census; must be driven, not skipped
		gotMaster, err := client.GetMasterAccount(t.Context(), &guarddutysdk.GetMasterAccountInput{
			DetectorId: aws.String(detectorID),
		})
		require.NoError(t, err)
		require.NotNil(t, gotMaster.Master)
		assert.Equal(t, invitingAccountID, aws.ToString(gotMaster.Master.AccountId))

		//nolint:staticcheck // legacy op still in the uncovered census; must be driven, not skipped
		_, err = client.DisassociateFromMasterAccount(
			t.Context(),
			&guarddutysdk.DisassociateFromMasterAccountInput{DetectorId: aws.String(detectorID)},
		)
		require.NoError(t, err)

		_, err = client.DeclineInvitations(t.Context(), &guarddutysdk.DeclineInvitationsInput{
			AccountIds: []string{invitingAccountID},
		})
		require.NoError(t, err)

		_, err = client.InviteMembers(t.Context(), &guarddutysdk.InviteMembersInput{
			DetectorId: aws.String(detectorID),
			AccountIds: []string{invitingAccountID},
		})
		require.NoError(t, err)

		_, err = client.DeleteInvitations(t.Context(), &guarddutysdk.DeleteInvitationsInput{
			AccountIds: []string{invitingAccountID},
		})
		require.NoError(t, err)
	})

	t.Run("investigations_family", func(t *testing.T) {
		t.Parallel()

		client := newSlice16GuardDutyClient(t)
		detectorID := slice16CreateDetector(t, client)

		created, err := client.CreateInvestigation(t.Context(), &guarddutysdk.CreateInvestigationInput{
			DetectorId:    aws.String(detectorID),
			TriggerPrompt: aws.String("Analyze findings in my organization"),
		})
		require.NoError(t, err)
		investigationID := aws.ToString(created.InvestigationId)
		require.NotEmpty(t, investigationID)

		got, err := client.GetInvestigation(t.Context(), &guarddutysdk.GetInvestigationInput{
			DetectorId:      aws.String(detectorID),
			InvestigationId: aws.String(investigationID),
		})
		require.NoError(t, err)
		require.NotNil(t, got.Investigation)
		assert.Equal(t, investigationID, aws.ToString(got.Investigation.InvestigationId))

		listed, err := client.ListInvestigations(t.Context(), &guarddutysdk.ListInvestigationsInput{
			DetectorId: aws.String(detectorID),
		})
		require.NoError(t, err)
		require.NotEmpty(t, listed.Investigations)
	})

	t.Run("coverage_and_trial_family", func(t *testing.T) {
		t.Parallel()

		client := newSlice16GuardDutyClient(t)
		detectorID := slice16CreateDetector(t, client)

		listed, err := client.ListCoverage(t.Context(), &guarddutysdk.ListCoverageInput{
			DetectorId: aws.String(detectorID),
		})
		require.NoError(t, err)
		assert.NotNil(t, listed.Resources)

		trial, err := client.GetRemainingFreeTrialDays(t.Context(), &guarddutysdk.GetRemainingFreeTrialDaysInput{
			DetectorId: aws.String(detectorID),
			AccountIds: []string{"000000000000"},
		})
		require.NoError(t, err)
		assert.NotNil(t, trial.Accounts)
	})

	t.Run("tags_family", func(t *testing.T) {
		t.Parallel()

		client := newSlice16GuardDutyClient(t)
		detectorID := slice16CreateDetector(t, client)
		detectorARN := "arn:aws:guardduty:us-east-1:000000000000:detector/" + detectorID

		_, err := client.TagResource(t.Context(), &guarddutysdk.TagResourceInput{
			ResourceArn: aws.String(detectorARN),
			Tags:        map[string]string{"env": "prod"},
		})
		require.NoError(t, err)

		listed, err := client.ListTagsForResource(t.Context(), &guarddutysdk.ListTagsForResourceInput{
			ResourceArn: aws.String(detectorARN),
		})
		require.NoError(t, err)
		require.Contains(t, listed.Tags, "env")

		_, err = client.UntagResource(t.Context(), &guarddutysdk.UntagResourceInput{
			ResourceArn: aws.String(detectorARN),
			TagKeys:     []string{"env"},
		})
		require.NoError(t, err)

		listed, err = client.ListTagsForResource(t.Context(), &guarddutysdk.ListTagsForResourceInput{
			ResourceArn: aws.String(detectorARN),
		})
		require.NoError(t, err)
		assert.Empty(t, listed.Tags)
	})
}
