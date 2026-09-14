package macie2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	macie2sdk "github.com/aws/aws-sdk-go-v2/service/macie2"
	"github.com/aws/aws-sdk-go-v2/service/macie2/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/macie2"
)

// newMacie2Client stands up a fresh backend/handler pair plus a real macie2
// SDK client bound to it, reusing newTestMacie2SDKClient (defined in
// wire_field_fixes_test.go).
func newMacie2Client(t *testing.T) (*macie2.InMemoryBackend, *macie2sdk.Client) {
	t.Helper()

	b := macie2.NewInMemoryBackend("000000000000", "us-east-1")
	h := macie2.NewHandler(b)

	return b, newTestMacie2SDKClient(t, h)
}

// TestRealClient_AdminAndFindings drives every op the census still listed
// as uncovered before this pass (gopherstack-n3zi typed-client coverage).
// Each case builds its own fresh backend/client.
func TestRealClient_AdminAndFindings(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "enablement",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newMacie2Client(t)

				_, err := client.EnableMacie(t.Context(), &macie2sdk.EnableMacieInput{
					FindingPublishingFrequency: types.FindingPublishingFrequencyFifteenMinutes,
					Status:                     types.MacieStatusEnabled,
				})
				require.NoError(t, err)

				session, err := client.GetMacieSession(t.Context(), &macie2sdk.GetMacieSessionInput{})
				require.NoError(t, err)
				assert.Equal(
					t,
					types.FindingPublishingFrequencyFifteenMinutes,
					session.FindingPublishingFrequency,
				)
				assert.Equal(t, types.MacieStatusEnabled, session.Status)

				_, err = client.UpdateMacieSession(t.Context(), &macie2sdk.UpdateMacieSessionInput{
					FindingPublishingFrequency: types.FindingPublishingFrequencyOneHour,
				})
				require.NoError(t, err)

				session, err = client.GetMacieSession(t.Context(), &macie2sdk.GetMacieSessionInput{})
				require.NoError(t, err)
				assert.Equal(t, types.FindingPublishingFrequencyOneHour, session.FindingPublishingFrequency)

				_, err = client.DisableMacie(t.Context(), &macie2sdk.DisableMacieInput{})
				require.NoError(t, err)

				_, err = client.GetMacieSession(t.Context(), &macie2sdk.GetMacieSessionInput{})
				require.Error(t, err)
			},
		},
		{
			name: "allow_list_lifecycle",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newMacie2Client(t)

				created, err := client.CreateAllowList(t.Context(), &macie2sdk.CreateAllowListInput{
					Name: aws.String("al1"),
					Criteria: &types.AllowListCriteria{
						Regex: aws.String("test-.*"),
					},
				})
				require.NoError(t, err)
				id := aws.ToString(created.Id)
				require.NotEmpty(t, id)

				got, err := client.GetAllowList(
					t.Context(),
					&macie2sdk.GetAllowListInput{Id: aws.String(id)},
				)
				require.NoError(t, err)
				assert.Equal(t, "al1", aws.ToString(got.Name))
				require.NotNil(t, got.Criteria)
				assert.Equal(t, "test-.*", aws.ToString(got.Criteria.Regex))

				listed, err := client.ListAllowLists(t.Context(), &macie2sdk.ListAllowListsInput{})
				require.NoError(t, err)
				require.Len(t, listed.AllowLists, 1)
				assert.Equal(t, id, aws.ToString(listed.AllowLists[0].Id))

				_, err = client.UpdateAllowList(t.Context(), &macie2sdk.UpdateAllowListInput{
					Id:   aws.String(id),
					Name: aws.String("al1-renamed"),
					Criteria: &types.AllowListCriteria{
						Regex: aws.String("updated-.*"),
					},
				})
				require.NoError(t, err)

				got, err = client.GetAllowList(
					t.Context(),
					&macie2sdk.GetAllowListInput{Id: aws.String(id)},
				)
				require.NoError(t, err)
				assert.Equal(t, "al1-renamed", aws.ToString(got.Name))
				assert.Equal(t, "updated-.*", aws.ToString(got.Criteria.Regex))

				_, err = client.DeleteAllowList(
					t.Context(),
					&macie2sdk.DeleteAllowListInput{Id: aws.String(id)},
				)
				require.NoError(t, err)

				_, err = client.GetAllowList(t.Context(), &macie2sdk.GetAllowListInput{Id: aws.String(id)})
				require.Error(t, err)
			},
		},
		{
			name: "custom_data_identifier",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newMacie2Client(t)

				created, err := client.CreateCustomDataIdentifier(
					t.Context(),
					&macie2sdk.CreateCustomDataIdentifierInput{
						Name:  aws.String("cdi1"),
						Regex: aws.String(`\d{3}-\d{2}-\d{4}`),
					},
				)
				require.NoError(t, err)
				id := aws.ToString(created.CustomDataIdentifierId)
				require.NotEmpty(t, id)

				batch, err := client.BatchGetCustomDataIdentifiers(
					t.Context(),
					&macie2sdk.BatchGetCustomDataIdentifiersInput{
						Ids: []string{id, "does-not-exist"},
					},
				)
				require.NoError(t, err)
				require.Len(t, batch.CustomDataIdentifiers, 1)
				assert.Equal(t, "cdi1", aws.ToString(batch.CustomDataIdentifiers[0].Name))
				require.Len(t, batch.NotFoundIdentifierIds, 1)
				assert.Equal(t, "does-not-exist", batch.NotFoundIdentifierIds[0])

				tested, err := client.TestCustomDataIdentifier(
					t.Context(),
					&macie2sdk.TestCustomDataIdentifierInput{
						Regex:      aws.String(`\d{3}-\d{2}-\d{4}`),
						SampleText: aws.String("SSN: 123-45-6789 and 987-65-4321"),
					},
				)
				require.NoError(t, err)
				require.NotNil(t, tested.MatchCount)
				assert.Equal(t, int32(2), *tested.MatchCount)
			},
		},
		{
			name: "findings_filter_lifecycle",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newMacie2Client(t)

				created, err := client.CreateFindingsFilter(
					t.Context(),
					&macie2sdk.CreateFindingsFilterInput{
						Name:   aws.String("ff1"),
						Action: types.FindingsFilterActionArchive,
						FindingCriteria: &types.FindingCriteria{
							Criterion: map[string]types.CriterionAdditionalProperties{
								"type": {Eq: []string{"SensitiveData:S3Object/Personal"}},
							},
						},
					},
				)
				require.NoError(t, err)
				id := aws.ToString(created.Id)
				require.NotEmpty(t, id)

				got, err := client.GetFindingsFilter(
					t.Context(),
					&macie2sdk.GetFindingsFilterInput{Id: aws.String(id)},
				)
				require.NoError(t, err)
				assert.Equal(t, "ff1", aws.ToString(got.Name))
				assert.Equal(t, types.FindingsFilterActionArchive, got.Action)

				listed, err := client.ListFindingsFilters(
					t.Context(),
					&macie2sdk.ListFindingsFiltersInput{},
				)
				require.NoError(t, err)
				require.Len(t, listed.FindingsFilterListItems, 1)
				assert.Equal(t, id, aws.ToString(listed.FindingsFilterListItems[0].Id))

				_, err = client.UpdateFindingsFilter(t.Context(), &macie2sdk.UpdateFindingsFilterInput{
					Id:          aws.String(id),
					Name:        aws.String("ff1-renamed"),
					Description: aws.String("updated"),
				})
				require.NoError(t, err)

				got, err = client.GetFindingsFilter(
					t.Context(),
					&macie2sdk.GetFindingsFilterInput{Id: aws.String(id)},
				)
				require.NoError(t, err)
				assert.Equal(t, "ff1-renamed", aws.ToString(got.Name))
				assert.Equal(t, "updated", aws.ToString(got.Description))

				_, err = client.DeleteFindingsFilter(
					t.Context(),
					&macie2sdk.DeleteFindingsFilterInput{Id: aws.String(id)},
				)
				require.NoError(t, err)

				_, err = client.GetFindingsFilter(
					t.Context(),
					&macie2sdk.GetFindingsFilterInput{Id: aws.String(id)},
				)
				require.Error(t, err)
			},
		},
		{
			name: "findings",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newMacie2Client(t)

				_, err := client.CreateSampleFindings(t.Context(), &macie2sdk.CreateSampleFindingsInput{})
				require.NoError(t, err)

				listed, err := client.ListFindings(t.Context(), &macie2sdk.ListFindingsInput{})
				require.NoError(t, err)
				require.Len(t, listed.FindingIds, 1)
				findingID := listed.FindingIds[0]

				got, err := client.GetFindings(t.Context(), &macie2sdk.GetFindingsInput{
					FindingIds: []string{findingID},
				})
				require.NoError(t, err)
				require.Len(t, got.Findings, 1)
				assert.Equal(t, findingID, aws.ToString(got.Findings[0].Id))
				assert.Equal(t, types.FindingCategoryClassification, got.Findings[0].Category)
				require.NotNil(t, got.Findings[0].Sample)
				assert.True(t, *got.Findings[0].Sample)

				stats, err := client.GetFindingStatistics(t.Context(), &macie2sdk.GetFindingStatisticsInput{
					GroupBy: types.GroupByType,
				})
				require.NoError(t, err)
				require.Len(t, stats.CountsByGroup, 1)
				assert.Equal(t, int64(1), aws.ToInt64(stats.CountsByGroup[0].Count))

				avail, err := client.GetSensitiveDataOccurrencesAvailability(
					t.Context(),
					&macie2sdk.GetSensitiveDataOccurrencesAvailabilityInput{
						FindingId: aws.String(findingID),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, types.AvailabilityCodeUnavailable, avail.Code)
			},
		},
		{
			name: "findings_publication_config",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newMacie2Client(t)

				_, err := client.PutFindingsPublicationConfiguration(
					t.Context(),
					&macie2sdk.PutFindingsPublicationConfigurationInput{
						SecurityHubConfiguration: &types.SecurityHubConfiguration{
							PublishClassificationFindings: aws.Bool(true),
							PublishPolicyFindings:         aws.Bool(false),
						},
					},
				)
				require.NoError(t, err)

				got, err := client.GetFindingsPublicationConfiguration(
					t.Context(),
					&macie2sdk.GetFindingsPublicationConfigurationInput{},
				)
				require.NoError(t, err)
				require.NotNil(t, got.SecurityHubConfiguration)
				assert.True(t, aws.ToBool(got.SecurityHubConfiguration.PublishClassificationFindings))
				assert.False(t, aws.ToBool(got.SecurityHubConfiguration.PublishPolicyFindings))
			},
		},
		{
			name: "member_invitation",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newMacie2Client(t)

				const memberAccountID = "222222222222"

				_, err := client.CreateMember(t.Context(), &macie2sdk.CreateMemberInput{
					Account: &types.AccountDetail{
						AccountId: aws.String(memberAccountID),
						Email:     aws.String("member@example.com"),
					},
				})
				require.NoError(t, err)

				listed, err := client.ListMembers(t.Context(), &macie2sdk.ListMembersInput{})
				require.NoError(t, err)
				require.Len(t, listed.Members, 1)
				assert.Equal(t, memberAccountID, aws.ToString(listed.Members[0].AccountId))

				_, err = client.UpdateMemberSession(t.Context(), &macie2sdk.UpdateMemberSessionInput{
					Id:     aws.String(memberAccountID),
					Status: types.MacieStatusPaused,
				})
				require.NoError(t, err)

				_, err = client.DeleteMember(
					t.Context(),
					&macie2sdk.DeleteMemberInput{Id: aws.String(memberAccountID)},
				)
				require.NoError(t, err)

				listed, err = client.ListMembers(t.Context(), &macie2sdk.ListMembersInput{})
				require.NoError(t, err)
				assert.Empty(t, listed.Members)

				const invitedAccountID = "333333333333"

				_, err = client.CreateInvitations(t.Context(), &macie2sdk.CreateInvitationsInput{
					AccountIds: []string{invitedAccountID},
				})
				require.NoError(t, err)

				count, err := client.GetInvitationsCount(t.Context(), &macie2sdk.GetInvitationsCountInput{})
				require.NoError(t, err)
				require.NotNil(t, count.InvitationsCount)
				assert.Equal(t, int64(1), *count.InvitationsCount)

				_, err = client.DeleteInvitations(t.Context(), &macie2sdk.DeleteInvitationsInput{
					AccountIds: []string{invitedAccountID},
				})
				require.NoError(t, err)

				count, err = client.GetInvitationsCount(t.Context(), &macie2sdk.GetInvitationsCountInput{})
				require.NoError(t, err)
				assert.Equal(t, int64(0), aws.ToInt64(count.InvitationsCount))
			},
		},
		{
			name: "administrator_master",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newMacie2Client(t)

				_, err := client.AcceptInvitation(t.Context(), &macie2sdk.AcceptInvitationInput{
					AdministratorAccountId: aws.String("444444444444"),
					InvitationId:           aws.String("invite-1"),
				})
				require.NoError(t, err)

				master, err := client.GetMasterAccount(t.Context(), &macie2sdk.GetMasterAccountInput{})
				require.NoError(t, err)
				require.NotNil(t, master.Master)
				assert.Equal(t, "444444444444", aws.ToString(master.Master.AccountId))

				_, err = client.DisassociateFromMasterAccount(
					t.Context(),
					&macie2sdk.DisassociateFromMasterAccountInput{},
				)
				require.NoError(t, err)

				master, err = client.GetMasterAccount(t.Context(), &macie2sdk.GetMasterAccountInput{})
				require.NoError(t, err)
				assert.Nil(t, master.Master)

				_, err = client.AcceptInvitation(t.Context(), &macie2sdk.AcceptInvitationInput{
					AdministratorAccountId: aws.String("555555555555"),
					InvitationId:           aws.String("invite-2"),
				})
				require.NoError(t, err)

				_, err = client.DisassociateFromAdministratorAccount(
					t.Context(),
					&macie2sdk.DisassociateFromAdministratorAccountInput{},
				)
				require.NoError(t, err)

				master, err = client.GetMasterAccount(t.Context(), &macie2sdk.GetMasterAccountInput{})
				require.NoError(t, err)
				assert.Nil(t, master.Master)
			},
		},
		{
			name: "organization",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newMacie2Client(t)

				const adminAccountID = "666666666666"

				_, err := client.EnableOrganizationAdminAccount(
					t.Context(),
					&macie2sdk.EnableOrganizationAdminAccountInput{
						AdminAccountId: aws.String(adminAccountID),
					},
				)
				require.NoError(t, err)

				listed, err := client.ListOrganizationAdminAccounts(
					t.Context(),
					&macie2sdk.ListOrganizationAdminAccountsInput{},
				)
				require.NoError(t, err)
				require.Len(t, listed.AdminAccounts, 1)
				assert.Equal(t, adminAccountID, aws.ToString(listed.AdminAccounts[0].AccountId))

				_, err = client.DisableOrganizationAdminAccount(
					t.Context(),
					&macie2sdk.DisableOrganizationAdminAccountInput{
						AdminAccountId: aws.String(adminAccountID),
					},
				)
				require.NoError(t, err)

				listed, err = client.ListOrganizationAdminAccounts(
					t.Context(),
					&macie2sdk.ListOrganizationAdminAccountsInput{},
				)
				require.NoError(t, err)
				assert.Empty(t, listed.AdminAccounts)

				_, err = client.UpdateOrganizationConfiguration(
					t.Context(),
					&macie2sdk.UpdateOrganizationConfigurationInput{
						AutoEnable: aws.Bool(true),
					},
				)
				require.NoError(t, err)

				cfg, err := client.DescribeOrganizationConfiguration(
					t.Context(),
					&macie2sdk.DescribeOrganizationConfigurationInput{},
				)
				require.NoError(t, err)
				assert.True(t, aws.ToBool(cfg.AutoEnable))
			},
		},
		{
			name: "automated_discovery",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newMacie2Client(t)

				_, err := client.UpdateAutomatedDiscoveryConfiguration(
					t.Context(),
					&macie2sdk.UpdateAutomatedDiscoveryConfigurationInput{
						Status:                        types.AutomatedDiscoveryStatusEnabled,
						AutoEnableOrganizationMembers: types.AutoEnableModeAll,
					},
				)
				require.NoError(t, err)

				cfg, err := client.GetAutomatedDiscoveryConfiguration(
					t.Context(),
					&macie2sdk.GetAutomatedDiscoveryConfigurationInput{},
				)
				require.NoError(t, err)
				assert.Equal(t, types.AutomatedDiscoveryStatusEnabled, cfg.Status)
				assert.Equal(t, types.AutoEnableModeAll, cfg.AutoEnableOrganizationMembers)

				const discoveryAccountID = "777777777777"

				_, err = client.BatchUpdateAutomatedDiscoveryAccounts(
					t.Context(),
					&macie2sdk.BatchUpdateAutomatedDiscoveryAccountsInput{
						Accounts: []types.AutomatedDiscoveryAccountUpdate{
							{
								AccountId: aws.String(discoveryAccountID),
								Status:    types.AutomatedDiscoveryAccountStatusEnabled,
							},
						},
					},
				)
				require.NoError(t, err)

				accounts, err := client.ListAutomatedDiscoveryAccounts(
					t.Context(),
					&macie2sdk.ListAutomatedDiscoveryAccountsInput{},
				)
				require.NoError(t, err)
				require.Len(t, accounts.Items, 1)
				assert.Equal(t, discoveryAccountID, aws.ToString(accounts.Items[0].AccountId))
				assert.Equal(t, types.AutomatedDiscoveryAccountStatusEnabled, accounts.Items[0].Status)
			},
		},
		{
			name: "buckets",
			run: func(t *testing.T) {
				t.Helper()

				b, client := newMacie2Client(t)

				macie2.SeedS3Bucket(b, macie2.S3BucketMetadata{
					AccountID:   "000000000000",
					BucketArn:   "arn:aws:s3:::typed14-bucket",
					BucketName:  "typed14-bucket",
					Region:      "us-east-1",
					ObjectCount: 5,
				})

				described, err := client.DescribeBuckets(t.Context(), &macie2sdk.DescribeBucketsInput{})
				require.NoError(t, err)
				require.Len(t, described.Buckets, 1)
				assert.Equal(t, "typed14-bucket", aws.ToString(described.Buckets[0].BucketName))
			},
		},
		{
			name: "classification_jobs",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newMacie2Client(t)

				created, err := client.CreateClassificationJob(
					t.Context(),
					&macie2sdk.CreateClassificationJobInput{
						Name:    aws.String("job1-" + uuid.NewString()[:8]),
						JobType: types.JobTypeOneTime,
						S3JobDefinition: &types.S3JobDefinition{
							BucketDefinitions: []types.S3BucketDefinitionForJob{},
						},
					},
				)
				require.NoError(t, err)
				jobID := aws.ToString(created.JobId)
				require.NotEmpty(t, jobID)

				listed, err := client.ListClassificationJobs(
					t.Context(),
					&macie2sdk.ListClassificationJobsInput{},
				)
				require.NoError(t, err)
				require.Len(t, listed.Items, 1)
				assert.Equal(t, jobID, aws.ToString(listed.Items[0].JobId))

				_, err = client.UpdateClassificationJob(
					t.Context(),
					&macie2sdk.UpdateClassificationJobInput{
						JobId:     aws.String(jobID),
						JobStatus: types.JobStatusUserPaused,
					},
				)
				require.NoError(t, err)

				described, err := client.DescribeClassificationJob(
					t.Context(),
					&macie2sdk.DescribeClassificationJobInput{
						JobId: aws.String(jobID),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, types.JobStatusUserPaused, described.JobStatus)
				require.NotNil(t, described.UserPausedDetails)
			},
		},
		{
			name: "classification_export_config",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newMacie2Client(t)

				_, err := client.PutClassificationExportConfiguration(
					t.Context(),
					&macie2sdk.PutClassificationExportConfigurationInput{
						Configuration: &types.ClassificationExportConfiguration{
							S3Destination: &types.S3Destination{
								BucketName: aws.String("export-bucket"),
								KmsKeyArn:  aws.String("arn:aws:kms:us-east-1:000000000000:key/abc"),
								KeyPrefix:  aws.String("results/"),
							},
						},
					},
				)
				require.NoError(t, err)

				got, err := client.GetClassificationExportConfiguration(
					t.Context(),
					&macie2sdk.GetClassificationExportConfigurationInput{},
				)
				require.NoError(t, err)
				require.NotNil(t, got.Configuration)
				require.NotNil(t, got.Configuration.S3Destination)
				assert.Equal(t, "export-bucket", aws.ToString(got.Configuration.S3Destination.BucketName))
				assert.Equal(t, "results/", aws.ToString(got.Configuration.S3Destination.KeyPrefix))
			},
		},
		{
			name: "classification_scope",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newMacie2Client(t)

				listed, err := client.ListClassificationScopes(
					t.Context(),
					&macie2sdk.ListClassificationScopesInput{},
				)
				require.NoError(t, err)
				require.Len(t, listed.ClassificationScopes, 1)
				scopeID := aws.ToString(listed.ClassificationScopes[0].Id)
				require.NotEmpty(t, scopeID)

				_, err = client.UpdateClassificationScope(
					t.Context(),
					&macie2sdk.UpdateClassificationScopeInput{
						Id: aws.String(scopeID),
						S3: &types.S3ClassificationScopeUpdate{
							Excludes: &types.S3ClassificationScopeExclusionUpdate{
								Operation:   types.ClassificationScopeUpdateOperationAdd,
								BucketNames: []string{"excluded-bucket"},
							},
						},
					},
				)
				require.NoError(t, err)

				got, err := client.GetClassificationScope(
					t.Context(),
					&macie2sdk.GetClassificationScopeInput{
						Id: aws.String(scopeID),
					},
				)
				require.NoError(t, err)
				require.NotNil(t, got.S3)
				require.NotNil(t, got.S3.Excludes)
				assert.Equal(t, []string{"excluded-bucket"}, got.S3.Excludes.BucketNames)
			},
		},
		{
			name: "reveal_configuration",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newMacie2Client(t)

				_, err := client.UpdateRevealConfiguration(
					t.Context(),
					&macie2sdk.UpdateRevealConfigurationInput{
						Configuration: &types.RevealConfiguration{
							Status: types.RevealStatusEnabled,
						},
					},
				)
				require.NoError(t, err)

				got, err := client.GetRevealConfiguration(
					t.Context(),
					&macie2sdk.GetRevealConfigurationInput{},
				)
				require.NoError(t, err)
				require.NotNil(t, got.Configuration)
				assert.Equal(t, types.RevealStatusEnabled, got.Configuration.Status)
			},
		},
		{
			name: "usage",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newMacie2Client(t)

				stats, err := client.GetUsageStatistics(t.Context(), &macie2sdk.GetUsageStatisticsInput{})
				require.NoError(t, err)
				assert.Empty(t, stats.Records)

				totals, err := client.GetUsageTotals(t.Context(), &macie2sdk.GetUsageTotalsInput{})
				require.NoError(t, err)
				require.NotEmpty(t, totals.UsageTotals)
				assert.Equal(t, types.Currency("USD"), totals.UsageTotals[0].Currency)
			},
		},
		{
			name: "managed_data_identifiers",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newMacie2Client(t)

				listed, err := client.ListManagedDataIdentifiers(
					t.Context(),
					&macie2sdk.ListManagedDataIdentifiersInput{},
				)
				require.NoError(t, err)
				assert.NotEmpty(t, listed.Items)
			},
		},
		{
			name: "resource_profile",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newMacie2Client(t)

				const bucketARN = "arn:aws:s3:::resource-profile-bucket"

				artifacts, err := client.ListResourceProfileArtifacts(
					t.Context(),
					&macie2sdk.ListResourceProfileArtifactsInput{
						ResourceArn: aws.String(bucketARN),
					},
				)
				require.NoError(t, err)
				assert.Empty(t, artifacts.Artifacts)

				detections, err := client.ListResourceProfileDetections(
					t.Context(),
					&macie2sdk.ListResourceProfileDetectionsInput{ResourceArn: aws.String(bucketARN)},
				)
				require.NoError(t, err)
				assert.Empty(t, detections.Detections)

				_, err = client.UpdateResourceProfileDetections(
					t.Context(),
					&macie2sdk.UpdateResourceProfileDetectionsInput{
						ResourceArn: aws.String(bucketARN),
					},
				)
				require.NoError(t, err)
			},
		},
		{
			name: "tags",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newMacie2Client(t)

				created, err := client.CreateAllowList(t.Context(), &macie2sdk.CreateAllowListInput{
					Name:     aws.String("tag-al"),
					Criteria: &types.AllowListCriteria{Regex: aws.String("tag-.*")},
				})
				require.NoError(t, err)
				resourceARN := aws.ToString(created.Arn)
				require.NotEmpty(t, resourceARN)

				_, err = client.TagResource(t.Context(), &macie2sdk.TagResourceInput{
					ResourceArn: aws.String(resourceARN),
					Tags:        map[string]string{"env": "prod"},
				})
				require.NoError(t, err)

				listed, err := client.ListTagsForResource(t.Context(), &macie2sdk.ListTagsForResourceInput{
					ResourceArn: aws.String(resourceARN),
				})
				require.NoError(t, err)
				assert.Equal(t, map[string]string{"env": "prod"}, listed.Tags)

				_, err = client.UntagResource(t.Context(), &macie2sdk.UntagResourceInput{
					ResourceArn: aws.String(resourceARN),
					TagKeys:     []string{"env"},
				})
				require.NoError(t, err)

				listed, err = client.ListTagsForResource(t.Context(), &macie2sdk.ListTagsForResourceInput{
					ResourceArn: aws.String(resourceARN),
				})
				require.NoError(t, err)
				assert.Empty(t, listed.Tags)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
