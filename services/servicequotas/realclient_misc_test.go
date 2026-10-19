package servicequotas_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sqsdk "github.com/aws/aws-sdk-go-v2/service/servicequotas"
	sqtypes "github.com/aws/aws-sdk-go-v2/service/servicequotas/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/servicequotas"
)

const ec2QuotaARN = "arn:aws:servicequotas:us-east-1:000000000000:ec2/L-1216C47A"

func TestAutoManagement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture)
		name string
	}{
		{
			name: "never_started_is_disabled",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				out, err := f.client.GetAutoManagementConfiguration(
					t.Context(),
					&sqsdk.GetAutoManagementConfigurationInput{},
				)
				require.NoError(t, err)
				assert.Equal(t, sqtypes.OptInStatusDisabled, out.OptInStatus)
			},
		},
		{
			name: "start_get_update_stop",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				_, err := f.client.StartAutoManagement(t.Context(), &sqsdk.StartAutoManagementInput{
					OptInLevel: sqtypes.OptInLevelAccount, OptInType: sqtypes.OptInTypeNotifyOnly,
					NotificationArn: aws.String("arn:aws:sns:us-east-1:000000000000:alerts"),
					ExclusionList:   map[string][]string{"ec2": {"L-1216C47A"}},
				})
				require.NoError(t, err)

				out, err := f.client.GetAutoManagementConfiguration(
					t.Context(),
					&sqsdk.GetAutoManagementConfigurationInput{},
				)
				require.NoError(t, err)
				assert.Equal(t, sqtypes.OptInStatusEnabled, out.OptInStatus)
				assert.Equal(t, sqtypes.OptInTypeNotifyOnly, out.OptInType)
				require.Len(t, out.ExclusionList["ec2"], 1)
				assert.Contains(t, aws.ToString(out.ExclusionList["ec2"][0].QuotaName), "On-Demand Standard")

				_, err = f.client.StartAutoManagement(t.Context(), &sqsdk.StartAutoManagementInput{
					OptInLevel: sqtypes.OptInLevelAccount, OptInType: sqtypes.OptInTypeNotifyOnly,
				})
				assert.Equal(t, "ResourceAlreadyExistsException", apiCode(err))

				_, err = f.client.UpdateAutoManagement(t.Context(), &sqsdk.UpdateAutoManagementInput{
					OptInType: sqtypes.OptInTypeNotifyAndAdjust, ExclusionList: map[string][]string{},
				})
				require.NoError(t, err)

				out, err = f.client.GetAutoManagementConfiguration(
					t.Context(),
					&sqsdk.GetAutoManagementConfigurationInput{},
				)
				require.NoError(t, err)
				assert.Equal(t, sqtypes.OptInTypeNotifyAndAdjust, out.OptInType)
				assert.Empty(t, out.ExclusionList)

				_, err = f.client.StopAutoManagement(t.Context(), &sqsdk.StopAutoManagementInput{})
				require.NoError(t, err)

				out, err = f.client.GetAutoManagementConfiguration(
					t.Context(),
					&sqsdk.GetAutoManagementConfigurationInput{},
				)
				require.NoError(t, err)
				assert.Equal(t, sqtypes.OptInStatusDisabled, out.OptInStatus)

				_, err = f.client.StopAutoManagement(t.Context(), &sqsdk.StopAutoManagementInput{})
				assert.Equal(t, "NoSuchResourceException", apiCode(err))

				_, err = f.client.UpdateAutoManagement(
					t.Context(),
					&sqsdk.UpdateAutoManagementInput{OptInType: sqtypes.OptInTypeNotifyOnly},
				)
				assert.Equal(t, "NoSuchResourceException", apiCode(err))
			},
		},
		{
			name: "invalid_inputs",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				_, err := f.client.StartAutoManagement(t.Context(), &sqsdk.StartAutoManagementInput{
					OptInLevel: sqtypes.OptInLevelAccount, OptInType: "Bogus",
				})
				assert.Equal(t, "IllegalArgumentException", apiCode(err))

				_, err = f.client.StartAutoManagement(t.Context(), &sqsdk.StartAutoManagementInput{
					OptInLevel: sqtypes.OptInLevelAccount, OptInType: sqtypes.OptInTypeNotifyOnly,
					ExclusionList: map[string][]string{"ec2": {"L-NOPE"}},
				})
				assert.Equal(t, "IllegalArgumentException", apiCode(err))

				_, err = f.client.StartAutoManagement(t.Context(), &sqsdk.StartAutoManagementInput{
					OptInLevel: sqtypes.OptInLevelAccount, OptInType: sqtypes.OptInTypeNotifyOnly,
					NotificationArn: aws.String("not-an-arn"),
				})
				assert.Equal(t, "IllegalArgumentException", apiCode(err))

				_, err = f.client.UpdateAutoManagement(t.Context(), &sqsdk.UpdateAutoManagementInput{})
				assert.Equal(t, "IllegalArgumentException", apiCode(err))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t, newFixture(t))
		})
	}
}

func TestUtilizationReport(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture)
		name string
	}{
		{
			name: "progresses_to_completed",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				f.backend.SetUsageSource(fakeUsage{"ec2/L-1216C47A": 4})

				start, err := f.client.StartQuotaUtilizationReport(
					t.Context(),
					&sqsdk.StartQuotaUtilizationReportInput{},
				)
				require.NoError(t, err)
				assert.Equal(t, sqtypes.ReportStatusPending, start.Status)

				get := func(limit *int32, token *string) *sqsdk.GetQuotaUtilizationReportOutput {
					out, gerr := f.client.GetQuotaUtilizationReport(t.Context(), &sqsdk.GetQuotaUtilizationReportInput{
						ReportId: start.ReportId, MaxResults: limit, NextToken: token,
					})
					require.NoError(t, gerr)

					return out
				}

				assert.Equal(t, sqtypes.ReportStatusPending, get(nil, nil).Status)

				f.clock.advance(600 * time.Millisecond)
				assert.Equal(t, sqtypes.ReportStatusInProgress, get(nil, nil).Status)

				f.clock.advance(600 * time.Millisecond)

				done := get(aws.Int32(2), nil)
				assert.Equal(t, sqtypes.ReportStatusCompleted, done.Status)
				assert.NotNil(t, done.GeneratedAt)
				assert.Greater(t, aws.ToInt32(done.TotalCount), int32(2))
				require.Len(t, done.Quotas, 2)
				assert.Equal(t, "L-1216C47A", aws.ToString(done.Quotas[0].QuotaCode))
				assert.InDelta(t, 80, aws.ToFloat64(done.Quotas[0].Utilization), 0.001)
				assert.NotNil(t, done.NextToken)

				next := get(aws.Int32(100), done.NextToken)
				assert.Len(t, next.Quotas, int(aws.ToInt32(done.TotalCount))-2)
				assert.Nil(t, next.NextToken)
			},
		},
		{
			name: "second_report_blocked_until_complete",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				_, err := f.client.StartQuotaUtilizationReport(t.Context(), &sqsdk.StartQuotaUtilizationReportInput{})
				require.NoError(t, err)

				_, err = f.client.StartQuotaUtilizationReport(t.Context(), &sqsdk.StartQuotaUtilizationReportInput{})
				assert.Equal(t, "InvalidResourceStateException", apiCode(err))

				f.clock.advance(time.Hour)

				_, err = f.client.StartQuotaUtilizationReport(t.Context(), &sqsdk.StartQuotaUtilizationReportInput{})
				require.NoError(t, err)
			},
		},
		{
			name: "unknown_report",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				_, err := f.client.GetQuotaUtilizationReport(t.Context(),
					&sqsdk.GetQuotaUtilizationReportInput{ReportId: aws.String("nope")})
				assert.Equal(t, "NoSuchResourceException", apiCode(err))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t, newFixture(t))
		})
	}
}

func TestTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture)
		name string
	}{
		{
			name: "tag_list_untag",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				_, err := f.client.TagResource(t.Context(), &sqsdk.TagResourceInput{
					ResourceARN: aws.String(ec2QuotaARN),
					Tags: []sqtypes.Tag{
						{Key: aws.String("env"), Value: aws.String("dev")},
						{Key: aws.String("k"), Value: aws.String("v")},
					},
				})
				require.NoError(t, err)

				out, err := f.client.ListTagsForResource(
					t.Context(),
					&sqsdk.ListTagsForResourceInput{ResourceARN: aws.String(ec2QuotaARN)},
				)
				require.NoError(t, err)
				assert.Len(t, out.Tags, 2)

				_, err = f.client.UntagResource(t.Context(), &sqsdk.UntagResourceInput{
					ResourceARN: aws.String(ec2QuotaARN), TagKeys: []string{"k"},
				})
				require.NoError(t, err)

				out, err = f.client.ListTagsForResource(
					t.Context(),
					&sqsdk.ListTagsForResourceInput{ResourceARN: aws.String(ec2QuotaARN)},
				)
				require.NoError(t, err)
				require.Len(t, out.Tags, 1)
				assert.Equal(t, "env", aws.ToString(out.Tags[0].Key))
			},
		},
		{
			name: "unknown_quota_arn",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				_, err := f.client.ListTagsForResource(t.Context(), &sqsdk.ListTagsForResourceInput{
					ResourceARN: aws.String("arn:aws:servicequotas:us-east-1:000000000000:ec2/L-NOPE"),
				})
				assert.Equal(t, "NoSuchResourceException", apiCode(err))
			},
		},
		{
			name: "malformed_arn",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				_, err := f.client.ListTagsForResource(
					t.Context(),
					&sqsdk.ListTagsForResourceInput{ResourceARN: aws.String("nope")},
				)
				assert.Equal(t, "IllegalArgumentException", apiCode(err))
			},
		},
		{
			name: "too_many_tags",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				for round := range 2 {
					tags := make([]sqtypes.Tag, 0, 30)
					for i := range 30 {
						tags = append(
							tags,
							sqtypes.Tag{
								Key:   aws.String(string(rune('a'+round)) + string(rune('a'+i))),
								Value: aws.String("v"),
							},
						)
					}

					_, err := f.client.TagResource(
						t.Context(),
						&sqsdk.TagResourceInput{ResourceARN: aws.String(ec2QuotaARN), Tags: tags},
					)
					if round == 1 {
						assert.Equal(t, "TooManyTagsException", apiCode(err))
					} else {
						require.NoError(t, err)
					}
				}
			},
		},
		{
			name: "reserved_prefix",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				_, err := f.client.TagResource(t.Context(), &sqsdk.TagResourceInput{
					ResourceARN: aws.String(
						ec2QuotaARN,
					),
					Tags: []sqtypes.Tag{{Key: aws.String("aws:x"), Value: aws.String("v")}},
				})
				assert.Equal(t, "IllegalArgumentException", apiCode(err))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t, newFixture(t))
		})
	}
}

func TestPersistence_RoundTrip(t *testing.T) {
	t.Parallel()

	f := orgFixture(t, goodOrg())
	req := requestIncrease(t, f, 20, nil)
	f.clock.advance(time.Hour)
	putItem(t, f, 30)

	_, err := f.client.AssociateServiceQuotaTemplate(t.Context(), &sqsdk.AssociateServiceQuotaTemplateInput{})
	require.NoError(t, err)

	_, err = f.client.TagResource(t.Context(), &sqsdk.TagResourceInput{
		ResourceARN: aws.String(ec2QuotaARN), Tags: []sqtypes.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
	})
	require.NoError(t, err)

	_, err = f.client.StartAutoManagement(t.Context(), &sqsdk.StartAutoManagementInput{
		OptInLevel: sqtypes.OptInLevelAccount, OptInType: sqtypes.OptInTypeNotifyOnly,
	})
	require.NoError(t, err)

	snap := f.backend.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	restored := servicequotas.NewInMemoryBackend(testAccount, "us-east-1")
	require.NoError(t, restored.Restore(t.Context(), snap))

	g := newFixtureFromBackend(t, restored, f.clock)
	restored.SetOrgDirectory(fakeOrgs{"000000000000": goodOrg()})

	assert.InDelta(t, 20, appliedValue(t, g), 0)
	assert.Equal(t, sqtypes.RequestStatusApproved, getRequest(t, g, req.Id).Status)

	assoc, err := g.client.GetAssociationForServiceQuotaTemplate(
		t.Context(),
		&sqsdk.GetAssociationForServiceQuotaTemplateInput{},
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		sqtypes.ServiceQuotaTemplateAssociationStatusAssociated,
		assoc.ServiceQuotaTemplateAssociationStatus,
	)

	tags, err := g.client.ListTagsForResource(
		t.Context(),
		&sqsdk.ListTagsForResourceInput{ResourceARN: aws.String(ec2QuotaARN)},
	)
	require.NoError(t, err)
	assert.Len(t, tags.Tags, 1)

	cfg, err := g.client.GetAutoManagementConfiguration(t.Context(), &sqsdk.GetAutoManagementConfigurationInput{})
	require.NoError(t, err)
	assert.Equal(t, sqtypes.OptInStatusEnabled, cfg.OptInStatus)

	list, err := g.client.ListServiceQuotaIncreaseRequestsInTemplate(
		t.Context(),
		&sqsdk.ListServiceQuotaIncreaseRequestsInTemplateInput{},
	)
	require.NoError(t, err)
	assert.Len(t, list.ServiceQuotaIncreaseRequestInTemplateList, 1)
}
