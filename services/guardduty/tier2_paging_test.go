package guardduty_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	guarddutysdk "github.com/aws/aws-sdk-go-v2/service/guardduty"
	"github.com/aws/aws-sdk-go-v2/service/guardduty/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

func seedPagedDetector(t *testing.T, c *guarddutysdk.Client) string {
	t.Helper()

	created, err := c.CreateDetector(t.Context(), &guarddutysdk.CreateDetectorInput{
		Enable: aws.Bool(true),
		Features: []types.DetectorFeatureConfiguration{
			{Name: types.DetectorFeatureS3DataEvents, Status: types.FeatureStatusEnabled},
			{Name: types.DetectorFeatureEksAuditLogs, Status: types.FeatureStatusEnabled},
			{Name: types.DetectorFeatureEbsMalwareProtection, Status: types.FeatureStatusEnabled},
		},
	})
	require.NoError(t, err)

	return aws.ToString(created.DetectorId)
}

// TestRealClient_UsageAndOrgConfigPage covers maxResults/nextToken on GetUsageStatistics
// (body) and DescribeOrganizationConfiguration (query).
func TestRealClient_UsageAndOrgConfigPage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fetch func(t *testing.T, c *guarddutysdk.Client, id string, tok *string) (int, *string)
		name  string
		total int
	}{
		{
			name:  "usage_sum_by_feature",
			total: 3,
			fetch: func(t *testing.T, c *guarddutysdk.Client, id string, tok *string) (int, *string) {
				t.Helper()

				out, err := c.GetUsageStatistics(t.Context(), &guarddutysdk.GetUsageStatisticsInput{
					DetectorId: aws.String(id), UsageStatisticType: types.UsageStatisticTypeSumByFeatures,
					UsageCriteria: &types.UsageCriteria{}, MaxResults: aws.Int32(2), NextToken: tok,
				})
				require.NoError(t, err)

				return len(out.UsageStatistics.SumByFeature), out.NextToken
			},
		},
		{
			name:  "org_config_features",
			total: 3,
			fetch: func(t *testing.T, c *guarddutysdk.Client, id string, tok *string) (int, *string) {
				t.Helper()

				out, err := c.DescribeOrganizationConfiguration(
					t.Context(),
					&guarddutysdk.DescribeOrganizationConfigurationInput{
						DetectorId: aws.String(id), MaxResults: aws.Int32(2), NextToken: tok,
					},
				)
				require.NoError(t, err)

				return len(out.Features), out.NextToken
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestGuardDutyClient(t, newTestHandler(t))
			id := seedPagedDetector(t, c)

			if tt.name == "org_config_features" {
				_, err := c.UpdateOrganizationConfiguration(
					t.Context(),
					&guarddutysdk.UpdateOrganizationConfigurationInput{
						DetectorId: aws.String(id), AutoEnableOrganizationMembers: types.AutoEnableMembersAll,
						Features: []types.OrganizationFeatureConfiguration{
							{Name: types.OrgFeatureS3DataEvents, AutoEnable: types.OrgFeatureStatusAll},
							{Name: types.OrgFeatureEksAuditLogs, AutoEnable: types.OrgFeatureStatusAll},
							{Name: types.OrgFeatureEbsMalwareProtection, AutoEnable: types.OrgFeatureStatusAll},
						},
					},
				)
				require.NoError(t, err)
			}

			var token *string

			got, pages := 0, 0

			for {
				n, next := tt.fetch(t, c, id, token)
				got += n
				pages++

				if next == nil {
					break
				}

				token = next

				require.LessOrEqual(t, pages, 3)
			}

			require.Equal(t, tt.total, got)
			require.Equal(t, 2, pages)
		})
	}
}

func TestRealClient_CoverageAndUsageRejectBadToken(t *testing.T) {
	t.Parallel()

	c := newTestGuardDutyClient(t, newTestHandler(t))
	id := seedPagedDetector(t, c)

	_, listErr := c.ListCoverage(t.Context(), &guarddutysdk.ListCoverageInput{
		DetectorId: aws.String(id), NextToken: aws.String("%%%"),
	})
	_, usageErr := c.GetUsageStatistics(t.Context(), &guarddutysdk.GetUsageStatisticsInput{
		DetectorId: aws.String(id), UsageStatisticType: types.UsageStatisticTypeSumByFeatures,
		UsageCriteria: &types.UsageCriteria{}, NextToken: aws.String("%%%"),
	})

	for _, err := range []error{listErr, usageErr} {
		var apiErr smithy.APIError

		require.ErrorAs(t, err, &apiErr)
		require.Equal(t, "BadRequestException", apiErr.ErrorCode())
	}
}
