package guardduty_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	guarddutysdk "github.com/aws/aws-sdk-go-v2/service/guardduty"
	"github.com/aws/aws-sdk-go-v2/service/guardduty/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemberFeatureDerivedReports(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		memberStatus types.FeatureStatus
		wantCount    int32
		wantTrial    bool
	}{
		{name: "enabled", memberStatus: types.FeatureStatusEnabled, wantCount: 1, wantTrial: true},
		{name: "disabled", memberStatus: types.FeatureStatusDisabled, wantCount: 0, wantTrial: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			const member = "444455556666"

			client := newRealClient(t)
			detectorID := createRealClientDetector(t, client)

			_, err := client.CreateMembers(t.Context(), &guarddutysdk.CreateMembersInput{
				DetectorId: aws.String(detectorID),
				AccountDetails: []types.AccountDetail{
					{AccountId: aws.String(member), Email: aws.String("m@example.com")},
				},
			})
			require.NoError(t, err)

			_, err = client.UpdateMemberDetectors(t.Context(), &guarddutysdk.UpdateMemberDetectorsInput{
				DetectorId: aws.String(detectorID),
				AccountIds: []string{member},
				Features: []types.MemberFeaturesConfiguration{
					{Name: types.OrgFeatureS3DataEvents, Status: tt.memberStatus},
				},
			})
			require.NoError(t, err)

			stats, err := client.GetOrganizationStatistics(t.Context(), &guarddutysdk.GetOrganizationStatisticsInput{})
			require.NoError(t, err)

			var gotCount int32

			for _, f := range stats.OrganizationDetails.OrganizationStatistics.CountByFeature {
				if f.Name == types.OrgFeatureS3DataEvents {
					gotCount = aws.ToInt32(f.EnabledAccountsCount)
				}
			}

			assert.Equal(t, tt.wantCount, gotCount)

			trial, err := client.GetRemainingFreeTrialDays(t.Context(), &guarddutysdk.GetRemainingFreeTrialDaysInput{
				DetectorId: aws.String(detectorID), AccountIds: []string{member},
			})
			require.NoError(t, err)
			require.Len(t, trial.Accounts, 1)

			var hasTrial bool

			for _, f := range trial.Accounts[0].Features {
				if f.Name == types.FreeTrialFeatureResultS3DataEvents {
					hasTrial = true

					assert.Positive(t, aws.ToInt32(f.FreeTrialDaysRemaining))
				}
			}

			assert.Equal(t, tt.wantTrial, hasTrial)
		})
	}
}
