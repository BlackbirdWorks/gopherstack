package databrew_test

import (
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	databrewsdk "github.com/aws/aws-sdk-go-v2/service/databrew"
	"github.com/aws/aws-sdk-go-v2/service/databrew/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/databrew"
)

// The fixture was written by HEAD code before these fields were typed.
func TestRestore_HeadSnapshotTypedConfigs(t *testing.T) {
	t.Parallel()

	data, readErr := os.ReadFile("testdata/head_snapshot_typed_configs.json")
	require.NoError(t, readErr)

	rulesetArn := "arn:aws:databrew:us-east-1:123456789012:ruleset/rs"

	tests := []struct {
		check func(t *testing.T, c *databrewsdk.Client)
		name  string
	}{
		{name: "ruleset_rules", check: func(t *testing.T, c *databrewsdk.Client) {
			t.Helper()
			got, err := c.DescribeRuleset(t.Context(), &databrewsdk.DescribeRulesetInput{Name: aws.String("rs")})
			require.NoError(t, err)
			require.Len(t, got.Rules, 2)
			r0, r1 := got.Rules[0], got.Rules[1]
			require.NotNil(t, r0.Threshold)
			assert.InDelta(t, 0.0, r0.Threshold.Value, 0)
			assert.Equal(t, types.ThresholdTypeLessThan, r0.Threshold.Type)
			assert.Equal(t, types.ThresholdUnitPercentage, r0.Threshold.Unit)
			require.Len(t, r0.ColumnSelectors, 2)
			assert.Equal(t, "c1", aws.ToString(r0.ColumnSelectors[0].Name))
			assert.Equal(t, "^x.*", aws.ToString(r0.ColumnSelectors[1].Regex))
			assert.Equal(t, map[string]string{":v": "1"}, r0.SubstitutionMap)
			require.NotNil(t, r1.Threshold)
			assert.InDelta(t, 12.5, r1.Threshold.Value, 0)
			assert.True(t, r1.Disabled)
		}},
		{name: "job_profile_configuration", check: func(t *testing.T, c *databrewsdk.Client) {
			t.Helper()
			got, err := c.DescribeJob(t.Context(), &databrewsdk.DescribeJobInput{Name: aws.String("pj")})
			require.NoError(t, err)
			pc := got.ProfileConfiguration
			require.NotNil(t, pc)
			assert.Equal(t, []string{"MIN", "MAX"}, pc.DatasetStatisticsConfiguration.IncludedStatistics)
			require.Len(t, pc.DatasetStatisticsConfiguration.Overrides, 1)
			assert.Equal(t, "MIN", aws.ToString(pc.DatasetStatisticsConfiguration.Overrides[0].Statistic))
			assert.Equal(t, map[string]string{"k": "v"}, pc.DatasetStatisticsConfiguration.Overrides[0].Parameters)
			require.Len(t, pc.ColumnStatisticsConfigurations, 1)
			assert.Equal(t, "c1", aws.ToString(pc.ColumnStatisticsConfigurations[0].Selectors[0].Name))
			assert.Equal(t, []string{"MEDIAN"}, pc.ColumnStatisticsConfigurations[0].Statistics.IncludedStatistics)
			assert.Equal(t, []string{"USA_SSN"}, pc.EntityDetectorConfiguration.EntityTypes)
			assert.Equal(t, []string{"MIN"}, pc.EntityDetectorConfiguration.AllowedStatistics[0].Statistics)
			assert.Equal(t, "^p", aws.ToString(pc.ProfileColumns[0].Regex))
			require.Len(t, got.ValidationConfigurations, 1)
			assert.Equal(t, rulesetArn, aws.ToString(got.ValidationConfigurations[0].RulesetArn))
			assert.Equal(t, types.ValidationModeCheckAll, got.ValidationConfigurations[0].ValidationMode)
		}},
		{name: "job_run_validation_configurations", check: func(t *testing.T, c *databrewsdk.Client) {
			t.Helper()
			got, err := c.ListJobRuns(t.Context(), &databrewsdk.ListJobRunsInput{Name: aws.String("pj")})
			require.NoError(t, err)
			require.Len(t, got.JobRuns, 1)
			require.Len(t, got.JobRuns[0].ValidationConfigurations, 1)
			assert.Equal(t, rulesetArn, aws.ToString(got.JobRuns[0].ValidationConfigurations[0].RulesetArn))
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := databrew.NewInMemoryBackend("123456789012", "us-east-1")
			require.NoError(t, b.Restore(t.Context(), data))
			tc.check(t, newRoundTripClient(t, databrew.NewHandler(b)))
		})
	}
}
