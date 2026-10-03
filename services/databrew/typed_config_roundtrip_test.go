package databrew_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	databrewsdk "github.com/aws/aws-sdk-go-v2/service/databrew"
	"github.com/aws/aws-sdk-go-v2/service/databrew/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/databrew"
)

func newTypedConfigClient(t *testing.T) *databrewsdk.Client {
	t.Helper()

	return newRoundTripClient(t, databrew.NewHandler(databrew.NewInMemoryBackend("123456789012", "us-east-1")))
}

func TestRuleset_ThresholdAndColumnSelectorsRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		threshold *types.Threshold
		name      string
		wantErr   string
	}{
		{name: "zero_value_kept", threshold: &types.Threshold{
			Value: 0, Type: types.ThresholdTypeLessThan, Unit: types.ThresholdUnitPercentage,
		}},
		{name: "fractional", threshold: &types.Threshold{Value: 12.5, Type: types.ThresholdTypeGreaterThan}},
		{name: "bad_type", threshold: &types.Threshold{Value: 1, Type: "NEAR"}, wantErr: "ValidationException"},
		{name: "bad_unit", threshold: &types.Threshold{Value: 1, Unit: "BYTES"}, wantErr: "ValidationException"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTypedConfigClient(t)
			rule := types.Rule{
				Name:            aws.String("r1"),
				CheckExpression: aws.String("COLUMN_COMPLETENESS > :v"),
				SubstitutionMap: map[string]string{":v": "1"},
				Threshold:       tc.threshold,
				ColumnSelectors: []types.ColumnSelector{{Name: aws.String("c1")}, {Regex: aws.String("^x.*")}},
			}
			_, err := client.CreateRuleset(t.Context(), &databrewsdk.CreateRulesetInput{
				Name: aws.String("rs"), TargetArn: aws.String("arn:aws:databrew:us-east-1:123456789012:dataset/d"),
				Rules: []types.Rule{rule},
			})
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)

				return
			}
			require.NoError(t, err)

			got, err := client.DescribeRuleset(t.Context(), &databrewsdk.DescribeRulesetInput{Name: aws.String("rs")})
			require.NoError(t, err)
			require.Len(t, got.Rules, 1)
			assert.InDelta(t, tc.threshold.Value, got.Rules[0].Threshold.Value, 0.0001)
			assert.Equal(t, tc.threshold.Type, got.Rules[0].Threshold.Type)
			assert.Equal(t, tc.threshold.Unit, got.Rules[0].Threshold.Unit)
			require.Len(t, got.Rules[0].ColumnSelectors, 2)
			assert.Equal(t, "c1", aws.ToString(got.Rules[0].ColumnSelectors[0].Name))
			assert.Equal(t, "^x.*", aws.ToString(got.Rules[0].ColumnSelectors[1].Regex))

			rule.Threshold = &types.Threshold{Value: 3, Type: "NEAR"}
			_, err = client.UpdateRuleset(t.Context(), &databrewsdk.UpdateRulesetInput{
				Name: aws.String("rs"), Rules: []types.Rule{rule},
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "ValidationException")
		})
	}
}

func TestProfileJob_TypedConfigurationRoundTrip(t *testing.T) {
	t.Parallel()

	okCfg := &types.ProfileConfiguration{
		DatasetStatisticsConfiguration: &types.StatisticsConfiguration{
			IncludedStatistics: []string{"MIN", "MAX"},
			Overrides: []types.StatisticOverride{{
				Statistic: aws.String("MIN"), Parameters: map[string]string{"k": "v"},
			}},
		},
		ColumnStatisticsConfigurations: []types.ColumnStatisticsConfiguration{{
			Selectors:  []types.ColumnSelector{{Name: aws.String("c1")}},
			Statistics: &types.StatisticsConfiguration{IncludedStatistics: []string{"MEDIAN"}},
		}},
		EntityDetectorConfiguration: &types.EntityDetectorConfiguration{
			EntityTypes:       []string{"USA_SSN"},
			AllowedStatistics: []types.AllowedStatistics{{Statistics: []string{"MIN"}}},
		},
		ProfileColumns: []types.ColumnSelector{{Regex: aws.String("^p")}},
	}

	tests := []struct {
		cfg     *types.ProfileConfiguration
		name    string
		wantErr string
		vcs     []types.ValidationConfiguration
	}{
		{
			name: "full",
			cfg:  okCfg,
			vcs: []types.ValidationConfiguration{
				{
					RulesetArn: aws.String(
						"arn:aws:databrew:us-east-1:123456789012:ruleset/rs",
					),
					ValidationMode: "CHECK_ALL",
				},
			},
		},
		{
			name: "empty_entity_types",
			cfg: &types.ProfileConfiguration{
				EntityDetectorConfiguration: &types.EntityDetectorConfiguration{EntityTypes: []string{}},
			},
			wantErr: "ValidationException",
		},
		{
			name: "override_without_statistic",
			cfg: &types.ProfileConfiguration{DatasetStatisticsConfiguration: &types.StatisticsConfiguration{
				Overrides: []types.StatisticOverride{{Statistic: aws.String(""), Parameters: map[string]string{}}},
			}},
			wantErr: "ValidationException",
		},
		{
			name: "bad_validation_mode",
			vcs: []types.ValidationConfiguration{{
				RulesetArn: aws.String("arn:aws:databrew:us-east-1:123456789012:ruleset/rs"), ValidationMode: "SOME",
			}},
			wantErr: "ValidationException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTypedConfigClient(t)
			_, err := client.CreateDataset(t.Context(), &databrewsdk.CreateDatasetInput{
				Name: aws.String("ds"),
				Input: &types.Input{
					S3InputDefinition: &types.S3Location{Bucket: aws.String("b"), Key: aws.String("k")},
				},
			})
			require.NoError(t, err)

			_, err = client.CreateProfileJob(t.Context(), &databrewsdk.CreateProfileJobInput{
				Name: aws.String("pj"), DatasetName: aws.String("ds"),
				RoleArn:                  aws.String("arn:aws:iam::123456789012:role/r"),
				OutputLocation:           &types.S3Location{Bucket: aws.String("out")},
				Configuration:            tc.cfg,
				ValidationConfigurations: tc.vcs,
			})
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)

				return
			}
			require.NoError(t, err)

			got, err := client.DescribeJob(t.Context(), &databrewsdk.DescribeJobInput{Name: aws.String("pj")})
			require.NoError(t, err)
			assert.Equal(t, tc.cfg, got.ProfileConfiguration)
			require.Len(t, got.ValidationConfigurations, 1)
			assert.Equal(t, types.ValidationModeCheckAll, got.ValidationConfigurations[0].ValidationMode)

			_, err = client.UpdateProfileJob(t.Context(), &databrewsdk.UpdateProfileJobInput{
				Name: aws.String("pj"), RoleArn: aws.String("arn:aws:iam::123456789012:role/r"),
				OutputLocation: &types.S3Location{Bucket: aws.String("out")},
				Configuration: &types.ProfileConfiguration{
					EntityDetectorConfiguration: &types.EntityDetectorConfiguration{EntityTypes: []string{}},
				},
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "ValidationException")
		})
	}
}
