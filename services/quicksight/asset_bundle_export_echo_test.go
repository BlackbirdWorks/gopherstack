package quicksight_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	quicksightsdk "github.com/aws/aws-sdk-go-v2/service/quicksight"
	qstypes "github.com/aws/aws-sdk-go-v2/service/quicksight/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// StartAssetBundleExportJob ValidationStrategy / CloudFormationOverridePropertyConfiguration are echoed by Describe.
func TestAssetBundleExportJob_ValidationStrategyAndOverridesEchoed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		strategy   *qstypes.AssetBundleExportJobValidationStrategy
		wantStrict *bool
		name       string
	}{
		{name: "omitted"},
		{
			name:       "strict",
			strategy:   &qstypes.AssetBundleExportJobValidationStrategy{StrictModeForAllResources: true},
			wantStrict: aws.Bool(true),
		},
		{
			name:       "lenient",
			strategy:   &qstypes.AssetBundleExportJobValidationStrategy{StrictModeForAllResources: false},
			wantStrict: aws.Bool(false),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newQuickSightTestClient(t)
			ctx := t.Context()

			in := &quicksightsdk.StartAssetBundleExportJobInput{
				AwsAccountId:           aws.String("000000000000"),
				AssetBundleExportJobId: aws.String("job-1"),
				ExportFormat:           qstypes.AssetBundleExportFormatCloudformationJson,
				ResourceArns:           []string{"arn:aws:quicksight:us-east-1:000000000000:dashboard/d1"},
				ValidationStrategy:     tt.strategy,
				CloudFormationOverridePropertyConfiguration: &qstypes.AssetBundleCloudFormationOverridePropertyConfiguration{
					ResourceIdOverrideConfiguration: &qstypes.AssetBundleExportJobResourceIdOverrideConfiguration{
						PrefixForAllResources: true,
					},
				},
			}

			_, err := client.StartAssetBundleExportJob(ctx, in)
			require.NoError(t, err)

			out, err := client.DescribeAssetBundleExportJob(ctx, &quicksightsdk.DescribeAssetBundleExportJobInput{
				AwsAccountId: aws.String("000000000000"), AssetBundleExportJobId: aws.String("job-1"),
			})
			require.NoError(t, err)

			if tt.wantStrict == nil {
				assert.Nil(t, out.ValidationStrategy)
			} else {
				require.NotNil(t, out.ValidationStrategy)
				assert.Equal(t, *tt.wantStrict, out.ValidationStrategy.StrictModeForAllResources)
			}

			require.NotNil(t, out.CloudFormationOverridePropertyConfiguration)
			require.NotNil(t, out.CloudFormationOverridePropertyConfiguration.ResourceIdOverrideConfiguration)
			assert.True(
				t,
				out.CloudFormationOverridePropertyConfiguration.ResourceIdOverrideConfiguration.PrefixForAllResources,
			)
		})
	}
}
