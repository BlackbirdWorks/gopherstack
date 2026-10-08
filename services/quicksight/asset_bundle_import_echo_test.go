package quicksight_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	quicksightsdk "github.com/aws/aws-sdk-go-v2/service/quicksight"
	qstypes "github.com/aws/aws-sdk-go-v2/service/quicksight/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAssetBundleImportJob_OverridesEchoed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		strategy   *qstypes.AssetBundleImportJobOverrideValidationStrategy
		tags       *qstypes.AssetBundleImportJobOverrideTags
		wantStrict *bool
		name       string
		wantTags   bool
	}{
		{name: "none"},
		{
			name:       "strict",
			strategy:   &qstypes.AssetBundleImportJobOverrideValidationStrategy{StrictModeForAllResources: true},
			wantStrict: aws.Bool(true),
		},
		{
			name: "tags",
			tags: &qstypes.AssetBundleImportJobOverrideTags{
				DataSources: []qstypes.AssetBundleImportJobDataSourceOverrideTags{{
					DataSourceIds: []string{"ds-1"},
					Tags:          []qstypes.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
				}},
			},
			wantTags: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newQuickSightTestClient(t)
			ctx := t.Context()

			_, err := client.StartAssetBundleImportJob(ctx, &quicksightsdk.StartAssetBundleImportJobInput{
				AwsAccountId:               aws.String("000000000000"),
				AssetBundleImportJobId:     aws.String("job-1"),
				AssetBundleImportSource:    &qstypes.AssetBundleImportSource{Body: []byte("zip")},
				OverrideValidationStrategy: tt.strategy,
				OverrideTags:               tt.tags,
			})
			require.NoError(t, err)

			out, err := client.DescribeAssetBundleImportJob(ctx, &quicksightsdk.DescribeAssetBundleImportJobInput{
				AwsAccountId: aws.String("000000000000"), AssetBundleImportJobId: aws.String("job-1"),
			})
			require.NoError(t, err)

			if tt.wantStrict == nil {
				assert.Nil(t, out.OverrideValidationStrategy)
			} else {
				require.NotNil(t, out.OverrideValidationStrategy)
				assert.Equal(t, *tt.wantStrict, out.OverrideValidationStrategy.StrictModeForAllResources)
			}

			if tt.wantTags {
				require.NotNil(t, out.OverrideTags)
				require.Len(t, out.OverrideTags.DataSources, 1)
				assert.Equal(t, []string{"ds-1"}, out.OverrideTags.DataSources[0].DataSourceIds)
			} else {
				assert.Nil(t, out.OverrideTags)
			}
		})
	}
}
