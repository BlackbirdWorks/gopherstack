package appsync_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appsyncsdk "github.com/aws/aws-sdk-go-v2/service/appsync"
	appsynctypes "github.com/aws/aws-sdk-go-v2/service/appsync/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

func TestGraphqlAPI_EnhancedMetricsAndMergedRole_RealClient(t *testing.T) {
	t.Parallel()

	const roleARN = "arn:aws:iam::000000000000:role/merged-exec"

	valid := &appsynctypes.EnhancedMetricsConfig{
		DataSourceLevelMetricsBehavior: appsynctypes.DataSourceLevelMetricsBehaviorPerDataSourceMetrics,
		OperationLevelMetricsConfig:    appsynctypes.OperationLevelMetricsConfigEnabled,
		ResolverLevelMetricsBehavior:   appsynctypes.ResolverLevelMetricsBehaviorFullRequestResolverMetrics,
	}
	invalid := &appsynctypes.EnhancedMetricsConfig{
		DataSourceLevelMetricsBehavior: "BOGUS",
		OperationLevelMetricsConfig:    appsynctypes.OperationLevelMetricsConfigEnabled,
		ResolverLevelMetricsBehavior:   appsynctypes.ResolverLevelMetricsBehaviorPerResolverMetrics,
	}

	tests := []struct {
		name    string
		emc     *appsynctypes.EnhancedMetricsConfig
		wantErr string
	}{
		{name: "round_trips", emc: valid},
		{name: "invalid_enum_rejected", emc: invalid, wantErr: "BadRequestException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestAppsyncClient(t, appsync.NewHandler(
				appsync.NewInMemoryBackend("000000000000", tagsRTRegion, ""),
			))

			created, err := client.CreateGraphqlApi(t.Context(), &appsyncsdk.CreateGraphqlApiInput{
				Name:                      aws.String("metrics-api"),
				AuthenticationType:        appsynctypes.AuthenticationTypeApiKey,
				ApiType:                   appsynctypes.GraphQLApiTypeMerged,
				EnhancedMetricsConfig:     tt.emc,
				MergedApiExecutionRoleArn: aws.String(roleARN),
			})
			if tt.wantErr != "" {
				var apiErr smithy.APIError
				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, tt.wantErr, apiErr.ErrorCode())

				return
			}

			require.NoError(t, err)

			apiID := created.GraphqlApi.ApiId
			assert.Equal(t, tt.emc, created.GraphqlApi.EnhancedMetricsConfig)
			assert.Equal(t, roleARN, aws.ToString(created.GraphqlApi.MergedApiExecutionRoleArn))

			got, err := client.GetGraphqlApi(t.Context(), &appsyncsdk.GetGraphqlApiInput{ApiId: apiID})
			require.NoError(t, err)
			assert.Equal(t, tt.emc, got.GraphqlApi.EnhancedMetricsConfig)

			updated, err := client.UpdateGraphqlApi(t.Context(), &appsyncsdk.UpdateGraphqlApiInput{
				ApiId:              apiID,
				Name:               aws.String("metrics-api"),
				AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
				EnhancedMetricsConfig: &appsynctypes.EnhancedMetricsConfig{
					DataSourceLevelMetricsBehavior: appsynctypes.DataSourceLevelMetricsBehaviorFullRequestDataSourceMetrics,
					OperationLevelMetricsConfig:    appsynctypes.OperationLevelMetricsConfigDisabled,
					ResolverLevelMetricsBehavior:   appsynctypes.ResolverLevelMetricsBehaviorPerResolverMetrics,
				},
			})
			require.NoError(t, err)
			assert.Equal(t, appsynctypes.OperationLevelMetricsConfigDisabled,
				updated.GraphqlApi.EnhancedMetricsConfig.OperationLevelMetricsConfig)
			assert.Equal(t, roleARN, aws.ToString(updated.GraphqlApi.MergedApiExecutionRoleArn))

			list, err := client.ListGraphqlApis(t.Context(), &appsyncsdk.ListGraphqlApisInput{})
			require.NoError(t, err)
			require.Len(t, list.GraphqlApis, 1)
			assert.Equal(t, roleARN, aws.ToString(list.GraphqlApis[0].MergedApiExecutionRoleArn))
		})
	}
}
