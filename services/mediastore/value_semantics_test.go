package mediastore_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	mediastoresdk "github.com/aws/aws-sdk-go-v2/service/mediastore"
	"github.com/aws/aws-sdk-go-v2/service/mediastore/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContainer_AccessLoggingAndPoliciesRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name         string
		startLogging bool
		stopLogging  bool
		wantLogging  bool
	}{
		{name: "default off"},
		{name: "start enables", startLogging: true, wantLogging: true},
		{name: "stop disables", startLogging: true, stopLogging: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := newTestMediaStoreClient(t, newTestHandler(t))
			name := aws.String("vs-container")

			created, err := client.CreateContainer(ctx, &mediastoresdk.CreateContainerInput{ContainerName: name})
			require.NoError(t, err)
			assert.False(t, aws.ToBool(created.Container.AccessLoggingEnabled))

			if tc.startLogging {
				_, err = client.StartAccessLogging(ctx, &mediastoresdk.StartAccessLoggingInput{ContainerName: name})
				require.NoError(t, err)
			}

			if tc.stopLogging {
				_, err = client.StopAccessLogging(ctx, &mediastoresdk.StopAccessLoggingInput{ContainerName: name})
				require.NoError(t, err)
			}

			desc, err := client.DescribeContainer(ctx, &mediastoresdk.DescribeContainerInput{ContainerName: name})
			require.NoError(t, err)
			assert.Equal(t, tc.wantLogging, aws.ToBool(desc.Container.AccessLoggingEnabled))
			assert.Equal(t, created.Container.CreationTime, desc.Container.CreationTime)
			assert.Equal(t, created.Container.Endpoint, desc.Container.Endpoint)
		})
	}
}

func TestContainer_CorsAndMetricPolicyReplacement(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := newTestMediaStoreClient(t, newTestHandler(t))
	name := aws.String("vs-policies")

	_, err := client.CreateContainer(ctx, &mediastoresdk.CreateContainerInput{ContainerName: name})
	require.NoError(t, err)

	_, err = client.PutCorsPolicy(ctx, &mediastoresdk.PutCorsPolicyInput{
		ContainerName: name,
		CorsPolicy: []types.CorsRule{{
			AllowedOrigins: []string{"*"}, AllowedHeaders: []string{"*"},
			AllowedMethods: []types.MethodName{types.MethodNameGet},
			ExposeHeaders:  []string{"x-a"}, MaxAgeSeconds: 60,
		}},
	})
	require.NoError(t, err)

	_, err = client.PutCorsPolicy(ctx, &mediastoresdk.PutCorsPolicyInput{
		ContainerName: name,
		CorsPolicy:    []types.CorsRule{{AllowedOrigins: []string{"a"}, AllowedHeaders: []string{"b"}}},
	})
	require.NoError(t, err)

	got, err := client.GetCorsPolicy(ctx, &mediastoresdk.GetCorsPolicyInput{ContainerName: name})
	require.NoError(t, err)
	require.Len(t, got.CorsPolicy, 1)
	assert.Empty(t, got.CorsPolicy[0].AllowedMethods)
	assert.Empty(t, got.CorsPolicy[0].ExposeHeaders)
	assert.Zero(t, got.CorsPolicy[0].MaxAgeSeconds)

	_, err = client.PutMetricPolicy(ctx, &mediastoresdk.PutMetricPolicyInput{
		ContainerName: name,
		MetricPolicy: &types.MetricPolicy{
			ContainerLevelMetrics: types.ContainerLevelMetricsEnabled,
			MetricPolicyRules: []types.MetricPolicyRule{
				{ObjectGroup: aws.String("/a/*"), ObjectGroupName: aws.String("g")},
			},
		},
	})
	require.NoError(t, err)

	_, err = client.PutMetricPolicy(ctx, &mediastoresdk.PutMetricPolicyInput{
		ContainerName: name,
		MetricPolicy:  &types.MetricPolicy{ContainerLevelMetrics: types.ContainerLevelMetricsDisabled},
	})
	require.NoError(t, err)

	mp, err := client.GetMetricPolicy(ctx, &mediastoresdk.GetMetricPolicyInput{ContainerName: name})
	require.NoError(t, err)
	assert.Equal(t, types.ContainerLevelMetricsDisabled, mp.MetricPolicy.ContainerLevelMetrics)
	assert.Empty(t, mp.MetricPolicy.MetricPolicyRules)
}
