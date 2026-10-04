package ecs_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecssdk "github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestECS_ClusterCapacityProviderList_RejectsUnknown(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantCode  string
		providers []string
	}{
		{name: "unknown", providers: []string{"typo-cp"}, wantCode: "ClientException"},
		{name: "builtin", providers: []string{"FARGATE", "FARGATE_SPOT"}},
		{name: "empty", providers: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestECSClient(t, newTestHandler(t))
			_, err := client.CreateCluster(t.Context(), &ecssdk.CreateClusterInput{ClusterName: aws.String("c")})
			require.NoError(t, err)

			_, putErr := client.PutClusterCapacityProviders(t.Context(), &ecssdk.PutClusterCapacityProvidersInput{
				Cluster:                         aws.String("c"),
				CapacityProviders:               tt.providers,
				DefaultCapacityProviderStrategy: []ecstypes.CapacityProviderStrategyItem{},
			})

			_, createErr := client.CreateCluster(t.Context(), &ecssdk.CreateClusterInput{
				ClusterName:       aws.String("c2"),
				CapacityProviders: tt.providers,
			})

			if tt.wantCode == "" {
				require.NoError(t, putErr)
				require.NoError(t, createErr)

				return
			}

			for _, e := range []error{putErr, createErr} {
				var apiErr smithy.APIError

				require.ErrorAs(t, e, &apiErr)
				assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
			}

			desc, err := client.DescribeClusters(
				t.Context(), &ecssdk.DescribeClustersInput{Clusters: []string{"c", "c2"}},
			)
			require.NoError(t, err)
			require.Len(t, desc.Clusters, 1)
			assert.Empty(t, desc.Clusters[0].CapacityProviders)
		})
	}
}
