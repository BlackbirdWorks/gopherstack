package terraform_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dsqlsdk "github.com/aws/aws-sdk-go-v2/service/dsql"
	"github.com/aws/aws-sdk-go-v2/service/dsql/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createDSQLClient returns an Aurora DSQL client pointed at the shared test container.
func createDSQLClient(t *testing.T) *dsqlsdk.Client {
	t.Helper()

	return createClientWithEndpoint(t, dsqlsdk.NewFromConfig, endpoint)
}

// TestTerraform_AuroraDsql provisions an aws_dsql_cluster via Terraform, then
// verifies it is visible via the Aurora DSQL SDK with the expected
// deletion-protection setting, tags, and a wire-shaped VPC endpoint service
// name. The pinned aws provider (~> 5.0, resolving to hashicorp/aws
// v5.100.0) does not yet expose an aws_dsql_cluster_policy resource, so
// cluster-policy coverage lives in services/dsql's own unit tests instead.
func TestTerraform_AuroraDsql(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "aurora-dsql",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{}
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()

				client := createDSQLClient(t)

				listOut, err := client.ListClusters(ctx, &dsqlsdk.ListClustersInput{})
				require.NoError(t, err, "ListClusters should succeed after terraform apply")
				require.Len(t, listOut.Clusters, 1, "exactly one cluster should exist after terraform apply")

				identifier := listOut.Clusters[0].Identifier

				out, err := client.GetCluster(ctx, &dsqlsdk.GetClusterInput{Identifier: identifier})
				require.NoError(t, err, "GetCluster should succeed after terraform apply")
				assert.False(t, aws.ToBool(out.DeletionProtectionEnabled))
				assert.Equal(t, map[string]string{"Environment": "test", "Owner": "terraform"}, out.Tags)

				vpcOut, err := client.GetVpcEndpointServiceName(ctx, &dsqlsdk.GetVpcEndpointServiceNameInput{
					Identifier: identifier,
				})
				require.NoError(t, err, "GetVpcEndpointServiceName should succeed after terraform apply")
				assert.NotEmpty(t, aws.ToString(vpcOut.ServiceName))

				var apiErr smithy.APIError

				_, err = client.GetClusterPolicy(ctx, &dsqlsdk.GetClusterPolicyInput{Identifier: identifier})
				require.Error(t, err, "no cluster policy was set by the terraform fixture")
				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())
				assert.Equal(t, types.ClusterStatusActive, out.Status)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}
