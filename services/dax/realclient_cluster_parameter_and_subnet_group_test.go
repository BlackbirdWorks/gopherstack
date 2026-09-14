package dax_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	daxsdk "github.com/aws/aws-sdk-go-v2/service/dax"
	"github.com/aws/aws-sdk-go-v2/service/dax/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dax"
)

// TestRealClient_ClusterParameterAndSubnetGroup drives dax's typed-coverage-blind ops
// (gopherstack-n3zi) through the real aws-sdk-go-v2 client.
func TestRealClient_ClusterParameterAndSubnetGroup(t *testing.T) {
	t.Parallel()
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "cluster lifecycle", run: func(t *testing.T) {
			t.Helper()

			backend := dax.NewInMemoryBackend("123456789012", "us-east-1")
			client := newTestDAXSDKClient(t, dax.NewHandler(backend))
			ctx := t.Context()

			_, err := client.CreateCluster(ctx, &daxsdk.CreateClusterInput{
				ClusterName:       aws.String("s15-cluster"),
				NodeType:          aws.String("dax.r5.large"),
				IamRoleArn:        aws.String("arn:aws:iam::123456789012:role/DAXRole"),
				ReplicationFactor: 1,
			})
			require.NoError(t, err)

			descOut, err := client.DescribeClusters(ctx, &daxsdk.DescribeClustersInput{
				ClusterNames: []string{"s15-cluster"},
			})
			require.NoError(t, err)
			require.Len(t, descOut.Clusters, 1)
			assert.Equal(t, "s15-cluster", aws.ToString(descOut.Clusters[0].ClusterName))

			updOut, err := client.UpdateCluster(ctx, &daxsdk.UpdateClusterInput{
				ClusterName: aws.String("s15-cluster"),
				Description: aws.String("s15 updated"),
			})
			require.NoError(t, err)
			assert.Equal(t, "s15 updated", aws.ToString(updOut.Cluster.Description))

			incOut, err := client.IncreaseReplicationFactor(ctx, &daxsdk.IncreaseReplicationFactorInput{
				ClusterName:          aws.String("s15-cluster"),
				NewReplicationFactor: 3,
			})
			require.NoError(t, err)
			assert.EqualValues(t, 3, aws.ToInt32(incOut.Cluster.TotalNodes))

			delOut, err := client.DeleteCluster(ctx, &daxsdk.DeleteClusterInput{ClusterName: aws.String("s15-cluster")})
			require.NoError(t, err)
			assert.Equal(t, "deleting", aws.ToString(delOut.Cluster.Status))

			_, err = client.DescribeClusters(ctx, &daxsdk.DescribeClustersInput{
				ClusterNames: []string{"s15-cluster"},
			})
			require.Error(t, err)
		}},
		{name: "parameter group lifecycle", run: func(t *testing.T) {
			t.Helper()

			backend := dax.NewInMemoryBackend("123456789012", "us-east-1")
			client := newTestDAXSDKClient(t, dax.NewHandler(backend))
			ctx := t.Context()

			_, err := client.CreateParameterGroup(ctx, &daxsdk.CreateParameterGroupInput{
				ParameterGroupName: aws.String("s15-params"),
				Description:        aws.String("s15 param group"),
			})
			require.NoError(t, err)

			updOut, err := client.UpdateParameterGroup(ctx, &daxsdk.UpdateParameterGroupInput{
				ParameterGroupName: aws.String("s15-params"),
				ParameterNameValues: []types.ParameterNameValue{
					{ParameterName: aws.String("record-ttl-millis"), ParameterValue: aws.String("60000")},
				},
			})
			require.NoError(t, err)
			assert.Equal(t, "s15-params", aws.ToString(updOut.ParameterGroup.ParameterGroupName))

			descOut, err := client.DescribeParameters(ctx, &daxsdk.DescribeParametersInput{
				ParameterGroupName: aws.String("s15-params"),
			})
			require.NoError(t, err)
			found := false
			for _, p := range descOut.Parameters {
				if aws.ToString(p.ParameterName) == "record-ttl-millis" {
					found = true
					assert.Equal(t, "60000", aws.ToString(p.ParameterValue))
				}
			}
			assert.True(t, found, "record-ttl-millis must appear in DescribeParameters")

			defaultOut, err := client.DescribeDefaultParameters(ctx, &daxsdk.DescribeDefaultParametersInput{})
			require.NoError(t, err)
			assert.NotEmpty(t, defaultOut.Parameters)
		}},
		{name: "subnet group update", run: func(t *testing.T) {
			t.Helper()

			backend := dax.NewInMemoryBackend("123456789012", "us-east-1")
			client := newTestDAXSDKClient(t, dax.NewHandler(backend))
			ctx := t.Context()

			_, err := client.CreateSubnetGroup(ctx, &daxsdk.CreateSubnetGroupInput{
				SubnetGroupName: aws.String("s15-subnets"),
				SubnetIds:       []string{"subnet-0123abcd"},
			})
			require.NoError(t, err)

			updOut, err := client.UpdateSubnetGroup(ctx, &daxsdk.UpdateSubnetGroupInput{
				SubnetGroupName: aws.String("s15-subnets"),
				Description:     aws.String("s15 updated subnets"),
				SubnetIds:       []string{"subnet-0123abcd", "subnet-04567890"},
			})
			require.NoError(t, err)
			assert.Equal(t, "s15 updated subnets", aws.ToString(updOut.SubnetGroup.Description))
			assert.Len(t, updOut.SubnetGroup.Subnets, 2)
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
