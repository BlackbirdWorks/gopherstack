package neptune_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	neptunesdk "github.com/aws/aws-sdk-go-v2/service/neptune"
	"github.com/aws/aws-sdk-go-v2/service/neptune/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune"
)

// TestCreateOpsWithTags_RoundTrip drives every neptune Create* op whose real
// Input struct accepts Tags (neptune@v1.48.4: api_op_CreateDBCluster.go:239-ish,
// CreateDBInstance, CreateDBClusterEndpoint.go:57, CreateDBClusterParameterGroup.go:79,
// CreateDBClusterSnapshot.go:59, CreateDBSubnetGroup.go:52, CreateDBParameterGroup.go:79,
// CreateEventSubscription.go:99, CreateGlobalCluster.go:68) through the real SDK
// client and asserts ListTagsForResource sees what was supplied at creation
// (gopherstack-2mwl). Only CreateDBCluster/CreateDBInstance decoded tags before
// this fix; the other seven silently dropped Tags on the floor, and
// validateResourceARN's kind switch (tags.go) was separately missing cases for
// cluster-endpoint/pg/es/global-cluster, so even an explicit TagResource call
// against those ARNs errored.
func TestCreateOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, client *neptunesdk.Client) string
		name  string
	}{
		{
			name: "db cluster",
			setup: func(t *testing.T, client *neptunesdk.Client) string {
				t.Helper()
				out, err := client.CreateDBCluster(t.Context(), &neptunesdk.CreateDBClusterInput{
					DBClusterIdentifier: aws.String("tagged-cluster"),
					Engine:              aws.String("neptune"),
					Tags:                []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.DBCluster.DBClusterArn)
			},
		},
		{
			name: "db instance",
			setup: func(t *testing.T, client *neptunesdk.Client) string {
				t.Helper()
				_, err := client.CreateDBCluster(t.Context(), &neptunesdk.CreateDBClusterInput{
					DBClusterIdentifier: aws.String("inst-source-cluster"),
					Engine:              aws.String("neptune"),
				})
				require.NoError(t, err)

				out, err := client.CreateDBInstance(t.Context(), &neptunesdk.CreateDBInstanceInput{
					DBInstanceIdentifier: aws.String("tagged-instance"),
					DBClusterIdentifier:  aws.String("inst-source-cluster"),
					DBInstanceClass:      aws.String("db.r5.large"),
					Engine:               aws.String("neptune"),
					Tags:                 []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.DBInstance.DBInstanceArn)
			},
		},
		{
			name: "db cluster endpoint",
			setup: func(t *testing.T, client *neptunesdk.Client) string {
				t.Helper()
				_, err := client.CreateDBCluster(t.Context(), &neptunesdk.CreateDBClusterInput{
					DBClusterIdentifier: aws.String("ep-source-cluster"),
					Engine:              aws.String("neptune"),
				})
				require.NoError(t, err)

				out, err := client.CreateDBClusterEndpoint(t.Context(), &neptunesdk.CreateDBClusterEndpointInput{
					DBClusterEndpointIdentifier: aws.String("tagged-endpoint"),
					DBClusterIdentifier:         aws.String("ep-source-cluster"),
					EndpointType:                aws.String("READER"),
					Tags:                        []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.DBClusterEndpointArn)
			},
		},
		{
			name: "db cluster parameter group",
			setup: func(t *testing.T, client *neptunesdk.Client) string {
				t.Helper()
				out, err := client.CreateDBClusterParameterGroup(
					t.Context(),
					&neptunesdk.CreateDBClusterParameterGroupInput{
						DBClusterParameterGroupName: aws.String("tagged-cpg"),
						DBParameterGroupFamily:      aws.String("neptune1.3"),
						Description:                 aws.String("test"),
						Tags:                        []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
					},
				)
				require.NoError(t, err)

				return aws.ToString(out.DBClusterParameterGroup.DBClusterParameterGroupArn)
			},
		},
		{
			name: "db cluster snapshot",
			setup: func(t *testing.T, client *neptunesdk.Client) string {
				t.Helper()
				_, err := client.CreateDBCluster(t.Context(), &neptunesdk.CreateDBClusterInput{
					DBClusterIdentifier: aws.String("snap-source-cluster"),
					Engine:              aws.String("neptune"),
				})
				require.NoError(t, err)

				out, err := client.CreateDBClusterSnapshot(t.Context(), &neptunesdk.CreateDBClusterSnapshotInput{
					DBClusterSnapshotIdentifier: aws.String("tagged-snap"),
					DBClusterIdentifier:         aws.String("snap-source-cluster"),
					Tags:                        []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.DBClusterSnapshot.DBClusterSnapshotArn)
			},
		},
		{
			name: "db subnet group",
			setup: func(t *testing.T, client *neptunesdk.Client) string {
				t.Helper()
				out, err := client.CreateDBSubnetGroup(t.Context(), &neptunesdk.CreateDBSubnetGroupInput{
					DBSubnetGroupName:        aws.String("tagged-subnet-group"),
					DBSubnetGroupDescription: aws.String("test"),
					SubnetIds:                []string{"subnet-1"},
					Tags:                     []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.DBSubnetGroup.DBSubnetGroupArn)
			},
		},
		{
			name: "db parameter group",
			setup: func(t *testing.T, client *neptunesdk.Client) string {
				t.Helper()
				out, err := client.CreateDBParameterGroup(t.Context(), &neptunesdk.CreateDBParameterGroupInput{
					DBParameterGroupName:   aws.String("tagged-pg"),
					DBParameterGroupFamily: aws.String("neptune1.3"),
					Description:            aws.String("test"),
					Tags:                   []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.DBParameterGroup.DBParameterGroupArn)
			},
		},
		{
			name: "event subscription",
			setup: func(t *testing.T, client *neptunesdk.Client) string {
				t.Helper()
				out, err := client.CreateEventSubscription(t.Context(), &neptunesdk.CreateEventSubscriptionInput{
					SubscriptionName: aws.String("tagged-event-sub"),
					SnsTopicArn:      aws.String("arn:aws:sns:us-east-1:000000000000:topic"),
					Tags:             []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.EventSubscription.EventSubscriptionArn)
			},
		},
		{
			name: "global cluster",
			setup: func(t *testing.T, client *neptunesdk.Client) string {
				t.Helper()
				out, err := client.CreateGlobalCluster(t.Context(), &neptunesdk.CreateGlobalClusterInput{
					GlobalClusterIdentifier: aws.String("tagged-global"),
					Engine:                  aws.String("neptune"),
					EngineVersion:           aws.String("1.3.0.0"),
					Tags:                    []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.GlobalCluster.GlobalClusterArn)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := neptune.NewInMemoryBackend("000000000000", testRegion)
			h := neptune.NewHandler(backend)
			client := newTestNeptuneClient(t, h)

			resourceARN := tt.setup(t, client)
			require.NotEmpty(t, resourceARN)

			out, err := client.ListTagsForResource(t.Context(), &neptunesdk.ListTagsForResourceInput{
				ResourceName: aws.String(resourceARN),
			})
			require.NoError(t, err)

			require.Len(t, out.TagList, 1)
			assert.Equal(t, "env", aws.ToString(out.TagList[0].Key))
			assert.Equal(t, "prod", aws.ToString(out.TagList[0].Value))
		})
	}
}
