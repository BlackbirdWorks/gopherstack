package docdb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	docdbsdk "github.com/aws/aws-sdk-go-v2/service/docdb"
	"github.com/aws/aws-sdk-go-v2/service/docdb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/docdb"
)

// TestCreateOpsWithTags_RoundTrip drives every docdb Create* op whose real
// Input struct accepts Tags (docdb@v1.51.4: CreateDBCluster.go:239,
// CreateDBInstance.go, CreateDBSubnetGroup.go, CreateDBClusterParameterGroup.go,
// CreateDBClusterSnapshot.go, CreateEventSubscription.go:99) through the real
// SDK client and asserts ListTagsForResource sees what was supplied at
// creation (gopherstack-2mwl). CreateGlobalCluster has no Tags field in the
// real SDK, so it is excluded.
func TestCreateOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, client *docdbsdk.Client) string
		name  string
	}{
		{
			name: "db cluster",
			setup: func(t *testing.T, client *docdbsdk.Client) string {
				t.Helper()
				out, err := client.CreateDBCluster(t.Context(), &docdbsdk.CreateDBClusterInput{
					DBClusterIdentifier: aws.String("tagged-cluster"),
					Engine:              aws.String("docdb"),
					Tags:                []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.DBCluster.DBClusterArn)
			},
		},
		{
			name: "db instance",
			setup: func(t *testing.T, client *docdbsdk.Client) string {
				t.Helper()
				_, err := client.CreateDBCluster(t.Context(), &docdbsdk.CreateDBClusterInput{
					DBClusterIdentifier: aws.String("inst-source-cluster"),
					Engine:              aws.String("docdb"),
				})
				require.NoError(t, err)

				out, err := client.CreateDBInstance(t.Context(), &docdbsdk.CreateDBInstanceInput{
					DBInstanceIdentifier: aws.String("tagged-instance"),
					DBClusterIdentifier:  aws.String("inst-source-cluster"),
					DBInstanceClass:      aws.String("db.r5.large"),
					Engine:               aws.String("docdb"),
					Tags:                 []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.DBInstance.DBInstanceArn)
			},
		},
		{
			name: "db subnet group",
			setup: func(t *testing.T, client *docdbsdk.Client) string {
				t.Helper()
				out, err := client.CreateDBSubnetGroup(t.Context(), &docdbsdk.CreateDBSubnetGroupInput{
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
			name: "db cluster parameter group",
			setup: func(t *testing.T, client *docdbsdk.Client) string {
				t.Helper()
				out, err := client.CreateDBClusterParameterGroup(
					t.Context(),
					&docdbsdk.CreateDBClusterParameterGroupInput{
						DBClusterParameterGroupName: aws.String("tagged-cpg"),
						DBParameterGroupFamily:      aws.String("docdb4.0"),
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
			setup: func(t *testing.T, client *docdbsdk.Client) string {
				t.Helper()
				_, err := client.CreateDBCluster(t.Context(), &docdbsdk.CreateDBClusterInput{
					DBClusterIdentifier: aws.String("snap-source-cluster"),
					Engine:              aws.String("docdb"),
				})
				require.NoError(t, err)

				out, err := client.CreateDBClusterSnapshot(t.Context(), &docdbsdk.CreateDBClusterSnapshotInput{
					DBClusterSnapshotIdentifier: aws.String("tagged-snap"),
					DBClusterIdentifier:         aws.String("snap-source-cluster"),
					Tags:                        []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.DBClusterSnapshot.DBClusterSnapshotArn)
			},
		},
		{
			name: "event subscription",
			setup: func(t *testing.T, client *docdbsdk.Client) string {
				t.Helper()
				out, err := client.CreateEventSubscription(t.Context(), &docdbsdk.CreateEventSubscriptionInput{
					SubscriptionName: aws.String("tagged-event-sub"),
					SnsTopicArn:      aws.String("arn:aws:sns:us-east-1:000000000000:topic"),
					Tags:             []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.EventSubscription.EventSubscriptionArn)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := docdb.NewInMemoryBackend("000000000000", rtTestRegion)
			h := docdb.NewHandler(backend)
			client := newTestDocDBClient(t, h)

			resourceARN := tt.setup(t, client)
			require.NotEmpty(t, resourceARN)

			out, err := client.ListTagsForResource(t.Context(), &docdbsdk.ListTagsForResourceInput{
				ResourceName: aws.String(resourceARN),
			})
			require.NoError(t, err)

			require.Len(t, out.TagList, 1)
			assert.Equal(t, "env", aws.ToString(out.TagList[0].Key))
			assert.Equal(t, "prod", aws.ToString(out.TagList[0].Value))
		})
	}
}
