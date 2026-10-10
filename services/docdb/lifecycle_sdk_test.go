package docdb_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	docdbsdk "github.com/aws/aws-sdk-go-v2/service/docdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/docdb"
)

func TestSDK_CreateReportsCreatingUntilDelay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantFirst  string
		delay      time.Duration
		wantWaiter bool
	}{
		{name: "instant", delay: 0, wantFirst: "available"},
		{name: "delayed", delay: 150 * time.Millisecond, wantFirst: "creating", wantWaiter: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := docdb.NewInMemoryBackend("123456789012", "us-east-1")
			backend.SetLifecycleDelay(tt.delay)
			client := newTestDocDBClient(t, docdb.NewHandler(backend))
			ctx := t.Context()

			cl, err := client.CreateDBCluster(ctx, &docdbsdk.CreateDBClusterInput{
				DBClusterIdentifier: aws.String("c-1"), Engine: aws.String("docdb"),
				MasterUsername: aws.String("admin"), MasterUserPassword: aws.String("password-123"),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantFirst, aws.ToString(cl.DBCluster.Status))

			inst, err := client.CreateDBInstance(ctx, &docdbsdk.CreateDBInstanceInput{
				DBInstanceIdentifier: aws.String("i-1"), DBInstanceClass: aws.String("db.r5.large"),
				Engine: aws.String("docdb"), DBClusterIdentifier: aws.String("c-1"),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantFirst, aws.ToString(inst.DBInstance.DBInstanceStatus))

			waiter := docdbsdk.NewDBInstanceAvailableWaiter(client, func(o *docdbsdk.DBInstanceAvailableWaiterOptions) {
				o.MinDelay = 20 * time.Millisecond
				o.MaxDelay = 50 * time.Millisecond
			})
			require.NoError(t, waiter.Wait(ctx, &docdbsdk.DescribeDBInstancesInput{
				DBInstanceIdentifier: aws.String("i-1"),
			}, 10*time.Second))

			require.Eventually(t, func() bool {
				out, derr := client.DescribeDBClusters(ctx, &docdbsdk.DescribeDBClustersInput{
					DBClusterIdentifier: aws.String("c-1"),
				})

				return derr == nil && aws.ToString(out.DBClusters[0].Status) == "available"
			}, 5*time.Second, 20*time.Millisecond)
		})
	}
}
