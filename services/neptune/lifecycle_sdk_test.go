package neptune_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	neptunesdk "github.com/aws/aws-sdk-go-v2/service/neptune"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune"
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

			backend := neptune.NewInMemoryBackend("123456789012", "us-east-1")
			backend.SetLifecycleDelay(tt.delay)
			client := newTestNeptuneClient(t, neptune.NewHandler(backend))
			ctx := t.Context()

			cl, err := client.CreateDBCluster(ctx, &neptunesdk.CreateDBClusterInput{
				DBClusterIdentifier: aws.String("c-1"), Engine: aws.String("neptune"),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantFirst, aws.ToString(cl.DBCluster.Status))

			inst, err := client.CreateDBInstance(ctx, &neptunesdk.CreateDBInstanceInput{
				DBInstanceIdentifier: aws.String("i-1"), DBInstanceClass: aws.String("db.r5.large"),
				Engine: aws.String("neptune"), DBClusterIdentifier: aws.String("c-1"),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantFirst, aws.ToString(inst.DBInstance.DBInstanceStatus))

			waiter := neptunesdk.NewDBInstanceAvailableWaiter(
				client,
				func(o *neptunesdk.DBInstanceAvailableWaiterOptions) {
					o.MinDelay = 20 * time.Millisecond
					o.MaxDelay = 50 * time.Millisecond
				},
			)
			require.NoError(t, waiter.Wait(ctx, &neptunesdk.DescribeDBInstancesInput{
				DBInstanceIdentifier: aws.String("i-1"),
			}, 10*time.Second))

			require.Eventually(t, func() bool {
				out, derr := client.DescribeDBClusters(ctx, &neptunesdk.DescribeDBClustersInput{
					DBClusterIdentifier: aws.String("c-1"),
				})

				return derr == nil && aws.ToString(out.DBClusters[0].Status) == "available"
			}, 5*time.Second, 20*time.Millisecond)
		})
	}
}
