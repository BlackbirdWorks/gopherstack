package integration_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_RDS_DBInstanceAvailableWaiter verifies that DBInstanceAvailableWaiter
// succeeds immediately after CreateDBInstance because the status is "available".
func TestIntegration_RDS_DBInstanceAvailableWaiter(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createRDSClient(t)
	ctx := t.Context()

	id := "waiter-rds-" + uuid.NewString()[:8]

	_, err := client.CreateDBInstance(ctx, &rdssdk.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String(id),
		DBInstanceClass:      aws.String("db.t3.micro"),
		Engine:               aws.String("postgres"),
		MasterUsername:       aws.String("admin"),
		MasterUserPassword:   aws.String("password123"),
		AllocatedStorage:     aws.Int32(20),
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = client.DeleteDBInstance(ctx, &rdssdk.DeleteDBInstanceInput{
			DBInstanceIdentifier: aws.String(id),
			SkipFinalSnapshot:    aws.Bool(true),
		})
	})

	waiter := rdssdk.NewDBInstanceAvailableWaiter(client)
	start := time.Now()
	err = waiter.Wait(ctx, &rdssdk.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(id),
	}, 5*time.Second)
	elapsed := time.Since(start)

	require.NoError(t, err, "DBInstanceAvailableWaiter should succeed after instance is created")
	assert.Less(t, elapsed, 2*time.Second, "DBInstanceAvailableWaiter should complete quickly, took %v", elapsed)
}

// TestIntegration_RDS_DBInstanceDeletedWaiter verifies that DBInstanceDeletedWaiter
// succeeds after DeleteDBInstance.
func TestIntegration_RDS_DBInstanceDeletedWaiter(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createRDSClient(t)
	ctx := t.Context()

	id := "waiter-del-rds-" + uuid.NewString()[:8]

	_, err := client.CreateDBInstance(ctx, &rdssdk.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String(id),
		DBInstanceClass:      aws.String("db.t3.micro"),
		Engine:               aws.String("postgres"),
		MasterUsername:       aws.String("admin"),
		MasterUserPassword:   aws.String("password123"),
		AllocatedStorage:     aws.Int32(20),
	})
	require.NoError(t, err)

	// Delete the instance
	_, err = client.DeleteDBInstance(ctx, &rdssdk.DeleteDBInstanceInput{
		DBInstanceIdentifier: aws.String(id),
		SkipFinalSnapshot:    aws.Bool(true),
	})
	require.NoError(t, err)

	waiter := rdssdk.NewDBInstanceDeletedWaiter(client)
	start := time.Now()
	err = waiter.Wait(ctx, &rdssdk.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(id),
	}, 5*time.Second)
	elapsed := time.Since(start)

	require.NoError(t, err, "DBInstanceDeletedWaiter should succeed after instance is deleted")
	assert.Less(t, elapsed, 2*time.Second, "DBInstanceDeletedWaiter should complete quickly, took %v", elapsed)
}
