package rds_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestModifyCurrentDBClusterCapacity_ReturnsFullResult_RealClient covers a
// missing-field bug (gopherstack-7185, rdssweep1): the real
// ModifyCurrentDBClusterCapacityOutput carries PendingCapacity,
// SecondsBeforeTimeout and TimeoutAction alongside DBClusterIdentifier and
// CurrentCapacity (rds@v1.124.1 api_op_ModifyCurrentDBClusterCapacity.go) -
// gopherstack dropped all three, so a caller reading the timeout/action of an
// in-flight scaling request always saw zero values.
func TestModifyCurrentDBClusterCapacity_ReturnsFullResult_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestRDSHandler()
	client := newTestRDSClient(t, h)

	clusterID := "sweep1-cluster"
	_, err := client.CreateDBCluster(t.Context(), &rdssdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String(clusterID),
		Engine:              aws.String("aurora-postgresql"),
		MasterUsername:      aws.String("admin"),
		MasterUserPassword:  aws.String("SuperSecret123!"),
		EngineMode:          aws.String("serverless"),
	})
	require.NoError(t, err)

	out, err := client.ModifyCurrentDBClusterCapacity(t.Context(), &rdssdk.ModifyCurrentDBClusterCapacityInput{
		DBClusterIdentifier:  aws.String(clusterID),
		Capacity:             aws.Int32(4),
		SecondsBeforeTimeout: aws.Int32(120),
		TimeoutAction:        aws.String("RollbackCapacityChange"),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(4), aws.ToInt32(out.CurrentCapacity))
	assert.Equal(t, int32(4), aws.ToInt32(out.PendingCapacity),
		"PendingCapacity zero - ModifyCurrentDBClusterCapacity dropped it entirely")
	assert.Equal(t, int32(120), aws.ToInt32(out.SecondsBeforeTimeout),
		"SecondsBeforeTimeout zero - ModifyCurrentDBClusterCapacity dropped it entirely")
	assert.Equal(t, "RollbackCapacityChange", aws.ToString(out.TimeoutAction),
		"TimeoutAction empty - ModifyCurrentDBClusterCapacity dropped it entirely")
}
