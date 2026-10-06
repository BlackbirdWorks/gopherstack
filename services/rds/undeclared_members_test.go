package rds_test

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_UndeclaredMembersAbsent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		body   string
		want   string
		absent []string
	}{
		{
			name: "db_cluster",
			body: "Action=CreateDBCluster&Version=2014-10-31&DBClusterIdentifier=c1&Engine=aurora-mysql" +
				"&MasterUsername=admin&EnableOptimizedWrites=true",
			want:   "<DBClusterIdentifier>c1</DBClusterIdentifier>",
			absent: []string{"OptimizedWritesEnabled"},
		},
		{
			name: "db_instance",
			body: "Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=i1&DBInstanceClass=db.r6g.large" +
				"&Engine=postgres&MasterUsername=admin&AllocatedStorage=20&EnableOptimizedWrites=true&StorageOptimized=true",
			want:   "<DBInstanceIdentifier>i1</DBInstanceIdentifier>",
			absent: []string{"OptimizedWritesEnabled", "StorageOptimized"},
		},
		{
			name:   "global_cluster",
			body:   "Action=CreateGlobalCluster&Version=2014-10-31&GlobalClusterIdentifier=g1&Engine=aurora-mysql",
			want:   "<GlobalClusterIdentifier>g1</GlobalClusterIdentifier>",
			absent: []string{"PrimaryRegion"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := postRDSForm(t, newBatch3Handler(t), tc.body)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), tc.want)

			for _, a := range tc.absent {
				assert.NotContains(t, rec.Body.String(), a)
			}
		})
	}
}

func TestSDK_DBInstanceAndClusterRoundTripAfterTrim(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		id   string
	}{
		{name: "first", id: "rt-first"},
		{name: "second", id: "rt-second"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, client := newRealClientBackendAndClient(t)
			ctx := t.Context()

			cl, err := client.CreateDBCluster(ctx, &rdssdk.CreateDBClusterInput{
				DBClusterIdentifier: aws.String(tc.id),
				Engine:              aws.String("aurora-mysql"),
				MasterUsername:      aws.String("admin"),
			})
			require.NoError(t, err)
			assert.Equal(t, tc.id, aws.ToString(cl.DBCluster.DBClusterIdentifier))
			assert.NotEmpty(t, aws.ToString(cl.DBCluster.DBClusterArn))

			inst, err := client.CreateDBInstance(ctx, &rdssdk.CreateDBInstanceInput{
				DBInstanceIdentifier: aws.String(tc.id + "-i"),
				DBInstanceClass:      aws.String("db.r6g.large"),
				Engine:               aws.String("postgres"),
				MasterUsername:       aws.String("admin"),
				AllocatedStorage:     aws.Int32(20),
			})
			require.NoError(t, err)
			assert.Equal(t, tc.id+"-i", aws.ToString(inst.DBInstance.DBInstanceIdentifier))
			assert.NotEmpty(t, aws.ToString(inst.DBInstance.DBInstanceArn))
			assert.Equal(t, int32(20), aws.ToInt32(inst.DBInstance.AllocatedStorage))
		})
	}
}
