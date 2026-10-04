package rds_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/blackbirdworks/gopherstack/services/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_DeleteTenantDatabaseFinalSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		finalID    *string
		skip       *bool
		errContain string
		wantSnap   bool
	}{
		{name: "final snapshot taken", finalID: aws.String("tfinal"), wantSnap: true},
		{name: "skip", skip: aws.Bool(true)},
		{name: "neither", errContain: "InvalidParameterValue"},
		{name: "both", finalID: aws.String("tfinal"), skip: aws.Bool(true), errContain: "InvalidParameterValue"},
		{name: "duplicate snapshot", finalID: aws.String("existing"), errContain: "DBSnapshotAlreadyExists"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend, client := newRealClientBackendAndClient(t)
			ctx := t.Context()

			_, err := backend.CreateDBInstance(
				"tinst", "oracle-ee-cdb", "db.t3.micro", "", "admin", "", 20, rds.DBInstanceOptions{},
			)
			require.NoError(t, err)
			waitForInstanceAvailable(t, backend, "tinst")
			_, err = backend.CreateTenantDatabase("tinst", "tenant1", "admin")
			require.NoError(t, err)
			_, err = backend.CreateDBSnapshot("existing", "tinst")
			require.NoError(t, err)

			_, err = client.DeleteTenantDatabase(ctx, &rdssdk.DeleteTenantDatabaseInput{
				DBInstanceIdentifier: aws.String("tinst"), TenantDBName: aws.String("tenant1"),
				FinalDBSnapshotIdentifier: tt.finalID, SkipFinalSnapshot: tt.skip,
			})

			left, derr := client.DescribeTenantDatabases(ctx, &rdssdk.DescribeTenantDatabasesInput{
				DBInstanceIdentifier: aws.String("tinst"),
			})
			require.NoError(t, derr)

			if tt.errContain != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContain)
				assert.Len(t, left.TenantDatabases, 1)

				return
			}
			require.NoError(t, err)
			assert.Empty(t, left.TenantDatabases)

			if !tt.wantSnap {
				return
			}
			snaps, err := client.DescribeDBSnapshots(ctx, &rdssdk.DescribeDBSnapshotsInput{
				DBSnapshotIdentifier: tt.finalID,
			})
			require.NoError(t, err)
			require.Len(t, snaps.DBSnapshots, 1)
			tds, err := client.DescribeDBSnapshotTenantDatabases(ctx, &rdssdk.DescribeDBSnapshotTenantDatabasesInput{
				DBSnapshotIdentifier: tt.finalID,
			})
			require.NoError(t, err)
			require.Len(t, tds.DBSnapshotTenantDatabases, 1)
			assert.Equal(t, "tenant1", aws.ToString(tds.DBSnapshotTenantDatabases[0].TenantDBName))
		})
	}
}
