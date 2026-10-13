package rds_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createFlowInstance(t *testing.T, client *rdssdk.Client, id string) {
	t.Helper()

	_, err := client.CreateDBInstance(t.Context(), &rdssdk.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String(id), Engine: aws.String("postgres"),
		DBInstanceClass: aws.String("db.t3.small"), MasterUsername: aws.String("flowadmin"),
		MasterUserPassword: aws.String("Passw0rd!x"), AllocatedStorage: aws.Int32(20),
	})
	require.NoError(t, err)
}

func TestDBInstanceEndpointWire(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		engine   string
		wantPort int32
	}{
		{name: "postgres", engine: "postgres", wantPort: 5432},
		{name: "mysql", engine: "mysql", wantPort: 3306},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestRDSClient(t, newTestRDSHandler(t))

			out, err := client.CreateDBInstance(t.Context(), &rdssdk.CreateDBInstanceInput{
				DBInstanceIdentifier: aws.String("ep-db"), Engine: aws.String(tt.engine),
				DBInstanceClass: aws.String("db.t3.micro"), MasterUsername: aws.String("admin"),
				MasterUserPassword: aws.String("Passw0rd!x"), AllocatedStorage: aws.Int32(20),
			})
			require.NoError(t, err)
			require.NotNil(t, out.DBInstance.Endpoint)
			assert.NotEmpty(t, aws.ToString(out.DBInstance.Endpoint.Address))
			assert.Equal(t, tt.wantPort, aws.ToInt32(out.DBInstance.Endpoint.Port))
		})
	}
}

func TestRestoreFromSnapshotKeepsSourceIdentity(t *testing.T) {
	t.Parallel()

	client := newTestRDSClient(t, newTestRDSHandler(t))
	createFlowInstance(t, client, "flow-src")

	snap, err := client.CreateDBSnapshot(t.Context(), &rdssdk.CreateDBSnapshotInput{
		DBInstanceIdentifier: aws.String("flow-src"), DBSnapshotIdentifier: aws.String("flow-snap"),
	})
	require.NoError(t, err)
	assert.Equal(t, "flowadmin", aws.ToString(snap.DBSnapshot.MasterUsername))

	tests := []struct {
		name      string
		class     *string
		wantClass string
	}{
		{name: "inherits class", wantClass: "db.t3.small"},
		{name: "class override", class: aws.String("db.t3.medium"), wantClass: "db.t3.medium"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			restored, restoreErr := client.RestoreDBInstanceFromDBSnapshot(t.Context(),
				&rdssdk.RestoreDBInstanceFromDBSnapshotInput{
					DBInstanceIdentifier: aws.String("flow-restored-" + tt.wantClass[len(tt.wantClass)-5:]),
					DBSnapshotIdentifier: aws.String("flow-snap"), DBInstanceClass: tt.class,
				})
			require.NoError(t, restoreErr)

			inst := restored.DBInstance
			assert.Equal(t, "flowadmin", aws.ToString(inst.MasterUsername))
			assert.Equal(t, tt.wantClass, aws.ToString(inst.DBInstanceClass))
			assert.Equal(t, "creating", aws.ToString(inst.DBInstanceStatus))
		})
	}
}

func TestParameterGroupPendingReboot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		applyMethod types.ApplyMethod
		wantStatus  string
	}{
		{name: "pending reboot", applyMethod: types.ApplyMethodPendingReboot, wantStatus: "pending-reboot"},
		{name: "immediate", applyMethod: types.ApplyMethodImmediate, wantStatus: "in-sync"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestRDSClient(t, newTestRDSHandler(t))
			createFlowInstance(t, client, "pg-db")

			_, err := client.CreateDBParameterGroup(t.Context(), &rdssdk.CreateDBParameterGroupInput{
				DBParameterGroupName: aws.String("flow-pg"), DBParameterGroupFamily: aws.String("postgres15"),
				Description: aws.String("d"),
			})
			require.NoError(t, err)

			_, err = client.ModifyDBInstance(t.Context(), &rdssdk.ModifyDBInstanceInput{
				DBInstanceIdentifier: aws.String("pg-db"), DBParameterGroupName: aws.String("flow-pg"),
				ApplyImmediately: aws.Bool(true),
			})
			require.NoError(t, err)

			status := func() string {
				out, descErr := client.DescribeDBInstances(t.Context(), &rdssdk.DescribeDBInstancesInput{
					DBInstanceIdentifier: aws.String("pg-db"),
				})
				require.NoError(t, descErr)
				require.Len(t, out.DBInstances, 1)
				require.Len(t, out.DBInstances[0].DBParameterGroups, 1)

				return aws.ToString(out.DBInstances[0].DBParameterGroups[0].ParameterApplyStatus)
			}

			assert.Equal(t, "pending-reboot", status(), "attaching a new group needs a reboot")

			require.Eventually(t, func() bool {
				out, descErr := client.DescribeDBInstances(t.Context(), &rdssdk.DescribeDBInstancesInput{
					DBInstanceIdentifier: aws.String("pg-db"),
				})

				return descErr == nil && aws.ToString(out.DBInstances[0].DBInstanceStatus) == "available"
			}, 5*time.Second, 20*time.Millisecond)

			_, err = client.RebootDBInstance(t.Context(), &rdssdk.RebootDBInstanceInput{
				DBInstanceIdentifier: aws.String("pg-db"),
			})
			require.NoError(t, err)
			assert.Equal(t, "in-sync", status())

			_, err = client.ModifyDBParameterGroup(t.Context(), &rdssdk.ModifyDBParameterGroupInput{
				DBParameterGroupName: aws.String("flow-pg"),
				Parameters: []types.Parameter{{
					ParameterName: aws.String("max_connections"), ParameterValue: aws.String("200"),
					ApplyMethod: tt.applyMethod,
				}},
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, status())
		})
	}
}
