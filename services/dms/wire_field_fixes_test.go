package dms_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dmssdk "github.com/aws/aws-sdk-go-v2/service/databasemigrationservice"
	"github.com/aws/aws-sdk-go-v2/service/databasemigrationservice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dms"
)

// TestCreateDataMigration_SettingsNestUnderDataMigrationSettings_RealClient
// proves DataMigration.DataMigrationSettings round-trips. The real
// deserializer (databasemigrationservice@v1.66.4 deserializers.go:16304,
// case "DataMigrationSettings") nests NumberOfJobs and a
// CloudwatchLogsEnabled field inside that sub-object -- gopherstack wrote
// both flat on DataMigration under EnableCloudwatchLogs, a name the real
// DataMigration case list (same file, same func) has no case for at all, so
// every real client's DataMigration.DataMigrationSettings decoded nil.
func TestCreateDataMigration_SettingsNestUnderDataMigrationSettings_RealClient(t *testing.T) {
	t.Parallel()

	b := dms.NewInMemoryBackend("123456789012", "us-east-1")
	h := dms.NewHandler(b)
	client := newTestDMSClient(t, h)

	out, err := client.CreateDataMigration(t.Context(), &dmssdk.CreateDataMigrationInput{
		DataMigrationName:          aws.String("wire-dm"),
		MigrationProjectIdentifier: aws.String("proj-1"),
		ServiceAccessRoleArn:       aws.String("arn:aws:iam::123456789012:role/dms-role"),
		DataMigrationType:          types.MigrationTypeValueFullLoad,
		NumberOfJobs:               aws.Int32(3),
		EnableCloudwatchLogs:       aws.Bool(true),
	})
	require.NoError(t, err, "real SDK client must decode CreateDataMigration without error")

	require.NotNil(t, out.DataMigration)
	require.NotNil(
		t,
		out.DataMigration.DataMigrationSettings,
		"DataMigration.DataMigrationSettings must decode non-nil",
	)
	assert.Equal(t, int32(3), *out.DataMigration.DataMigrationSettings.NumberOfJobs)
	assert.True(t, *out.DataMigration.DataMigrationSettings.CloudwatchLogsEnabled)
}

// TestReplicationInstance_SubnetGroupAndVpcSecurityGroups_RealClient proves a
// second write-only-state bug on ReplicationInstance, found sweeping past the
// already-audited KmsKeyId/DnsNameServers/NetworkType/PreferredMaintenanceWindow
// settings: real CreateReplicationInstanceInput
// (databasemigrationservice@v1.66.4 api_op_CreateReplicationInstance.go) also
// carries ReplicationSubnetGroupIdentifier and VpcSecurityGroupIds, and real
// types.ReplicationInstance's response carries both back (ReplicationSubnetGroup
// *types.ReplicationSubnetGroup, VpcSecurityGroups []types.VpcSecurityGroupMembership).
// gopherstack accepted neither: the response's ReplicationSubnetGroup was a
// hardcoded empty-identifier placeholder and VpcSecurityGroups a hardcoded
// empty list, regardless of what a real client requested -- a genuine
// accept-and-drop (VpcSecurityGroupIds was never even decoded) that also made
// an already-existing, readable field (ReplicationSubnetGroup.Identifier)
// permanently blank. VpcSecurityGroupIds is additionally accepted by
// ModifyReplicationInstance; asserted here too.
func TestReplicationInstance_SubnetGroupAndVpcSecurityGroups_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	client := newTestDMSClient(t, h)

	sgOut, err := client.CreateReplicationSubnetGroup(t.Context(), &dmssdk.CreateReplicationSubnetGroupInput{
		ReplicationSubnetGroupIdentifier:  aws.String("wire-sg"),
		ReplicationSubnetGroupDescription: aws.String("wire fixes test"),
		SubnetIds:                         []string{"subnet-1", "subnet-2"},
	})
	require.NoError(t, err)

	created, err := client.CreateReplicationInstance(t.Context(), &dmssdk.CreateReplicationInstanceInput{
		ReplicationInstanceIdentifier:    aws.String("subnet-vpc-ri"),
		ReplicationInstanceClass:         aws.String("dms.t3.micro"),
		ReplicationSubnetGroupIdentifier: sgOut.ReplicationSubnetGroup.ReplicationSubnetGroupIdentifier,
		VpcSecurityGroupIds:              []string{"sg-abc123"},
	})
	require.NoError(t, err)
	require.NotNil(t, created.ReplicationInstance)

	ri := created.ReplicationInstance
	require.NotNil(
		t, ri.ReplicationSubnetGroup,
		"ReplicationSubnetGroup must never be nil (terraform-provider-aws reads it unconditionally)",
	)
	assert.Equal(
		t,
		"wire-sg",
		aws.ToString(ri.ReplicationSubnetGroup.ReplicationSubnetGroupIdentifier),
		"CreateReplicationInstance must store and echo the real ReplicationSubnetGroupIdentifier, not a blank placeholder",
	)
	require.Len(t, ri.VpcSecurityGroups, 1)
	assert.Equal(t, "sg-abc123", aws.ToString(ri.VpcSecurityGroups[0].VpcSecurityGroupId))

	// An unknown subnet group identifier must be rejected, not silently
	// accepted and dropped.
	_, err = client.CreateReplicationInstance(t.Context(), &dmssdk.CreateReplicationInstanceInput{
		ReplicationInstanceIdentifier:    aws.String("subnet-vpc-ri-2"),
		ReplicationInstanceClass:         aws.String("dms.t3.micro"),
		ReplicationSubnetGroupIdentifier: aws.String("does-not-exist"),
	})
	require.Error(t, err)

	modified, err := client.ModifyReplicationInstance(t.Context(), &dmssdk.ModifyReplicationInstanceInput{
		ReplicationInstanceArn: ri.ReplicationInstanceArn,
		VpcSecurityGroupIds:    []string{"sg-def456", "sg-ghi789"},
	})
	require.NoError(t, err)
	require.Len(t, modified.ReplicationInstance.VpcSecurityGroups, 2)
	assert.ElementsMatch(t, []string{"sg-def456", "sg-ghi789"}, []string{
		aws.ToString(modified.ReplicationInstance.VpcSecurityGroups[0].VpcSecurityGroupId),
		aws.ToString(modified.ReplicationInstance.VpcSecurityGroups[1].VpcSecurityGroupId),
	})
	assert.Equal(
		t,
		"wire-sg",
		aws.ToString(modified.ReplicationInstance.ReplicationSubnetGroup.ReplicationSubnetGroupIdentifier),
		"ReplicationSubnetGroupIdentifier is create-only in the real API; Modify must not clear it",
	)

	descOut, err := client.DescribeReplicationInstances(t.Context(), &dmssdk.DescribeReplicationInstancesInput{})
	require.NoError(t, err)

	var found *types.ReplicationInstance

	for i := range descOut.ReplicationInstances {
		if aws.ToString(descOut.ReplicationInstances[i].ReplicationInstanceIdentifier) == "subnet-vpc-ri" {
			found = &descOut.ReplicationInstances[i]

			break
		}
	}

	require.NotNil(t, found, "DescribeReplicationInstances must return the instance")
	assert.Equal(t, "wire-sg", aws.ToString(found.ReplicationSubnetGroup.ReplicationSubnetGroupIdentifier))
	require.Len(t, found.VpcSecurityGroups, 2)
}

// TestReplicationTask_CDCSettings_RealClient proves a third write-only-state
// bug: real CreateReplicationTaskInput/ModifyReplicationTaskInput
// (databasemigrationservice@v1.66.4 api_op_CreateReplicationTask.go /
// api_op_ModifyReplicationTask.go) both carry CdcStartPosition,
// CdcStopPosition, and TaskData -- all three are also real top-level
// types.ReplicationTask response members. gopherstack accepted none of them
// on either op: a real client's CDC checkpoint positions and task data were
// silently discarded by encoding/json with no error, and there was no field
// anywhere in the domain model to have stored them even if the wire had
// accepted them.
func TestReplicationTask_CDCSettings_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	client := newTestDMSClient(t, h)

	srcOut, err := client.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
		EndpointIdentifier: aws.String("cdc-src"),
		EndpointType:       types.ReplicationEndpointTypeValueSource,
		EngineName:         aws.String("mysql"),
	})
	require.NoError(t, err)

	tgtOut, err := client.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
		EndpointIdentifier: aws.String("cdc-tgt"),
		EndpointType:       types.ReplicationEndpointTypeValueTarget,
		EngineName:         aws.String("mysql"),
	})
	require.NoError(t, err)

	riOut, err := client.CreateReplicationInstance(t.Context(), &dmssdk.CreateReplicationInstanceInput{
		ReplicationInstanceIdentifier: aws.String("cdc-ri"),
		ReplicationInstanceClass:      aws.String("dms.t3.micro"),
	})
	require.NoError(t, err)

	created, err := client.CreateReplicationTask(t.Context(), &dmssdk.CreateReplicationTaskInput{
		ReplicationTaskIdentifier: aws.String("cdc-task"),
		SourceEndpointArn:         srcOut.Endpoint.EndpointArn,
		TargetEndpointArn:         tgtOut.Endpoint.EndpointArn,
		ReplicationInstanceArn:    riOut.ReplicationInstance.ReplicationInstanceArn,
		MigrationType:             types.MigrationTypeValueCdc,
		TableMappings:             aws.String(`{"rules":[]}`),
		CdcStartPosition:          aws.String("mysql-bin-changelog.000024:373"),
		CdcStopPosition:           aws.String("server_time:2026-01-01T00:00:00"),
		TaskData:                  aws.String(`{"TaskSettings":{}}`),
	})
	require.NoError(t, err)
	require.NotNil(t, created.ReplicationTask)
	assert.Equal(t, "mysql-bin-changelog.000024:373", aws.ToString(created.ReplicationTask.CdcStartPosition),
		"CreateReplicationTask must store and echo CdcStartPosition, not silently drop it")
	assert.Equal(t, "server_time:2026-01-01T00:00:00", aws.ToString(created.ReplicationTask.CdcStopPosition))
	assert.JSONEq(t, `{"TaskSettings":{}}`, aws.ToString(created.ReplicationTask.TaskData))

	modified, err := client.ModifyReplicationTask(t.Context(), &dmssdk.ModifyReplicationTaskInput{
		ReplicationTaskArn: created.ReplicationTask.ReplicationTaskArn,
		CdcStopPosition:    aws.String("server_time:2026-06-01T00:00:00"),
	})
	require.NoError(t, err)
	assert.Equal(t, "server_time:2026-06-01T00:00:00", aws.ToString(modified.ReplicationTask.CdcStopPosition))
	assert.Equal(t, "mysql-bin-changelog.000024:373", aws.ToString(modified.ReplicationTask.CdcStartPosition),
		"unset fields on Modify must not clear existing values")

	descOut, err := client.DescribeReplicationTasks(t.Context(), &dmssdk.DescribeReplicationTasksInput{})
	require.NoError(t, err)

	var found *types.ReplicationTask

	for i := range descOut.ReplicationTasks {
		if aws.ToString(descOut.ReplicationTasks[i].ReplicationTaskIdentifier) == "cdc-task" {
			found = &descOut.ReplicationTasks[i]

			break
		}
	}

	require.NotNil(t, found, "DescribeReplicationTasks must return the task")
	assert.Equal(t, "server_time:2026-06-01T00:00:00", aws.ToString(found.CdcStopPosition))
	assert.JSONEq(t, `{"TaskSettings":{}}`, aws.ToString(found.TaskData))
}
