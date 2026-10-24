package app

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	timestreaminfluxdbbackend "github.com/blackbirdworks/gopherstack/services/timestreaminfluxdb"
)

func TestInitializeServices_TimestreamInfluxDBTagsWiring(t *testing.T) {
	t.Parallel()

	byName, rgtH := newRGTWiringEnv(t)

	h, ok := byName["TimestreamInfluxDB"].(*timestreaminfluxdbbackend.Handler)
	require.True(t, ok)

	ec2H, ok := byName["EC2"].(*ec2backend.Handler)
	require.True(t, ok)

	ctx := t.Context()
	tags := map[string]string{"Team": "emu"}
	h.Backend.SetLifecycleDelay(time.Nanosecond)

	ec2 := ec2H.BackendFor("us-east-1")
	vpc, err := ec2.CreateVpc("10.0.0.0/16", "")
	require.NoError(t, err)

	subA, err := ec2.CreateSubnet(vpc.ID, "10.0.1.0/24", "us-east-1a")
	require.NoError(t, err)

	subB, err := ec2.CreateSubnet(vpc.ID, "10.0.2.0/24", "us-east-1b")
	require.NoError(t, err)

	inst, err := h.Backend.CreateDBInstance(ctx, timestreaminfluxdbbackend.InstanceInput{
		Name: "inst", Password: "Passw0rdPassw0rd", DBInstanceType: "db.influx.medium", AllocatedStorage: 100,
		SubnetIDs: []string{subA.ID}, SecurityGroupIDs: []string{"sg-bbb222"}, Tags: tags,
	})
	require.NoError(t, err)

	clusterID, _, err := h.Backend.CreateDBCluster(ctx, timestreaminfluxdbbackend.ClusterInput{
		Name: "clu", Password: "Passw0rdPassw0rd", DBInstanceType: "db.influx.large",
		SubnetIDs: []string{subA.ID, subB.ID}, SecurityGroupIDs: []string{"sg-bbb222"},
		DeploymentType: "MULTI_NODE_READ_REPLICAS", Tags: tags,
	})
	require.NoError(t, err)

	cluster, err := h.Backend.GetDBCluster(ctx, clusterID)
	require.NoError(t, err)

	group, err := h.Backend.CreateDBParameterGroup(ctx, timestreaminfluxdbbackend.GroupInput{Name: "grp", Tags: tags})
	require.NoError(t, err)

	backup, err := h.Backend.CreateDBBackup(ctx, timestreaminfluxdbbackend.BackupInput{
		Name: "bak", ResourceID: inst.ID, Tags: tags,
	})
	require.NoError(t, err)

	list := func(arn string) func(context.Context) (map[string]string, error) {
		return func(ctx context.Context) (map[string]string, error) { return h.Backend.ListTagsForResource(ctx, arn) }
	}

	runRGTWiringCases(t, rgtH, []rgtWiringCase{
		{
			name: "instance", arn: inst.Arn,
			resourceType: "timestream-influxdb:db-instance",
			listTags:     list(inst.Arn),
		},
		{
			name: "cluster", arn: cluster.Arn,
			resourceType: "timestream-influxdb:db-cluster",
			listTags:     list(cluster.Arn),
		},
		{
			name: "group", arn: group.Arn,
			resourceType: "timestream-influxdb:db-parameter-group",
			listTags:     list(group.Arn),
		},
		{
			name: "backup", arn: backup.Arn,
			resourceType: "timestream-influxdb:db-backup",
			listTags:     list(backup.Arn),
		},
	})
}
