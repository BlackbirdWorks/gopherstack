package opsworks

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestFormatOpsWorksTimeNonUTCZone builds every timestamp input in a non-UTC
// FixedZone (gopherstack-c29a): the old literal "+00:00" suffix printed the
// input's own wall-clock digits unconverted, so a UTC-only test could not
// have caught it -- it happened to produce the same suffix by coincidence.
// Real OpsWorks always reports UTC (aws-cli 2.4.18 opsworks describe-stacks
// doc example: "2013-08-01T22:53:42+00:00"), so a +05:00 input must convert.
func TestFormatOpsWorksTimeNonUTCZone(t *testing.T) {
	t.Parallel()

	zone := time.FixedZone("test+05", 5*3600)
	created := time.Date(2020, 1, 2, 3, 4, 5, 0, zone)
	const want = "2020-01-01T22:04:05+00:00"

	stack := stacksToJSON([]*Stack{{StackID: "s1", CreatedAt: created}})[0]
	layer := layersToJSON([]*Layer{{LayerID: "l1", CreatedAt: created}})[0]
	instance := instancesToJSON([]*Instance{{InstanceID: "i1", CreatedAt: created}})[0]
	app := appsToJSON([]*App{{AppID: "a1", CreatedAt: created}})[0]
	deployment := deploymentsToJSON([]*Deployment{{DeploymentID: "d1", CreatedAt: created}})[0]
	cmdCreated := commandsToJSON([]*Command{{CommandID: "c1", CreatedAt: created}})[0]
	cmdAcked := commandsToJSON([]*Command{{CommandID: "c1", AcknowledgedAt: created}})[0]
	cmdDone := commandsToJSON([]*Command{{CommandID: "c1", CompletedAt: created}})[0]
	ecsCluster := ecsClustersToJSON([]*EcsCluster{{EcsClusterArn: "arn:1", RegisteredAt: created}})[0]

	tests := []struct {
		name string
		got  string
	}{
		{"stack", stack[keyCreatedAt].(string)},
		{"layer", layer[keyCreatedAt].(string)},
		{"instance", instance[keyCreatedAt].(string)},
		{"app", app[keyCreatedAt].(string)},
		{"deployment", deployment[keyCreatedAt].(string)},
		{"command_created", cmdCreated[keyCreatedAt].(string)},
		{"command_acknowledged", cmdAcked["AcknowledgedAt"].(string)},
		{"command_completed", cmdDone["CompletedAt"].(string)},
		{"ecs_cluster", ecsCluster["RegisteredAt"].(string)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, want, tt.got)
		})
	}
}

// TestFormatOpsWorksTimeDeploymentCompletedAt covers deploymentsToJSON's
// separate CompletedAt branch (handler_deployments.go), which is guarded by
// an IsZero/CreatedAt-equality check the other conversions don't have.
func TestFormatOpsWorksTimeDeploymentCompletedAt(t *testing.T) {
	t.Parallel()

	zone := time.FixedZone("test-08", -8*3600)
	created := time.Date(2021, 6, 15, 1, 2, 3, 0, zone)
	completed := created.Add(time.Hour)

	got := deploymentsToJSON([]*Deployment{{DeploymentID: "d1", CreatedAt: created, CompletedAt: completed}})[0]

	require.Equal(t, "2021-06-15T09:02:03+00:00", got[keyCreatedAt])
	require.Equal(t, "2021-06-15T10:02:03+00:00", got["CompletedAt"])
}
