package ecs_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

func TestInventoryResources_DaemonTaskDefinition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tags       []ecs.Tag
		wantTagged bool
	}{
		{name: "untagged_daemon_task_definition", tags: nil, wantTagged: false},
		{name: "tagged_daemon_task_definition", tags: []ecs.Tag{{Key: "team", Value: "emu"}}, wantTagged: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ecs.NewInMemoryBackend("000000000000", "us-east-1", ecs.NewNoopRunner())

			td, err := b.RegisterDaemonTaskDefinition(ecs.RegisterDaemonTaskDefinitionInput{
				Family:               "daemon-fam",
				ContainerDefinitions: []ecs.DaemonContainerDefinition{{Name: "agent", Image: "agent:1"}},
				Tags:                 tt.tags,
			})
			require.NoError(t, err)

			inventory := map[string]bool{}
			for _, e := range b.InventoryResources() {
				inventory[e.ARN] = true
			}

			tagged := map[string]bool{}
			for _, e := range b.TaggedResources() {
				tagged[e.ARN] = true
			}

			assert.True(t, inventory[td.DaemonTaskDefinitionArn])
			assert.Equal(t, tt.wantTagged, tagged[td.DaemonTaskDefinitionArn])
		})
	}
}

func TestInventoryResources_ExpressGatewayService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tags       []ecs.Tag
		wantTagged bool
	}{
		{name: "untagged_express_gateway_service", tags: nil, wantTagged: false},
		{name: "tagged_express_gateway_service", tags: []ecs.Tag{{Key: "team", Value: "emu"}}, wantTagged: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ecs.NewInMemoryBackend("000000000000", "us-east-1", ecs.NewNoopRunner())

			svc, err := b.CreateExpressGatewayService(ecs.CreateExpressGatewayServiceInput{
				InfrastructureRoleArn: "arn:aws:iam::000000000000:role/infra",
				ExecutionRoleArn:      "arn:aws:iam::000000000000:role/exec",
				PrimaryContainer:      &ecs.ExpressGatewayContainer{Image: "nginx"},
				Tags:                  tt.tags,
			})
			require.NoError(t, err)

			inventory := map[string]bool{}
			for _, e := range b.InventoryResources() {
				inventory[e.ARN] = true
			}

			tagged := map[string]bool{}
			for _, e := range b.TaggedResources() {
				tagged[e.ARN] = true
			}

			assert.True(t, inventory[svc.ServiceArn])
			assert.Equal(t, tt.wantTagged, tagged[svc.ServiceArn])
		})
	}
}
