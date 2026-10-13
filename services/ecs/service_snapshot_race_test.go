package ecs_test

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

func TestUpdateServiceResultDoesNotAliasLiveDeployments(t *testing.T) {
	t.Parallel()

	b, td1 := newDrainTestBackend(t)
	td2, err := b.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
		Family:               "drain-td",
		ContainerDefinitions: []ecs.ContainerDefinition{{Name: "app", Image: "busybox"}},
	})
	require.NoError(t, err)

	for i := range 100 {
		name := fmt.Sprintf("race-svc-%d", i)
		_, createErr := b.CreateService(ecs.CreateServiceInput{ServiceName: name, TaskDefinition: td1})
		require.NoError(t, createErr)

		updated, updErr := b.UpdateService(
			ecs.UpdateServiceInput{Service: name, TaskDefinition: &td2.TaskDefinitionArn},
		)
		require.NoError(t, updErr)

		sdArn := ""
		for _, d := range updated.Deployments {
			if d.Status == "PRIMARY" {
				sdArn = strings.Replace(updated.ServiceArn, ":service/", ":service-deployment/", 1) + "/" + d.ID
			}
		}
		require.NotEmpty(t, sdArn)

		var wg sync.WaitGroup
		wg.Go(func() {
			for range 50 {
				for _, d := range updated.Deployments {
					_ = d.RolloutState
				}
			}
		})
		wg.Go(func() { _, _ = b.StopServiceDeployment(sdArn, "ROLLBACK") })
		wg.Wait()
	}
}
