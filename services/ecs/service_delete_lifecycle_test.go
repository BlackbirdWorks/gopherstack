package ecs_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

// TestDeleteService_DrainsToInactive proves the api_op_DeleteService.go
// lifecycle (gopherstack terraform destroy-waiter gap): DeleteService moves a
// service to DRAINING (still DescribeServices-visible, omitted from
// ListServices), and it settles at INACTIVE once the modeled drain window
// passes, still describable but never listed. A same-named CreateService is
// refused while DRAINING/ACTIVE but allowed again once INACTIVE.
func TestDeleteService_DrainsToInactive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "delete moves service to DRAINING, omitted from ListServices",
			run: func(t *testing.T) {
				t.Helper()

				b, tdArn := newDrainTestBackend(t)

				_, err := b.CreateService(ecs.CreateServiceInput{
					ServiceName:    "drain-svc",
					TaskDefinition: tdArn,
					DesiredCount:   0,
				})
				require.NoError(t, err)

				_, err = b.DeleteService("", "drain-svc", false)
				require.NoError(t, err)

				svcs, failures, err := b.DescribeServices("", []string{"drain-svc"})
				require.NoError(t, err)
				require.Empty(t, failures)
				require.Len(t, svcs, 1)
				assert.Equal(t, "DRAINING", svcs[0].Status)

				arns, err := b.ListServices("", "", "")
				require.NoError(t, err)
				assert.NotContains(t, arns, svcs[0].ServiceArn)
			},
		},
		{
			name: "DRAINING settles at INACTIVE after the drain deadline, still describable",
			run: func(t *testing.T) {
				t.Helper()

				synctest.Test(t, func(t *testing.T) {
					b, tdArn := newDrainTestBackend(t)

					_, err := b.CreateService(ecs.CreateServiceInput{
						ServiceName:    "drain-svc-2",
						TaskDefinition: tdArn,
						DesiredCount:   0,
					})
					require.NoError(t, err)

					_, err = b.DeleteService("", "drain-svc-2", false)
					require.NoError(t, err)

					time.Sleep(pastServiceDrainDelay)

					svcs, failures, err := b.DescribeServices("", []string{"drain-svc-2"})
					require.NoError(t, err)
					require.Empty(t, failures)
					require.Len(t, svcs, 1)
					assert.Equal(t, "INACTIVE", svcs[0].Status)

					arns, err := b.ListServices("", "", "")
					require.NoError(t, err)
					assert.Empty(t, arns)
				})
			},
		},
		{
			name: "same-named CreateService refused while DRAINING",
			run: func(t *testing.T) {
				t.Helper()

				b, tdArn := newDrainTestBackend(t)

				_, err := b.CreateService(ecs.CreateServiceInput{
					ServiceName:    "drain-svc-3",
					TaskDefinition: tdArn,
					DesiredCount:   0,
				})
				require.NoError(t, err)

				_, err = b.DeleteService("", "drain-svc-3", false)
				require.NoError(t, err)

				_, err = b.CreateService(ecs.CreateServiceInput{
					ServiceName:    "drain-svc-3",
					TaskDefinition: tdArn,
					DesiredCount:   0,
				})
				require.Error(t, err)
			},
		},
		{
			name: "same-named CreateService succeeds once INACTIVE",
			run: func(t *testing.T) {
				t.Helper()

				synctest.Test(t, func(t *testing.T) {
					b, tdArn := newDrainTestBackend(t)

					_, err := b.CreateService(ecs.CreateServiceInput{
						ServiceName:    "drain-svc-4",
						TaskDefinition: tdArn,
						DesiredCount:   0,
					})
					require.NoError(t, err)

					_, err = b.DeleteService("", "drain-svc-4", false)
					require.NoError(t, err)

					time.Sleep(pastServiceDrainDelay)

					svc, err := b.CreateService(ecs.CreateServiceInput{
						ServiceName:    "drain-svc-4",
						TaskDefinition: tdArn,
						DesiredCount:   0,
					})
					require.NoError(t, err)
					assert.Equal(t, "ACTIVE", svc.Status)

					arns, err := b.ListServices("", "", "")
					require.NoError(t, err)
					assert.Contains(t, arns, svc.ServiceArn)
				})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// newDrainTestBackend returns a fresh backend with a registered task
// definition, ready for CreateService.
func newDrainTestBackend(t *testing.T) (*ecs.InMemoryBackend, string) {
	t.Helper()

	b := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	td, err := b.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
		Family:               "drain-td",
		ContainerDefinitions: []ecs.ContainerDefinition{{Name: "app", Image: "alpine"}},
	})
	require.NoError(t, err)

	return b, td.TaskDefinitionArn
}
