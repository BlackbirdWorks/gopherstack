package apprunner_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apprunner"
)

// TestAddOperation_CapsPerServiceHistory verifies that a service's operation
// history is bounded so repeatedly updating a long-lived service cannot grow
// svc.Operations without limit.
func TestAddOperation_CapsPerServiceHistory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		updates int
	}{
		{name: "just over cap", updates: apprunner.MaxOperationsPerService + 10},
		{name: "far over cap", updates: apprunner.MaxOperationsPerService * 3},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := apprunner.NewInMemoryBackend("123456789012", "us-east-1")

			svc, err := b.CreateService(apprunner.CreateServiceParams{
				Name: "svc",
				Source: apprunner.SourceConfig{
					ImageRepository: &apprunner.ImageSource{
						ImageIdentifier:     "public.ecr.aws/x/y:latest",
						ImageRepositoryType: "ECR_PUBLIC",
					},
				},
			})
			require.NoError(t, err)

			for range tc.updates {
				_, err = b.UpdateService(apprunner.UpdateServiceParams{
					ServiceArn: svc.ServiceArn,
					Instance:   &apprunner.InstanceConfig{CPU: "2 vCPU"},
				})
				require.NoError(t, err)
			}

			length, capacity := apprunner.ServiceOperationStats(b, svc.ServiceArn)

			require.LessOrEqual(t, length, apprunner.MaxOperationsPerService,
				"per-service operation history must be capped")
			require.LessOrEqual(t, capacity, apprunner.MaxOperationsPerService,
				"trimmed operation slice must not retain an oversized backing array")
		})
	}
}

// TestDeleteService_CascadesCustomDomains verifies DeleteService removes the
// service's b.customDomains entry rather than leaving a ghost row keyed by
// an ARN no service can ever reference again (DescribeCustomDomains itself
// 404s on a deleted ServiceArn, so an orphaned entry would be permanently
// unreachable dead state -- a map-growth leak in a long-running process).
func TestDeleteService_CascadesCustomDomains(t *testing.T) {
	t.Parallel()

	b := apprunner.NewInMemoryBackend("123456789012", "us-east-1")

	svc, err := b.CreateService(apprunner.CreateServiceParams{
		Name: "domain-svc",
		Source: apprunner.SourceConfig{
			ImageRepository: &apprunner.ImageSource{
				ImageIdentifier:     "public.ecr.aws/x/y:latest",
				ImageRepositoryType: "ECR_PUBLIC",
			},
		},
	})
	require.NoError(t, err)

	_, err = b.AssociateCustomDomain(svc.ServiceArn, "example.com", false)
	require.NoError(t, err)
	require.Equal(t, 1, apprunner.CustomDomainMapEntries(b),
		"AssociateCustomDomain must add a customDomains entry")

	_, err = b.DeleteService(svc.ServiceArn)
	require.NoError(t, err)

	require.Equal(t, 0, apprunner.CustomDomainMapEntries(b),
		"DeleteService must cascade-clean the service's customDomains entry")
}
