package apprunner

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func customDomainMapEntries(b *InMemoryBackend) int {
	b.mu.RLock("test.customDomainMapEntries")
	defer b.mu.RUnlock()

	return len(b.customDomains)
}

// TestDeleteService_CascadesCustomDomains verifies DeleteService removes the
// service's b.customDomains entry rather than leaving a ghost row keyed by
// an ARN no service can ever reference again (DescribeCustomDomains itself
// 404s on a deleted ServiceArn, so an orphaned entry would be permanently
// unreachable dead state -- a map-growth leak in a long-running process).
func TestDeleteService_CascadesCustomDomains(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("123456789012", "us-east-1")

	svc, err := b.CreateService(CreateServiceParams{
		Name: "domain-svc",
		Source: SourceConfig{
			ImageRepository: &ImageSource{
				ImageIdentifier:     "public.ecr.aws/x/y:latest",
				ImageRepositoryType: "ECR_PUBLIC",
			},
		},
	})
	require.NoError(t, err)

	_, err = b.AssociateCustomDomain(svc.ServiceArn, "example.com", false)
	require.NoError(t, err)
	require.Equal(t, 1, customDomainMapEntries(b),
		"AssociateCustomDomain must add a customDomains entry")

	_, err = b.DeleteService(svc.ServiceArn)
	require.NoError(t, err)

	require.Equal(t, 0, customDomainMapEntries(b),
		"DeleteService must cascade-clean the service's customDomains entry")
}
