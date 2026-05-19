package servicediscovery_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/servicediscovery"
)

func TestServiceDiscovery_PersistenceSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *servicediscovery.InMemoryBackend)
		verify func(t *testing.T, b *servicediscovery.InMemoryBackend)
		name   string
	}{
		{
			name:  "empty",
			setup: func(_ *servicediscovery.InMemoryBackend) {},
			verify: func(t *testing.T, b *servicediscovery.InMemoryBackend) {
				t.Helper()

				assert.Empty(t, b.ListNamespaces(servicediscovery.ListNamespacesFilter{}))
			},
		},
		{
			name: "namespace_and_service_preserved",
			setup: func(b *servicediscovery.InMemoryBackend) {
				_, err := b.CreateHTTPNamespace("my-ns", "desc", nil)
				if err != nil {
					return
				}

				nsList := b.ListNamespaces(servicediscovery.ListNamespacesFilter{})
				if len(nsList) == 0 {
					return
				}

				_, _ = b.CreateService("my-svc", nsList[0].ID, "desc", "", nil, nil, nil, nil)
			},
			verify: func(t *testing.T, b *servicediscovery.InMemoryBackend) {
				t.Helper()

				nsList := b.ListNamespaces(servicediscovery.ListNamespacesFilter{})
				require.Len(t, nsList, 1)
				assert.Equal(t, "my-ns", nsList[0].Name)
				// Verify ARN index rebuilt (tag ops work)
				err := b.TagResource(nsList[0].ARN, map[string]string{"env": "test"})
				require.NoError(t, err)
				gotTags, err := b.ListTagsForResource(nsList[0].ARN)
				require.NoError(t, err)
				assert.Equal(t, "test", gotTags["env"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := servicediscovery.NewInMemoryBackend("123456789012", "us-east-1")
			tt.setup(b)

			snap := b.Snapshot()
			require.NotNil(t, snap)

			b2 := servicediscovery.NewInMemoryBackend("123456789012", "us-east-1")
			err := b2.Restore(snap)
			require.NoError(t, err)

			tt.verify(t, b2)
		})
	}
}

// TestServiceDiscovery_DeleteOrder verifies the AWS-correct delete ordering:
// namespace cannot be deleted while services exist, and service cannot be deleted
// while instances are registered. Explicit cleanup is required.
func TestServiceDiscovery_DeleteOrder(t *testing.T) {
	t.Parallel()

	b := servicediscovery.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.CreateHTTPNamespace("my-ns", "", nil)
	require.NoError(t, err)

	nsList := b.ListNamespaces(servicediscovery.ListNamespacesFilter{})
	require.Len(t, nsList, 1)

	nsID := nsList[0].ID

	svc, err := b.CreateService("my-svc", nsID, "", "", nil, nil, nil, nil)
	require.NoError(t, err)

	_, err = b.RegisterInstance(svc.ID, "inst-1", map[string]string{"ip": "1.2.3.4"})
	require.NoError(t, err)

	// Namespace delete refused while service exists.
	_, err = b.DeleteNamespace(nsID)
	require.ErrorIs(t, err, servicediscovery.ErrResourceInUse)

	// Service delete refused while instance registered.
	err = b.DeleteService(svc.ID)
	require.ErrorIs(t, err, servicediscovery.ErrResourceInUse)

	// Deregister instance first.
	_, err = b.DeregisterInstance(svc.ID, "inst-1")
	require.NoError(t, err)

	// Now service can be deleted.
	err = b.DeleteService(svc.ID)
	require.NoError(t, err)

	_, err = b.GetService(svc.ID)
	require.ErrorIs(t, err, servicediscovery.ErrServiceNotFound)

	// Now namespace can be deleted.
	_, err = b.DeleteNamespace(nsID)
	require.NoError(t, err)

	_, err = b.GetNamespace(nsID)
	require.ErrorIs(t, err, servicediscovery.ErrNamespaceNotFound)
}
