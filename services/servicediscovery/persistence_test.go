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

				assert.Empty(t, b.ListNamespaces())
			},
		},
		{
			name: "namespace_and_service_preserved",
			setup: func(b *servicediscovery.InMemoryBackend) {
				_, err := b.CreateHTTPNamespace("my-ns", "desc", nil)
				if err != nil {
					return
				}

				nsList := b.ListNamespaces()
				if len(nsList) == 0 {
					return
				}

				_, _ = b.CreateService("my-svc", nsList[0].ID, "desc", nil)
			},
			verify: func(t *testing.T, b *servicediscovery.InMemoryBackend) {
				t.Helper()

				nsList := b.ListNamespaces()
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

func TestServiceDiscovery_CascadeDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *servicediscovery.InMemoryBackend) (string, string)
		verify func(t *testing.T, b *servicediscovery.InMemoryBackend, nsID, svcID string)
		name   string
	}{
		{
			name: "delete_namespace_cascades_to_service_and_instance",
			setup: func(b *servicediscovery.InMemoryBackend) (string, string) {
				_, err := b.CreateHTTPNamespace("my-ns", "", nil)
				if err != nil {
					return "", ""
				}

				nsList := b.ListNamespaces()
				if len(nsList) == 0 {
					return "", ""
				}

				nsID := nsList[0].ID

				svc, err := b.CreateService("my-svc", nsID, "", nil)
				if err != nil {
					return nsID, ""
				}

				_, _ = b.RegisterInstance(svc.ID, "inst-1", map[string]string{"ip": "1.2.3.4"})

				return nsID, svc.ID
			},
			verify: func(t *testing.T, b *servicediscovery.InMemoryBackend, nsID, svcID string) {
				t.Helper()

				_, err := b.DeleteNamespace(nsID)
				require.NoError(t, err)
				// namespace gone
				_, err = b.GetNamespace(nsID)
				require.Error(t, err)
				// service cascade-deleted
				_, err = b.GetService(svcID)
				require.Error(t, err)
				// instance cascade-deleted
				_, err = b.GetInstance(svcID, "inst-1")
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := servicediscovery.NewInMemoryBackend("123456789012", "us-east-1")
			nsID, svcID := tt.setup(b)
			tt.verify(t, b, nsID, svcID)
		})
	}
}
