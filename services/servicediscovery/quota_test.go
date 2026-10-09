package servicediscovery_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/servicediscovery"
)

func TestResourceLimitExceeded(t *testing.T) {
	t.Parallel()

	registerN := func(t *testing.T, b *servicediscovery.InMemoryBackend, svcID string, n int) {
		t.Helper()

		for i := range n {
			_, err := b.RegisterInstance(
				svcID,
				fmt.Sprintf("i-%d", i),
				map[string]string{"AWS_INSTANCE_IPV4": "10.0.0.1"},
			)
			require.NoError(t, err)
		}
	}

	namespaceID := func(t *testing.T, b *servicediscovery.InMemoryBackend, name string) string {
		t.Helper()

		opID, err := b.CreateHTTPNamespace(name, "", nil)
		require.NoError(t, err)

		op, err := b.GetOperation(opID)
		require.NoError(t, err)

		return op.Targets["NAMESPACE"]
	}

	newService := func(t *testing.T, b *servicediscovery.InMemoryBackend, nsID, name string) string {
		t.Helper()

		svc, err := b.CreateService(name, nsID, "", "", nil, nil, nil, nil)
		require.NoError(t, err)

		return svc.ID
	}

	tests := []struct {
		run  func(t *testing.T, b *servicediscovery.InMemoryBackend) error
		name string
	}{
		{name: "namespaces_per_region", run: func(t *testing.T, b *servicediscovery.InMemoryBackend) error {
			t.Helper()

			for i := range 50 {
				_, err := b.CreateHTTPNamespace(fmt.Sprintf("ns%d", i), "", nil)
				require.NoError(t, err)
			}

			_, err := b.CreateHTTPNamespace("one-too-many", "", nil)

			return err
		}},
		{name: "instances_per_service", run: func(t *testing.T, b *servicediscovery.InMemoryBackend) error {
			t.Helper()

			ns := namespaceID(t, b, "ns")

			svc := newService(t, b, ns, "svc")
			registerN(t, b, svc, 1000)

			_, err := b.RegisterInstance(svc, "i-0", map[string]string{"k": "updated"})
			require.NoError(t, err)

			_, err = b.RegisterInstance(svc, "extra", map[string]string{"k": "v"})

			return err
		}},
		{name: "instances_per_namespace", run: func(t *testing.T, b *servicediscovery.InMemoryBackend) error {
			t.Helper()

			ns := namespaceID(t, b, "ns")

			registerN(t, b, newService(t, b, ns, "a"), 1000)
			registerN(t, b, newService(t, b, ns, "b"), 1000)

			_, err := b.RegisterInstance(newService(t, b, ns, "c"), "extra", map[string]string{"k": "v"})

			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.run(t, servicediscovery.NewInMemoryBackend("000000000000", "us-east-1"))
			require.ErrorIs(t, err, servicediscovery.ErrResourceLimitExceeded)
		})
	}
}

func TestResourceLimitExceeded_WireCode(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 50 {
		rec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": fmt.Sprintf("ns%d", i)})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "overflow"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ResourceLimitExceeded")
}
