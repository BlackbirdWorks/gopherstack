package servicediscovery_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/servicediscovery"
)

func TestBackend_Region(t *testing.T) {
	t.Parallel()

	b := servicediscovery.NewInMemoryBackend("000000000000", "eu-west-1")
	assert.Equal(t, "eu-west-1", b.Region())
}

func TestBackend_ListNamespaces(t *testing.T) {
	t.Parallel()

	b := servicediscovery.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateHTTPNamespace("ns-b", "", nil)
	require.NoError(t, err)

	_, err = b.CreateHTTPNamespace("ns-a", "", nil)
	require.NoError(t, err)

	list := b.ListNamespaces(servicediscovery.ListNamespacesFilter{})
	require.Len(t, list, 2)
	assert.Equal(t, "ns-a", list[0].Name, "namespaces should be sorted by name")
}

// TestExportCountHelpers verifies that count helpers work correctly.
func TestExportCountHelpers(t *testing.T) {
	t.Parallel()

	b, h := newBackendAndHandler(t)

	assert.Equal(t, 0, servicediscovery.NamespaceCount(b))
	assert.Equal(t, 0, servicediscovery.ServiceCount(b))
	assert.Equal(t, 0, servicediscovery.InstanceCount(b))
	assert.Equal(t, 0, servicediscovery.OperationCount(b))
	assert.Equal(t, 0, servicediscovery.ServiceAttributeCount(b))
	assert.Equal(t, 30, servicediscovery.HandlerOpsLen(h))
}

// TestStorageBackendInterfaceCompiles verifies that Handler.Backend
// is the StorageBackend interface type (compile-time check via assignment).
func TestStorageBackendInterfaceCompiles(t *testing.T) {
	t.Parallel()

	b := servicediscovery.NewInMemoryBackend("000000000000", "us-east-1")
	var _ servicediscovery.StorageBackend = b // compile-time assertion

	h := servicediscovery.NewHandler(b)
	assert.NotNil(t, h.Backend)
}

// TestAddSeedHelpers verifies AddNamespaceInternal / AddServiceInternal work.
func TestAddSeedHelpers(t *testing.T) {
	t.Parallel()

	b, _ := newBackendAndHandler(t)

	ns := servicediscovery.NewNamespaceForTest("ns-seed-001", "seeded-ns", "HTTP")
	servicediscovery.AddNamespaceInternal(b, ns)
	assert.Equal(t, 1, servicediscovery.NamespaceCount(b))

	svc := servicediscovery.NewServiceForTest("svc-seed-001", "seeded-svc", "ns-seed-001")
	servicediscovery.AddServiceInternal(b, svc)
	assert.Equal(t, 1, servicediscovery.ServiceCount(b))

	inst := servicediscovery.NewInstanceForTest("inst-001", "svc-seed-001", map[string]string{"ip": "1.2.3.4"})
	servicediscovery.AddInstanceInternal(b, inst)
	assert.Equal(t, 1, servicediscovery.InstanceCount(b))
}

// TestBackendReset clears everything via Handler.Reset().
func TestBackendReset(t *testing.T) {
	t.Parallel()

	b, h := newBackendAndHandler(t)

	ns := servicediscovery.NewNamespaceForTest("ns-r", "reset-ns", "HTTP")
	servicediscovery.AddNamespaceInternal(b, ns)
	assert.Equal(t, 1, servicediscovery.NamespaceCount(b))

	h.Reset()
	assert.Equal(t, 0, servicediscovery.NamespaceCount(b))
}
