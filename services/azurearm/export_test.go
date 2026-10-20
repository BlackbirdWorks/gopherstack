package azurearm

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// This file exposes internals needed by the blackbox (azurearm_test) test
// suite, following this repo's export_test.go convention (see e.g.
// services/cosmosdb) so tests can stay in package azurearm_test rather than
// reaching for whitebox package azurearm.

// RegistryProviderNamespaces returns the namespaces of every registered
// dedicated ResourceProvider (test-only alias for Registry.Providers, kept
// separate in case Providers' own semantics change).
func (r *Registry) RegistryProviderNamespaces() []string {
	return r.Providers()
}

// NewTestRegistryWithStorage builds a Registry over a fresh
// InMemoryBackend with a StorageProvider registered, matching Provider.Init's
// wiring, for handler-level tests that don't need a full Provider/CLI.
func NewTestRegistryWithStorage() (*InMemoryBackend, *Registry) {
	backend := NewInMemoryBackend()
	registry := NewRegistry(backend)
	registry.Register(NewStorageProvider(StorageEndpointConfig{}, nil))

	return backend, registry
}

// NewTestHandlerWithKeyVault builds a Handler with a Microsoft.KeyVault
// provider over dataPlane.
func NewTestHandlerWithKeyVault(dataPlane KeyVaults, settings Settings) *Handler {
	backend := NewInMemoryBackend()
	registry := NewRegistry(backend)
	registry.Register(NewKeyVaultProvider(KeyVaultEndpointConfig{}, dataPlane))

	return NewHandler(backend, registry, nil, settings)
}

// NewTestHandlerWithCosmos builds a Handler with a Microsoft.DocumentDB provider over dataPlane.
func NewTestHandlerWithCosmos(dataPlane CosmosResources, settings Settings) *Handler {
	backend := NewInMemoryBackend()
	registry := NewRegistry(backend)
	registry.Register(NewCosmosProvider(CosmosEndpointConfig{Override: settings.AdvertiseCosmosDB}, dataPlane))

	return NewHandler(backend, registry, nil, settings)
}

// EventHubSnapshotJSON marshals the provider's ARM-side snapshot.
func EventHubSnapshotJSON(p *EventHubProvider) []byte {
	b, _ := json.Marshal(p.snapshot())

	return b
}

// EventHubRestoreJSON restores a provider from EventHubSnapshotJSON output.
func EventHubRestoreJSON(t *testing.T, p *EventHubProvider, data []byte) {
	t.Helper()

	var s eventHubSnapshot

	require.NoError(t, json.Unmarshal(data, &s))
	p.restore(&s)
}
