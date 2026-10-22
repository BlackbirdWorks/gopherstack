package azurearm

import (
	"encoding/json"
	"testing"
	"time"

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

// NewTestHandlerWithEventGrid builds a Handler with a Microsoft.EventGrid provider over dataPlane.
func NewTestHandlerWithEventGrid(dataPlane EventGrid, settings Settings) *Handler {
	backend := NewInMemoryBackend()
	registry := NewRegistry(backend)
	registry.Register(NewEventGridProvider(EventGridEndpointConfig{Override: settings.AdvertiseEventGrid}, dataPlane))

	return NewHandler(backend, registry, nil, settings)
}

// EventGridSnapshotJSON marshals the provider's ARM-side snapshot.
func EventGridSnapshotJSON(p *EventGridProvider) []byte {
	b, _ := json.Marshal(p.snapshot())

	return b
}

// EventGridRestoreJSON restores a provider from EventGridSnapshotJSON output.
func EventGridRestoreJSON(t *testing.T, p *EventGridProvider, data []byte) {
	t.Helper()

	var s eventGridSnapshot

	require.NoError(t, json.Unmarshal(data, &s))
	p.restore(&s)
}

// NewTestHandlerWithAppConfig builds a Handler with a Microsoft.AppConfiguration provider over dataPlane.
func NewTestHandlerWithAppConfig(dataPlane AppConfig, settings Settings) *Handler {
	backend := NewInMemoryBackend()
	registry := NewRegistry(backend)
	registry.Register(NewAppConfigProvider(AppConfigEndpointConfig{Override: settings.AdvertiseAppConfig}, dataPlane))

	return NewHandler(backend, registry, nil, settings)
}

// AppConfigSnapshotJSON marshals the provider's ARM-side snapshot.
func AppConfigSnapshotJSON(p *AppConfigProvider) []byte {
	b, _ := json.Marshal(p.snapshot())

	return b
}

// AppConfigRestoreJSON restores a provider from AppConfigSnapshotJSON output.
func AppConfigRestoreJSON(t *testing.T, p *AppConfigProvider, data []byte) {
	t.Helper()

	var s appConfigSnapshot

	require.NoError(t, json.Unmarshal(data, &s))
	p.restore(&s)
}

// EscapeKVName and DecodeKVName expose the ARM key-value name codec.
func EscapeKVName(key, label string) string {
	if label == "" {
		return escapeKV(key)
	}

	return escapeKV(key) + acKVSeparator + escapeKV(label)
}

// DecodeKVName splits an ARM key-value name into key and label.
func DecodeKVName(name string) (string, string, error) { return decodeKVName(name) }

// NewTestHandlerWithContainerRegistry builds a Handler with a Microsoft.ContainerRegistry provider over dataPlane.
func NewTestHandlerWithContainerRegistry(dataPlane ContainerRegistries, settings Settings) *Handler {
	backend := NewInMemoryBackend()
	registry := NewRegistry(backend)
	registry.Register(NewContainerRegistryProvider(ContainerRegistryEndpointConfig{Override: settings.AdvertiseACR}, dataPlane))

	return NewHandler(backend, registry, nil, settings)
}

// ContainerRegistrySnapshotJSON marshals the provider's ARM-side snapshot.
func ContainerRegistrySnapshotJSON(p *ContainerRegistryProvider) []byte {
	b, _ := json.Marshal(p.snapshot())

	return b
}

// ContainerRegistryRestoreJSON restores a provider from ContainerRegistrySnapshotJSON output.
func ContainerRegistryRestoreJSON(t *testing.T, p *ContainerRegistryProvider, data []byte) {
	t.Helper()

	var s containerRegistrySnapshot

	require.NoError(t, json.Unmarshal(data, &s))
	p.restore(&s)
}

// NewTestHandlerWithRedis builds a Handler with a Microsoft.Cache provider over dataPlane, reading time from now.
func NewTestHandlerWithRedis(dataPlane Redis, now func() time.Time) (*Handler, *RedisProvider) {
	backend := NewInMemoryBackend()
	registry := NewRegistry(backend)
	p := NewRedisProvider(dataPlane)
	p.now = now
	registry.Register(p)

	return NewHandler(backend, registry, nil, DefaultSettings()), p
}

// RedisSnapshotJSON marshals the provider's ARM-side snapshot.
func RedisSnapshotJSON(p *RedisProvider) []byte {
	b, _ := json.Marshal(p.snapshot())

	return b
}

// RedisRestoreJSON restores a provider from RedisSnapshotJSON output.
func RedisRestoreJSON(t *testing.T, p *RedisProvider, data []byte) {
	t.Helper()

	var s redisSnapshot

	require.NoError(t, json.Unmarshal(data, &s))
	p.restore(&s)
}
