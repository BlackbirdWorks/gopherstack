package kafkaconnect

import "github.com/blackbirdworks/gopherstack/pkgs/store"

func connectorKeyFn(v *Connector) string { return v.ARN }

func customPluginKeyFn(v *CustomPlugin) string { return v.ARN }

func workerConfigurationKeyFn(v *WorkerConfiguration) string { return v.ARN }

func connectorOperationKeyFn(v *ConnectorOperation) string { return v.ARN }

// registerAllTables registers every backend resource table exactly once.
// Must be called during construction only -- store.Register panics on a
// duplicate name.
func registerAllTables(b *InMemoryBackend) {
	b.connectors = store.Register(b.registry, "connectors", store.New(connectorKeyFn))
	b.customPlugins = store.Register(b.registry, "customPlugins", store.New(customPluginKeyFn))
	b.workerConfigurations = store.Register(
		b.registry, "workerConfigurations", store.New(workerConfigurationKeyFn),
	)
	b.connectorOperations = store.Register(b.registry, "connectorOperations", store.New(connectorOperationKeyFn))
}
