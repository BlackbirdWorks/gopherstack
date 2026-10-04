package kafkaconnect

import (
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// InMemoryBackend is the in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	connectors           *store.Table[Connector]
	customPlugins        *store.Table[CustomPlugin]
	workerConfigurations *store.Table[WorkerConfiguration]
	connectorOperations  *store.Table[ConnectorOperation]
	registry             *store.Registry
	mu                   *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new in-memory MSK Connect backend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		registry: store.NewRegistry(),
		mu:       lockmetrics.New("kafkaconnect"),
	}

	registerAllTables(b)

	return b
}

// Reset clears all backend state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
}

func (b *InMemoryBackend) connectorByName(name string) (*Connector, bool) {
	for _, c := range b.connectors.All() {
		if c.Name == name {
			return c, true
		}
	}

	return nil, false
}

func (b *InMemoryBackend) customPluginByName(name string) (*CustomPlugin, bool) {
	for _, p := range b.customPlugins.All() {
		if p.Name == name {
			return p, true
		}
	}

	return nil, false
}

func (b *InMemoryBackend) workerConfigurationByName(name string) (*WorkerConfiguration, bool) {
	for _, w := range b.workerConfigurations.All() {
		if w.Name == name {
			return w, true
		}
	}

	return nil, false
}
