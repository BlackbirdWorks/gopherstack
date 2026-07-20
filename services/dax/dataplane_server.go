package dax

import (
	"context"
	"net"

	"github.com/blackbirdworks/gopherstack/services/dax/dataplane"
	dynamodb "github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// defaultDataPlaneAddr is the address the DAX data-plane listener binds to by
// default. Port 8111 matches the port reported by DescribeClusters so a DAX
// client configured with the cluster endpoint reaches the right port.
const defaultDataPlaneAddr = ":8111"

// DataPlane bundles the DAX binary-protocol listener with the DynamoDB backend
// it delegates item operations to. DAX is a write-through cache in front of
// DynamoDB; for emulation purposes the cache is a pass-through to a real
// in-memory DynamoDB store.
type DataPlane struct {
	server  *dataplane.Server
	backend *dynamodb.InMemoryDB
	addr    string
}

// newDataPlane constructs a DAX data-plane bound to its own DynamoDB backend.
// ctx is the process lifecycle context used for the data-plane's logging.
func newDataPlane(ctx context.Context, addr string, daxBackend StorageBackend) *DataPlane {
	if addr == "" {
		addr = defaultDataPlaneAddr
	}

	backend := dynamodb.NewInMemoryDB()

	return &DataPlane{
		server:  dataplane.NewServer(ctx, backend, daxBackend),
		backend: backend,
		addr:    addr,
	}
}

// Start binds the listener and begins serving DAX protocol connections.
func (d *DataPlane) Start() error {
	return d.server.Listen(d.addr)
}

// Addr returns the bound listener address, or nil if not yet listening.
func (d *DataPlane) Addr() net.Addr {
	return d.server.Addr()
}

// Stop closes the listener and all active connections.
func (d *DataPlane) Stop() {
	if d.server != nil {
		_ = d.server.Close()
	}
}

// EnableDataPlane attaches a DAX data-plane listener to the handler. It is
// idempotent. The returned data plane exposes the bound address for tests.
func (h *Handler) EnableDataPlane(ctx context.Context, addr string) *DataPlane {
	if h.DataPlane != nil {
		return h.DataPlane
	}

	h.DataPlane = newDataPlane(ctx, addr, h.Backend)

	return h.DataPlane
}

// StartWorker starts the DAX data-plane listener if one has been enabled. It
// satisfies service.BackgroundWorker so the listener comes up with the service.
func (h *Handler) StartWorker(_ context.Context) error {
	if h.DataPlane == nil {
		return nil
	}

	return h.DataPlane.Start()
}

// Shutdown stops the DAX data-plane listener. It satisfies service.Shutdowner.
func (h *Handler) Shutdown(_ context.Context) {
	if h.DataPlane != nil {
		h.DataPlane.Stop()
	}
}

// DataPlaneBackend returns the DynamoDB backend the data plane delegates to,
// for tests and dashboards. It is nil when the data plane is disabled.
func (h *Handler) DataPlaneBackend() *dynamodb.InMemoryDB {
	if h.DataPlane == nil {
		return nil
	}

	return h.DataPlane.backend
}
