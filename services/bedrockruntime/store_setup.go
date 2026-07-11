package bedrockruntime

// Code in this file supports Phase 3.3 of the datalayer refactor: the single
// map[string]*AsyncInvoke resource collection on InMemoryBackend is
// registered as a *store.Table[AsyncInvoke]. See pkgs/store's package doc
// for the underlying primitive, and services/sesv2/services/dax for the
// clean-direct-register template this file follows.
//
// AsyncInvoke already carries its own identity field (InvocationArn, set
// once at creation from arn.Build in StartAsyncInvoke and never mutated
// afterward), so it registers directly with no composite key and no DTO
// wrapper.
//
// tokenIndex (clientToken -> InvocationArn) stays a plain raw
// map[string]string: its values are strings, not *AsyncInvoke, so it does
// not fit store.Table/store.Index's keyed-by-*V shape (matching
// services/textract's clientTokenToJobID/adapterClientTokenToID). invocations
// (the fixed-capacity invocationRing) also stays raw: it is an
// ORDER-SENSITIVE ring buffer, and store.Index does not preserve insertion
// order.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func asyncInvokeTableKeyFn(v *AsyncInvoke) string { return v.InvocationArn }

// registerAllTables registers the backend's one store-backed resource
// collection on b.registry exactly once. It must be called during
// construction only (immediately after b.registry is created -- see
// NewInMemoryBackendWithContext), never on every Reset() -- store.Register
// panics on a duplicate name, so runtime resets go through
// b.registry.ResetAll() instead (see InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.asyncInvokes = store.Register(b.registry, "asyncInvokes", store.New(asyncInvokeTableKeyFn))
}
