package bedrockruntime

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// bedrockruntimeSnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to a registered table's value type or
// backendSnapshot itself would make an older snapshot unsafe to decode as the
// current shape. Restore compares this against the persisted value and
// discards (registry.ResetAll plus resetting every raw field, not a partial
// decode) any mismatch -- see Restore below. This is the first version:
// BedrockRuntime had no persistence at all before Phase 3.3 (Handler had no
// Snapshot/Restore, so cli.go's generic setupPersistence never picked it up,
// and neither InMemoryBackend nor Handler implemented persistence.Persistable
// -- see the Handler.Snapshot/Restore delegation at the bottom of this file),
// so there is no legacy snapshot shape to be compatible with -- any snapshot
// without a matching Version (including one with no version field, which
// decodes as 0) is discarded the same way any other incompatible snapshot is.
const bedrockruntimeSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the BedrockRuntime
// backend.
//
// Tables holds one JSON-encoded array for the "asyncInvokes" table
// registered in store_setup.go, produced by b.registry.SnapshotAll():
// AsyncInvoke's own InvocationArn field is its store.Table key, so it needs
// no DTO wrapper.
//
// TokenIndex, Invocations, and AsyncInvokeCounter are the raw
// (non-store.Table) fields left unconverted by Phase 3.3 (see store_setup.go
// and the InMemoryBackend field comments in backend.go):
//   - TokenIndex's values are plain strings, not *AsyncInvoke, so there is
//     nothing for store.Table to key on.
//   - Invocations is an ORDER-SENSITIVE fixed-capacity ring buffer;
//     store.Index does not preserve insertion order, so it is persisted here
//     as a plain ordered slice (invocationRing.snapshot()'s output) instead.
//   - AsyncInvokeCounter is a bare scalar used to mint new async-invoke ARNs
//     (see StartAsyncInvoke in backend.go); it must survive a restore so a
//     freshly restored backend never reissues an ARN already handed out
//     before the snapshot.
type backendSnapshot struct {
	Tables             map[string]json.RawMessage `json:"tables"`
	TokenIndex         map[string]string          `json:"tokenIndex"`
	AccountID          string                     `json:"accountID"`
	Region             string                     `json:"region"`
	Invocations        []*Invocation              `json:"invocations"`
	AsyncInvokeCounter int                        `json:"asyncInvokeCounter"`
	Version            int                        `json:"version"`
}

// Snapshot serializes the backend state to JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "bedrockruntime: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:            bedrockruntimeSnapshotVersion,
		Tables:             tables,
		TokenIndex:         b.tokenIndex,
		Invocations:        b.invocations.snapshot(),
		AsyncInvokeCounter: b.asyncInvokeCounter,
		AccountID:          b.accountID,
		Region:             b.region,
	}

	return persistence.MarshalSnapshot(ctx, "bedrockruntime", snap)
}

// Restore deserializes backend state from a snapshot. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "bedrockruntime", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != bedrockruntimeSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"bedrockruntime: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", bedrockruntimeSnapshotVersion)

		b.registry.ResetAll()
		b.tokenIndex = make(map[string]string)
		b.invocations.reset()
		b.asyncInvokeCounter = 0
		b.accountID = snap.AccountID
		b.region = snap.Region

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("bedrockruntime: restore snapshot tables: %w", err)
	}

	if snap.TokenIndex == nil {
		snap.TokenIndex = make(map[string]string)
	}
	b.tokenIndex = snap.TokenIndex

	b.invocations.reset()
	for _, inv := range snap.Invocations {
		b.invocations.push(inv)
	}

	b.asyncInvokeCounter = snap.AsyncInvokeCounter
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
// Handler previously had no Snapshot/Restore of its own -- and neither did
// InMemoryBackend -- so cli.go's generic setupPersistence (which
// type-asserts the registered service.Registerable, i.e. the Handler, for a
// Snapshot/Restore pair) never picked BedrockRuntime up at all: dead wiring,
// with no persistence underneath it either. This delegation (matching the
// cleanrooms/codecommit pattern) is what wires BedrockRuntime into
// persistence for the first time.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
