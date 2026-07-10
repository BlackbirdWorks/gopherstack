package dynamodbstreams_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/dynamodbstreams"
)

// TestHandler_OwnsNoState documents the outcome of the pkgs/store datalayer
// conversion audit (Phase 3.3) for this service: dynamodbstreams.Handler owns
// zero maps of its own. Handler.Streams is a ddbbackend.StreamsBackend
// reference wired in cli.go (wireDynamoDBStreams) directly to the DynamoDB
// service's *InMemoryDB backend — the same object that services/dynamodb
// already snapshots/restores under the "DynamoDB" persistence key (see
// services/dynamodb/persistence.go, which persists Tables including their
// StreamRecords ring buffers and StreamARN).
//
// Because this service holds no independent state, it must NOT implement the
// duck-typed persistable shape (Snapshot(ctx) []byte / Restore(ctx, []byte)
// error) that setupPersistence() in cli.go auto-registers by Name(). Doing so
// would register a second "DynamoDBStreams" persistence entry that duplicates
// the entire DynamoDB backend snapshot (not just stream data) under a second
// key and restores it a second time into the same shared backend object —
// pure waste, and a behavior change this conversion is not authorized to
// make.
//
// This test guards that invariant: if a future change makes *Handler satisfy
// the persistable shape, this test will fail, forcing a conscious decision
// rather than an accidental double-registration.
func TestHandler_OwnsNoState(t *testing.T) {
	t.Parallel()

	// Mirrors the local "persistable" interface defined in cli.go's
	// setupPersistence, which auto-registers any service.Registerable
	// satisfying this shape.
	type persistable interface {
		Snapshot(ctx context.Context) []byte
		Restore(context.Context, []byte) error
	}

	h := dynamodbstreams.NewHandler(nil)

	_, hasPersistable := any(h).(persistable)
	assert.False(t, hasPersistable, "Handler must not implement the persistable shape; "+
		"stream state is owned and already persisted by services/dynamodb")
}
