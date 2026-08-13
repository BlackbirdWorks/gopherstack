package cloudfrontkeyvaluestore_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/cloudfrontkeyvaluestore"
)

// TestHandler_OwnsNoState documents why this Handler must not implement
// cli.go's duck-typed persistable shape (Snapshot(ctx) []byte /
// Restore(ctx, []byte) error). Handler.Backend is a *cloudfront.InMemoryBackend
// reference wired in cli.go (wireCloudFrontKeyValueStore) directly to the
// CloudFront service's own backend -- the same object that already
// snapshots/restores KVS store metadata and key/value data under the
// "CloudFront" persistence key (see services/cloudfront/persistence.go).
// Implementing Snapshot/Restore here would register a second
// "CloudFront KeyValueStore" persistence entry that duplicates and re-restores
// that same shared backend object. Mirrors
// services/dynamodbstreams/persistence_test.go's guard for the identical
// backend-borrowing shape.
func TestHandler_OwnsNoState(t *testing.T) {
	t.Parallel()

	type persistable interface {
		Snapshot(ctx context.Context) []byte
		Restore(context.Context, []byte) error
	}

	h := cloudfrontkeyvaluestore.NewHandler(nil)

	_, hasPersistable := any(h).(persistable)
	assert.False(t, hasPersistable, "Handler must not implement the persistable shape; "+
		"KVS state is owned and already persisted by services/cloudfront")
}
