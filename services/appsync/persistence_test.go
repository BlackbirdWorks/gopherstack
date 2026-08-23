package appsync_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/services/appsync"
)

// Test_InMemoryBackend_SnapshotRestore exercises a full Snapshot->Restore
// round trip across every resource kind the backend supports: every
// store.Table-backed ("clean") collection registered in store_setup.go, plus
// the deliberately-unconverted raw apiKeys map that persistence.go handles
// explicitly. This is the backend-level counterpart to
// Test_RegistrySnapshotRestore in store_setup_test.go, which only exercises
// the store.Registry portion in isolation (and specifically asserts apiKeys
// is NOT carried by that lower-level path).
//
// This is a single sequential lifecycle test (populate -> snapshot -> restore
// -> verify), not a table of independent cases, so it does not use t.Run --
// see .claude/memories/parity-principles.md's test-isolation note: a
// sequential lifecycle asserted end-to-end in one function is the documented
// exception to the table-test convention.
func Test_InMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	b := newTestBackend()
	ids := populateRegistryFixture(t, b)

	// Tag the primary API too, so the round trip also proves *tags.Tags
	// (present on GraphqlAPI and DataSource) survives Snapshot/Restore.
	require.NoError(t, b.TagResource(ids.apiID, map[string]string{"env": "test"}))

	// DataSourceIntrospection: not scoped to any API (see models.go's doc comment
	// on the type), so it isn't covered by populateRegistryFixture. Proves the
	// "introspections" table added to store_setup.go round-trips too.
	introspection, err := b.StartDataSourceIntrospection(&appsync.RDSDataAPIConfig{
		DatabaseName: "mydb",
		ResourceARN:  "arn:aws:rds:us-east-1:000000000000:cluster:mycluster",
		SecretARN:    "arn:aws:secretsmanager:us-east-1:000000000000:secret:mysecret",
	})
	require.NoError(t, err)

	data := b.Snapshot(ctx)
	require.NotNil(t, data)

	restored := newTestBackend()
	require.NoError(t, restored.Restore(ctx, data))

	verifyRegistryFixtureAPIs(t, restored, ids)
	verifyRegistryFixtureChildren(t, restored, ids)

	gotCache, err := restored.GetAPICache(ids.apiID)
	require.NoError(t, err)
	assert.Equal(t, "SMALL", gotCache.Type)

	gotDomain, err := restored.GetDomainName("example.com")
	require.NoError(t, err)
	assert.Equal(t, "example.com", gotDomain.DomainName)

	gotAssoc, err := restored.GetAPIAssociation("example.com")
	require.NoError(t, err)
	assert.Equal(t, ids.apiID, gotAssoc.APIID)

	assocs, err := restored.ListSourceAPIAssociations(ids.mergedAPIID)
	require.NoError(t, err)
	require.Len(t, assocs, 1)
	assert.Equal(t, ids.apiID, assocs[0].SourceAPIID)

	// apiKeys: unlike the registry-only round trip, persistence.go's
	// Snapshot/Restore handles this explicitly, so it MUST survive.
	keys, err := restored.ListAPIKeys(ids.apiID)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	assert.Equal(t, "test key", keys[0].Description)

	// Tags: proves the *tags.Tags field on GraphqlAPI round-trips through
	// JSON (MarshalJSON/UnmarshalJSON) and is usable post-restore (not
	// left Closed).
	gotTags, err := restored.ListTagsForResource(ids.apiID)
	require.NoError(t, err)
	assert.Equal(t, "test", gotTags["env"])

	gotIntrospection, err := restored.GetDataSourceIntrospection(introspection.IntrospectionID)
	require.NoError(t, err)
	assert.Equal(t, appsync.DataSourceIntrospectionStatusSuccess, gotIntrospection.IntrospectionStatus)
}

// Test_InMemoryBackend_Restore_IncompatibleVersion verifies that a snapshot
// whose version doesn't match the current backend is discarded cleanly
// rather than partially decoded: the backend resets to empty state (dropping
// both registry-backed tables and the explicit apiKeys map) and Restore
// returns no error.
func Test_InMemoryBackend_Restore_IncompatibleVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "future version",
			data: []byte(`{"version":999,"tables":{}}`),
		},
		{
			name: "no version field (pre-persistence.go shape decodes as zero)",
			data: []byte(`{"tables":{}}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			b := newTestBackend()
			ids := populateRegistryFixture(t, b)

			err := b.Restore(ctx, tt.data)
			require.NoError(t, err)

			_, err = b.GetGraphqlAPI(ids.apiID)
			require.Error(t, err)

			apis, err := b.ListGraphqlAPIs("")
			require.NoError(t, err)
			assert.Empty(t, apis)
		})
	}
}

// Test_InMemoryBackend_Restore_V1PipelineConfigDiscarded proves
// gopherstack-hjdd's fix: a v1 snapshot holding Resolver.PipelineConfig in
// the pre-d83f4b5d3 bare-array shape must be discarded cleanly now that
// appsyncSnapshotVersion is 2, rather than erroring Restore outright when
// the registered "resolvers" table's custom UnmarshalJSON can't decode a
// JSON array into the new {functions: [...]} object field.
func Test_InMemoryBackend_Restore_V1PipelineConfigDiscarded(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	b := newTestBackend()

	v1Snapshot := []byte(`{
		"version": 1,
		"tables": {
			"resolvers": [{
				"apiId": "api-1",
				"typeName": "Query",
				"fieldName": "getThing",
				"resolverArn": "arn:aws:appsync:us-east-1:000000000000:apis/api-1/types/Query/resolvers/getThing",
				"kind": "PIPELINE",
				"pipelineConfig": ["fn-1", "fn-2"]
			}]
		}
	}`)

	require.NoError(t, b.Restore(ctx, v1Snapshot),
		"a v1 snapshot must be discarded via the version guard, not error out of RestoreAll")

	_, err := b.GetResolver("api-1", "Query", "getThing")
	require.ErrorIs(t, err, appsync.ErrNotFound,
		"incompatible-version snapshot must reset to empty, not partially decode")
}

// Test_InMemoryBackend_Restore_InvalidJSON verifies malformed JSON is
// reported as an error rather than silently discarded or partially applied.
func Test_InMemoryBackend_Restore_InvalidJSON(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// Test_InMemoryBackend_Snapshot_Empty verifies Snapshot on a fresh backend
// produces a non-nil blob that Restore can consume cleanly (the empty-state
// baseline every other case in this file builds on).
func Test_InMemoryBackend_Snapshot_Empty(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	b := newTestBackend()

	data := b.Snapshot(ctx)
	require.NotNil(t, data)

	restored := newTestBackend()
	require.NoError(t, restored.Restore(ctx, data))

	apis, err := restored.ListGraphqlAPIs("")
	require.NoError(t, err)
	assert.Empty(t, apis)
}

// Test_Handler_SnapshotRestore verifies Handler.Snapshot/Restore delegate to
// the backend (the shape persistence.Manager drives -- see
// setupPersistence in cli.go, which registers any service.Registerable
// satisfying this Snapshot(ctx)/Restore(ctx, []byte) interface
// automatically, with no per-service registration needed).
func Test_Handler_SnapshotRestore(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	b := newTestBackend()
	h := appsync.NewHandler(b)

	// Compile-time proof Handler satisfies the persistence layer's contract.
	var _ persistence.Persistable = h

	api, err := b.CreateGraphqlAPI("api1", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	data := h.Snapshot(ctx)
	require.NotNil(t, data)

	restoredBackend := newTestBackend()
	restoredHandler := appsync.NewHandler(restoredBackend)
	require.NoError(t, restoredHandler.Restore(ctx, data))

	got, err := restoredBackend.GetGraphqlAPI(api.APIID)
	require.NoError(t, err)
	assert.Equal(t, "api1", got.Name)
}
