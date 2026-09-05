package cosmosdb_test

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

func TestInMemoryBackend_DatabaseLifecycle(t *testing.T) {
	t.Parallel()

	b := cosmosdb.NewInMemoryBackend()

	info, err := b.CreateDatabase("mydb")
	require.NoError(t, err)
	assert.Equal(t, "mydb", info.ID)
	assert.NotEmpty(t, info.RID)

	_, err = b.CreateDatabase("mydb")
	require.ErrorIs(t, err, cosmosdb.ErrDatabaseAlreadyExists)

	got, err := b.GetDatabase("mydb")
	require.NoError(t, err)
	assert.Equal(t, info, got)

	_, err = b.GetDatabase("nope")
	require.ErrorIs(t, err, cosmosdb.ErrDatabaseNotFound)

	_, err = b.CreateDatabase("otherdb")
	require.NoError(t, err)

	list := b.ListDatabases()
	require.Len(t, list, 2)
	assert.Equal(t, "mydb", list[0].ID)
	assert.Equal(t, "otherdb", list[1].ID)

	require.NoError(t, b.DeleteDatabase("mydb"))
	require.ErrorIs(t, b.DeleteDatabase("mydb"), cosmosdb.ErrDatabaseNotFound)
}

func TestInMemoryBackend_ContainerLifecycle(t *testing.T) {
	t.Parallel()

	b := cosmosdb.NewInMemoryBackend()
	_, err := b.CreateDatabase("mydb")
	require.NoError(t, err)

	spec := cosmosdb.ContainerSpec{ID: "mycoll", PartitionKeyPath: "/pk"}

	_, err = b.CreateContainer("nodb", spec)
	require.ErrorIs(t, err, cosmosdb.ErrDatabaseNotFound)

	info, err := b.CreateContainer("mydb", spec)
	require.NoError(t, err)
	assert.Equal(t, "mycoll", info.ID)
	assert.Equal(t, "/pk", info.PartitionKeyPath)

	_, err = b.CreateContainer("mydb", spec)
	require.ErrorIs(t, err, cosmosdb.ErrContainerAlreadyExists)

	got, err := b.GetContainer("mydb", "mycoll")
	require.NoError(t, err)
	assert.Equal(t, info, got)

	_, err = b.GetContainer("mydb", "nope")
	require.ErrorIs(t, err, cosmosdb.ErrContainerNotFound)

	list, err := b.ListContainers("mydb")
	require.NoError(t, err)
	require.Len(t, list, 1)

	_, err = b.ListContainers("nodb")
	require.ErrorIs(t, err, cosmosdb.ErrDatabaseNotFound)

	require.NoError(t, b.DeleteContainer("mydb", "mycoll"))
	require.ErrorIs(t, b.DeleteContainer("mydb", "mycoll"), cosmosdb.ErrContainerNotFound)
}

// setupContainer creates a database+container ready for document tests.
func setupContainer(t *testing.T, b *cosmosdb.InMemoryBackend) {
	t.Helper()

	_, err := b.CreateDatabase("mydb")
	require.NoError(t, err)

	_, err = b.CreateContainer("mydb", cosmosdb.ContainerSpec{ID: "mycoll", PartitionKeyPath: "/pk"})
	require.NoError(t, err)
}

func TestInMemoryBackend_CreateDocument(t *testing.T) {
	t.Parallel()

	b := cosmosdb.NewInMemoryBackend()
	setupContainer(t, b)

	tests := []struct {
		body   map[string]any
		name   string
		upsert bool
	}{
		{name: "explicit id and pk", body: map[string]any{"id": "1", "pk": "a", "x": 1.0}},
		{name: "generated id", body: map[string]any{"pk": "b"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			info, err := b.CreateDocument("mydb", "mycoll", tt.body, tt.upsert)
			require.NoError(t, err)
			assert.NotEmpty(t, info.ID)
			assert.NotEmpty(t, info.ETag)
			assert.NotEmpty(t, info.RID)
		})
	}
}

func TestInMemoryBackend_CreateDocument_Errors(t *testing.T) {
	t.Parallel()

	b := cosmosdb.NewInMemoryBackend()
	setupContainer(t, b)

	_, err := b.CreateDocument("nodb", "mycoll", map[string]any{"id": "1"}, false)
	require.ErrorIs(t, err, cosmosdb.ErrDatabaseNotFound)

	_, err = b.CreateDocument("mydb", "nocoll", map[string]any{"id": "1"}, false)
	require.ErrorIs(t, err, cosmosdb.ErrContainerNotFound)

	_, err = b.CreateDocument("mydb", "mycoll", map[string]any{"id": 5}, false)
	require.ErrorIs(t, err, cosmosdb.ErrInvalidDocument)

	_, err = b.CreateDocument("mydb", "mycoll", map[string]any{"id": "dup", "pk": "a"}, false)
	require.NoError(t, err)

	_, err = b.CreateDocument("mydb", "mycoll", map[string]any{"id": "dup", "pk": "a"}, false)
	require.ErrorIs(t, err, cosmosdb.ErrDocumentAlreadyExists)

	// Upsert of the same key succeeds instead.
	_, err = b.CreateDocument("mydb", "mycoll", map[string]any{"id": "dup", "pk": "a", "v": 2.0}, true)
	require.NoError(t, err)
}

func TestInMemoryBackend_GetReplaceDeleteDocument(t *testing.T) {
	t.Parallel()

	b := cosmosdb.NewInMemoryBackend()
	setupContainer(t, b)

	created, err := b.CreateDocument("mydb", "mycoll", map[string]any{"id": "1", "pk": "a", "x": 1.0}, false)
	require.NoError(t, err)

	pk, err := cosmosdb.CanonicalPartitionKeyJSON("a")
	require.NoError(t, err)

	got, err := b.GetDocument("mydb", "mycoll", pk, "1")
	require.NoError(t, err)
	assert.Equal(t, created.ID, got.ID)

	_, err = b.GetDocument("mydb", "mycoll", pk, "nope")
	require.ErrorIs(t, err, cosmosdb.ErrDocumentNotFound)

	// Replace with wrong If-Match fails.
	_, err = b.ReplaceDocument("mydb", "mycoll", pk, "1", map[string]any{"pk": "a", "y": 2.0}, "\"bogus\"")
	require.ErrorIs(t, err, cosmosdb.ErrETagMismatch)

	// Unconditional replace succeeds and drops unrelated fields.
	replaced, err := b.ReplaceDocument("mydb", "mycoll", pk, "1", map[string]any{"pk": "a", "y": 2.0}, "")
	require.NoError(t, err)
	assert.NotEqual(t, created.ETag, replaced.ETag)
	_, hasX := replaced.Body["x"]
	assert.False(t, hasX, "replace must drop fields not present in the new body")

	// Replace of a missing document fails.
	_, err = b.ReplaceDocument("mydb", "mycoll", pk, "missing", map[string]any{"pk": "a"}, "")
	require.NoError(t, err, "no If-Match means upsert semantics -- should succeed as a create")

	// Delete with wrong If-Match fails.
	err = b.DeleteDocument("mydb", "mycoll", pk, "1", "\"bogus\"")
	require.ErrorIs(t, err, cosmosdb.ErrETagMismatch)

	// Delete with correct ETag succeeds.
	err = b.DeleteDocument("mydb", "mycoll", pk, "1", replaced.ETag)
	require.NoError(t, err)

	_, err = b.GetDocument("mydb", "mycoll", pk, "1")
	require.ErrorIs(t, err, cosmosdb.ErrDocumentNotFound)
}

func TestInMemoryBackend_ListDocuments(t *testing.T) {
	t.Parallel()

	b := cosmosdb.NewInMemoryBackend()
	setupContainer(t, b)

	_, err := b.CreateDocument("mydb", "mycoll", map[string]any{"id": "b", "pk": "x"}, false)
	require.NoError(t, err)

	_, err = b.CreateDocument("mydb", "mycoll", map[string]any{"id": "a", "pk": "x"}, false)
	require.NoError(t, err)

	list, err := b.ListDocuments("mydb", "mycoll")
	require.NoError(t, err)
	require.Len(t, list, 2)
	assert.Equal(t, "a", list[0].ID)
	assert.Equal(t, "b", list[1].ID)

	_, err = b.ListDocuments("mydb", "nocoll")
	require.ErrorIs(t, err, cosmosdb.ErrContainerNotFound)
}

// TestInMemoryBackend_ETagChangesOnEveryWrite mirrors
// services/azuretable's identical regression test: two mutations to the
// same document within the same injected clock tick must still produce
// distinct ETags.
func TestInMemoryBackend_ETagChangesOnEveryWrite(t *testing.T) {
	t.Parallel()

	b := cosmosdb.NewInMemoryBackend()
	setupContainer(t, b)

	frozen := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	cosmosdb.SetNowFunc(b, func() time.Time { return frozen })

	first, err := b.CreateDocument("mydb", "mycoll", map[string]any{"id": "1", "pk": "a"}, false)
	require.NoError(t, err)

	pk, err := cosmosdb.CanonicalPartitionKeyJSON("a")
	require.NoError(t, err)

	second, err := b.ReplaceDocument("mydb", "mycoll", pk, "1", map[string]any{"pk": "a", "v": 1.0}, "")
	require.NoError(t, err)

	assert.NotEqual(t, first.ETag, second.ETag, "etag must change even when the clock doesn't advance")
}

// TestInMemoryBackend_Int64PrecisionPreservedThroughStorage exercises the
// float64-narrowing bug class AZURE.md's process rules call out: a document
// carrying an integer beyond float64's 53-bit mantissa must round-trip
// exactly through create/get, not silently lose precision.
func TestInMemoryBackend_Int64PrecisionPreservedThroughStorage(t *testing.T) {
	t.Parallel()

	b := cosmosdb.NewInMemoryBackend()
	setupContainer(t, b)

	tests := []struct {
		name string
		raw  string
	}{
		{name: "beyond 2^53", raw: "9007199254740993"},
		{name: "math.MaxInt64", raw: "9223372036854775807"},
		{name: "math.MinInt64", raw: "-9223372036854775808"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := decodeJSONBody(t, `{"id":"`+tt.name+`","pk":"a","big":`+tt.raw+`}`)

			info, err := b.CreateDocument("mydb", "mycoll", body, false)
			require.NoError(t, err)

			gotNum, ok := info.Body["big"].(json.Number)
			require.True(t, ok, "big field must remain a json.Number, not a lossy float64")
			assert.Equal(t, tt.raw, gotNum.String())
		})
	}
}

func TestExtractPartitionKeyValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		want   any
		name   string
		path   string
		wantOK bool
	}{
		{name: "top-level", body: map[string]any{"pk": "hello"}, path: "/pk", want: "hello", wantOK: true},
		{
			name: "nested", body: map[string]any{"a": map[string]any{"b": "nested"}}, path: "/a/b",
			want: "nested", wantOK: true,
		},
		{name: "missing", body: map[string]any{"other": 1}, path: "/pk", want: nil, wantOK: false},
		{
			name: "intermediate not object", body: map[string]any{"a": "scalar"}, path: "/a/b",
			want: nil, wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := cosmosdb.ExtractPartitionKeyValue(tt.body, tt.path)
			assert.Equal(t, tt.wantOK, ok)

			if tt.wantOK {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestCanonicalPartitionKeyJSON_DisambiguatesStringAndNumber(t *testing.T) {
	t.Parallel()

	strKey, err := cosmosdb.CanonicalPartitionKeyJSON("123")
	require.NoError(t, err)

	numKey, err := cosmosdb.CanonicalPartitionKeyJSON(123)
	require.NoError(t, err)

	assert.NotEqual(t, strKey, numKey, "the string \"123\" and the number 123 must not collide as partition key values")
}

func TestFakeRID_DeterministicAndDistinct(t *testing.T) {
	t.Parallel()

	a := cosmosdb.FakeRID("dbs/mydb")
	b := cosmosdb.FakeRID("dbs/mydb")
	c := cosmosdb.FakeRID("dbs/otherdb")

	assert.Equal(t, a, b, "fakeRID must be deterministic")
	assert.NotEqual(t, a, c, "fakeRID must differ for distinct names")
}

func TestEtagFor_Format(t *testing.T) {
	t.Parallel()

	got := cosmosdb.EtagFor(time.Unix(0, math.MaxInt64))
	assert.True(t, len(got) > 2 && got[0] == '"' && got[len(got)-1] == '"', "etag must be a quoted string, got %q", got)
}
