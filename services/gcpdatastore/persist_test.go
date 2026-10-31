package gcpdatastore_test

import (
	"net/http"
	"testing"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/datastore/admin/apiv1/adminpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/gcprest/resttest"
	"github.com/blackbirdworks/gopherstack/services/gcpdatastore"
)

func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	src := newLooseEnv(t)
	idx := src.createIndex(t, "Task", adminpb.Index_NONE, prop("Done", false), prop("Priority", false))

	_, err := src.client.Put(
		t.Context(),
		datastore.NameKey("P", "n", datastore.NameKey("Parent", "p", nil)),
		&task{Name: "nested", Tags: []string{"q"}},
	)
	require.NoError(t, err)

	data := src.handler.Snapshot(t.Context())
	require.NotEmpty(t, data)

	dst := newEnv(t)
	require.NoError(t, dst.handler.Restore(t.Context(), data))

	keys := dst.keys(t, datastore.NewQuery("Task"), false)
	assert.Equal(t, []string{"a", "b", "c", "d", "e"}, names(keys))

	var got task
	require.NoError(
		t,
		dst.client.Get(t.Context(), datastore.NameKey("P", "n", datastore.NameKey("Parent", "p", nil)), &got),
	)
	assert.Equal(t, []string{"q"}, got.Tags)

	_, err = dst.admin.GetIndex(t.Context(), &adminpb.GetIndexRequest{ProjectId: project, IndexId: idx.GetIndexId()})
	require.NoError(t, err)

	dst.handler.Reset()

	keys = dst.keys(t, datastore.NewQuery("Task"), false)
	assert.Empty(t, keys)
}

func TestRestoreRejectsGarbage(t *testing.T) {
	t.Parallel()

	b := gcpdatastore.NewInMemoryBackend()
	assert.Error(t, b.Restore(t.Context(), []byte("not json")))
}

func TestREST(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	resttest.Run(t, e.handler.Addr(), []resttest.Step{
		{
			Name: "commit", Method: http.MethodPost, Path: "/v1/projects/p:commit",
			Body: `{"mode":"NON_TRANSACTIONAL","mutations":[{"upsert":{"key":{"path":[{"kind":"Task","name":"r1"}]},` +
				`"properties":{"n":{"integerValue":"5"}}}}]}`,
			Status: 200, Contains: []string{`"mutationResults"`, `"commitTime"`},
		},
		{
			Name: "lookup", Method: http.MethodPost, Path: "/v1/projects/p:lookup",
			Body:   `{"keys":[{"path":[{"kind":"Task","name":"r1"}]},{"path":[{"kind":"Task","name":"nope"}]}]}`,
			Status: 200, Contains: []string{`"found"`, `"missing"`, `"integerValue":"5"`},
		},
		{
			Name:   "run query",
			Method: http.MethodPost,
			Path:   "/v1/projects/p:runQuery",
			Body: `{"query":{"kind":[{"name":"Task"}],"filter":{"propertyFilter":` +
				`{"property":{"name":"n"},"op":"GREATER_THAN","value":{"integerValue":"1"}}}}}`,
			Status:   200,
			Contains: []string{`"entityResults"`, `"NO_MORE_RESULTS"`},
		},
		{
			Name:   "gql query",
			Method: http.MethodPost,
			Path:   "/v1/projects/p:runQuery",
			Body: `{"gqlQuery":{"queryString":"SELECT * FROM Task WHERE n = @v",` +
				`"namedBindings":{"v":{"value":{"integerValue":"5"}}}}}`,
			Status:   200,
			Contains: []string{`"r1"`},
		},
		{
			Name:   "aggregation",
			Method: http.MethodPost,
			Path:   "/v1/projects/p:runAggregationQuery",
			Body: `{"aggregationQuery":{"nestedQuery":{"kind":[{"name":"Task"}]},` +
				`"aggregations":[{"count":{},"alias":"c"}]}}`,
			Status:   200,
			Contains: []string{`"c"`, `"integerValue":"1"`},
		},
		{
			Name: "begin transaction", Method: http.MethodPost, Path: "/v1/projects/p:beginTransaction",
			Body: `{}`, Status: 200, Contains: []string{`"transaction"`},
		},
		{
			Name: "allocate ids", Method: http.MethodPost, Path: "/v1/projects/p:allocateIds",
			Body: `{"keys":[{"path":[{"kind":"Task"}]}]}`, Status: 200, Contains: []string{`"id"`},
		},
		{
			Name: "bad request maps to 400", Method: http.MethodPost, Path: "/v1/projects/p:lookup",
			Body: `{"keys":[]}`, Status: 400, Contains: []string{`"INVALID_ARGUMENT"`},
		},
		{
			Name: "list indexes", Method: http.MethodGet, Path: "/v1/projects/p/indexes",
			Status: 200,
		},
	})
}
