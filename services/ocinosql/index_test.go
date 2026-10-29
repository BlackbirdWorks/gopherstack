package ocinosql_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/nosql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (e *env) createIndex(t *testing.T, table, name string, keys ...nosql.IndexKey) nosql.CreateIndexResponse {
	t.Helper()

	resp, err := e.client.CreateIndex(t.Context(), nosql.CreateIndexRequest{
		TableNameOrId: new(table),
		Name:          new(name), Keys: keys, CompartmentId: new(testCompartment),
	})
	require.NoError(t, err)

	return resp
}

func TestIndexLifecycleAndOrdering(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	seedUsers(t, e)

	orderQ := "SELECT id FROM users ORDER BY age"

	_, err := e.client.Query(t.Context(), nosql.QueryRequest{
		CompartmentId: new(testCompartment), Statement: new(orderQ)})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "no index yet")

	created := e.createIndex(t, "users", "idx_age", nosql.IndexKey{ColumnName: new("age")})
	e.createIndex(t, "users", "idx_score", nosql.IndexKey{
		ColumnName: new("info"), JsonPath: new("score"), JsonFieldType: new("integer"),
	})

	got, err := e.client.GetIndex(t.Context(), nosql.GetIndexRequest{
		TableNameOrId: new("users"), IndexName: new("idx_age"), CompartmentId: new(testCompartment),
	})
	require.NoError(t, err)
	assert.Equal(t, nosql.IndexLifecycleStateCreating, got.LifecycleState)
	assert.Equal(t, "users", *got.TableName)

	_, err = e.client.Query(t.Context(), nosql.QueryRequest{
		CompartmentId: new(testCompartment), Statement: new(orderQ)})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "index still CREATING")

	e.settle()

	assert.Equal(t, nosql.WorkRequestStatusSucceeded, e.workRequest(t, created.OpcWorkRequestId).Status)

	asc := e.query(t, orderQ, 0, nil)
	assert.Equal(t, []int{4, 5, 2, 1, 6, 3}, ids(asc.Items), "missing values sort first, ties break on primary key")

	desc := e.query(t, "SELECT id FROM users ORDER BY age DESC", 0, nil)
	assert.Equal(t, []int{3, 6, 1, 2, 5, 4}, ids(desc.Items))

	byScore := e.query(t, "SELECT id FROM users ORDER BY info.score DESC LIMIT 3", 0, nil)
	assert.Equal(t, []int{2, 5, 1}, ids(byScore.Items))

	_, err = e.client.Query(t.Context(), nosql.QueryRequest{
		CompartmentId: new(testCompartment), Statement: new("SELECT id FROM users ORDER BY age ASC, id DESC")})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "mixed directions")

	list, err := e.client.ListIndexes(t.Context(), nosql.ListIndexesRequest{
		TableNameOrId: new("users"), CompartmentId: new(testCompartment),
		SortBy: nosql.ListIndexesSortByName, SortOrder: nosql.ListIndexesSortOrderAsc,
	})
	require.NoError(t, err)
	require.Len(t, list.Items, 2)
	assert.Equal(t, "idx_age", *list.Items[0].Name)
	assert.Equal(t, nosql.IndexLifecycleStateActive, list.Items[0].LifecycleState)
	assert.Equal(t, "score", *list.Items[1].Keys[0].JsonPath)
	assert.Equal(t, "INTEGER", *list.Items[1].Keys[0].JsonFieldType)

	del, err := e.client.DeleteIndex(t.Context(), nosql.DeleteIndexRequest{
		TableNameOrId: new("users"), IndexName: new("idx_age"), CompartmentId: new(testCompartment),
	})
	require.NoError(t, err)

	g2, err := e.client.GetIndex(t.Context(), nosql.GetIndexRequest{
		TableNameOrId: new("users"), IndexName: new("idx_age"), CompartmentId: new(testCompartment),
	})
	require.NoError(t, err)
	assert.Equal(t, nosql.IndexLifecycleStateDeleting, g2.LifecycleState)

	e.settle()
	assert.Equal(t, nosql.WorkRequestStatusSucceeded, e.workRequest(t, del.OpcWorkRequestId).Status)

	_, err = e.client.GetIndex(t.Context(), nosql.GetIndexRequest{
		TableNameOrId: new("users"), IndexName: new("idx_age"), CompartmentId: new(testCompartment),
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.Query(t.Context(), nosql.QueryRequest{
		CompartmentId: new(testCompartment), Statement: new(orderQ)})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "index dropped")
}

func TestIndexErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.mkTable(
		t,
		"t",
		"CREATE TABLE t (id INTEGER, name STRING, j JSON, arr ARRAY(STRING), bin BINARY, PRIMARY KEY(id))",
	)
	e.createIndex(t, "t", "idx_name", nosql.IndexKey{ColumnName: new("name")})
	e.settle()

	tests := []struct {
		name   string
		idx    string
		keys   []nosql.IndexKey
		status int
	}{
		{name: "duplicate", idx: "IDX_NAME", keys: []nosql.IndexKey{{ColumnName: new("name")}}, status: 409},
		{name: "no keys", idx: "i1", keys: []nosql.IndexKey{}, status: 400},
		{name: "unknown column", idx: "i2", keys: []nosql.IndexKey{{ColumnName: new("zzz")}}, status: 400},
		{name: "json without path", idx: "i3", keys: []nosql.IndexKey{{ColumnName: new("j")}}, status: 400},
		{
			name:   "json bad type",
			idx:    "i4",
			keys:   []nosql.IndexKey{{ColumnName: new("j"), JsonPath: new("a"), JsonFieldType: new("blob")}},
			status: 400,
		},
		{
			name:   "path on plain column",
			idx:    "i5",
			keys:   []nosql.IndexKey{{ColumnName: new("name"), JsonPath: new("a"), JsonFieldType: new("string")}},
			status: 400,
		},
		{name: "array column", idx: "i6", keys: []nosql.IndexKey{{ColumnName: new("arr")}}, status: 400},
		{name: "binary column", idx: "i7", keys: []nosql.IndexKey{{ColumnName: new("bin")}}, status: 400},
		{
			name:   "repeated key",
			idx:    "i8",
			keys:   []nosql.IndexKey{{ColumnName: new("name")}, {ColumnName: new("NAME")}},
			status: 400,
		},
		{name: "bad index name", idx: "9x", keys: []nosql.IndexKey{{ColumnName: new("name")}}, status: 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.client.CreateIndex(t.Context(), nosql.CreateIndexRequest{
				TableNameOrId: new("t"),
				Name:          new(tt.idx), Keys: tt.keys, CompartmentId: new(testCompartment),
			})
			assert.Equal(t, tt.status, svcErr(t, err).GetHTTPStatusCode())
		})
	}

	resp, err := e.client.CreateIndex(t.Context(), nosql.CreateIndexRequest{
		TableNameOrId: new("t"),
		Name:          new("idx_name"), Keys: []nosql.IndexKey{{ColumnName: new("name")}},
		CompartmentId: new(testCompartment), IsIfNotExists: new(true),
	})
	require.NoError(t, err)
	assert.NotNil(t, resp.OpcWorkRequestId)

	_, err = e.client.UpdateTable(t.Context(), nosql.UpdateTableRequest{
		TableNameOrId: new("t"),
		CompartmentId: new(testCompartment), DdlStatement: new("ALTER TABLE t (DROP name)"),
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "indexed column cannot be dropped")

	_, err = e.client.DeleteIndex(t.Context(), nosql.DeleteIndexRequest{
		TableNameOrId: new("t"), IndexName: new("ghost"), CompartmentId: new(testCompartment),
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	noop, err := e.client.DeleteIndex(t.Context(), nosql.DeleteIndexRequest{
		TableNameOrId: new("t"), IndexName: new("ghost"), CompartmentId: new(testCompartment), IsIfExists: new(true),
	})
	require.NoError(t, err)
	assert.Nil(t, noop.OpcWorkRequestId)

	_, err = e.client.ListIndexes(
		t.Context(),
		nosql.ListIndexesRequest{TableNameOrId: new("nope"), CompartmentId: new(testCompartment)},
	)
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}
