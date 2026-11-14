package gcpbigquery_test

import (
	"errors"
	"net/http"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

func TestRoutinesREST(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	ctx := t.Context()
	ds := env.dataset(t, "rt")

	add := ds.Routine("add_one")
	require.NoError(t, add.Create(ctx, &bigquery.RoutineMetadata{
		Type:       bigquery.ScalarFunctionRoutine,
		Language:   "SQL",
		Body:       "x + 1",
		ReturnType: &bigquery.StandardSQLDataType{TypeKind: "INT64"},
		Arguments: []*bigquery.RoutineArgument{
			{Name: "x", Kind: "FIXED_TYPE", DataType: &bigquery.StandardSQLDataType{TypeKind: "INT64"}},
		},
	}))

	assert.Equal(t, [][]bigquery.Value{{int64(42)}}, env.rows(t, "SELECT rt.add_one(41)"))

	md, err := add.Metadata(ctx)
	require.NoError(t, err)
	assert.Equal(t, bigquery.ScalarFunctionRoutine, md.Type)
	assert.Equal(t, "x + 1", md.Body)
	assert.Equal(t, "SQL", md.Language)
	assert.NotEmpty(t, md.ETag)
	require.Len(t, md.Arguments, 1)
	assert.Equal(t, "INT64", md.Arguments[0].DataType.TypeKind)

	upd, err := add.Update(ctx, &bigquery.RoutineMetadataToUpdate{Body: "x + 100"}, md.ETag)
	require.NoError(t, err)
	assert.Equal(t, "x + 100", upd.Body)
	assert.Equal(t, [][]bigquery.Value{{int64(101)}}, env.rows(t, "SELECT rt.add_one(1)"))

	var gerr *googleapi.Error

	require.ErrorAs(t, add.Create(ctx, &bigquery.RoutineMetadata{
		Type: bigquery.ScalarFunctionRoutine, Body: "1",
	}), &gerr)
	assert.Equal(t, http.StatusConflict, gerr.Code)

	bad := ds.Routine("bad")
	require.ErrorAs(t, bad.Create(ctx, &bigquery.RoutineMetadata{
		Type: bigquery.ScalarFunctionRoutine, Body: "nope + 1",
	}), &gerr)
	assert.Equal(t, http.StatusBadRequest, gerr.Code)
	assert.Contains(t, gerr.Message, "Unrecognized name")

	_, err = ds.Routine("missing").Metadata(ctx)
	require.ErrorAs(t, err, &gerr)
	assert.Equal(t, http.StatusNotFound, gerr.Code)

	require.NoError(t, ds.Routine("tvf").Create(ctx, &bigquery.RoutineMetadata{
		Type: bigquery.TableValuedFunctionRoutine,
		Body: "SELECT n, n * k AS scaled FROM UNNEST([1, 2, 3]) AS n",
		Arguments: []*bigquery.RoutineArgument{
			{Name: "k", Kind: "FIXED_TYPE", DataType: &bigquery.StandardSQLDataType{TypeKind: "INT64"}},
		},
	}))
	assert.Equal(
		t,
		[][]bigquery.Value{{int64(3), int64(6)}},
		env.rows(t, "SELECT n, scaled FROM rt.tvf(2) WHERE n > 2"),
	)

	var ids []string

	it := ds.Routines(ctx)

	for {
		r, ierr := it.Next()
		if errors.Is(ierr, iterator.Done) {
			break
		}

		require.NoError(t, ierr)

		ids = append(ids, r.RoutineID)
	}

	assert.Equal(t, []string{"add_one", "tvf"}, ids)

	require.NoError(t, add.Delete(ctx))
	require.ErrorAs(t, add.Delete(ctx), &gerr)
	assert.Equal(t, http.StatusNotFound, gerr.Code)
}

func TestRoutinesDDLAndPersistence(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	ctx := t.Context()
	ds := env.dataset(t, "ddlr")

	for _, sql := range []string{
		"CREATE FUNCTION ddlr.twice(x FLOAT64) RETURNS FLOAT64 AS (x * 2)",
		"CREATE TABLE FUNCTION ddlr.evens(n INT64) AS (SELECT x FROM UNNEST(GENERATE_ARRAY(1, n)) AS x WHERE MOD(x, 2) = 0)",
	} {
		job, err := env.client.Query(sql).Run(ctx)
		require.NoError(t, err, sql)

		status, err := job.Wait(ctx)
		require.NoError(t, err, sql)
		require.NoError(t, status.Err(), sql)
	}

	md, err := ds.Routine("twice").Metadata(ctx)
	require.NoError(t, err)
	assert.Equal(t, bigquery.ScalarFunctionRoutine, md.Type)
	assert.Equal(t, "x * 2", md.Body)
	assert.Equal(t, "FLOAT64", md.ReturnType.TypeKind)

	data := env.backend.Snapshot(ctx)
	other := newSDK(t)
	require.NoError(t, other.backend.Restore(ctx, data))

	assert.Equal(t, [][]bigquery.Value{{4.0}}, other.rows(t, "SELECT ddlr.twice(2.0)"))
	assert.Equal(t, [][]bigquery.Value{{int64(2)}, {int64(4)}}, other.rows(t, "SELECT x FROM ddlr.evens(5) ORDER BY x"))

	job, err := env.client.Query("DROP FUNCTION ddlr.twice").Run(ctx)
	require.NoError(t, err)

	_, err = job.Wait(ctx)
	require.NoError(t, err)

	_, err = ds.Routine("twice").Metadata(ctx)
	require.Error(t, err)

	require.Error(t, ds.Delete(ctx))
}
