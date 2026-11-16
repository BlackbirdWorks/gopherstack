package gcpbigquery_test

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMaterializedViews(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	ctx := t.Context()
	ds := env.dataset(t, "mv")

	run := func(sql string) error {
		_, err := env.client.Query(sql).Read(ctx)

		return err
	}

	require.NoError(t, run("CREATE TABLE mv.base (k STRING, n INT64)"))
	require.NoError(t, run("INSERT INTO mv.base VALUES ('a', 1), ('a', 2), ('b', 5)"))
	require.NoError(t, run(`CREATE MATERIALIZED VIEW mv.sums
		OPTIONS (enable_refresh = true, refresh_interval_minutes = 60)
		AS SELECT k, SUM(n) AS total FROM mv.base GROUP BY k`))

	md, err := ds.Table("sums").Metadata(ctx)
	require.NoError(t, err)
	assert.Equal(t, bigquery.MaterializedView, md.Type)
	require.NotNil(t, md.MaterializedView)
	assert.Contains(t, md.MaterializedView.Query, "SUM(n)")
	assert.True(t, md.MaterializedView.EnableRefresh)
	assert.Equal(t, int64(60), int64(md.MaterializedView.RefreshInterval.Minutes()))
	require.Len(t, md.Schema, 2)

	assert.Equal(t,
		[][]bigquery.Value{{"a", int64(3)}, {"b", int64(5)}},
		env.rows(t, "SELECT k, total FROM mv.sums ORDER BY k"))

	require.NoError(t, run("INSERT INTO mv.base VALUES ('a', 10)"))
	assert.Equal(t, [][]bigquery.Value{{int64(13)}}, env.rows(t, "SELECT total FROM mv.sums WHERE k = 'a'"))

	before := md.MaterializedView.LastRefreshTime
	require.NoError(t, run("CALL BQ.REFRESH_MATERIALIZED_VIEW('mv.sums')"))

	md, err = ds.Table("sums").Metadata(ctx)
	require.NoError(t, err)
	assert.False(t, md.MaterializedView.LastRefreshTime.Before(before))

	require.Error(t, run("INSERT INTO mv.sums VALUES ('c', 1)"))
	require.Error(t, run("DROP VIEW mv.sums"))
	require.Error(t, run("DROP MATERIALIZED VIEW mv.base"))
	require.NoError(t, run("DROP MATERIALIZED VIEW mv.sums"))

	_, err = ds.Table("sums").Metadata(ctx)
	require.Error(t, err)
}

func TestMaterializedViewRESTInsert(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	ctx := t.Context()
	ds := env.dataset(t, "mvr")

	_, err := env.client.Query("CREATE TABLE mvr.base (n INT64)").Read(ctx)
	require.NoError(t, err)

	tbl := ds.Table("m")
	require.NoError(t, tbl.Create(ctx, &bigquery.TableMetadata{
		MaterializedView: &bigquery.MaterializedViewDefinition{
			Query:         "SELECT COUNT(*) AS c FROM mvr.base",
			EnableRefresh: false,
		},
	}))

	md, err := tbl.Metadata(ctx)
	require.NoError(t, err)
	assert.Equal(t, bigquery.MaterializedView, md.Type)
	assert.False(t, md.MaterializedView.EnableRefresh)
	assert.NotZero(t, md.MaterializedView.LastRefreshTime)
	require.Len(t, md.Schema, 1)

	require.Error(t, tbl.Create(ctx, &bigquery.TableMetadata{MaterializedView: &bigquery.MaterializedViewDefinition{}}))
}
