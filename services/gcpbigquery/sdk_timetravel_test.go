package gcpbigquery_test

import (
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// nextMillisecond spins until the wall clock reaches a new millisecond so writes and snapshots get distinct times.
func nextMillisecond() {
	start := time.Now().UnixMilli()

	for time.Now().UnixMilli() == start {
		continue
	}
}

func TestTimeTravel(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	ctx := t.Context()
	env.dataset(t, "tt")

	run := func(sql string) {
		t.Helper()

		_, err := env.client.Query(sql).Read(ctx)
		require.NoError(t, err, sql)
	}

	run("CREATE TABLE tt.t (n INT64)")
	run("INSERT INTO tt.t VALUES (1)")

	nextMillisecond()

	t1 := time.Now()

	nextMillisecond()
	run("INSERT INTO tt.t VALUES (2)")
	nextMillisecond()

	t2 := time.Now()

	nextMillisecond()
	run("DELETE FROM tt.t WHERE n = 1")

	asOf := func(at time.Time) [][]bigquery.Value { return asOfRows(t, env, at) }

	assert.Equal(t, [][]bigquery.Value{{int64(1)}}, asOf(t1))
	assert.Equal(t, [][]bigquery.Value{{int64(1)}, {int64(2)}}, asOf(t2))
	assert.Equal(t, [][]bigquery.Value{{int64(2)}}, env.rows(t, "SELECT n FROM tt.t ORDER BY n"))
	assert.Equal(t, [][]bigquery.Value{{int64(2)}}, asOf(time.Now()))

	other := newSDK(t)
	require.NoError(t, other.backend.Restore(ctx, env.backend.Snapshot(ctx)))
	assert.Equal(t, [][]bigquery.Value{{int64(1)}, {int64(2)}}, asOfRows(t, other, t2))
}

func asOfRows(t *testing.T, env *sdkEnv, at time.Time) [][]bigquery.Value {
	t.Helper()

	q := env.client.Query("SELECT n FROM tt.t FOR SYSTEM_TIME AS OF @at ORDER BY n")
	q.Parameters = []bigquery.QueryParameter{{Name: "at", Value: at}}

	it, err := q.Read(t.Context())
	require.NoError(t, err)

	var out [][]bigquery.Value

	for {
		var row []bigquery.Value
		if it.Next(&row) != nil {
			return out
		}

		out = append(out, row)
	}
}

func TestTimeTravelErrors(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	ctx := t.Context()
	env.dataset(t, "tte")

	_, err := env.client.Query("CREATE TABLE tte.t (n INT64)").Read(ctx)
	require.NoError(t, err)

	tests := []struct {
		name string
		sql  string
		msg  string
	}{
		{
			"future",
			"SELECT * FROM tte.t FOR SYSTEM_TIME AS OF TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)",
			"future",
		},
		{
			"too old",
			"SELECT * FROM tte.t FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 8 DAY)",
			"7 days",
		},
		{
			"before creation",
			"SELECT * FROM tte.t FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)",
			"did not exist",
		},
		{"not a timestamp", "SELECT * FROM tte.t FOR SYSTEM_TIME AS OF 5", "TIMESTAMP"},
		{
			"two points in time",
			"SELECT * FROM tte.t a FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP(), tte.t b",
			"two different points in time",
		},
		{
			"cte", "WITH c AS (SELECT 1 AS n) SELECT * FROM c FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP()", "WITH clause",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, qerr := env.client.Query(tt.sql).Read(ctx)
			require.Error(t, qerr)
			assert.Contains(t, qerr.Error(), tt.msg)
		})
	}
}
