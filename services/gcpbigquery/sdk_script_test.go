package gcpbigquery_test

import (
	"errors"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
)

func TestScriptJobs(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	ctx := t.Context()
	env.dataset(t, "scr")

	script := `
DECLARE total INT64 DEFAULT 0;
CREATE TABLE scr.nums (n INT64);
INSERT INTO scr.nums VALUES (1), (2), (3);
FOR r IN (SELECT n FROM scr.nums ORDER BY n) DO
  SET total = total + r.n;
END FOR;
SELECT total AS total, (SELECT COUNT(*) FROM scr.nums) AS cnt;`

	job, err := env.client.Query(script).Run(ctx)
	require.NoError(t, err)

	status, err := job.Wait(ctx)
	require.NoError(t, err)
	require.NoError(t, status.Err())

	assert.Equal(t, "SCRIPT", status.Statistics.Details.(*bigquery.QueryStatistics).StatementType)
	assert.Equal(t, int64(3), status.Statistics.NumChildJobs)

	it, err := job.Read(ctx)
	require.NoError(t, err)

	var row []bigquery.Value

	require.NoError(t, it.Next(&row))
	assert.Equal(t, []bigquery.Value{int64(6), int64(3)}, row)

	jobs := env.client.Jobs(ctx)
	jobs.ParentJobID = job.ID()

	var kids []string

	for {
		child, ierr := jobs.Next()
		if errors.Is(ierr, iterator.Done) {
			break
		}

		require.NoError(t, ierr)

		cs, serr := child.Status(ctx)
		require.NoError(t, serr)
		assert.Equal(t, job.ID(), cs.Statistics.ParentJobID)

		kids = append(kids, child.ID())
	}

	assert.Len(t, kids, 3)

	top := env.client.Jobs(ctx)

	for {
		j, ierr := top.Next()
		if errors.Is(ierr, iterator.Done) {
			break
		}

		require.NoError(t, ierr)
		st, serr := j.Status(ctx)
		require.NoError(t, serr)
		assert.Empty(t, st.Statistics.ParentJobID)
	}
}

func TestScriptFailureKeepsEarlierWork(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	ctx := t.Context()
	env.dataset(t, "scf")

	_, err := env.client.Query(`
CREATE TABLE scf.t (n INT64);
INSERT INTO scf.t VALUES (1);
BEGIN TRANSACTION;
INSERT INTO scf.t VALUES (2);
SELECT 1 / 0;`).Read(ctx)
	require.Error(t, err)

	assert.Equal(t, [][]bigquery.Value{{int64(1)}}, env.rows(t, "SELECT n FROM scf.t"))
}

func TestStoredProcedureViaSDK(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	ctx := t.Context()
	ds := env.dataset(t, "sp")

	_, err := env.client.Query(`CREATE PROCEDURE sp.fill(n INT64)
BEGIN
  CREATE OR REPLACE TABLE sp.out AS SELECT x FROM UNNEST(GENERATE_ARRAY(1, n)) AS x;
END`).Read(ctx)
	require.NoError(t, err)

	md, err := ds.Routine("fill").Metadata(ctx)
	require.NoError(t, err)
	assert.Equal(t, bigquery.ProcedureRoutine, md.Type)
	require.Len(t, md.Arguments, 1)

	_, err = env.client.Query("CALL sp.fill(4)").Read(ctx)
	require.NoError(t, err)

	assert.Equal(t, [][]bigquery.Value{{int64(10)}}, env.rows(t, "SELECT SUM(x) FROM sp.out"))
}
