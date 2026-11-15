package gcpsql_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sqladmin "google.golang.org/api/sqladmin/v1"
)

func TestDockerExecuteSQL(t *testing.T) {
	t.Parallel()
	assertNoContainers(t, "xsql-pg", "xsql-my")

	for _, version := range dockerVersions() {
		t.Run(version, func(t *testing.T) {
			t.Parallel()

			e := dockerEnv(t)
			ctx, cancel := context.WithTimeout(t.Context(), dockerWait)
			defer cancel()

			runExecuteSQLFlow(ctx, t, e, version)
		})
	}
}

func runExecuteSQLFlow(ctx context.Context, t *testing.T, e *env, version string) {
	t.Helper()

	name := "xsql-" + shortKind(version)
	s := dockerSettings(version, false)
	s.DataApiAccess = "ALLOW_DATA_API"

	op, err := e.svc.Instances.Insert(project, &sqladmin.DatabaseInstance{
		Name: name, DatabaseVersion: version, Region: "us-central1", RootPassword: "root-pw", Settings: s,
	}).Context(ctx).Do()
	e.mustWait(t, op, err)

	op, err = e.svc.Databases.Insert(project, name, &sqladmin.Database{Name: "app"}).Context(ctx).Do()
	e.mustWait(t, op, err)

	op, err = e.svc.Users.Insert(project, name, &sqladmin.User{Name: "alice", Host: "%", Password: "alice-pw"}).
		Context(ctx).Do()
	e.mustWait(t, op, err)

	run := func(user, sqlText string, limit int64) *sqladmin.SqlInstancesExecuteSqlResponse {
		resp, rerr := e.svc.Instances.ExecuteSql(project, name, &sqladmin.ExecuteSqlPayload{
			Database: "app", User: user, SqlStatement: sqlText, RowLimit: limit,
		}).Context(ctx).Do()
		require.NoError(t, rerr)

		return resp
	}

	resp := run("alice", "CREATE TABLE items (id int primary key, name varchar(50)); "+
		"INSERT INTO items VALUES (1, 'a;b'), (2, NULL), (3, 'c'); SELECT id, name FROM items ORDER BY id", 0)
	require.Nil(t, resp.Status)
	require.Len(t, resp.Results, 3)
	assert.Empty(t, resp.Results[0].Columns)
	assert.NotEmpty(t, resp.Results[1].Message)

	sel := resp.Results[2]
	require.Len(t, sel.Columns, 2)
	assert.Equal(t, "id", sel.Columns[0].Name)
	assert.NotEmpty(t, sel.Columns[1].Type)
	require.Len(t, sel.Rows, 3)
	assert.Equal(t, "a;b", sel.Rows[0].Values[1].Value)
	assert.True(t, sel.Rows[1].Values[1].NullValue)

	limited := run("alice", "SELECT id FROM items ORDER BY id", 2)
	require.Len(t, limited.Results, 1)
	assert.Len(t, limited.Results[0].Rows, 2)

	bad := run("alice", "SELEC nothing", 0)
	require.NotNil(t, bad.Status)
	assert.NotEmpty(t, bad.Status.Message)
	assert.Empty(t, bad.Results)

	denied := run("nobody", "SELECT 1", 0)
	require.NotNil(t, denied.Status, "an unknown database user cannot log in")
}
