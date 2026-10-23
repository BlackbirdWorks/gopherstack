package gcpsql_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sqladmin "google.golang.org/api/sqladmin/v1"
)

func TestDatabases(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	e.createInstance(t, "d1", "POSTGRES_16", nil)

	op, err := e.svc.Databases.Insert(project, "d1", &sqladmin.Database{Name: "app"}).Context(ctx).Do()
	done := e.mustWait(t, op, err)
	assert.Equal(t, "CREATE_DATABASE", done.OperationType)
	assert.True(t, e.launcher.engine(0).snapshot().dbs["app"])

	db, err := e.svc.Databases.Get(project, "d1", "app").Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, "sql#database", db.Kind)
	assert.Equal(t, "d1", db.Instance)
	assert.Equal(t, "en_US.UTF8", db.Collation)

	list, err := e.svc.Databases.List(project, "d1").Context(ctx).Do()
	require.NoError(t, err)
	require.Len(t, list.Items, 2)
	assert.Equal(t, "app", list.Items[0].Name)
	assert.Equal(t, "postgres", list.Items[1].Name)

	_, err = e.svc.Databases.Insert(project, "d1", &sqladmin.Database{Name: "app"}).Context(ctx).Do()
	code, _ := apiCode(t, err)
	assert.Equal(t, http.StatusConflict, code)

	_, err = e.svc.Databases.Delete(project, "d1", "postgres").Context(ctx).Do()
	code, _ = apiCode(t, err)
	assert.Equal(t, http.StatusBadRequest, code)

	_, err = e.svc.Databases.Patch(project, "d1", "app", &sqladmin.Database{Charset: "LATIN1"}).Context(ctx).Do()
	code, _ = apiCode(t, err)
	assert.Equal(t, http.StatusBadRequest, code)

	op, err = e.svc.Databases.Delete(project, "d1", "app").Context(ctx).Do()
	e.mustWait(t, op, err)
	assert.False(t, e.launcher.engine(0).snapshot().dbs["app"])

	_, err = e.svc.Databases.Get(project, "d1", "app").Context(ctx).Do()
	code, _ = apiCode(t, err)
	assert.Equal(t, http.StatusNotFound, code)
}

func TestMySQLSystemDatabases(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, "m1", "MYSQL_8_0", nil)

	list, err := e.svc.Databases.List(project, "m1").Context(t.Context()).Do()
	require.NoError(t, err)

	names := make([]string, 0, len(list.Items))
	for _, d := range list.Items {
		names = append(names, d.Name)
		assert.Equal(t, "utf8mb4", d.Charset)
	}

	assert.Equal(t, []string{"information_schema", "mysql", "performance_schema", "sys"}, names)
}

func TestUsers(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	e.createInstance(t, "u1", "MYSQL_8_4", nil)

	op, err := e.svc.Users.Insert(project, "u1", &sqladmin.User{Name: "app", Host: "10.%", Password: "s3cret"}).
		Context(ctx).Do()
	e.mustWait(t, op, err)
	assert.Equal(t, "s3cret", e.launcher.engine(0).snapshot().users["app@10.%"])

	got, err := e.svc.Users.Get(project, "u1", "app").Host("10.%").Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, "10.%", got.Host)
	assert.Equal(t, "BUILT_IN", got.Type)
	assert.Empty(t, got.Password)

	op, err = e.svc.Users.Update(project, "u1", &sqladmin.User{Name: "app", Host: "10.%", Password: "n3w"}).
		Name("app").Host("10.%").Context(ctx).Do()
	e.mustWait(t, op, err)
	assert.Equal(t, "n3w", e.launcher.engine(0).snapshot().users["app@10.%"])

	list, err := e.svc.Users.List(project, "u1").Context(ctx).Do()
	require.NoError(t, err)
	require.Len(t, list.Items, 2)
	assert.Equal(t, "app", list.Items[0].Name)
	assert.Equal(t, "root", list.Items[1].Name)
	assert.Equal(t, "%", list.Items[1].Host)

	_, err = e.svc.Users.Insert(project, "u1", &sqladmin.User{Name: "app", Host: "10.%"}).Context(ctx).Do()
	code, _ := apiCode(t, err)
	assert.Equal(t, http.StatusConflict, code)

	op, err = e.svc.Users.Delete(project, "u1").Name("app").Host("10.%").Context(ctx).Do()
	e.mustWait(t, op, err)
	assert.NotContains(t, e.launcher.engine(0).snapshot().users, "app@10.%")
}

func TestPostgresUsersIgnoreHostAndIAMTypes(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	e.createInstance(t, "u2", "POSTGRES_15", nil)

	op, err := e.svc.Users.Insert(project, "u2", &sqladmin.User{Name: "dev@example.com", Type: "CLOUD_IAM_USER"}).
		Context(ctx).
		Do()
	e.mustWait(t, op, err)

	got, err := e.svc.Users.Get(project, "u2", "dev@example.com").Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, "CLOUD_IAM_USER", got.Type)
	assert.Empty(t, got.Host)

	op, err = e.svc.Users.Update(project, "u2", &sqladmin.User{Name: "postgres", Password: "newroot"}).
		Name("postgres").Context(ctx).Do()
	e.mustWait(t, op, err)
	assert.Equal(t, "newroot", e.launcher.engine(0).snapshot().users["postgres@"])
}

func TestEngineFailureRevertsRecord(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	e.createInstance(t, "u3", "POSTGRES_15", nil)

	op, err := e.svc.Users.Insert(project, "u3", &sqladmin.User{Name: "boom", Password: "x"}).Context(ctx).Do()
	require.NoError(t, err)

	done := e.wait(t, op)
	require.NotNil(t, done.Error)
	assert.Contains(t, done.Error.Errors[0].Message, "role rejected")

	_, err = e.svc.Users.Get(project, "u3", "boom").Context(ctx).Do()
	code, _ := apiCode(t, err)
	assert.Equal(t, http.StatusNotFound, code)
}
