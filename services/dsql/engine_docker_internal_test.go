package dsql

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"net"
	"net/http"
	"net/url"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
	dsqlauth "github.com/aws/aws-sdk-go-v2/feature/dsql/auth"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/pgengine"
)

const (
	dockerEngineWait = 4 * time.Minute
	testAccount      = "123456789012"
	testRegion       = "us-east-1"
)

func dockerRuntime(t *testing.T) pgengine.Runtime {
	t.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not available")
	}

	rt, err := container.NewRuntime(container.Config{})
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	er, ok := rt.(pgengine.Runtime)
	if !ok {
		t.Skip("container runtime cannot stop/start containers")
	}

	if err = rt.Ping(t.Context()); err != nil {
		t.Skipf("container daemon unreachable: %v", err)
	}

	if err = exec.CommandContext(t.Context(), "docker", "image", "inspect", defaultEngineImage).Run(); err != nil {
		t.Skipf("image %s not present", defaultEngineImage)
	}

	return er
}

func containerExists(t *testing.T, id string) bool {
	t.Helper()

	out, err := exec.CommandContext(t.Context(), "docker", "ps", "-aq", "--filter", "name=dsql-"+id).Output()
	require.NoError(t, err)

	return strings.TrimSpace(string(out)) != ""
}

type dataPlane struct {
	b     *InMemoryBackend
	pool  *x509.CertPool
	c     *Cluster
	host  string
	creds credentials.StaticCredentialsProvider
	port  int
}

func newDataPlane(t *testing.T) *dataPlane {
	t.Helper()

	rt := dockerRuntime(t)
	dp := &dataPlane{
		b:     NewInMemoryBackend(),
		creds: credentials.NewStaticCredentialsProvider(fakeAccessKey, fakeSecretKey, ""),
	}

	allow := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"dsql:*","Resource":"*"}]}`
	cfg := IAMAuthConfig{Identities: fakeIdentities{policies: []string{allow}}, Enforce: true}

	require.NoError(t, dp.b.EnableDockerEngine(DockerConfig{
		Runtime: rt,
		Auth:    func() *IAMAuthConfig { return &cfg },
	}))
	t.Cleanup(dp.b.Close)

	c, err := dp.b.CreateCluster(testAccount, testRegion, CreateClusterInput{})
	require.NoError(t, err)
	require.Equal(t, statusCreating, c.Status)

	require.Eventually(t, func() bool {
		got, gerr := dp.b.GetCluster(c.Identifier)

		return gerr == nil && got.Status == statusActive
	}, dockerEngineWait, 500*time.Millisecond)

	dp.c, err = dp.b.GetCluster(c.Identifier)
	require.NoError(t, err)

	var ok bool

	dp.host, dp.port, ok = dp.b.ConnectAddr(c.Identifier)
	require.True(t, ok)
	require.True(t, containerExists(t, c.Identifier))

	dp.pool = x509.NewCertPool()
	require.True(t, dp.pool.AppendCertsFromPEM(dp.b.EngineCAPEM()))

	return dp
}

func (dp *dataPlane) instance(t *testing.T) *dockerInstance {
	t.Helper()

	dp.b.mu.RLock("test")
	defer dp.b.mu.RUnlock()

	inst, ok := dp.b.engine.units[dp.c.Identifier].inst.(*dockerInstance)
	require.True(t, ok)

	return inst
}

func (dp *dataPlane) token(t *testing.T, admin bool) string {
	t.Helper()

	fn := dsqlauth.GenerateDbConnectAuthToken
	if admin {
		fn = dsqlauth.GenerateDBConnectAdminAuthToken
	}

	tok, err := fn(t.Context(), dp.c.Endpoint, testRegion, dp.creds)
	require.NoError(t, err)

	return tok
}

// open connects the way DSQL clients do: TLS, database postgres, the token as the password.
func (dp *dataPlane) open(ctx context.Context, user, password string, mode pgx.QueryExecMode) (*pgx.Conn, error) {
	return dp.openDB(ctx, "postgres", user, password, mode)
}

func (dp *dataPlane) openDB(
	ctx context.Context, db, user, password string, mode pgx.QueryExecMode,
) (*pgx.Conn, error) {
	u := url.URL{
		Scheme: "postgres", User: url.UserPassword(user, password),
		Host: net.JoinHostPort(dp.host, strconv.Itoa(dp.port)), Path: "/" + db,
	}

	cfg, err := pgx.ParseConfig(u.String())
	if err != nil {
		return nil, err
	}

	cfg.TLSConfig = &tls.Config{RootCAs: dp.pool, ServerName: "localhost", MinVersion: tls.VersionTLS12}
	cfg.DefaultQueryExecMode = mode

	return pgx.ConnectConfig(ctx, cfg)
}

func (dp *dataPlane) mustOpen(t *testing.T, user, password string, mode pgx.QueryExecMode) *pgx.Conn {
	t.Helper()

	conn, err := dp.open(t.Context(), user, password, mode)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.WithoutCancel(t.Context())) })

	return conn
}

// expiredToken is a correctly signed admin token whose 15 minute lifetime ended an hour ago.
func (dp *dataPlane) expiredToken(t *testing.T) string {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://"+dp.c.Endpoint, nil)
	require.NoError(t, err)

	q := req.URL.Query()
	q.Set("Action", tokenActionAdmin)
	q.Set("X-Amz-Expires", "900")
	req.URL.RawQuery = q.Encode()

	creds, err := dp.creds.Retrieve(t.Context())
	require.NoError(t, err)

	empty := sha256.Sum256(nil)
	signed, _, err := v4.NewSigner().
		PresignHTTP(t.Context(), creds, req, hex.EncodeToString(empty[:]), tokenSigningName,
			testRegion, time.Now().Add(-time.Hour).UTC())
	require.NoError(t, err)

	return strings.TrimPrefix(signed, "https://")
}

func TestDockerDataPlane(t *testing.T) {
	t.Parallel()

	dp := newDataPlane(t)

	ctx, cancel := context.WithTimeout(t.Context(), dockerEngineWait)
	defer cancel()

	t.Run("sessions", func(t *testing.T) {
		t.Run("sql over both protocols", func(t *testing.T) {
			t.Parallel()

			admin := dp.mustOpen(t, "admin", dp.token(t, true), pgx.QueryExecModeCacheStatement)

			_, err := admin.Exec(ctx, "CREATE TABLE plain_items (id int PRIMARY KEY, v text)")
			require.NoError(t, err)

			_, err = admin.Exec(ctx, "INSERT INTO plain_items VALUES (1, 'a'), (2, 'b'), (3, 'b')")
			require.NoError(t, err)

			var n int

			require.NoError(t, admin.QueryRow(ctx, "SELECT count(*) FROM plain_items WHERE v = $1", "b").Scan(&n))
			assert.Equal(t, 2, n)

			var user, db string

			require.NoError(t, admin.QueryRow(ctx, "SELECT current_user, current_database()").Scan(&user, &db))
			assert.Equal(t, "admin", user)
			assert.Equal(t, "postgres", db)
		})

		t.Run("async index over the extended protocol", func(t *testing.T) {
			t.Parallel()

			admin := dp.mustOpen(t, "admin", dp.token(t, true), pgx.QueryExecModeCacheStatement)

			_, err := admin.Exec(ctx, "CREATE TABLE ext_items (id int PRIMARY KEY, v text)")
			require.NoError(t, err)

			var job string

			require.NoError(t, admin.QueryRow(ctx, "CREATE INDEX ASYNC ext_items_v_idx ON ext_items (v)").Scan(&job))
			assert.Len(t, job, 26)

			var ok bool

			require.NoError(t, admin.QueryRow(ctx, "SELECT sys.wait_for_job($1)", job).Scan(&ok))
			assert.True(t, ok)

			var status, jobType, name string

			require.NoError(t, admin.QueryRow(ctx,
				"SELECT status, job_type, object_name FROM sys.jobs WHERE job_id = $1", job).
				Scan(&status, &jobType, &name))
			assert.Equal(t, "completed", status)
			assert.Equal(t, "INDEX_BUILD", jobType)
			assert.Equal(t, "public.ext_items_v_idx", name)

			var valid bool

			require.NoError(t, admin.QueryRow(ctx,
				"SELECT indisvalid FROM pg_index WHERE indexrelid = 'public.ext_items_v_idx'::regclass").Scan(&valid))
			assert.True(t, valid)
		})

		t.Run("async unique index over the simple protocol fails on duplicates", func(t *testing.T) {
			t.Parallel()

			conn := dp.mustOpen(t, "admin", dp.token(t, true), pgx.QueryExecModeSimpleProtocol)

			_, err := conn.Exec(ctx, "CREATE TABLE dup_items (id int PRIMARY KEY, v text)")
			require.NoError(t, err)

			_, err = conn.Exec(ctx, "INSERT INTO dup_items VALUES (1, 'b'), (2, 'b')")
			require.NoError(t, err)

			var job string

			require.NoError(t, conn.QueryRow(ctx, "CREATE UNIQUE INDEX ASYNC dup_items_v ON dup_items (v);").Scan(&job))

			var ok bool

			require.NoError(t, conn.QueryRow(ctx, "SELECT sys.wait_for_job('"+job+"')").Scan(&ok))
			assert.False(t, ok)

			var status, details string

			require.NoError(t, conn.QueryRow(ctx, "SELECT status, details FROM sys.jobs WHERE job_id = '"+job+"'").
				Scan(&status, &details))
			assert.Equal(t, "failed", status)
			assert.Equal(t, uniqueFailure, details)
		})

		t.Run("unsupported statements", func(t *testing.T) {
			t.Parallel()

			admin := dp.mustOpen(t, "admin", dp.token(t, true), pgx.QueryExecModeCacheStatement)

			for _, stmt := range []string{"TRUNCATE plain_items", "CREATE TEMP TABLE x (a int)", "VACUUM"} {
				_, err := admin.Exec(ctx, stmt)

				var pgErr *pgconn.PgError

				require.ErrorAs(t, err, &pgErr, stmt)
				assert.Equal(t, unsupportedSQLState, pgErr.Code, stmt)
			}
		})

		t.Run("transaction rules", func(t *testing.T) {
			t.Parallel()

			admin := dp.mustOpen(t, "admin", dp.token(t, true), pgx.QueryExecModeSimpleProtocol)

			_, err := admin.Exec(ctx, "CREATE TABLE rules_items (id int PRIMARY KEY, v text)")
			require.NoError(t, err)

			tests := []struct {
				name  string
				code  string
				msg   string
				stmts []string
			}{
				{
					name: "ddl then dml", code: "0A000", msg: "ddl and dml are not supported in the same transaction",
					stmts: []string{"CREATE TABLE r1 (a int)", "INSERT INTO rules_items VALUES (1000000, 'x')"},
				},
				{
					name: "dml then ddl", code: "0A000", msg: "ddl and dml are not supported in the same transaction",
					stmts: []string{"INSERT INTO rules_items VALUES (1000001, 'x')", "CREATE TABLE r2 (a int)"},
				},
				{
					name: "two ddl", code: "0A000", msg: "only one DDL statement is allowed per transaction",
					stmts: []string{"CREATE TABLE r3 (a int)", "CREATE TABLE r4 (a int)"},
				},
				{
					name: "3001 rows", code: "54000", msg: "transaction row limit exceeded",
					stmts: []string{"INSERT INTO rules_items SELECT g, 'x' FROM generate_series(1, 3001) g"},
				},
				{
					name: "3000 rows across statements", code: "54000", msg: "transaction row limit exceeded",
					stmts: []string{
						"INSERT INTO rules_items SELECT g, 'x' FROM generate_series(10001, 12000) g",
						"INSERT INTO rules_items SELECT g, 'x' FROM generate_series(20001, 22000) g",
					},
				},
				{name: "3000 rows ok", stmts: []string{
					"INSERT INTO rules_items SELECT g, 'x' FROM generate_series(30001, 33000) g",
				}},
				{
					name:  "reads next to ddl ok",
					stmts: []string{"SELECT count(*) FROM rules_items", "CREATE TABLE r5 (a int)"},
				},
				{
					name: "update counts rows", code: "54000", msg: "transaction row limit exceeded",
					stmts: []string{
						"UPDATE rules_items SET v = 'y' WHERE id > 30000",
						"DELETE FROM rules_items WHERE id > 30000 AND id < 30002",
					},
				},
			}

			for _, tt := range tests {
				tx, berr := admin.Begin(ctx)
				require.NoError(t, berr, tt.name)

				var runErr error

				for _, stmt := range tt.stmts {
					if _, runErr = tx.Exec(ctx, stmt); runErr != nil {
						break
					}
				}

				if tt.code == "" {
					require.NoError(t, runErr, tt.name)
					require.NoError(t, tx.Commit(ctx), tt.name)

					continue
				}

				var pgErr *pgconn.PgError

				require.ErrorAs(t, runErr, &pgErr, tt.name)
				assert.Equal(t, tt.code, pgErr.Code, tt.name)
				assert.Equal(t, tt.msg, pgErr.Message, tt.name)
				require.NoError(t, tx.Rollback(ctx), tt.name)
			}

			var iso string

			require.NoError(t, admin.QueryRow(ctx, "SHOW transaction_isolation").Scan(&iso))
			assert.Equal(t, "repeatable read", iso)

			var pgErr *pgconn.PgError

			_, err = admin.Exec(ctx, "CREATE INDEX plain_idx ON rules_items (v)")
			require.ErrorAs(t, err, &pgErr)
			assert.Equal(t, unsupportedSQLState, pgErr.Code)
			assert.Equal(t, plainIndexMessage, pgErr.Message)
		})

		t.Run("wait_for_job inside a repeatable read transaction", func(t *testing.T) {
			t.Parallel()

			admin := dp.mustOpen(t, "admin", dp.token(t, true), pgx.QueryExecModeSimpleProtocol)

			_, err := admin.Exec(ctx, "CREATE TABLE rr_items (id int PRIMARY KEY, v text)")
			require.NoError(t, err)

			var job string

			require.NoError(t, admin.QueryRow(ctx, "CREATE INDEX ASYNC rr_idx ON rr_items (v)").Scan(&job))

			tx, err := admin.Begin(ctx)
			require.NoError(t, err)

			var ok bool

			require.NoError(t, tx.QueryRow(ctx, "SELECT sys.wait_for_job('"+job+"')").Scan(&ok))
			assert.True(t, ok)
			require.NoError(t, tx.Commit(ctx))
		})

		t.Run("rejected logins", func(t *testing.T) {
			t.Parallel()

			cases := map[string]struct{ user, password string }{
				"expired token":       {user: "admin", password: dp.expiredToken(t)},
				"plain password":      {user: "admin", password: "hunter2"},
				"user token as admin": {user: "admin", password: dp.token(t, false)},
				"admin token as user": {user: "ghost", password: dp.token(t, true)},
				"ungranted user":      {user: "ghost", password: dp.token(t, false)},
			}

			for name, tc := range cases {
				_, err := dp.open(ctx, tc.user, tc.password, pgx.QueryExecModeSimpleProtocol)

				var pgErr *pgconn.PgError

				require.ErrorAs(t, err, &pgErr, name)
				assert.Equal(t, pgPasswordFailedCode, pgErr.Code, name)
			}
		})

		t.Run("only the postgres database exists", func(t *testing.T) {
			t.Parallel()

			_, err := dp.openDB(ctx, "other", "admin", dp.token(t, true), pgx.QueryExecModeSimpleProtocol)

			var pgErr *pgconn.PgError

			require.ErrorAs(t, err, &pgErr)
			assert.Equal(t, pgNoDatabaseCode, pgErr.Code)
		})

		t.Run("admin creates schemas", func(t *testing.T) {
			t.Parallel()

			admin := dp.mustOpen(t, "admin", dp.token(t, true), pgx.QueryExecModeSimpleProtocol)

			_, err := admin.Exec(ctx, "CREATE SCHEMA admin_made")
			require.NoError(t, err)
		})

		t.Run("async index jobs run as the owner", func(t *testing.T) {
			t.Parallel()

			admin := dp.mustOpen(t, "admin", dp.token(t, true), pgx.QueryExecModeSimpleProtocol)

			for _, stmt := range []string{
				"CREATE ROLE esc WITH LOGIN",
				"CREATE SCHEMA escs",
				"GRANT ALL ON SCHEMA escs TO esc",
				"AWS IAM GRANT esc TO '" + fakeUserARN + "'",
			} {
				_, err := admin.Exec(ctx, stmt)
				require.NoError(t, err, stmt)
			}

			esc := dp.mustOpen(t, "esc", dp.token(t, false), pgx.QueryExecModeSimpleProtocol)

			_, err := esc.Exec(ctx, "CREATE TABLE escs.t (id int PRIMARY KEY, v text)")
			require.NoError(t, err)

			var job string

			require.NoError(t, esc.QueryRow(ctx, "CREATE INDEX ASYNC t_v ON escs.t (v)").Scan(&job))

			var ok bool

			require.NoError(t, esc.QueryRow(ctx, "SELECT sys.wait_for_job('"+job+"')").Scan(&ok))
			assert.True(t, ok)

			payload := "RESET SESSION AUTHORIZATION; ALTER ROLE esc SUPERUSER"
			_, err = esc.Exec(ctx, "SELECT sys.gs_create_index_async("+quoteLiteral(payload)+")")
			require.Error(t, err, "the submit function refuses anything but one CREATE INDEX")

			super, err := dp.instance(t).superConn(ctx)
			require.NoError(t, err)

			t.Cleanup(func() { _ = super.Close(context.WithoutCancel(t.Context())) })

			const forged = "forgedjob00000000000000000a"

			_, err = super.Exec(ctx, `INSERT INTO sys.gs_jobs (job_id, status, job_type, update_time, ddl, owner)
				VALUES ($1, 'submitted', 'INDEX_BUILD', now(), $2, 'esc')`, forged, payload)
			require.NoError(t, err)

			_, err = super.Exec(ctx, "NOTIFY "+jobChannel)
			require.NoError(t, err)

			var status, details string

			require.Eventually(t, func() bool {
				return super.QueryRow(ctx, "SELECT status, coalesce(details, '') FROM sys.jobs WHERE job_id = $1",
					forged).Scan(&status, &details) == nil && status == "failed"
			}, dockerEngineWait, 100*time.Millisecond)
			assert.Equal(t, rejectedDDL, details)

			var super2 bool

			require.NoError(t, super.QueryRow(ctx, "SELECT rolsuper FROM pg_roles WHERE rolname = 'esc'").Scan(&super2))
			assert.False(t, super2)
		})

		t.Run("iam granted role", func(t *testing.T) {
			t.Parallel()

			admin := dp.mustOpen(t, "admin", dp.token(t, true), pgx.QueryExecModeCacheStatement)

			_, err := admin.Exec(ctx, "CREATE ROLE app WITH LOGIN")
			require.NoError(t, err)

			_, err = admin.Exec(ctx, "AWS IAM GRANT app TO '"+fakeUserARN+"'")
			require.NoError(t, err)

			var arn, role string

			require.NoError(
				t,
				admin.QueryRow(ctx, "SELECT arn, pg_role_name FROM sys.iam_pg_role_mappings WHERE pg_role_name = 'app'").
					Scan(&arn, &role),
			)
			assert.Equal(t, fakeUserARN, arn)
			assert.Equal(t, "app", role)

			app := dp.mustOpen(t, "app", dp.token(t, false), pgx.QueryExecModeCacheStatement)

			var cu string

			require.NoError(t, app.QueryRow(ctx, "SELECT current_user").Scan(&cu))
			assert.Equal(t, "app", cu)

			_, err = app.Exec(ctx, "CREATE TABLE mine (a int)")
			require.Error(t, err, "non-admin roles cannot create objects in the public schema")

			_, err = app.Exec(ctx, "AWS IAM GRANT app TO 'arn:aws:iam::123456789012:user/eve'")
			require.Error(t, err, "only admin may grant")

			_, err = admin.Exec(ctx, "AWS IAM REVOKE app FROM '"+fakeUserARN+"'")
			require.NoError(t, err)

			_, err = dp.open(ctx, "app", dp.token(t, false), pgx.QueryExecModeSimpleProtocol)
			require.Error(t, err)
		})
	})

	t.Run("delete removes the container", func(t *testing.T) {
		t.Parallel()

		require.True(t, containerExists(t, dp.c.Identifier))

		_, err := dp.b.DeleteCluster(dp.c.Identifier)
		require.NoError(t, err)

		require.Eventually(t, func() bool { return !containerExists(t, dp.c.Identifier) }, dockerEngineWait,
			500*time.Millisecond)

		_, _, ok := dp.b.ConnectAddr(dp.c.Identifier)
		assert.False(t, ok)
	})
}
