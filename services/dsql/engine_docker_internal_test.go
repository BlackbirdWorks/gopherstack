package dsql

import (
	"context"
	"crypto/tls"
	"crypto/x509"
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

	signed, _, err := v4.NewSigner().PresignHTTP(t.Context(), creds, req, emptyPayloadHash, tokenSigningName,
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

		t.Run("iam granted role", func(t *testing.T) {
			t.Parallel()

			admin := dp.mustOpen(t, "admin", dp.token(t, true), pgx.QueryExecModeCacheStatement)

			_, err := admin.Exec(ctx, "CREATE ROLE app WITH LOGIN")
			require.NoError(t, err)

			_, err = admin.Exec(ctx, "AWS IAM GRANT app TO '"+fakeUserARN+"'")
			require.NoError(t, err)

			var arn, role string

			require.NoError(t, admin.QueryRow(ctx, "SELECT arn, pg_role_name FROM sys.iam_pg_role_mappings").
				Scan(&arn, &role))
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
