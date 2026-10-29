package rds_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/rds"
	"github.com/blackbirdworks/gopherstack/services/s3"
)

const (
	pgExtPassword = "extpass123"
	pgExtRole     = "arn:aws:iam::123456789012:role/rds-ext"
	pgExtBucket   = "ext-bucket"
)

type pgExtEnv struct {
	db     *sql.DB
	client *rdssdk.Client
	s3     *httptest.Server
}

func startPgExtEnv(t *testing.T) *pgExtEnv {
	t.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not available")
	}

	rt, err := container.NewRuntime(container.Config{})
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	er, ok := rt.(rds.EngineRuntime)
	if !ok {
		t.Skip("container runtime cannot stop/start containers")
	}

	if err = rt.Ping(t.Context()); err != nil {
		t.Skipf("container daemon unreachable: %v", err)
	}

	s3e := echo.New()
	s3e.Any("/*", s3.NewHandler(s3.NewInMemoryBackend(nil)).Handler())

	mux := http.NewServeMux()
	mux.HandleFunc("/2015-03-31/functions/{name}/invocations", func(w http.ResponseWriter, r *http.Request) {
		in, _ := io.ReadAll(r.Body)
		w.Header().Set("X-Amz-Executed-Version", "$LATEST")

		if r.Header.Get("X-Amz-Invocation-Type") == "Event" {
			w.WriteHeader(http.StatusAccepted)

			return
		}

		out, _ := json.Marshal(map[string]any{"fn": r.PathValue("name"), "echo": json.RawMessage(in)})
		_, _ = w.Write(out)
	})
	mux.Handle("/", s3e)

	api := httptest.NewServer(mux)
	t.Cleanup(api.Close)

	b := rds.NewInMemoryBackend("123456789012", "us-east-1")
	b.EnableEngine(rds.EngineConfig{
		Runtime: &recordingEngineRuntime{EngineRuntime: er}, PostgresExtensions: true, APIEndpoint: api.URL,
	})
	t.Cleanup(b.Close)

	client := newTestRDSClient(t, rds.NewHandler(b))
	_, err = client.CreateDBInstance(t.Context(), &rdssdk.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String("ext1"), DBInstanceClass: aws.String("db.t3.micro"),
		Engine: aws.String("postgres"), DBName: aws.String("appdb"), MasterUsername: aws.String("master"),
		MasterUserPassword: aws.String(pgExtPassword), AllocatedStorage: aws.Int32(20),
	})
	require.NoError(t, err)

	var inst rds.DBInstance

	require.Eventually(t, func() bool {
		got, derr := b.DescribeDBInstances("ext1")
		require.NoError(t, derr)
		inst = got[0]
		requireNotFailed(t, b, "ext1", inst.DBInstanceStatus)

		return inst.DBInstanceStatus == "available"
	}, 2*dockerEngineWait, time.Second)

	return &pgExtEnv{db: openEngineDB(t, "postgres", inst, pgExtPassword), client: client, s3: api}
}

func (e *pgExtEnv) role(t *testing.T, feature string, add bool) {
	t.Helper()

	if add {
		_, err := e.client.AddRoleToDBInstance(t.Context(), &rdssdk.AddRoleToDBInstanceInput{
			DBInstanceIdentifier: aws.String("ext1"), RoleArn: aws.String(pgExtRole), FeatureName: aws.String(feature),
		})
		require.NoError(t, err)

		return
	}

	_, err := e.client.RemoveRoleFromDBInstance(t.Context(), &rdssdk.RemoveRoleFromDBInstanceInput{
		DBInstanceIdentifier: aws.String("ext1"), RoleArn: aws.String(pgExtRole), FeatureName: aws.String(feature),
	})
	require.NoError(t, err)
}

func (e *pgExtEnv) s3Do(t *testing.T, method, path, body string) (int, string) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, e.s3.URL+path, strings.NewReader(body))
	require.NoError(t, err)

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	out, _ := io.ReadAll(resp.Body)
	require.NoError(t, resp.Body.Close())

	return resp.StatusCode, string(out)
}

func TestPostgresExtensionsDocker(t *testing.T) {
	t.Parallel()

	env := startPgExtEnv(t)
	setupCtx := dbCtx(t)

	status, _ := env.s3Do(t, http.MethodPut, "/"+pgExtBucket, "")
	require.Equal(t, http.StatusOK, status)
	status, _ = env.s3Do(t, http.MethodPut, "/"+pgExtBucket+"/in/data file.csv", "1,alpha\n2,beta\n3,gamma\n")
	require.Equal(t, http.StatusOK, status)

	_, err := env.db.ExecContext(setupCtx, "CREATE EXTENSION aws_s3 CASCADE; CREATE EXTENSION aws_lambda CASCADE")
	require.NoError(t, err)

	env.role(t, "s3Import", true)

	tests := []struct {
		run  func(ctx context.Context, t *testing.T)
		name string
	}{
		{name: "postgis", run: func(ctx context.Context, t *testing.T) {
			t.Helper()

			_, qerr := env.db.ExecContext(ctx, `CREATE EXTENSION postgis;
CREATE TABLE places (name text, geom geometry(Point, 4326));
INSERT INTO places VALUES ('a', ST_SetSRID(ST_MakePoint(0, 0), 4326)), ('b', ST_SetSRID(ST_MakePoint(1, 1), 4326)),
  ('c', ST_SetSRID(ST_MakePoint(50, 50), 4326))`)
			require.NoError(t, qerr)

			rows, qerr := env.db.QueryContext(ctx, `SELECT name FROM places
WHERE ST_DWithin(geom, ST_SetSRID(ST_MakePoint(0, 0), 4326), 2)
ORDER BY ST_Distance(geom, ST_SetSRID(ST_MakePoint(0, 0), 4326))`)
			require.NoError(t, qerr)

			defer rows.Close()

			var got []string

			for rows.Next() {
				var n string

				require.NoError(t, rows.Scan(&n))
				got = append(got, n)
			}

			require.NoError(t, rows.Err())
			assert.Equal(t, []string{"a", "b"}, got)
		}},
		{name: "pgvector", run: func(ctx context.Context, t *testing.T) {
			t.Helper()

			_, qerr := env.db.ExecContext(ctx, `CREATE EXTENSION vector;
CREATE TABLE vecs (id int, embedding vector(3));
INSERT INTO vecs VALUES (1, '[1,1,1]'), (2, '[9,9,9]'), (3, '[1,2,1]')`)
			require.NoError(t, qerr)

			var id int

			require.NoError(t, env.db.QueryRowContext(ctx,
				"SELECT id FROM vecs ORDER BY embedding <-> '[8,8,8]' LIMIT 1").Scan(&id))
			assert.Equal(t, 2, id)
		}},
		{name: "s3_import", run: func(ctx context.Context, t *testing.T) {
			t.Helper()

			_, qerr := env.db.ExecContext(ctx, "CREATE TABLE imp (id int, name text)")
			require.NoError(t, qerr)

			var msg string

			require.NoError(t, env.db.QueryRowContext(ctx,
				`SELECT aws_s3.table_import_from_s3('imp', '', '(format csv)',
  aws_commons.create_s3_uri('`+pgExtBucket+`', 'in/data file.csv', 'us-east-1'))`).Scan(&msg))
			assert.Equal(t, `3 rows imported into relation "imp" from file in/data file.csv of 23 bytes`, msg)

			var n int

			require.NoError(t, env.db.QueryRowContext(ctx, "SELECT count(*) FROM imp WHERE name = 'beta'").Scan(&n))
			assert.Equal(t, 1, n)

			_, qerr = env.db.ExecContext(ctx, `SELECT aws_s3.table_import_from_s3('imp', '', '(format csv)', '`+
				pgExtBucket+`', 'in/missing.csv', 'us-east-1')`)
			require.ErrorContains(t, qerr, "does not exist")
		}},
		{name: "s3_export_role_lifecycle", run: func(ctx context.Context, t *testing.T) {
			t.Helper()

			_, qerr := env.db.ExecContext(ctx,
				"CREATE TABLE exp AS SELECT g AS id, 'row' || g AS name FROM generate_series(1, 4) g")
			require.NoError(t, qerr)

			const q = `SELECT * FROM aws_s3.query_export_to_s3('SELECT * FROM exp ORDER BY id', '` +
				pgExtBucket + `', 'out/exp.csv', 'us-east-1', 'format csv')`

			_, qerr = env.db.ExecContext(ctx, q)
			require.ErrorContains(t, qerr, "s3Export")

			env.role(t, "s3Export", true)

			var rows, files, bytes int64

			require.NoError(t, env.db.QueryRowContext(ctx, q).Scan(&rows, &files, &bytes))
			assert.EqualValues(t, 4, rows)
			assert.EqualValues(t, 1, files)

			code, body := env.s3Do(t, http.MethodGet, "/"+pgExtBucket+"/out/exp.csv", "")
			require.Equal(t, http.StatusOK, code)
			assert.Equal(t, "1,row1\n2,row2\n3,row3\n4,row4\n", body)
			assert.EqualValues(t, len(body), bytes)
		}},
		{name: "lambda_role_lifecycle", run: func(ctx context.Context, t *testing.T) {
			t.Helper()

			const q = `SELECT status_code, payload::text, executed_version FROM aws_lambda.invoke(
  aws_commons.create_lambda_function_arn('my-fn', 'us-east-1'), '{"k": 1}'::json)`

			_, qerr := env.db.ExecContext(ctx, q)
			require.ErrorContains(t, qerr, "Lambda")

			env.role(t, "Lambda", true)

			var (
				code    int
				payload string
				version string
			)

			require.NoError(t, env.db.QueryRowContext(ctx, q).Scan(&code, &payload, &version))
			assert.Equal(t, 200, code)
			assert.JSONEq(t, `{"fn":"my-fn","echo":{"k":1}}`, payload)
			assert.Equal(t, "$LATEST", version)

			var evCode int

			var evPayload sql.NullString

			require.NoError(t, env.db.QueryRowContext(ctx, `SELECT status_code, payload::text FROM aws_lambda.invoke(
  'my-fn', '{}'::jsonb, 'us-east-1', 'Event')`).Scan(&evCode, &evPayload))
			assert.Equal(t, 202, evCode)
			assert.False(t, evPayload.Valid)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(dbCtx(t), t)
		})
	}
}
