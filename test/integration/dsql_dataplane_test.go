package integration_test

import (
	"context"
	"encoding/json"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	dsqlauth "github.com/aws/aws-sdk-go-v2/feature/dsql/auth"
	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	dsqlsdk "github.com/aws/aws-sdk-go-v2/service/dsql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	dsqlDataPlaneImage   = "postgres:17"
	dsqlDataPlaneRegion  = "us-east-1"
	dsqlDataPlaneBootMax = 90 * time.Second
	dsqlClusterReadyMax  = 4 * time.Minute
)

// startDSQLDockerServer runs the pre-built linux binary on the host with the DSQL docker data plane,
// because the shared test container has no docker socket for sibling postgres containers.
func startDSQLDockerServer(t *testing.T) string {
	t.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not available")
	}

	if err := exec.CommandContext(t.Context(), "docker", "image", "inspect", dsqlDataPlaneImage).Run(); err != nil {
		t.Skipf("image %s not present", dsqlDataPlaneImage)
	}

	bin, err := filepath.Abs(preBuiltLinuxBinary)
	require.NoError(t, err)

	if _, err = os.Stat(bin); err != nil {
		t.Skip("bin/gopherstack-linux missing; run `task build-linux`")
	}

	ports := freePorts(t, 2)
	dataDir := t.TempDir()

	cmd := exec.CommandContext(t.Context(), bin)
	cmd.Env = append(os.Environ(),
		"PORT="+strconv.Itoa(ports[0]),
		"DSQL_ENGINE=docker",
		"DSQL_ENGINE_IMAGE="+dsqlDataPlaneImage,
		"PORT_RANGE_START="+strconv.Itoa(ports[1]),
		"PORT_RANGE_END="+strconv.Itoa(ports[1]+50),
		"DATA_DIR="+dataDir,
	)
	logFile, lerr := os.Create(filepath.Join(t.TempDir(), "server.log"))
	require.NoError(t, lerr)

	cmd.Stdout, cmd.Stderr = logFile, logFile

	t.Cleanup(func() {
		if t.Failed() {
			b, _ := os.ReadFile(logFile.Name())
			t.Logf("server log:\n%s", b)
		}
	})
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = 2 * time.Minute

	require.NoError(t, cmd.Start())

	t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		_ = cmd.Wait()
	})

	base := "http://127.0.0.1:" + strconv.Itoa(ports[0])

	require.Eventually(t, func() bool {
		resp, gerr := http.Get(base + "/_gopherstack/dsql/ca.pem") //nolint:noctx,gosec // local readiness probe
		if gerr != nil {
			return false
		}

		_ = resp.Body.Close()

		return resp.StatusCode == http.StatusOK
	}, dsqlDataPlaneBootMax, 200*time.Millisecond, "server with DSQL docker engine did not start")

	return base
}

func freePorts(t *testing.T, n int) []int {
	t.Helper()

	out := make([]int, 0, n)

	for range n {
		l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
		require.NoError(t, err)

		out = append(out, l.Addr().(*net.TCPAddr).Port)
		require.NoError(t, l.Close())
	}

	return out
}

func httpGetBody(ctx context.Context, u string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: %s", u, resp.Status) //nolint:err113 // test helper
	}

	return io.ReadAll(resp.Body)
}

// TestIntegration_DSQL_DataPlane drives a docker-backed cluster end to end: SDK create/waiter, IAM token over
// verified TLS via pgx, DDL/DML, CREATE INDEX ASYNC, documented limits, SDK delete.
func TestIntegration_DSQL_DataPlane(t *testing.T) {
	t.Parallel()

	base := startDSQLDockerServer(t)
	ctx := t.Context()

	iamClient := createIAMClientAt(t, base)

	_, err := iamClient.CreateUser(ctx, &iamsdk.CreateUserInput{UserName: aws.String("dsql-it")})
	require.NoError(t, err)

	key, err := iamClient.CreateAccessKey(ctx, &iamsdk.CreateAccessKeyInput{UserName: aws.String("dsql-it")})
	require.NoError(t, err)

	creds := credentials.NewStaticCredentialsProvider(
		aws.ToString(key.AccessKey.AccessKeyId), aws.ToString(key.AccessKey.SecretAccessKey), "")
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(dsqlDataPlaneRegion),
		config.WithCredentialsProvider(creds))
	require.NoError(t, err)

	client := dsqlsdk.NewFromConfig(cfg, func(o *dsqlsdk.Options) { o.BaseEndpoint = aws.String(base) })

	created, err := client.CreateCluster(ctx, &dsqlsdk.CreateClusterInput{})
	require.NoError(t, err)

	id := aws.ToString(created.Identifier)

	active, err := dsqlsdk.NewClusterActiveWaiter(client).
		WaitForOutput(ctx, &dsqlsdk.GetClusterInput{Identifier: created.Identifier}, dsqlClusterReadyMax)
	require.NoError(t, err)

	caPEM, err := httpGetBody(ctx, base+"/_gopherstack/dsql/ca.pem")
	require.NoError(t, err)

	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(caPEM))

	raw, err := httpGetBody(ctx, base+"/_gopherstack/dsql/clusters/"+id)
	require.NoError(t, err)

	host, port := parseDSQLConnInfo(t, raw)

	connect := func(ctx context.Context) *pgx.Conn {
		token, terr := dsqlauth.GenerateDBConnectAdminAuthToken(ctx, aws.ToString(active.Endpoint),
			dsqlDataPlaneRegion, creds)
		require.NoError(t, terr)

		u := url.URL{
			Scheme: "postgres", User: url.UserPassword("admin", token),
			Host: net.JoinHostPort(host, strconv.Itoa(port)), Path: "/postgres",
		}

		pc, perr := pgx.ParseConfig(u.String())
		require.NoError(t, perr)

		pc.TLSConfig = &tls.Config{RootCAs: pool, ServerName: "localhost", MinVersion: tls.VersionTLS12}

		conn, cerr := pgx.ConnectConfig(ctx, pc)
		require.NoError(t, cerr)
		t.Cleanup(func() { _ = conn.Close(context.WithoutCancel(ctx)) })

		return conn
	}

	conn := connect(ctx)

	t.Run("ddl dml and async index", func(t *testing.T) {
		_, err = conn.Exec(ctx, "CREATE TABLE items (id int PRIMARY KEY, v text)")
		require.NoError(t, err)

		_, err = conn.Exec(ctx, "INSERT INTO items VALUES (1, 'a'), (2, 'b'), (3, 'b')")
		require.NoError(t, err)

		var n int

		require.NoError(t, conn.QueryRow(ctx, "SELECT count(*) FROM items WHERE v = $1", "b").Scan(&n))
		assert.Equal(t, 2, n)

		var job string

		require.NoError(t, conn.QueryRow(ctx, "CREATE INDEX ASYNC items_v ON items (v)").Scan(&job))

		var ok bool

		require.NoError(t, conn.QueryRow(ctx, "SELECT sys.wait_for_job($1)", job).Scan(&ok))
		assert.True(t, ok)

		var status string

		require.NoError(t, conn.QueryRow(ctx, "SELECT status FROM sys.jobs WHERE job_id = $1", job).Scan(&status))
		assert.Equal(t, "completed", status)
	})

	t.Run("documented limits", func(t *testing.T) {
		var pgErr *pgconn.PgError

		_, err = conn.Exec(ctx, "CREATE INDEX plain_v ON items (v)")
		require.ErrorAs(t, err, &pgErr)
		assert.Equal(t, "0A000", pgErr.Code)

		tx, berr := conn.Begin(ctx)
		require.NoError(t, berr)

		_, err = tx.Exec(ctx, "INSERT INTO items SELECT g, 'x' FROM generate_series(100, 3200) g")
		require.ErrorAs(t, err, &pgErr)
		assert.Equal(t, "54000", pgErr.Code)
		require.NoError(t, tx.Rollback(ctx))
	})

	_, err = client.DeleteCluster(ctx, &dsqlsdk.DeleteClusterInput{Identifier: created.Identifier})
	require.NoError(t, err)

	require.NoError(t, dsqlsdk.NewClusterNotExistsWaiter(client).
		Wait(ctx, &dsqlsdk.GetClusterInput{Identifier: created.Identifier}, dsqlClusterReadyMax))

	require.Eventually(t, func() bool {
		out, perr := exec.CommandContext(ctx, "docker", "ps", "-aq", "--filter", "name=dsql-"+id).Output()

		return perr == nil && len(out) == 0
	}, time.Minute, 500*time.Millisecond, "cluster container should be removed")
}

func parseDSQLConnInfo(t *testing.T, raw []byte) (string, int) {
	t.Helper()

	var info struct {
		Host string `json:"host"`
		Port int    `json:"port"`
	}

	require.NoError(t, json.Unmarshal(raw, &info))
	require.NotEmpty(t, info.Host)
	require.NotZero(t, info.Port)

	return info.Host, info.Port
}
