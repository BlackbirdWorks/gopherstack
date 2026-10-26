package rds_test

import (
	"context"
	"database/sql"
	"net"
	"net/http/httptest"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	rdsdatasdk "github.com/aws/aws-sdk-go-v2/service/rdsdata"
	rdsdatatypes "github.com/aws/aws-sdk-go-v2/service/rdsdata/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/rds"
	"github.com/blackbirdworks/gopherstack/services/rdsdata"
)

const (
	autoPauseTestScale = 0.004
	autoPauseWait      = 20 * time.Second
	engineClusterPass  = "pw-12345678"
)

func createPausableCluster(t *testing.T, b *rds.InMemoryBackend, id, engine string, http bool) {
	t.Helper()

	_, err := b.CreateDBCluster(id, engine, "master", "appdb", "", 0,
		&rds.ServerlessV2ScalingConfiguration{MinCapacity: 0, MaxCapacity: 4},
		rds.DBClusterOptions{MasterUserPassword: engineClusterPass, EnableHTTPEndpoint: http})
	require.NoError(t, err)
}

func waitClusterAvailable(t *testing.T, b *rds.InMemoryBackend, id string, wait time.Duration) rds.DBCluster {
	t.Helper()

	var c rds.DBCluster

	require.Eventually(t, func() bool {
		got, err := b.DescribeDBClusters(id)
		require.NoError(t, err)
		requireNotFailed(t, b, id, got[0].Status)

		c = got[0]

		return c.Status == availableState && c.Port != 0
	}, wait, 10*time.Millisecond)

	return c
}

func TestServerlessV2EnginePauseResume(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		engine string
		hold   bool
	}{
		{name: "postgres", engine: "aurora-postgresql", hold: true},
		{name: "mysql", engine: "aurora-mysql"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newEngineFixture(t, engineWait)
			f.b.SetAutoPause(rds.AutoPauseConfig{IdleScale: autoPauseTestScale})
			createPausableCluster(t, f.b, "eng1", tt.engine, false)

			c := waitClusterAvailable(t, f.b, "eng1", engineWait)

			require.Eventually(
				t,
				func() bool { return f.b.IsClusterAutoPaused("eng1") },
				autoPauseWait,
				20*time.Millisecond,
			)
			require.Eventually(
				t,
				func() bool { return stopCount(f) == 1 },
				autoPauseWait,
				10*time.Millisecond,
			)

			got := waitClusterAvailable(t, f.b, "eng1", engineWait)
			assert.Equal(t, availableState, got.Status, "a paused cluster still reports available")
			assert.True(t, rds.ClusterAutoPaused(&got))

			conn, err := (&net.Dialer{}).DialContext(
				t.Context(),
				"tcp",
				net.JoinHostPort(c.Endpoint, strconv.Itoa(c.Port)),
			)
			require.NoError(t, err)

			t.Cleanup(func() { _ = conn.Close() })

			require.Eventually(t, func() bool {
				_, starts := f.rt.counts()

				return starts == 1 && !f.b.IsClusterAutoPaused("eng1")
			}, autoPauseWait, 10*time.Millisecond, "a client connection resumes the cluster")

			if tt.hold {
				require.Never(t, func() bool { return f.b.IsClusterAutoPaused("eng1") },
					3*time.Second, 100*time.Millisecond, "an open connection keeps the cluster awake")
			}

			require.NoError(t, conn.Close())
			require.Eventually(
				t,
				func() bool { return f.b.IsClusterAutoPaused("eng1") },
				autoPauseWait,
				20*time.Millisecond,
				"idle again after the connection closed",
			)
		})
	}
}

type staticSecret struct{}

func (staticSecret) SecretString(context.Context, string) (string, error) { return "{}", nil }

func newDataAPIClient(t *testing.T, bk *rdsdata.InMemoryBackend) *rdsdatasdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(rdsdata.NewHandler(bk)))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(t.Context(), awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	require.NoError(t, err)

	return rdsdatasdk.NewFromConfig(cfg, func(o *rdsdatasdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })
}

func TestServerlessV2DataAPIResumingException(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend(t)
	b.SetAutoPause(rds.AutoPauseConfig{IdleScale: autoPauseTestScale})
	createPausableCluster(t, b, "dapi1", "aurora-postgresql", true)

	data := rdsdata.NewInMemoryBackend("123456789012", "us-east-1").WithRealEngine(b, staticSecret{})
	t.Cleanup(data.Close)

	client := newDataAPIClient(t, data)
	arn := waitClusterAvailable(t, b, "dapi1", engineWait).DBClusterArn

	call := func() error {
		_, err := client.ExecuteStatement(t.Context(), &rdsdatasdk.ExecuteStatementInput{
			ResourceArn: aws.String(
				arn,
			), SecretArn: aws.String("arn:aws:secretsmanager:us-east-1:123456789012:secret:s"),
			Sql: aws.String("SELECT 1"),
		})

		return err
	}

	require.NoError(t, call(), "an awake cluster serves the call")

	require.Eventually(t, func() bool { return b.IsClusterAutoPaused("dapi1") }, autoPauseWait, 20*time.Millisecond)

	var resuming *rdsdatatypes.DatabaseResumingException

	require.ErrorAs(t, call(), &resuming, "the first call on a paused cluster")

	require.Eventually(t, func() bool { return !b.IsClusterAutoPaused("dapi1") }, autoPauseWait, 10*time.Millisecond)
	require.NoError(t, call(), "calls succeed once the cluster resumed")
}

func dockerContainerRunning(t *testing.T, id string) bool {
	t.Helper()

	out, err := exec.CommandContext(t.Context(), "docker", "inspect", "-f", "{{.State.Running}}", id).Output()
	require.NoError(t, err)

	return strings.TrimSpace(string(out)) == "true"
}

func TestServerlessV2DockerPauseResume(t *testing.T) {
	t.Parallel()

	b, rec := newDockerEngineBackend(t)
	b.SetAutoPause(rds.AutoPauseConfig{IdleScale: autoPauseTestScale})
	createPausableCluster(t, b, "dock1", "aurora-postgresql", false)

	c := waitClusterAvailable(t, b, "dock1", dockerEngineWait)
	addr := net.JoinHostPort(c.Endpoint, strconv.Itoa(c.Port))

	open := func() *sql.DB {
		db, err := sql.Open("pgx", "postgres://master:"+engineClusterPass+"@"+addr+"/appdb?sslmode=disable")
		require.NoError(t, err)

		return db
	}

	db := open()
	_, err := db.ExecContext(t.Context(), createItems)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), insertItem)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	ids := rec.containerIDs()
	require.Len(t, ids, 1)

	require.Eventually(t, func() bool { return b.IsClusterAutoPaused("dock1") }, autoPauseWait, 50*time.Millisecond)
	require.Eventually(t, func() bool { return !dockerContainerRunning(t, ids[0]) }, time.Minute, 250*time.Millisecond,
		"pausing stops the engine container")
	assert.True(t, dockerContainerExists(t, ids[0]), "the stopped container keeps its data volume")

	db = open()
	t.Cleanup(func() { _ = db.Close() })

	var name string

	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT name FROM items WHERE id = 1").Scan(&name),
		"connecting resumes the cluster")
	assert.Equal(t, "alpha", name, "data survives pause and resume")
	assert.True(t, dockerContainerRunning(t, ids[0]))
	assert.False(t, b.IsClusterAutoPaused("dock1"))
}

func stopCount(f *engineFixture) int {
	stops, _ := f.rt.counts()

	return stops
}
