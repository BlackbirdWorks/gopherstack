package gcpbigtable_test

import (
	"context"
	"testing"

	"cloud.google.com/go/bigtable"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/services/gcpbigtable"
)

const (
	project  = "test-project"
	zone     = "us-central1-b"
	instance = "test-instance"
	cluster  = "test-cluster"
)

type env struct {
	handler *gcpbigtable.Handler
	conn    *grpc.ClientConn
	iac     *bigtable.InstanceAdminClient
	ac      *bigtable.AdminClient
}

func newEnv(t *testing.T) *env {
	t.Helper()

	h := gcpbigtable.NewHandler(gcpbigtable.NewInMemoryBackend())
	h.Port = 0

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	conn, err := grpc.NewClient(h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	iac, err := bigtable.NewInstanceAdminClient(t.Context(), project, option.WithGRPCConn(conn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = iac.Close() })

	ac, err := bigtable.NewAdminClient(t.Context(), project, instance, option.WithGRPCConn(conn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = ac.Close() })

	return &env{handler: h, conn: conn, iac: iac, ac: ac}
}

func (e *env) createInstance(t *testing.T, id string) {
	t.Helper()

	require.NoError(t, e.iac.CreateInstance(t.Context(), &bigtable.InstanceConf{
		InstanceId:   id,
		DisplayName:  id,
		ClusterId:    cluster,
		Zone:         zone,
		NumNodes:     1,
		InstanceType: bigtable.PRODUCTION,
	}))
}

// seeded creates the default instance, a table with the given families and returns a data client for it.
func (e *env) seeded(t *testing.T, table string, families ...string) *bigtable.Table {
	t.Helper()

	e.createInstance(t, instance)
	require.NoError(t, e.ac.CreateTableFromConf(t.Context(), tableConf(table, families...)))

	return e.client(t).Open(table)
}

func (e *env) client(t *testing.T) *bigtable.Client {
	t.Helper()

	cfg := bigtable.ClientConfig{MetricsProvider: bigtable.NoopMetricsProvider{}}
	c, err := bigtable.NewClientWithConfig(t.Context(), project, instance, cfg, option.WithGRPCConn(e.conn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	return c
}

func tableConf(table string, families ...string) *bigtable.TableConf {
	conf := &bigtable.TableConf{TableID: table, ColumnFamilies: map[string]bigtable.Family{}}
	for _, f := range families {
		conf.ColumnFamilies[f] = bigtable.Family{}
	}

	return conf
}

func code(err error) codes.Code { return status.Code(err) }
