package gcpdatastore_test

import (
	"context"
	"testing"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/datastore/admin/apiv1/adminpb"
	"cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/blackbirdworks/gopherstack/services/gcpdatastore"
)

const project = "test-project"

type env struct {
	client  *datastore.Client
	raw     datastorepb.DatastoreClient
	admin   adminpb.DatastoreAdminClient
	handler *gcpdatastore.Handler
	conn    *grpc.ClientConn
}

func newEnv(t *testing.T) *env {
	t.Helper()

	h := gcpdatastore.NewHandler(gcpdatastore.NewInMemoryBackend())
	h.Port = 0

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	conn, err := grpc.NewClient(h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	client, err := datastore.NewClient(t.Context(), project, option.WithGRPCConn(conn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return &env{
		client: client, raw: datastorepb.NewDatastoreClient(conn), admin: adminpb.NewDatastoreAdminClient(conn),
		handler: h, conn: conn,
	}
}

type task struct {
	Name     string
	Note     string `datastore:",noindex"`
	Tags     []string
	Priority int
	Done     bool
	Score    float64
}

func (e *env) seed(t *testing.T, kind string, tasks map[string]task) {
	t.Helper()

	for name, tk := range tasks {
		tk.Name = name
		_, err := e.client.Put(t.Context(), datastore.NameKey(kind, name, nil), &tk)
		require.NoError(t, err)
	}
}

func names(keys []*datastore.Key) []string {
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, k.Name)
	}

	return out
}

func seedTasks() map[string]task {
	return map[string]task{
		"a": {Priority: 1, Done: true, Score: 1.5, Tags: []string{"x", "y"}, Note: "first"},
		"b": {Priority: 2, Done: false, Score: 2.5, Tags: []string{"y"}},
		"c": {Priority: 3, Done: true, Score: 3.5, Tags: []string{"z"}},
		"d": {Priority: 4, Done: false, Score: 4.5},
		"e": {Priority: 5, Done: true, Score: 5.5, Tags: []string{"x"}},
	}
}
