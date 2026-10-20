package integration_test

import (
	"testing"

	"cloud.google.com/go/firestore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func newFirestoreClient(t *testing.T) *firestore.Client {
	t.Helper()

	conn, err := grpc.NewClient(gcpAddr(t, "8080"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	client, err := firestore.NewClientWithDatabase(t.Context(), gcpProject, "(default)", option.WithGRPCConn(conn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return client
}

func TestIntegration_GCP_Firestore_Lifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := newFirestoreClient(t)
	ctx := t.Context()
	coll := client.Collection(gcpName("it-users"))

	for id, age := range map[string]int{"alice": 30, "bob": 41, "carol": 25} {
		_, err := coll.Doc(id).Set(ctx, map[string]any{"name": id, "age": age})
		require.NoError(t, err)
	}

	snap, err := coll.Doc("alice").Get(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(30), snap.Data()["age"])

	_, err = coll.Doc("alice").Update(ctx, []firestore.Update{{Path: "age", Value: 31}})
	require.NoError(t, err)

	docs, err := coll.Where("age", ">", 28).OrderBy("age", firestore.Asc).Documents(ctx).GetAll()
	require.NoError(t, err)
	require.Len(t, docs, 2)
	assert.Equal(t, "alice", docs[0].Ref.ID)
	assert.Equal(t, "bob", docs[1].Ref.ID)

	all, err := coll.Documents(ctx).GetAll()
	require.NoError(t, err)
	assert.Len(t, all, 3)

	_, err = coll.Doc("bob").Delete(ctx)
	require.NoError(t, err)

	_, err = coll.Doc("bob").Get(ctx)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestIntegration_GCP_Firestore_NotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := newFirestoreClient(t)

	_, err := client.Collection(gcpName("it-absent")).Doc("nobody").Update(t.Context(),
		[]firestore.Update{{Path: "a", Value: 1}})
	assert.Equal(t, codes.NotFound, status.Code(err))
}
