package app

import (
	"testing"

	"cloud.google.com/go/datastore/admin/apiv1/adminpb"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcpdatastorebackend "github.com/blackbirdworks/gopherstack/services/gcpdatastore"
	gcppubsubbackend "github.com/blackbirdworks/gopherstack/services/gcppubsub"
	gcpstoragebackend "github.com/blackbirdworks/gopherstack/services/gcpstorage"
)

func TestDatastoreExportWritesToGCS(t *testing.T) {
	t.Parallel()

	gcs := gcpstoragebackend.NewHandler(gcpstoragebackend.NewInMemoryBackend())
	ds := gcpdatastorebackend.NewHandler(gcpdatastorebackend.NewInMemoryBackend())

	wireGCPDatastore(map[string]service.Registerable{"GCPStorage": gcs, "GCPDatastore": ds})

	_, err := gcs.Backend.CreateBucket("proj", gcpstoragebackend.Patch{"name": []byte(`"lake"`)})
	require.NoError(t, err)

	op, err := ds.Backend.ExportEntities(t.Context(), &adminpb.ExportEntitiesRequest{
		ProjectId: "proj", OutputUrlPrefix: "gs://lake/dumps",
	})
	require.NoError(t, err)
	assert.True(t, op.GetDone())

	res, err := gcs.Backend.ListObjects("lake", gcpstoragebackend.ListOptions{Prefix: "dumps/"})
	require.NoError(t, err)
	require.Len(t, res.Items, 1)
	assert.Contains(t, res.Items[0].Name, ".overall_export_metadata")
}

func TestBatchNotifierPublishesToPubSub(t *testing.T) {
	t.Parallel()

	ps := gcppubsubbackend.NewHandler(gcppubsubbackend.NewInMemoryBackend())
	n := batchNotifier{b: ps.Backend}

	err := n.PublishNotification("projects/proj/topics/missing", []byte("{}"), map[string]string{"Type": "x"})
	require.Error(t, err)

	_, err = ps.Backend.CreateTopic(&pubsubpb.Topic{Name: "projects/proj/topics/jobs"})
	require.NoError(t, err)

	var got []map[string]string

	ps.Backend.ObservePublishes(func(_ string, m *pubsubpb.PubsubMessage) { got = append(got, m.GetAttributes()) })

	require.NoError(t, n.PublishNotification("projects/proj/topics/jobs", []byte("{}"), map[string]string{"Type": "x"}))
	require.Len(t, got, 1)
	assert.Equal(t, "x", got[0]["Type"])
}
