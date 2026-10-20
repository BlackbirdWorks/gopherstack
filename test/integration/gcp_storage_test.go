package integration_test

import (
	"errors"
	"io"
	"net/http"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func newGCSClient(t *testing.T) *storage.Client {
	t.Helper()

	client, err := storage.NewClient(t.Context(),
		option.WithEndpoint("http://"+gcpAddr(t, "4443")+"/storage/v1/"), option.WithoutAuthentication())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return client
}

func gcsStatus(err error) int {
	if gerr, ok := errors.AsType[*googleapi.Error](err); ok {
		return gerr.Code
	}

	return 0
}

func TestIntegration_GCP_Storage_Lifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := newGCSClient(t)
	ctx := t.Context()
	bucket := client.Bucket(gcpName("it-bkt"))

	require.NoError(t, bucket.Create(ctx, gcpProject, &storage.BucketAttrs{
		Location: "US", Labels: map[string]string{"env": "it"},
	}))

	attrs, err := bucket.Attrs(ctx)
	require.NoError(t, err)
	assert.Equal(t, "it", attrs.Labels["env"])

	updated, err := bucket.Update(ctx, storage.BucketAttrsToUpdate{StorageClass: "NEARLINE"})
	require.NoError(t, err)
	assert.Equal(t, "NEARLINE", updated.StorageClass)

	obj := bucket.Object("dir/hello.txt")
	w := obj.NewWriter(ctx)
	w.ContentType = "text/plain"
	_, err = w.Write([]byte("hello gcs"))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	r, err := obj.NewReader(ctx)
	require.NoError(t, err)

	data, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	assert.Equal(t, "hello gcs", string(data))

	oa, err := obj.Update(ctx, storage.ObjectAttrsToUpdate{Metadata: map[string]string{"k": "v"}})
	require.NoError(t, err)
	assert.Equal(t, "v", oa.Metadata["k"])

	var names []string

	it := bucket.Objects(ctx, &storage.Query{Prefix: "dir/"})
	for {
		o, nerr := it.Next()
		if errors.Is(nerr, iterator.Done) {
			break
		}

		require.NoError(t, nerr)

		names = append(names, o.Name)
	}

	assert.Equal(t, []string{"dir/hello.txt"}, names)

	require.NoError(t, obj.Delete(ctx))
	require.NoError(t, bucket.Delete(ctx))
}

func TestIntegration_GCP_Storage_NotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := newGCSClient(t)
	ctx := t.Context()
	bucket := client.Bucket(gcpName("it-bkt"))

	_, err := bucket.Attrs(ctx)
	require.ErrorIs(t, err, storage.ErrBucketNotExist)
	assert.Equal(t, http.StatusNotFound, gcsStatus(err))

	require.NoError(t, bucket.Create(ctx, gcpProject, nil))

	_, err = bucket.Object("missing").NewReader(ctx)
	require.ErrorIs(t, err, storage.ErrObjectNotExist)
}
