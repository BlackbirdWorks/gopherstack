package azureblob_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azureblob"
)

func TestInMemoryBackend_ContainerCreateListDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "create_list_delete"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := azureblob.NewInMemoryBackend()

			require.NoError(t, b.CreateContainer("c1"))
			require.ErrorIs(t, b.CreateContainer("c1"), azureblob.ErrContainerAlreadyExists)

			containers := b.ListContainers()
			require.Len(t, containers, 1)
			assert.Equal(t, "c1", containers[0].Name)

			require.NoError(t, b.DeleteContainer("c1"))
			assert.Empty(t, b.ListContainers())
			assert.ErrorIs(t, b.DeleteContainer("c1"), azureblob.ErrContainerNotFound)
		})
	}
}

func TestInMemoryBackend_BlobPutGetHeadDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data string
	}{
		{name: "roundtrip", data: "hello world"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := azureblob.NewInMemoryBackend()
			require.NoError(t, b.CreateContainer("c1"))

			info, err := b.PutBlob("c1", "blob1", []byte(tt.data), "text/plain")
			require.NoError(t, err)
			assert.Equal(t, int64(len(tt.data)), info.ContentLength)
			assert.NotEmpty(t, info.ETag)

			gotInfo, gotData, err := b.GetBlob("c1", "blob1")
			require.NoError(t, err)
			assert.Equal(t, tt.data, string(gotData))
			assert.Equal(t, info.ETag, gotInfo.ETag)

			headInfo, err := b.HeadBlob("c1", "blob1")
			require.NoError(t, err)
			assert.Equal(t, info.ContentLength, headInfo.ContentLength)

			require.NoError(t, b.DeleteBlob("c1", "blob1"))
			_, _, err = b.GetBlob("c1", "blob1")
			assert.ErrorIs(t, err, azureblob.ErrBlobNotFound)
		})
	}
}

func TestInMemoryBackend_MissingContainerErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		op   func(b *azureblob.InMemoryBackend) error
		name string
	}{
		{name: "put_blob", op: func(b *azureblob.InMemoryBackend) error {
			_, err := b.PutBlob("missing", "blob1", []byte("x"), "")

			return err
		}},
		{name: "get_blob", op: func(b *azureblob.InMemoryBackend) error {
			_, _, err := b.GetBlob("missing", "blob1")

			return err
		}},
		{name: "head_blob", op: func(b *azureblob.InMemoryBackend) error {
			_, err := b.HeadBlob("missing", "blob1")

			return err
		}},
		{name: "delete_blob", op: func(b *azureblob.InMemoryBackend) error {
			return b.DeleteBlob("missing", "blob1")
		}},
		{name: "list_blobs", op: func(b *azureblob.InMemoryBackend) error {
			_, err := b.ListBlobs("missing")

			return err
		}},
		{name: "delete_container", op: func(b *azureblob.InMemoryBackend) error {
			return b.DeleteContainer("missing")
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := azureblob.NewInMemoryBackend()
			err := tt.op(b)

			require.Error(t, err)
			assert.ErrorIs(t, err, azureblob.ErrContainerNotFound, tt.name)
		})
	}
}

func TestInMemoryBackend_MissingBlobErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "get_missing_blob"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := azureblob.NewInMemoryBackend()
			require.NoError(t, b.CreateContainer("c1"))

			_, _, err := b.GetBlob("c1", "does-not-exist")
			assert.ErrorIs(t, err, azureblob.ErrBlobNotFound, tt.name)
		})
	}
}

func TestInMemoryBackend_ListBlobsSortedByName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		blobs []string
		want  []string
	}{
		{name: "sorted", blobs: []string{"c.txt", "a.txt", "b.txt"}, want: []string{"a.txt", "b.txt", "c.txt"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := azureblob.NewInMemoryBackend()
			require.NoError(t, b.CreateContainer("c1"))

			for _, name := range tt.blobs {
				_, err := b.PutBlob("c1", name, []byte("x"), "")
				require.NoError(t, err)
			}

			blobs, err := b.ListBlobs("c1")
			require.NoError(t, err)

			got := make([]string, len(blobs))
			for i, bi := range blobs {
				got[i] = bi.Name
			}

			assert.Equal(t, tt.want, got, tt.name)
		})
	}
}

func TestInMemoryBackend_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "reset_clears_all"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := azureblob.NewInMemoryBackend()
			require.NoError(t, b.CreateContainer("c1"))
			_, err := b.PutBlob("c1", "blob1", []byte("x"), "")
			require.NoError(t, err)

			b.Reset()

			assert.Empty(t, b.ListContainers(), tt.name)
		})
	}
}

func TestInMemoryBackend_PutBlobOverwrites(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		firstBody    string
		secondBody   string
		wantData     string
		wantSameETag bool
	}{
		{
			name:         "overwrite_replaces_data",
			firstBody:    "first",
			secondBody:   "second-longer",
			wantData:     "second-longer",
			wantSameETag: false,
		},
		{
			// Regression test: real Azure Blob ETags change on every
			// mutation, not just on content changes -- an ETag derived
			// purely from the body would produce the same ETag here,
			// silently breaking If-Match/If-None-Match concurrency
			// semantics (see store.go's computeBlobETag/etagSeq).
			name:         "identical_content_still_changes_etag",
			firstBody:    "same-bytes",
			secondBody:   "same-bytes",
			wantData:     "same-bytes",
			wantSameETag: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := azureblob.NewInMemoryBackend()
			require.NoError(t, b.CreateContainer("c1"))

			firstInfo, err := b.PutBlob("c1", "blob1", []byte(tt.firstBody), "")
			require.NoError(t, err)

			secondInfo, err := b.PutBlob("c1", "blob1", []byte(tt.secondBody), "")
			require.NoError(t, err)

			_, data, err := b.GetBlob("c1", "blob1")
			require.NoError(t, err)
			assert.Equal(t, tt.wantData, string(data), tt.name)
			assert.Equal(t, int64(len(tt.wantData)), secondInfo.ContentLength, tt.name)

			assert.NotEmpty(t, firstInfo.ETag, tt.name)
			assert.NotEmpty(t, secondInfo.ETag, tt.name)

			if tt.wantSameETag {
				assert.Equal(t, firstInfo.ETag, secondInfo.ETag, tt.name)
			} else {
				assert.NotEqual(t, firstInfo.ETag, secondInfo.ETag, tt.name)
			}
		})
	}
}
