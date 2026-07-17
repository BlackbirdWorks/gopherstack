//go:build !integration

package mediastoredata_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/mediastoredata"
)

func TestInMemoryBackend_PutObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errSentinel      error
		name             string
		path             string
		contentType      string
		storageClass     string
		wantStorageClass string
		body             []byte
		wantErr          bool
	}{
		{
			name:             "stores_object_successfully",
			path:             "/video/clip.mp4",
			body:             []byte("video content"),
			contentType:      "video/mp4",
			storageClass:     "TEMPORAL",
			wantStorageClass: "TEMPORAL",
		},
		{
			name:        "empty_path_rejected",
			path:        "/",
			body:        []byte("data"),
			wantErr:     true,
			errSentinel: mediastoredata.ErrInvalidPath,
		},
		{
			name:        "dotdot_path_rejected",
			path:        "/a/../b",
			body:        []byte("data"),
			wantErr:     true,
			errSentinel: mediastoredata.ErrInvalidPath,
		},
		{
			name:        "path_too_long_rejected",
			path:        "/" + strings.Repeat("a", 901),
			body:        []byte("data"),
			wantErr:     true,
			errSentinel: mediastoredata.ErrInvalidPath,
		},
		{
			name:         "invalid_storage_class_rejected",
			path:         "/valid/path.mp4",
			body:         []byte("data"),
			storageClass: "GLACIER",
			wantErr:      true,
			errSentinel:  mediastoredata.ErrInvalidStorageClass,
		},
		{
			// "STANDARD" is a valid x-amz-upload-availability value but is NOT
			// a MediaStore Data StorageClass -- the only real StorageClass is
			// "TEMPORAL" (see aws-sdk-go-v2/service/mediastoredata/types.
			// StorageClass). Confusing the two must not silently succeed.
			name:         "standard_storage_class_rejected",
			path:         "/valid/path.mp4",
			body:         []byte("data"),
			storageClass: "STANDARD",
			wantErr:      true,
			errSentinel:  mediastoredata.ErrInvalidStorageClass,
		},
		{
			name:             "empty_storage_class_defaults_to_temporal",
			path:             "/valid/path.mp4",
			body:             []byte("data"),
			storageClass:     "",
			wantStorageClass: "TEMPORAL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			obj, err := b.PutObject(context.Background(), tt.path, tt.body, tt.contentType, "", tt.storageClass, "")

			if tt.wantErr {
				require.Error(t, err)
				if tt.errSentinel != nil {
					require.ErrorIs(t, err, tt.errSentinel)
				}

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, obj.ETag)
			assert.NotEmpty(t, obj.SHA256)
			assert.Equal(t, tt.wantStorageClass, obj.StorageClass)
			assert.Equal(t, int64(len(tt.body)), obj.ContentLength)
		})
	}
}

func TestInMemoryBackend_GetObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errSentinel error
		name        string
		putPath     string
		getPath     string
		body        []byte
		wantErr     bool
	}{
		{
			name:    "retrieves_existing_object",
			putPath: "/video/clip.mp4",
			getPath: "/video/clip.mp4",
			body:    []byte("hello world"),
		},
		{
			name:        "missing_object_not_found",
			putPath:     "",
			getPath:     "/missing/file.mp4",
			wantErr:     true,
			errSentinel: mediastoredata.ErrNotFound,
		},
		{
			name:        "invalid_path_rejected",
			getPath:     "/",
			wantErr:     true,
			errSentinel: mediastoredata.ErrInvalidPath,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if tt.putPath != "" {
				_, err := b.PutObject(context.Background(), tt.putPath, tt.body, "video/mp4", "", "TEMPORAL", "")
				require.NoError(t, err)
			}

			obj, err := b.GetObject(context.Background(), tt.getPath)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errSentinel != nil {
					require.ErrorIs(t, err, tt.errSentinel)
				}

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.body, obj.Body)
		})
	}
}

func TestInMemoryBackend_DeleteObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errSentinel error
		name        string
		path        string
		createFirst bool
		wantErr     bool
	}{
		{
			name:        "deletes_existing_object",
			path:        "/delete/me.mp4",
			createFirst: true,
		},
		{
			name:        "missing_object_returns_not_found",
			path:        "/missing/file.mp4",
			wantErr:     true,
			errSentinel: mediastoredata.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if tt.createFirst {
				_, err := b.PutObject(context.Background(), tt.path, []byte("data"), "video/mp4", "", "TEMPORAL", "")
				require.NoError(t, err)
			}

			err := b.DeleteObject(context.Background(), tt.path)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errSentinel != nil {
					require.ErrorIs(t, err, tt.errSentinel)
				}

				return
			}

			require.NoError(t, err)

			_, err = b.GetObject(context.Background(), tt.path)
			require.ErrorIs(t, err, awserr.ErrNotFound)
		})
	}
}

func TestInMemoryBackend_UpdateObjectMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errSentinel error
		name        string
		path        string
		contentType string
		cacheCtrl   string
		createFirst bool
		wantErr     bool
	}{
		{
			name:        "updates_content_type",
			path:        "/update/me.mp4",
			contentType: "application/octet-stream",
			cacheCtrl:   "no-cache",
			createFirst: true,
		},
		{
			name:        "missing_object_returns_not_found",
			path:        "/missing/file.mp4",
			wantErr:     true,
			errSentinel: mediastoredata.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if tt.createFirst {
				_, err := b.PutObject(context.Background(), tt.path, []byte("data"), "video/mp4", "", "TEMPORAL", "")
				require.NoError(t, err)
			}

			err := b.UpdateObjectMetadata(context.Background(), tt.path, tt.contentType, tt.cacheCtrl)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errSentinel != nil {
					require.ErrorIs(t, err, tt.errSentinel)
				}

				return
			}

			require.NoError(t, err)

			obj, err := b.GetObject(context.Background(), tt.path)
			require.NoError(t, err)
			assert.Equal(t, tt.contentType, obj.ContentType)
			assert.Equal(t, tt.cacheCtrl, obj.CacheControl)
		})
	}
}

func TestInMemoryBackend_UploadAvailability(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		uploadAvailability string
	}{
		{name: "STANDARD_stored_and_returned", uploadAvailability: "STANDARD"},
		{name: "STREAMING_stored_and_returned", uploadAvailability: "STREAMING"},
		{name: "empty_stored_and_returned", uploadAvailability: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			_, err := b.PutObject(
				context.Background(),
				"/avail/file.mp4", []byte("data"), "video/mp4", "", "TEMPORAL", tt.uploadAvailability,
			)
			require.NoError(t, err)

			obj, err := b.GetObject(context.Background(), "/avail/file.mp4")
			require.NoError(t, err)
			assert.Equal(t, tt.uploadAvailability, obj.UploadAvailability)
		})
	}
}
