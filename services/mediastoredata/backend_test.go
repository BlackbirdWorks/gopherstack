//go:build !integration

package mediastoredata_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/mediastoredata"
)

func newTestBackend() *mediastoredata.InMemoryBackend {
	return mediastoredata.NewInMemoryBackend()
}

func TestBackend_PutObject(t *testing.T) {
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
			name:             "standard_storage_class_accepted",
			path:             "/valid/path.mp4",
			body:             []byte("data"),
			storageClass:     "STANDARD",
			wantStorageClass: "STANDARD",
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
			obj, err := b.PutObject(tt.path, tt.body, tt.contentType, "", tt.storageClass, "")

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

func TestBackend_GetObject(t *testing.T) {
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
				_, err := b.PutObject(tt.putPath, tt.body, "video/mp4", "", "TEMPORAL", "")
				require.NoError(t, err)
			}

			obj, err := b.GetObject(tt.getPath)

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

func TestBackend_DeleteObject(t *testing.T) {
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
				_, err := b.PutObject(tt.path, []byte("data"), "video/mp4", "", "TEMPORAL", "")
				require.NoError(t, err)
			}

			err := b.DeleteObject(tt.path)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errSentinel != nil {
					require.ErrorIs(t, err, tt.errSentinel)
				}

				return
			}

			require.NoError(t, err)

			_, err = b.GetObject(tt.path)
			require.ErrorIs(t, err, awserr.ErrNotFound)
		})
	}
}

func TestBackend_UpdateObjectMetadata(t *testing.T) {
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
				_, err := b.PutObject(tt.path, []byte("data"), "video/mp4", "", "TEMPORAL", "")
				require.NoError(t, err)
			}

			err := b.UpdateObjectMetadata(tt.path, tt.contentType, tt.cacheCtrl)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errSentinel != nil {
					require.ErrorIs(t, err, tt.errSentinel)
				}

				return
			}

			require.NoError(t, err)

			obj, err := b.GetObject(tt.path)
			require.NoError(t, err)
			assert.Equal(t, tt.contentType, obj.ContentType)
			assert.Equal(t, tt.cacheCtrl, obj.CacheControl)
		})
	}
}

func TestBackend_UploadAvailability(t *testing.T) {
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
			_, err := b.PutObject("/avail/file.mp4", []byte("data"), "video/mp4", "", "TEMPORAL", tt.uploadAvailability)
			require.NoError(t, err)

			obj, err := b.GetObject("/avail/file.mp4")
			require.NoError(t, err)
			assert.Equal(t, tt.uploadAvailability, obj.UploadAvailability)
		})
	}
}

func TestBackend_Stats_RunningCounters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops       func(b *mediastoredata.InMemoryBackend)
		name      string
		wantBytes int64
		wantCount int
	}{
		{
			name:      "empty_store",
			ops:       func(_ *mediastoredata.InMemoryBackend) {},
			wantCount: 0,
			wantBytes: 0,
		},
		{
			name: "two_objects_summed",
			ops: func(b *mediastoredata.InMemoryBackend) {
				_, _ = b.PutObject("/a.mp4", []byte("hello"), "video/mp4", "", "TEMPORAL", "")
				_, _ = b.PutObject("/b.mp4", []byte("world!"), "video/mp4", "", "TEMPORAL", "")
			},
			wantCount: 2,
			wantBytes: 11,
		},
		{
			name: "delete_decrements",
			ops: func(b *mediastoredata.InMemoryBackend) {
				_, _ = b.PutObject("/x.mp4", []byte("data"), "video/mp4", "", "TEMPORAL", "")
				_ = b.DeleteObject("/x.mp4")
			},
			wantCount: 0,
			wantBytes: 0,
		},
		{
			name: "overwrite_replaces_bytes",
			ops: func(b *mediastoredata.InMemoryBackend) {
				_, _ = b.PutObject("/ov.mp4", []byte("short"), "video/mp4", "", "TEMPORAL", "")
				_, _ = b.PutObject("/ov.mp4", []byte("longer content"), "video/mp4", "", "TEMPORAL", "")
			},
			wantCount: 1,
			wantBytes: 14,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			tt.ops(b)

			stats := b.Stats()
			assert.Equal(t, tt.wantCount, stats.ObjectCount)
			assert.Equal(t, tt.wantBytes, stats.TotalBytes)
		})
	}
}

func TestBackend_ListItems_FolderSemantics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantTypes  map[string]string
		name       string
		objects    []string
		folderPath string
		wantNames  []string
	}{
		{
			name:       "direct_objects_in_root",
			objects:    []string{"/a.mp4", "/b.mp4"},
			folderPath: "",
			wantNames:  []string{"a.mp4", "b.mp4"},
			wantTypes:  map[string]string{"a.mp4": "OBJECT", "b.mp4": "OBJECT"},
		},
		{
			name:       "nested_shows_as_folder",
			objects:    []string{"/videos/clip1.mp4", "/videos/clip2.mp4"},
			folderPath: "",
			wantNames:  []string{"videos"},
			wantTypes:  map[string]string{"videos": "FOLDER"},
		},
		{
			name:       "list_subfolder",
			objects:    []string{"/videos/clip1.mp4", "/videos/clip2.mp4"},
			folderPath: "videos",
			wantNames:  []string{"clip1.mp4", "clip2.mp4"},
			wantTypes:  map[string]string{"clip1.mp4": "OBJECT", "clip2.mp4": "OBJECT"},
		},
		{
			name:       "empty_prefix_all_items",
			objects:    []string{"/root.mp4", "/sub/nested.mp4"},
			folderPath: "",
			wantNames:  []string{"root.mp4", "sub"},
			wantTypes:  map[string]string{"root.mp4": "OBJECT", "sub": "FOLDER"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			for _, path := range tt.objects {
				_, err := b.PutObject(path, []byte("data"), "video/mp4", "", "TEMPORAL", "")
				require.NoError(t, err)
			}

			out := b.ListItems(mediastoredata.ListItemsInput{FolderPath: tt.folderPath})
			require.NotNil(t, out)

			names := make([]string, 0, len(out.Items))
			for _, item := range out.Items {
				names = append(names, item.Name)
			}

			assert.Equal(t, tt.wantNames, names)

			for _, item := range out.Items {
				if wantType, ok := tt.wantTypes[item.Name]; ok {
					assert.Equal(t, wantType, item.Type, "item %q has wrong type", item.Name)
				}
			}
		})
	}
}

func TestBackend_ListItems_HMACPagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		objectCount   int
		pageSize      int
		wantPageCount int
	}{
		{
			name:          "exact_fit_single_page",
			objectCount:   5,
			pageSize:      5,
			wantPageCount: 1,
		},
		{
			name:          "two_pages",
			objectCount:   5,
			pageSize:      3,
			wantPageCount: 2,
		},
		{
			name:          "many_pages",
			objectCount:   10,
			pageSize:      3,
			wantPageCount: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			for i := range tt.objectCount {
				_, err := b.PutObject(fmt.Sprintf("/obj%02d.mp4", i), []byte("data"), "video/mp4", "", "TEMPORAL", "")
				require.NoError(t, err)
			}

			var (
				allItems  []*mediastoredata.Item
				nextToken string
				pages     int
			)

			for {
				out := b.ListItems(mediastoredata.ListItemsInput{
					MaxResults: tt.pageSize,
					NextToken:  nextToken,
				})
				pages++
				allItems = append(allItems, out.Items...)
				nextToken = out.NextToken

				if nextToken == "" {
					break
				}
			}

			assert.Equal(t, tt.wantPageCount, pages)
			assert.Len(t, allItems, tt.objectCount)
		})
	}
}

func TestBackend_ListItems_NoNameCollision(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		objects     []string
		folderPath  string
		wantNoNames []string
	}{
		{
			name:        "object_and_folder_same_prefix_no_collision",
			objects:     []string{"/a/b", "/a/b/c"},
			folderPath:  "a",
			wantNoNames: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			for _, path := range tt.objects {
				_, err := b.PutObject(path, []byte("x"), "application/octet-stream", "", "TEMPORAL", "")
				require.NoError(t, err)
			}

			out := b.ListItems(mediastoredata.ListItemsInput{FolderPath: tt.folderPath})

			seen := make(map[string]int)
			for _, item := range out.Items {
				seen[item.Name]++
			}

			for name, count := range seen {
				assert.Equal(t, 1, count, "item %q appears %d times, expected exactly once", name, count)
			}
		})
	}
}
