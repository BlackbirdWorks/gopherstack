//go:build !integration

package mediastoredata_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediastoredata"
)

func TestInMemoryBackend_ListItems_FolderSemantics(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

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
				_, err := b.PutObject(ctx, path, []byte("data"), "video/mp4", "", "TEMPORAL", "")
				require.NoError(t, err)
			}

			out := b.ListItems(ctx, mediastoredata.ListItemsInput{FolderPath: tt.folderPath})
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

func TestInMemoryBackend_ListItems_HMACPagination(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

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
				_, err := b.PutObject(
					ctx, fmt.Sprintf("/obj%02d.mp4", i), []byte("data"), "video/mp4", "", "TEMPORAL", "",
				)
				require.NoError(t, err)
			}

			var (
				allItems  []*mediastoredata.Item
				nextToken string
				pages     int
			)

			for {
				out := b.ListItems(ctx, mediastoredata.ListItemsInput{
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

func TestInMemoryBackend_ListItems_NoNameCollision(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

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
				_, err := b.PutObject(ctx, path, []byte("x"), "application/octet-stream", "", "TEMPORAL", "")
				require.NoError(t, err)
			}

			out := b.ListItems(ctx, mediastoredata.ListItemsInput{FolderPath: tt.folderPath})

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
