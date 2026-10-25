package gcpstorage_test

import (
	"encoding/json"
	"sync"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcpstorage"
)

type recorder struct {
	mu  sync.Mutex
	got []gcpstorage.ObjectEvent
}

func (r *recorder) observe(ev gcpstorage.ObjectEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.got = append(r.got, ev)
}

func (r *recorder) types() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]string, 0, len(r.got))
	for _, ev := range r.got {
		out = append(out, string(ev.Type)+":"+ev.Object.Name)
	}

	return out
}

func TestObjectEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		versioned bool
		act       func(t *testing.T, e *sdkEnv, b *storage.BucketHandle)
		want      []string
	}{
		{
			name: "upload finalizes",
			act: func(t *testing.T, _ *sdkEnv, b *storage.BucketHandle) {
				t.Helper()
				write(t, b.Object("a.txt"), []byte("one"), 0, nil)
			},
			want: []string{"finalized:a.txt"},
		},
		{
			name: "overwrite deletes the old object then finalizes",
			act: func(t *testing.T, _ *sdkEnv, b *storage.BucketHandle) {
				t.Helper()
				write(t, b.Object("a.txt"), []byte("one"), 0, nil)
				write(t, b.Object("a.txt"), []byte("two"), 0, nil)
			},
			want: []string{"finalized:a.txt", "deleted:a.txt", "finalized:a.txt"},
		},
		{
			name:      "versioned overwrite archives the old object",
			versioned: true,
			act: func(t *testing.T, _ *sdkEnv, b *storage.BucketHandle) {
				t.Helper()
				write(t, b.Object("a.txt"), []byte("one"), 0, nil)
				write(t, b.Object("a.txt"), []byte("two"), 0, nil)
			},
			want: []string{"finalized:a.txt", "archived:a.txt", "finalized:a.txt"},
		},
		{
			name: "metadata patch",
			act: func(t *testing.T, _ *sdkEnv, b *storage.BucketHandle) {
				t.Helper()
				write(t, b.Object("a.txt"), []byte("one"), 0, nil)
				_, err := b.Object("a.txt").Update(t.Context(), storage.ObjectAttrsToUpdate{
					Metadata: map[string]string{"k": "v"},
				})
				require.NoError(t, err)
			},
			want: []string{"finalized:a.txt", "metadataUpdated:a.txt"},
		},
		{
			name: "delete",
			act: func(t *testing.T, _ *sdkEnv, b *storage.BucketHandle) {
				t.Helper()
				write(t, b.Object("a.txt"), []byte("one"), 0, nil)
				require.NoError(t, b.Object("a.txt").Delete(t.Context()))
			},
			want: []string{"finalized:a.txt", "deleted:a.txt"},
		},
		{
			name:      "versioned delete archives",
			versioned: true,
			act: func(t *testing.T, _ *sdkEnv, b *storage.BucketHandle) {
				t.Helper()
				write(t, b.Object("a.txt"), []byte("one"), 0, nil)
				require.NoError(t, b.Object("a.txt").Delete(t.Context()))
			},
			want: []string{"finalized:a.txt", "archived:a.txt"},
		},
		{
			name: "copy and compose finalize the destination",
			act: func(t *testing.T, _ *sdkEnv, b *storage.BucketHandle) {
				t.Helper()
				write(t, b.Object("src"), []byte("one"), 0, nil)
				_, err := b.Object("copy").CopierFrom(b.Object("src")).Run(t.Context())
				require.NoError(t, err)
				_, err = b.Object("joined").ComposerFrom(b.Object("src"), b.Object("copy")).Run(t.Context())
				require.NoError(t, err)
			},
			want: []string{"finalized:src", "finalized:copy", "finalized:joined"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newSDK(t)
			rec := &recorder{}
			e.backend.ObserveObjects(rec.observe)

			b := e.bucket(t, "ev-bucket", &storage.BucketAttrs{VersioningEnabled: tt.versioned})
			tt.act(t, e, b)

			assert.Equal(t, tt.want, rec.types())
		})
	}
}

func TestObjectEventData(t *testing.T) {
	t.Parallel()

	e := newSDK(t)
	rec := &recorder{}
	e.backend.ObserveObjects(rec.observe)

	b := e.bucket(t, "ev-data", nil)
	write(t, b.Object("dir/a.txt"), []byte("hello"), 0, func(w *storage.Writer) { w.ContentType = "text/plain" })

	require.Len(t, rec.got, 1)

	var data map[string]any
	require.NoError(t, json.Unmarshal(rec.got[0].Data(), &data))
	assert.Equal(t, "ev-data", data["bucket"])
	assert.Equal(t, "dir/a.txt", data["name"])
	assert.Equal(t, "5", data["size"])
	assert.Equal(t, "text/plain", data["contentType"])
	assert.NotEmpty(t, data["generation"])
	assert.Equal(t, "ev-data", rec.got[0].Object.Bucket)
}
