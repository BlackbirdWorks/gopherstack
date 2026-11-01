package gcpstorage_test

import (
	"net/http"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const week = 7 * 24 * time.Hour

func TestSoftDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, e *sdkEnv, clk *fakeClock, b *storage.BucketHandle)
		name string
	}{
		{
			name: "new buckets carry the default seven day policy",
			run: func(t *testing.T, _ *sdkEnv, _ *fakeClock, b *storage.BucketHandle) {
				attrs, err := b.Attrs(t.Context())
				require.NoError(t, err)
				require.NotNil(t, attrs.SoftDeletePolicy)
				assert.Equal(t, week, attrs.SoftDeletePolicy.RetentionDuration)
			},
		},
		{
			name: "delete soft-deletes then restore creates a new live generation",
			run: func(t *testing.T, _ *sdkEnv, clk *fakeClock, b *storage.BucketHandle) {
				first := write(t, b.Object("a"), []byte("data"), 0, func(w *storage.Writer) { w.StorageClass = "NEARLINE" })
				require.NoError(t, b.Object("a").Delete(t.Context()))

				assert.Empty(t, objectNames(t, b, nil))

				it := b.Objects(t.Context(), &storage.Query{SoftDeleted: true})
				got, err := it.Next()
				require.NoError(t, err)
				assert.Equal(t, first.Generation, got.Generation)
				assert.Equal(t, clk.now(), got.SoftDeleteTime)
				assert.Equal(t, clk.now().Add(week), got.HardDeleteTime)

				sd, err := b.Object("a").Generation(first.Generation).SoftDeleted().Attrs(t.Context())
				require.NoError(t, err)
				assert.Equal(t, first.Generation, sd.Generation)

				clk.advance(time.Hour)

				restored, err := b.Object("a").Generation(first.Generation).Restore(t.Context(), &storage.RestoreOptions{})
				require.NoError(t, err)
				assert.Greater(t, restored.Generation, first.Generation)
				assert.Equal(t, "STANDARD", restored.StorageClass)
				assert.True(t, restored.SoftDeleteTime.IsZero())
				assert.Equal(t, []byte("data"), readAll(t, b.Object("a")))

				assert.Equal(t, []string{"a"}, objectNames(t, b, nil))
				assert.Empty(t, objectNames(t, b, &storage.Query{SoftDeleted: true}))
			},
		},
		{
			name: "overwrite soft-deletes the replaced generation and restore overwrites the live one",
			run: func(t *testing.T, _ *sdkEnv, _ *fakeClock, b *storage.BucketHandle) {
				first := write(t, b.Object("a"), []byte("one"), 0, nil)
				write(t, b.Object("a"), []byte("two"), 0, nil)

				assert.Equal(t, []string{"a"}, objectNames(t, b, &storage.Query{SoftDeleted: true}))

				_, err := b.Object("a").Generation(first.Generation).Restore(t.Context(), &storage.RestoreOptions{})
				require.NoError(t, err)
				assert.Equal(t, []byte("one"), readAll(t, b.Object("a")))
				assert.Equal(t, []string{"a"}, objectNames(t, b, &storage.Query{SoftDeleted: true}), "the displaced live object is soft-deleted")
			},
		},
		{
			name: "restore of an unknown generation is not found",
			run: func(t *testing.T, _ *sdkEnv, _ *fakeClock, b *storage.BucketHandle) {
				write(t, b.Object("a"), []byte("x"), 0, nil)
				_, err := b.Object("a").Generation(12345).Restore(t.Context(), &storage.RestoreOptions{})
				assert.Equal(t, "notFound", apiReason(t, err, http.StatusNotFound))
			},
		},
		{
			name: "a zero retention duration disables soft delete",
			run: func(t *testing.T, _ *sdkEnv, _ *fakeClock, b *storage.BucketHandle) {
				_, err := b.Update(t.Context(), storage.BucketAttrsToUpdate{SoftDeletePolicy: &storage.SoftDeletePolicy{}})
				require.NoError(t, err)

				write(t, b.Object("a"), []byte("x"), 0, nil)
				require.NoError(t, b.Object("a").Delete(t.Context()))
				assert.Empty(t, objectNames(t, b, &storage.Query{SoftDeleted: true}))
			},
		},
		{
			name: "objects past the hard delete time are purged",
			run: func(t *testing.T, e *sdkEnv, clk *fakeClock, b *storage.BucketHandle) {
				first := write(t, b.Object("a"), []byte("x"), 0, nil)
				require.NoError(t, b.Object("a").Delete(t.Context()))

				clk.advance(week - time.Minute)
				e.backend.RunLifecycle()
				assert.Equal(t, []string{"a"}, objectNames(t, b, &storage.Query{SoftDeleted: true}))

				clk.advance(2 * time.Minute)
				e.backend.RunLifecycle()
				assert.Empty(t, objectNames(t, b, &storage.Query{SoftDeleted: true}))

				_, err := b.Object("a").Generation(first.Generation).Restore(t.Context(), &storage.RestoreOptions{})
				assert.Equal(t, "notFound", apiReason(t, err, http.StatusNotFound))
			},
		},
		{
			name: "invalid durations are rejected",
			run: func(t *testing.T, _ *sdkEnv, _ *fakeClock, b *storage.BucketHandle) {
				_, err := b.Update(t.Context(), storage.BucketAttrsToUpdate{
					SoftDeletePolicy: &storage.SoftDeletePolicy{RetentionDuration: 24 * time.Hour},
				})
				assert.Equal(t, "invalid", apiReason(t, err, http.StatusBadRequest))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, clk := newClockedSDK(t)
			tt.run(t, e, clk, e.bucket(t, "bkt", nil))
		})
	}
}
