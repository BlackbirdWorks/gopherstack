package gcpstorage_test

import (
	"net/http"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestObjectRetention(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run        func(t *testing.T, clk *fakeClock, b *storage.BucketHandle)
		name       string
		versioning bool
	}{
		{
			name: "blocks delete and overwrite until retain-until",
			run: func(t *testing.T, clk *fakeClock, b *storage.BucketHandle) {
				t.Helper()

				o := b.Object("a")
				attrs := write(t, o, []byte("1"), 0, func(w *storage.Writer) {
					w.Retention = &storage.ObjectRetention{Mode: "Unlocked", RetainUntil: clk.now().Add(time.Hour)}
				})
				require.NotNil(t, attrs.Retention)
				assert.Equal(t, "Unlocked", attrs.Retention.Mode)
				assert.Equal(t, clk.now().Add(time.Hour), attrs.Retention.RetainUntil)

				assert.Equal(t, "retentionPolicyNotMet", apiReason(t, o.Delete(t.Context()), http.StatusForbidden))

				w := o.NewWriter(t.Context())
				_, werr := w.Write([]byte("2"))
				require.NoError(t, werr)
				assert.Equal(t, "retentionPolicyNotMet", apiReason(t, w.Close(), http.StatusForbidden))

				_, err := o.Update(t.Context(), storage.ObjectAttrsToUpdate{ContentType: "text/plain"})
				require.NoError(t, err, "metadata stays editable")

				clk.advance(time.Hour + time.Second)
				require.NoError(t, o.Delete(t.Context()))
			},
		},
		{
			name: "unlocked changes need the override",
			run: func(t *testing.T, clk *fakeClock, b *storage.BucketHandle) {
				t.Helper()

				o := b.Object("a")
				write(t, o, []byte("1"), 0, func(w *storage.Writer) {
					w.Retention = &storage.ObjectRetention{Mode: "Unlocked", RetainUntil: clk.now().Add(time.Hour)}
				})

				longer := &storage.ObjectRetention{Mode: "Unlocked", RetainUntil: clk.now().Add(2 * time.Hour)}
				_, err := o.Update(t.Context(), storage.ObjectAttrsToUpdate{Retention: longer})
				require.NoError(t, err, "extending needs no override")

				shorter := &storage.ObjectRetention{Mode: "Unlocked", RetainUntil: clk.now().Add(time.Minute)}
				_, err = o.Update(t.Context(), storage.ObjectAttrsToUpdate{Retention: shorter})
				assert.Equal(t, "invalid", apiReason(t, err, http.StatusBadRequest))

				_, err = o.Update(t.Context(), storage.ObjectAttrsToUpdate{Retention: &storage.ObjectRetention{}})
				assert.Equal(t, "invalid", apiReason(t, err, http.StatusBadRequest), "removal needs the override")

				got, err := o.OverrideUnlockedRetention(true).
					Update(t.Context(), storage.ObjectAttrsToUpdate{Retention: shorter})
				require.NoError(t, err)
				assert.Equal(t, shorter.RetainUntil, got.Retention.RetainUntil)

				got, err = o.OverrideUnlockedRetention(true).
					Update(t.Context(), storage.ObjectAttrsToUpdate{Retention: &storage.ObjectRetention{}})
				require.NoError(t, err)
				assert.Nil(t, got.Retention)
				require.NoError(t, o.Delete(t.Context()))
			},
		},
		{
			name: "locked can only be extended",
			run: func(t *testing.T, clk *fakeClock, b *storage.BucketHandle) {
				t.Helper()

				o := b.Object("a")
				write(t, o, []byte("1"), 0, func(w *storage.Writer) {
					w.Retention = &storage.ObjectRetention{Mode: "Locked", RetainUntil: clk.now().Add(time.Hour)}
				})

				_, err := o.OverrideUnlockedRetention(true).
					Update(t.Context(), storage.ObjectAttrsToUpdate{Retention: &storage.ObjectRetention{}})
				assert.Equal(t, "retentionPolicyNotMet", apiReason(t, err, http.StatusForbidden))

				_, err = o.OverrideUnlockedRetention(true).Update(t.Context(), storage.ObjectAttrsToUpdate{
					Retention: &storage.ObjectRetention{Mode: "Unlocked", RetainUntil: clk.now().Add(2 * time.Hour)},
				})
				assert.Equal(t, "retentionPolicyNotMet", apiReason(t, err, http.StatusForbidden), "mode cannot change")

				_, err = o.Update(t.Context(), storage.ObjectAttrsToUpdate{
					Retention: &storage.ObjectRetention{Mode: "Locked", RetainUntil: clk.now().Add(2 * time.Hour)},
				})
				require.NoError(t, err)
			},
		},
		{
			name:       "a live version may become noncurrent",
			versioning: true,
			run: func(t *testing.T, clk *fakeClock, b *storage.BucketHandle) {
				t.Helper()

				o := b.Object("a")
				attrs := write(t, o, []byte("1"), 0, func(w *storage.Writer) {
					w.Retention = &storage.ObjectRetention{Mode: "Locked", RetainUntil: clk.now().Add(time.Hour)}
				})

				require.NoError(t, o.Delete(t.Context()), "delete archives the live version")

				err := o.Generation(attrs.Generation).Delete(t.Context())
				assert.Equal(
					t,
					"retentionPolicyNotMet",
					apiReason(t, err, http.StatusForbidden),
					"the version itself is kept",
				)
			},
		},
		{
			name: "validation",
			run: func(t *testing.T, clk *fakeClock, b *storage.BucketHandle) {
				t.Helper()

				w := b.Object("past").NewWriter(t.Context())
				w.Retention = &storage.ObjectRetention{Mode: "Unlocked", RetainUntil: clk.now().Add(-time.Hour)}
				_, err := w.Write([]byte("x"))
				require.NoError(t, err)
				assert.Equal(t, "invalid", apiReason(t, w.Close(), http.StatusBadRequest))

				w = b.Object("badmode").NewWriter(t.Context())
				w.Retention = &storage.ObjectRetention{Mode: "Strict", RetainUntil: clk.now().Add(time.Hour)}
				_, err = w.Write([]byte("x"))
				require.NoError(t, err)
				assert.Equal(t, "invalid", apiReason(t, w.Close(), http.StatusBadRequest))

				w = b.Object("hold").NewWriter(t.Context())
				w.EventBasedHold = true
				w.Retention = &storage.ObjectRetention{Mode: "Unlocked", RetainUntil: clk.now().Add(time.Hour)}
				_, err = w.Write([]byte("x"))
				require.NoError(t, err)
				assert.Equal(
					t,
					"invalid",
					apiReason(t, w.Close(), http.StatusBadRequest),
					"retention excludes event-based hold",
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, clk := newClockedSDK(t)
			b := e.client.Bucket("retained").SetObjectRetention(true)
			require.NoError(
				t,
				b.Create(t.Context(), sdkProject, &storage.BucketAttrs{VersioningEnabled: tt.versioning}),
			)
			tt.run(t, clk, b)
		})
	}
}

func TestObjectRetentionNeedsBucketOptIn(t *testing.T) {
	t.Parallel()

	e, clk := newClockedSDK(t)
	b := e.bucket(t, "plain", nil)

	w := b.Object("a").NewWriter(t.Context())
	w.Retention = &storage.ObjectRetention{Mode: "Unlocked", RetainUntil: clk.now().Add(time.Hour)}
	_, err := w.Write([]byte("x"))
	require.NoError(t, err)
	assert.Equal(t, "invalid", apiReason(t, w.Close(), http.StatusBadRequest))

	attrs, err := b.Attrs(t.Context())
	require.NoError(t, err)
	assert.Empty(t, attrs.ObjectRetentionMode)
}

func TestObjectRetentionModeIsPermanent(t *testing.T) {
	t.Parallel()

	e, _ := newClockedSDK(t)
	b := e.client.Bucket("opt-in").SetObjectRetention(true)
	require.NoError(t, b.Create(t.Context(), sdkProject, nil))

	attrs, err := b.Attrs(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "Enabled", attrs.ObjectRetentionMode)
}

func TestObjectRetentionSurvivesSnapshot(t *testing.T) {
	t.Parallel()

	e, clk := newClockedSDK(t)
	b := e.client.Bucket("snap").SetObjectRetention(true)
	require.NoError(t, b.Create(t.Context(), sdkProject, nil))

	until := clk.now().Add(time.Hour)
	write(t, b.Object("a"), []byte("1"), 0, func(w *storage.Writer) {
		w.Retention = &storage.ObjectRetention{Mode: "Locked", RetainUntil: until}
	})

	s2 := newSDK(t)
	s2.backend.SetClock(clk.now)
	require.NoError(t, s2.backend.Restore(t.Context(), e.backend.Snapshot(t.Context())))

	attrs, err := s2.client.Bucket("snap").Object("a").Attrs(t.Context())
	require.NoError(t, err)
	require.NotNil(t, attrs.Retention)
	assert.Equal(t, "Locked", attrs.Retention.Mode)
	assert.Equal(t, until, attrs.Retention.RetainUntil)
}
