package gcpstorage_test

import (
	"bytes"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const autoclassBig = 128 << 10

func TestAutoclass(t *testing.T) {
	t.Parallel()

	const dayLen = 24 * time.Hour

	tests := []struct {
		auto storage.Autoclass
		run  func(t *testing.T, e *sdkEnv, clk *fakeClock, b *storage.BucketHandle)
		name string
	}{
		{
			name: "objects start in standard and idle ones move to nearline",
			auto: storage.Autoclass{Enabled: true},
			run: func(t *testing.T, e *sdkEnv, clk *fakeClock, b *storage.BucketHandle) {
				t.Helper()

				o := b.Object("big")
				attrs := write(
					t,
					o,
					bytes.Repeat([]byte("a"), autoclassBig),
					0,
					func(w *storage.Writer) { w.StorageClass = "COLDLINE" },
				)
				assert.Equal(t, "STANDARD", attrs.StorageClass, "requested class is ignored")

				clk.advance(29 * dayLen)
				e.backend.RunLifecycle()
				got, err := o.Attrs(t.Context())
				require.NoError(t, err)
				assert.Equal(t, "STANDARD", got.StorageClass)

				clk.advance(2 * dayLen)
				e.backend.RunLifecycle()
				got, err = o.Attrs(t.Context())
				require.NoError(t, err)
				assert.Equal(t, "NEARLINE", got.StorageClass)

				_ = readAll(t, o)
				got, err = o.Attrs(t.Context())
				require.NoError(t, err)
				assert.Equal(t, "STANDARD", got.StorageClass, "a data read returns the object to standard")
			},
		},
		{
			name: "a metadata read is not an access",
			auto: storage.Autoclass{Enabled: true},
			run: func(t *testing.T, e *sdkEnv, clk *fakeClock, b *storage.BucketHandle) {
				t.Helper()

				o := b.Object("big")
				write(t, o, bytes.Repeat([]byte("a"), autoclassBig), 0, nil)

				clk.advance(20 * dayLen)
				_, err := o.Attrs(t.Context())
				require.NoError(t, err)

				clk.advance(11 * dayLen)
				e.backend.RunLifecycle()
				got, err := o.Attrs(t.Context())
				require.NoError(t, err)
				assert.Equal(t, "NEARLINE", got.StorageClass)
			},
		},
		{
			name: "small objects stay in standard",
			auto: storage.Autoclass{Enabled: true},
			run: func(t *testing.T, e *sdkEnv, clk *fakeClock, b *storage.BucketHandle) {
				t.Helper()

				o := b.Object("small")
				write(t, o, []byte("tiny"), 0, nil)
				clk.advance(400 * dayLen)
				e.backend.RunLifecycle()

				got, err := o.Attrs(t.Context())
				require.NoError(t, err)
				assert.Equal(t, "STANDARD", got.StorageClass)
			},
		},
		{
			name: "archive terminal class passes through coldline",
			auto: storage.Autoclass{Enabled: true, TerminalStorageClass: "ARCHIVE"},
			run: func(t *testing.T, e *sdkEnv, clk *fakeClock, b *storage.BucketHandle) {
				t.Helper()

				o := b.Object("big")
				write(t, o, bytes.Repeat([]byte("a"), autoclassBig), 0, nil)

				for _, step := range []struct {
					want string
					wait time.Duration
				}{{"STANDARD", 31 * dayLen}, {"COLDLINE", 60 * dayLen}, {"COLDLINE", 270 * dayLen}, {"ARCHIVE", 5 * dayLen}} {
					clk.advance(step.wait)
					e.backend.RunLifecycle()
					got, err := o.Attrs(t.Context())
					require.NoError(t, err)
					assert.Equal(t, step.want, got.StorageClass)
				}
			},
		},
		{
			name: "compose needs standard sources",
			auto: storage.Autoclass{Enabled: true},
			run: func(t *testing.T, e *sdkEnv, clk *fakeClock, b *storage.BucketHandle) {
				t.Helper()

				a, c := b.Object("a"), b.Object("c")
				write(t, a, bytes.Repeat([]byte("a"), autoclassBig), 0, nil)
				write(t, c, []byte("c"), 0, nil)

				_, err := b.Object("ok").ComposerFrom(a, c).Run(t.Context())
				require.NoError(t, err)

				clk.advance(31 * dayLen)
				e.backend.RunLifecycle()

				_, err = b.Object("bad").ComposerFrom(a, c).Run(t.Context())
				assert.Equal(t, "invalid", apiReason(t, err, 400))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, clk := newClockedSDK(t)
			b := e.bucket(t, "auto", &storage.BucketAttrs{Autoclass: &tt.auto})
			attrs, err := b.Attrs(t.Context())
			require.NoError(t, err)
			require.NotNil(t, attrs.Autoclass)
			assert.Equal(t, clk.now(), attrs.Autoclass.ToggleTime)
			assert.NotEmpty(t, attrs.Autoclass.TerminalStorageClass)

			tt.run(t, e, clk, b)
		})
	}
}

func TestAutoclassLifecycleExclusion(t *testing.T) {
	t.Parallel()

	rule := storage.LifecycleRule{
		Action:    storage.LifecycleAction{Type: storage.SetStorageClassAction, StorageClass: "NEARLINE"},
		Condition: storage.LifecycleCondition{AgeInDays: 30},
	}

	t.Run("create with both", func(t *testing.T) {
		t.Parallel()

		e := newSDK(t)
		err := e.client.Bucket("both").Create(t.Context(), sdkProject, &storage.BucketAttrs{
			Autoclass: &storage.Autoclass{
				Enabled: true,
			},
			Lifecycle: storage.Lifecycle{Rules: []storage.LifecycleRule{rule}},
		})
		assert.Equal(t, "invalid", apiReason(t, err, 400))
	})

	t.Run("enable on a bucket that has the rule", func(t *testing.T) {
		t.Parallel()

		e := newSDK(t)
		b := e.bucket(
			t,
			"rule-first",
			&storage.BucketAttrs{Lifecycle: storage.Lifecycle{Rules: []storage.LifecycleRule{rule}}},
		)

		_, err := b.Update(t.Context(), storage.BucketAttrsToUpdate{Autoclass: &storage.Autoclass{Enabled: true}})
		assert.Equal(t, "invalid", apiReason(t, err, 400))
	})

	t.Run("delete rules stay allowed", func(t *testing.T) {
		t.Parallel()

		e := newSDK(t)
		del := storage.LifecycleRule{
			Action:    storage.LifecycleAction{Type: storage.DeleteAction},
			Condition: storage.LifecycleCondition{AgeInDays: 30},
		}
		err := e.client.Bucket("del").Create(t.Context(), sdkProject, &storage.BucketAttrs{
			Autoclass: &storage.Autoclass{
				Enabled: true,
			},
			Lifecycle: storage.Lifecycle{Rules: []storage.LifecycleRule{del}},
		})
		assert.NoError(t, err)
	})
}

func TestAutoclassEnabledOnExistingBucketResetsObjects(t *testing.T) {
	t.Parallel()

	e, clk := newClockedSDK(t)
	b := e.bucket(t, "late", nil)
	o := b.Object("big")
	write(t, o, bytes.Repeat([]byte("a"), autoclassBig), 0, func(w *storage.Writer) { w.StorageClass = "COLDLINE" })

	_, err := b.Update(t.Context(), storage.BucketAttrsToUpdate{Autoclass: &storage.Autoclass{Enabled: true}})
	require.NoError(t, err)

	got, err := o.Attrs(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "STANDARD", got.StorageClass)

	clk.advance(29 * 24 * time.Hour)
	e.backend.RunLifecycle()

	got, err = o.Attrs(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "STANDARD", got.StorageClass, "the 30 days restart when autoclass is enabled")
}

func TestAutoclassAccessSurvivesSnapshot(t *testing.T) {
	t.Parallel()

	e, clk := newClockedSDK(t)
	b := e.bucket(t, "snap", &storage.BucketAttrs{Autoclass: &storage.Autoclass{Enabled: true}})
	write(t, b.Object("big"), bytes.Repeat([]byte("a"), autoclassBig), 0, nil)

	clk.advance(20 * 24 * time.Hour)
	_ = readAll(t, b.Object("big"))

	e2 := newSDK(t)
	e2.backend.SetClock(clk.now)
	require.NoError(t, e2.backend.Restore(t.Context(), e.backend.Snapshot(t.Context())))

	clk.advance(11 * 24 * time.Hour)
	e2.backend.RunLifecycle()

	got, err := e2.client.Bucket("snap").Object("big").Attrs(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "STANDARD", got.StorageClass, "last access, not creation, starts the idle clock")
}
