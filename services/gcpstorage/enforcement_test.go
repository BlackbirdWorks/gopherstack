package gcpstorage_test

import (
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

type fakeClock struct{ ns atomic.Int64 }

func newFakeClock() *fakeClock {
	c := &fakeClock{}
	c.ns.Store(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())

	return c
}

func (c *fakeClock) now() time.Time          { return time.Unix(0, c.ns.Load()).UTC() }
func (c *fakeClock) advance(d time.Duration) { c.ns.Add(int64(d)) }

func newClockedSDK(t *testing.T) (*sdkEnv, *fakeClock) {
	t.Helper()

	e := newSDK(t)
	clk := newFakeClock()
	e.backend.SetClock(clk.now)

	return e, clk
}

func apiReason(t *testing.T, err error, code int) string {
	t.Helper()

	var gerr *googleapi.Error

	require.ErrorAs(t, err, &gerr)
	assert.Equal(t, code, gerr.Code)
	require.NotEmpty(t, gerr.Errors)

	return gerr.Errors[0].Reason
}

func TestRetentionAndHolds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, e *sdkEnv, clk *fakeClock, b *storage.BucketHandle)
		name string
		bkt  storage.BucketAttrs
	}{
		{
			name: "retention blocks delete and overwrite until expiry",
			bkt:  storage.BucketAttrs{RetentionPolicy: &storage.RetentionPolicy{RetentionPeriod: time.Hour}},
			run: func(t *testing.T, _ *sdkEnv, clk *fakeClock, b *storage.BucketHandle) {
				o := b.Object("a")
				attrs := write(t, o, []byte("1"), 0, nil)
				assert.Equal(t, clk.now().Add(time.Hour), attrs.RetentionExpirationTime)

				assert.Equal(t, "retentionPolicyNotMet", apiReason(t, o.Delete(t.Context()), http.StatusForbidden))

				w := o.NewWriter(t.Context())
				_, werr := w.Write([]byte("2"))
				require.NoError(t, werr)
				assert.Equal(t, "retentionPolicyNotMet", apiReason(t, w.Close(), http.StatusForbidden))

				clk.advance(time.Hour + time.Second)
				require.NoError(t, o.Delete(t.Context()))
			},
		},
		{
			name: "temporary hold blocks delete and overwrite until released",
			run: func(t *testing.T, _ *sdkEnv, _ *fakeClock, b *storage.BucketHandle) {
				o := b.Object("a")
				write(t, o, []byte("1"), 0, nil)

				_, err := o.Update(t.Context(), storage.ObjectAttrsToUpdate{TemporaryHold: true})
				require.NoError(t, err)
				assert.Equal(t, "objectUnderActiveHold", apiReason(t, o.Delete(t.Context()), http.StatusForbidden))

				w := o.NewWriter(t.Context())
				_, err = w.Write([]byte("2"))
				require.NoError(t, err)
				assert.Equal(t, "objectUnderActiveHold", apiReason(t, w.Close(), http.StatusForbidden))

				_, err = o.Update(t.Context(), storage.ObjectAttrsToUpdate{TemporaryHold: false})
				require.NoError(t, err)
				require.NoError(t, o.Delete(t.Context()))
			},
		},
		{
			name: "event based hold restarts the retention clock on release",
			bkt:  storage.BucketAttrs{RetentionPolicy: &storage.RetentionPolicy{RetentionPeriod: time.Hour}},
			run: func(t *testing.T, _ *sdkEnv, clk *fakeClock, b *storage.BucketHandle) {
				o := b.Object("a")
				write(t, o, []byte("1"), 0, nil)

				_, err := o.Update(t.Context(), storage.ObjectAttrsToUpdate{EventBasedHold: true})
				require.NoError(t, err)

				clk.advance(2 * time.Hour)
				assert.Equal(t, "objectUnderActiveHold", apiReason(t, o.Delete(t.Context()), http.StatusForbidden))

				released, err := o.Update(t.Context(), storage.ObjectAttrsToUpdate{EventBasedHold: false})
				require.NoError(t, err)
				assert.Equal(t, clk.now().Add(time.Hour), released.RetentionExpirationTime)
				assert.Equal(t, "retentionPolicyNotMet", apiReason(t, o.Delete(t.Context()), http.StatusForbidden))

				clk.advance(time.Hour)
				require.NoError(t, o.Delete(t.Context()))
			},
		},
		{
			name: "default event based hold applies to new objects",
			run: func(t *testing.T, _ *sdkEnv, _ *fakeClock, b *storage.BucketHandle) {
				_, err := b.Update(t.Context(), storage.BucketAttrsToUpdate{DefaultEventBasedHold: true})
				require.NoError(t, err)

				o := b.Object("a")
				attrs := write(t, o, []byte("1"), 0, nil)
				assert.True(t, attrs.EventBasedHold)
				assert.Equal(t, "objectUnderActiveHold", apiReason(t, o.Delete(t.Context()), http.StatusForbidden))
			},
		},
		{
			name: "retention also blocks permanent version delete in a versioned bucket",
			bkt: storage.BucketAttrs{
				VersioningEnabled: true, RetentionPolicy: &storage.RetentionPolicy{RetentionPeriod: time.Hour},
			},
			run: func(t *testing.T, _ *sdkEnv, _ *fakeClock, b *storage.BucketHandle) {
				attrs := write(t, b.Object("a"), []byte("1"), 0, nil)
				err := b.Object("a").Generation(attrs.Generation).Delete(t.Context())
				assert.Equal(t, "retentionPolicyNotMet", apiReason(t, err, http.StatusForbidden))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, clk := newClockedSDK(t)
			attrs := tt.bkt
			tt.run(t, e, clk, e.bucket(t, "bkt", &attrs))
		})
	}
}

func TestLockedRetentionPolicy(t *testing.T) {
	t.Parallel()

	e, clk := newClockedSDK(t)
	b := e.bucket(t, "bkt", &storage.BucketAttrs{
		RetentionPolicy: &storage.RetentionPolicy{RetentionPeriod: time.Hour},
	})

	attrs, err := b.Attrs(t.Context())
	require.NoError(t, err)
	assert.False(t, attrs.RetentionPolicy.IsLocked)
	assert.Equal(t, clk.now(), attrs.RetentionPolicy.EffectiveTime)

	require.NoError(t, b.If(storage.BucketConditions{MetagenerationMatch: attrs.MetaGeneration}).LockRetentionPolicy(t.Context()))

	attrs, err = b.Attrs(t.Context())
	require.NoError(t, err)
	assert.True(t, attrs.RetentionPolicy.IsLocked)

	_, err = b.Update(t.Context(), storage.BucketAttrsToUpdate{RetentionPolicy: &storage.RetentionPolicy{}})
	assert.Equal(t, "badRequestException", apiReason(t, err, http.StatusBadRequest))

	_, err = b.Update(t.Context(), storage.BucketAttrsToUpdate{
		RetentionPolicy: &storage.RetentionPolicy{RetentionPeriod: time.Minute},
	})
	assert.Equal(t, "badRequestException", apiReason(t, err, http.StatusBadRequest))

	up, err := b.Update(t.Context(), storage.BucketAttrsToUpdate{
		RetentionPolicy: &storage.RetentionPolicy{RetentionPeriod: 2 * time.Hour},
	})
	require.NoError(t, err)
	assert.Equal(t, 2*time.Hour, up.RetentionPolicy.RetentionPeriod)
	assert.True(t, up.RetentionPolicy.IsLocked)

	nolock := e.bucket(t, "plain", nil)
	err = nolock.If(storage.BucketConditions{MetagenerationMatch: 1}).LockRetentionPolicy(t.Context())
	assert.Equal(t, "badRequest", apiReason(t, err, http.StatusBadRequest))
}

func objectNames(t *testing.T, b *storage.BucketHandle, q *storage.Query) []string {
	t.Helper()

	it := b.Objects(t.Context(), q)

	var out []string

	for {
		a, err := it.Next()
		if err == iterator.Done {
			return out
		}

		require.NoError(t, err)

		out = append(out, a.Name)
	}
}

func TestLifecycleRules(t *testing.T) {
	t.Parallel()

	del := storage.LifecycleAction{Type: storage.DeleteAction}
	rule := func(a storage.LifecycleAction, c storage.LifecycleCondition) storage.Lifecycle {
		return storage.Lifecycle{Rules: []storage.LifecycleRule{{Action: a, Condition: c}}}
	}

	tests := []struct {
		setup    func(t *testing.T, b *storage.BucketHandle, clk *fakeClock)
		verify   func(t *testing.T, b *storage.BucketHandle)
		name     string
		lc       storage.Lifecycle
		versions bool
		advance  time.Duration
	}{
		{
			name: "age deletes only objects old enough",
			lc:   rule(del, storage.LifecycleCondition{AgeInDays: 1}),
			setup: func(t *testing.T, b *storage.BucketHandle, clk *fakeClock) {
				write(t, b.Object("old"), []byte("1"), 0, nil)
				clk.advance(20 * time.Hour)
				write(t, b.Object("young"), []byte("1"), 0, nil)
			},
			advance: 6 * time.Hour,
			verify: func(t *testing.T, b *storage.BucketHandle) {
				assert.Equal(t, []string{"young"}, objectNames(t, b, nil))
				assert.Equal(t, []string{"old"}, objectNames(t, b, &storage.Query{SoftDeleted: true}))
			},
		},
		{
			name: "prefix suffix and storage class must all match",
			lc: rule(del, storage.LifecycleCondition{
				AgeInDays: 1, MatchesPrefix: []string{"tmp/"}, MatchesSuffix: []string{".log"},
				MatchesStorageClasses: []string{"STANDARD"},
			}),
			setup: func(t *testing.T, b *storage.BucketHandle, _ *fakeClock) {
				for _, n := range []string{"tmp/a.log", "tmp/a.txt", "keep/a.log"} {
					write(t, b.Object(n), []byte("1"), 0, nil)
				}
			},
			advance: 25 * time.Hour,
			verify: func(t *testing.T, b *storage.BucketHandle) {
				assert.Equal(t, []string{"keep/a.log", "tmp/a.txt"}, objectNames(t, b, nil))
			},
		},
		{
			name: "created before a date",
			lc:   rule(del, storage.LifecycleCondition{CreatedBefore: time.Date(2030, 1, 2, 0, 0, 0, 0, time.UTC)}),
			setup: func(t *testing.T, b *storage.BucketHandle, clk *fakeClock) {
				write(t, b.Object("early"), []byte("1"), 0, nil)
				clk.advance(48 * time.Hour)
				write(t, b.Object("late"), []byte("1"), 0, nil)
			},
			verify: func(t *testing.T, b *storage.BucketHandle) {
				assert.Equal(t, []string{"late"}, objectNames(t, b, nil))
			},
		},
		{
			name: "set storage class moves forward only",
			lc: rule(storage.LifecycleAction{Type: storage.SetStorageClassAction, StorageClass: "NEARLINE"},
				storage.LifecycleCondition{AgeInDays: 1}),
			setup: func(t *testing.T, b *storage.BucketHandle, _ *fakeClock) {
				write(t, b.Object("std"), []byte("1"), 0, nil)
				write(t, b.Object("arch"), []byte("1"), 0, func(w *storage.Writer) { w.StorageClass = "ARCHIVE" })
			},
			advance: 25 * time.Hour,
			verify: func(t *testing.T, b *storage.BucketHandle) {
				std, err := b.Object("std").Attrs(t.Context())
				require.NoError(t, err)
				assert.Equal(t, "NEARLINE", std.StorageClass)
				assert.Equal(t, time.Date(2030, 1, 2, 1, 0, 0, 0, time.UTC), std.Updated)

				arch, err := b.Object("arch").Attrs(t.Context())
				require.NoError(t, err)
				assert.Equal(t, "ARCHIVE", arch.StorageClass)
			},
		},
		{
			name:     "newer versions removes old noncurrent versions",
			lc:       rule(del, storage.LifecycleCondition{NumNewerVersions: 1}),
			versions: true,
			setup: func(t *testing.T, b *storage.BucketHandle, _ *fakeClock) {
				for range 3 {
					write(t, b.Object("v"), []byte("x"), 0, nil)
				}
			},
			verify: func(t *testing.T, b *storage.BucketHandle) {
				assert.Len(t, objectNames(t, b, &storage.Query{Versions: true}), 1)
			},
		},
		{
			name:     "delete on a live versioned object archives it",
			lc:       rule(del, storage.LifecycleCondition{AgeInDays: 1, Liveness: storage.Live}),
			versions: true,
			setup: func(t *testing.T, b *storage.BucketHandle, _ *fakeClock) {
				write(t, b.Object("v"), []byte("x"), 0, nil)
			},
			advance: 25 * time.Hour,
			verify: func(t *testing.T, b *storage.BucketHandle) {
				assert.Empty(t, objectNames(t, b, nil))
				assert.Len(t, objectNames(t, b, &storage.Query{Versions: true}), 1)
			},
		},
		{
			name:     "days since noncurrent time purges archived versions",
			lc:       rule(del, storage.LifecycleCondition{DaysSinceNoncurrentTime: 2, Liveness: storage.Archived}),
			versions: true,
			setup: func(t *testing.T, b *storage.BucketHandle, _ *fakeClock) {
				write(t, b.Object("v"), []byte("1"), 0, nil)
				write(t, b.Object("v"), []byte("2"), 0, nil)
			},
			advance: 49 * time.Hour,
			verify: func(t *testing.T, b *storage.BucketHandle) {
				assert.Len(t, objectNames(t, b, &storage.Query{Versions: true}), 1)
			},
		},
		{
			name: "held objects survive a delete rule",
			lc:   rule(del, storage.LifecycleCondition{AgeInDays: 1}),
			setup: func(t *testing.T, b *storage.BucketHandle, _ *fakeClock) {
				write(t, b.Object("held"), []byte("1"), 0, nil)
				write(t, b.Object("free"), []byte("1"), 0, nil)
				_, err := b.Object("held").Update(t.Context(), storage.ObjectAttrsToUpdate{TemporaryHold: true})
				require.NoError(t, err)
			},
			advance: 25 * time.Hour,
			verify: func(t *testing.T, b *storage.BucketHandle) {
				assert.Equal(t, []string{"held"}, objectNames(t, b, nil))
			},
		},
		{
			name: "delete beats set storage class",
			lc: storage.Lifecycle{Rules: []storage.LifecycleRule{
				{Action: storage.LifecycleAction{Type: storage.SetStorageClassAction, StorageClass: "COLDLINE"}, Condition: storage.LifecycleCondition{AgeInDays: 1}},
				{Action: del, Condition: storage.LifecycleCondition{AgeInDays: 1}},
			}},
			setup: func(t *testing.T, b *storage.BucketHandle, _ *fakeClock) {
				write(t, b.Object("a"), []byte("1"), 0, nil)
			},
			advance: 25 * time.Hour,
			verify: func(t *testing.T, b *storage.BucketHandle) {
				assert.Empty(t, objectNames(t, b, nil))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, clk := newClockedSDK(t)
			b := e.bucket(t, "bkt", &storage.BucketAttrs{Lifecycle: tt.lc, VersioningEnabled: tt.versions})

			tt.setup(t, b, clk)
			clk.advance(tt.advance)
			e.backend.RunLifecycle()
			tt.verify(t, b)
		})
	}
}
