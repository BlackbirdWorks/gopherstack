package azureappconfig_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ac "github.com/blackbirdworks/gopherstack/services/azureappconfig"
)

func TestFilterMatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		expr  string
		value string
		want  bool
		err   bool
	}{
		{name: "empty matches all", expr: "", value: "anything", want: true},
		{name: "star", expr: "*", value: "x", want: true},
		{name: "exact hit", expr: "abc", value: "abc", want: true},
		{name: "exact miss", expr: "abc", value: "abcd"},
		{name: "prefix", expr: "ab*", value: "abc", want: true},
		{name: "suffix", expr: "*bc", value: "abc", want: true},
		{name: "contains", expr: "*b*", value: "abc", want: true},
		{name: "list second", expr: "x,abc", value: "abc", want: true},
		{name: "null label", expr: `\0`, value: "", want: true},
		{name: "null label rejects value", expr: `\0`, value: "a"},
		{name: "star matches null", expr: "*", value: "", want: true},
		{name: "prefix rejects null", expr: "a*", value: ""},
		{name: "escaped star is literal", expr: `a\*`, value: "a*", want: true},
		{name: "escaped star is not a wildcard", expr: `a\*`, value: "ab"},
		{name: "escaped comma", expr: `a\,b`, value: "a,b", want: true},
		{name: "escaped backslash", expr: `a\\b`, value: `a\b`, want: true},
		{name: "five values", expr: "a,b,c,d,e", value: "e", want: true},
		{name: "six values", expr: "a,b,c,d,e,f", err: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ac.ParseFilterMatch(tt.expr, tt.value)
			if tt.err {
				require.ErrorIs(t, err, ac.ErrInvalidArgument)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestValidStoreName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want bool
	}{
		{name: "ok", in: "my-store-1", want: true},
		{name: "too short", in: "abcd"},
		{name: "too long", in: "a123456789a123456789a123456789a123456789a123456789x"},
		{name: "underscore", in: "my_store"},
		{name: "dot", in: "my.store"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, ac.ValidStoreName(tt.in))
		})
	}
}

func TestStoreLifecycleAndKeys(t *testing.T) {
	t.Parallel()

	b := ac.NewInMemoryBackend()

	_, err := b.PutStore("bad_name")
	require.ErrorIs(t, err, ac.ErrInvalidName)

	info, err := b.PutStore("Lifecycle")
	require.NoError(t, err)
	assert.Equal(t, "lifecycle", info.Name)

	again, err := b.PutStore("lifecycle")
	require.NoError(t, err)
	assert.Equal(t, info.Created, again.Created)

	keys, err := b.AccessKeys("LIFECYCLE")
	require.NoError(t, err)
	require.Len(t, keys, 4)
	assert.Equal(
		t,
		[]bool{false, false, true, true},
		[]bool{keys[0].ReadOnly, keys[1].ReadOnly, keys[2].ReadOnly, keys[3].ReadOnly},
	)

	owner, key, ok := b.KeyOwner(keys[2].ID)
	require.True(t, ok)
	assert.Equal(t, "lifecycle", owner)
	assert.Equal(t, keys[2].Secret, key.Secret)

	_, err = b.RegenerateKey("lifecycle", "missing-id")
	require.ErrorIs(t, err, ac.ErrKeyIDNotFound)

	fresh, err := b.RegenerateKey("lifecycle", keys[0].ID)
	require.NoError(t, err)
	assert.NotEqual(t, keys[0].Secret, fresh.Secret)
	assert.Equal(t, keys[0].Name, fresh.Name)

	_, err = b.SetKeyValue("lifecycle", "k", "", ac.KVInput{}, ac.Precondition{})
	require.NoError(t, err)
	require.NoError(t, b.DeleteStore("lifecycle"))
	require.ErrorIs(t, b.DeleteStore("lifecycle"), ac.ErrStoreNotFound)
	assert.False(t, b.HasStore("lifecycle"))
}

func TestSnapshotExpiryAndComposition(t *testing.T) {
	t.Parallel()

	b := ac.NewInMemoryBackend()
	clk := newClock()
	ac.SetNow(b, clk.Now)

	_, err := b.PutStore(storeName)
	require.NoError(t, err)

	for _, kv := range []struct{ key, label string }{{"k", "a"}, {"k", "b"}, {"m", ""}} {
		_, err = b.SetKeyValue(storeName, kv.key, kv.label, ac.KVInput{Value: new(kv.label)}, ac.Precondition{})
		require.NoError(t, err)
	}

	_, err = b.CreateSnapshot(storeName, "bad", ac.SnapshotSpec{Filters: []ac.Filter{{Key: "*"}}, Retention: 5})
	require.ErrorIs(t, err, ac.ErrInvalidArgument, "retention below the minimum")

	_, err = b.CreateSnapshot(storeName, "bad", ac.SnapshotSpec{Composition: "weird", Filters: []ac.Filter{{}}})
	require.ErrorIs(t, err, ac.ErrInvalidArgument)

	snap, err := b.CreateSnapshot(storeName, "s1", ac.SnapshotSpec{
		Filters: []ac.Filter{{Key: "k", Label: "a"}, {Key: "k", Label: "b"}}, Retention: 3600,
	})
	require.NoError(t, err)
	require.Len(t, snap.Items, 1, "composition key keeps one label per key; the later filter wins")
	assert.Equal(t, "b", snap.Items[0].Label)

	_, err = b.SetSnapshotStatus(storeName, "s1", "ready", ac.Precondition{})
	require.ErrorIs(t, err, ac.ErrSnapshotStatus)

	_, err = b.SetSnapshotStatus(storeName, "s1", "bogus", ac.Precondition{})
	require.ErrorIs(t, err, ac.ErrInvalidArgument)

	_, err = b.SetSnapshotStatus(storeName, "s1", "archived", ac.Precondition{IfMatch: "stale"})
	require.ErrorIs(t, err, ac.ErrPrecondition)

	arch, err := b.SetSnapshotStatus(storeName, "s1", "archived", ac.Precondition{IfMatch: snap.ETag})
	require.NoError(t, err)
	require.NotNil(t, arch.Expires)

	clk.Advance(2 * time.Hour)

	_, err = b.GetSnapshot(storeName, "s1")
	require.ErrorIs(t, err, ac.ErrSnapshotNotFound, "an archived snapshot disappears after its retention period")

	listed, err := b.ListSnapshots(storeName, "", nil)
	require.NoError(t, err)
	assert.Empty(t, listed)
}

func TestPersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := ac.NewInMemoryBackend()
	clk := newClock()
	ac.SetNow(b, clk.Now)

	_, err := b.PutStore(storeName)
	require.NoError(t, err)

	first, err := b.SetKeyValue(
		storeName,
		"k",
		"l",
		ac.KVInput{Value: new("v1"), Tags: map[string]string{"a": "b"}},
		ac.Precondition{},
	)
	require.NoError(t, err)

	clk.Advance(time.Minute)
	_, err = b.SetKeyValue(storeName, "k", "l", ac.KVInput{Value: new("v2")}, ac.Precondition{})
	require.NoError(t, err)
	_, err = b.SetLock(storeName, "k", "l", true, ac.Precondition{})
	require.NoError(t, err)

	_, err = b.CreateSnapshot(storeName, "snap", ac.SnapshotSpec{Filters: []ac.Filter{{Key: "k"}}})
	require.NoError(t, err)

	data := b.Snapshot(t.Context())

	restored := ac.NewInMemoryBackend()
	ac.SetNow(restored, clk.Now)
	require.NoError(t, restored.Restore(t.Context(), data))

	got, err := restored.GetKeyValue(storeName, "k", "l", time.Time{})
	require.NoError(t, err)
	assert.Equal(t, "v2", *got.Value)
	assert.True(t, got.Locked)

	old, err := restored.GetKeyValue(storeName, "k", "l", first.LastModified.Add(time.Second))
	require.NoError(t, err)
	assert.Equal(t, "v1", *old.Value, "history survives a restore")
	assert.Equal(t, "b", old.Tags["a"])

	snap, err := restored.GetSnapshot(storeName, "snap")
	require.NoError(t, err)
	assert.Len(t, snap.Items, 1)

	keysBefore, _ := b.AccessKeys(storeName)
	keysAfter, _ := restored.AccessKeys(storeName)
	assert.Equal(t, keysBefore, keysAfter)

	require.Error(t, restored.Restore(t.Context(), []byte(`{"version":1,"stores":{"x":null}}`)))
	require.NoError(t, restored.Restore(t.Context(), []byte(`{"version":99}`)))
	assert.False(t, restored.HasStore(storeName), "an incompatible snapshot starts empty")

	restored.Reset()
	assert.Equal(t, -1, ac.HistoryLen(restored, storeName))
}

func TestPointInTimeBoundaries(t *testing.T) {
	t.Parallel()

	b := ac.NewInMemoryBackend()
	clk := newClock()
	ac.SetNow(b, clk.Now)

	_, err := b.PutStore(storeName)
	require.NoError(t, err)

	created := clk.Now()
	_, err = b.SetKeyValue(storeName, "k", "", ac.KVInput{Value: new("v")}, ac.Precondition{})
	require.NoError(t, err)

	clk.Advance(time.Hour)

	deleted := clk.Now()
	_, err = b.DeleteKeyValue(storeName, "k", "", ac.Precondition{})
	require.NoError(t, err)

	clk.Advance(time.Hour)

	tests := []struct {
		at   time.Time
		name string
		want bool
	}{
		{name: "before creation", at: created.Add(-time.Second)},
		{name: "at creation", at: created, want: true},
		{name: "between", at: created.Add(30 * time.Minute), want: true},
		{name: "at deletion", at: deleted},
		{name: "after deletion", at: deleted.Add(time.Minute)},
		{name: "now", at: time.Time{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, getErr := b.GetKeyValue(storeName, "k", "", tt.at)
			assert.Equal(t, tt.want, getErr == nil)
		})
	}
}
