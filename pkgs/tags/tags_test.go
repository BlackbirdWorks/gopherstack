package tags_test

import (
	"encoding/json"
	"sort"
	"sync"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTags_New(t *testing.T) {
	t.Parallel()

	tg := tags.New("test.new")
	require.NotNil(t, tg)
	assert.Equal(t, 0, tg.Len())
}

func TestTags_FromMap(t *testing.T) {
	t.Parallel()

	src := map[string]string{"env": "prod", "team": "platform"}
	tg := tags.FromMap("test.frommap", src)

	assert.Equal(t, 2, tg.Len())

	v, ok := tg.Get("env")
	assert.True(t, ok)
	assert.Equal(t, "prod", v)

	// Mutating src must not affect tg.
	src["env"] = "staging"

	got, _ := tg.Get("env")
	assert.Equal(t, "prod", got)
}

func TestTags_FromMap_Empty(t *testing.T) {
	t.Parallel()

	tg := tags.FromMap("test.frommap.empty", map[string]string{})
	assert.Equal(t, 0, tg.Len())
}

func TestTags_SetGet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "simple", key: "env", value: "prod"},
		{name: "empty_value", key: "empty", value: ""},
		{name: "unicode", key: "região", value: "brasil"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tg := tags.New("test.setget." + tt.name)
			tg.Set(tt.key, tt.value)

			got, ok := tg.Get(tt.key)
			assert.True(t, ok)
			assert.Equal(t, tt.value, got)
		})
	}
}

func TestTags_Get_Missing(t *testing.T) {
	t.Parallel()

	tg := tags.New("test.get.missing")

	got, ok := tg.Get("nonexistent")
	assert.False(t, ok)
	assert.Empty(t, got)
}

func TestTags_Delete(t *testing.T) {
	t.Parallel()

	tg := tags.New("test.delete")
	tg.Set("key", "value")

	tg.Delete("key")

	_, ok := tg.Get("key")
	assert.False(t, ok)
	assert.Equal(t, 0, tg.Len())
}

func TestTags_Delete_Missing(t *testing.T) {
	t.Parallel()

	tg := tags.New("test.delete.missing")

	assert.NotPanics(t, func() { tg.Delete("nonexistent") })
}

func TestTags_HasTag(t *testing.T) {
	t.Parallel()

	tg := tags.New("test.hastag")
	tg.Set("env", "prod")

	assert.True(t, tg.HasTag("env"))
	assert.False(t, tg.HasTag("team"))
}

func TestTags_Len(t *testing.T) {
	t.Parallel()

	tg := tags.New("test.len")
	assert.Equal(t, 0, tg.Len())

	tg.Set("a", "1")
	assert.Equal(t, 1, tg.Len())

	tg.Set("b", "2")
	assert.Equal(t, 2, tg.Len())

	tg.Delete("a")
	assert.Equal(t, 1, tg.Len())
}

func TestTags_Clone(t *testing.T) {
	t.Parallel()

	tg := tags.New("test.clone")
	tg.Set("env", "prod")
	tg.Set("team", "platform")

	cp := tg.Clone()
	assert.Equal(t, map[string]string{"env": "prod", "team": "platform"}, cp)

	// Mutating clone must not affect original.
	cp["env"] = "staging"

	got, _ := tg.Get("env")
	assert.Equal(t, "prod", got)
}

func TestTags_Clone_Empty(t *testing.T) {
	t.Parallel()

	tg := tags.New("test.clone.empty")
	cp := tg.Clone()
	assert.NotNil(t, cp)
	assert.Empty(t, cp)
}

func TestTags_Merge(t *testing.T) {
	t.Parallel()

	tg := tags.New("test.merge")
	tg.Set("existing", "value")

	tg.Merge(map[string]string{
		"new":      "entry",
		"existing": "overwritten",
	})

	assert.Equal(t, 2, tg.Len())

	v1, _ := tg.Get("existing")
	assert.Equal(t, "overwritten", v1)

	v2, _ := tg.Get("new")
	assert.Equal(t, "entry", v2)
}

func TestTags_Merge_Empty(t *testing.T) {
	t.Parallel()

	tg := tags.New("test.merge.empty")
	tg.Set("key", "value")

	tg.Merge(map[string]string{})
	assert.Equal(t, 1, tg.Len())
}

func TestTags_DeleteKeys(t *testing.T) {
	t.Parallel()

	tg := tags.New("test.deletekeys")
	tg.Set("a", "1")
	tg.Set("b", "2")
	tg.Set("c", "3")

	tg.DeleteKeys([]string{"a", "c", "nonexistent"})

	assert.Equal(t, 1, tg.Len())
	assert.False(t, tg.HasTag("a"))
	assert.True(t, tg.HasTag("b"))
	assert.False(t, tg.HasTag("c"))
}

func TestTags_MatchesFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		filter map[string]string
		name   string
		want   bool
	}{
		{name: "empty_filter_matches_all", filter: map[string]string{}, want: true},
		{name: "exact_match", filter: map[string]string{"env": "prod"}, want: true},
		{name: "all_match", filter: map[string]string{"env": "prod", "team": "platform"}, want: true},
		{name: "wrong_value", filter: map[string]string{"env": "staging"}, want: false},
		{name: "missing_key", filter: map[string]string{"region": "us-east-1"}, want: false},
		{name: "partial_miss", filter: map[string]string{"env": "prod", "region": "us-east-1"}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tg := tags.New("test.matchesfilter." + tt.name)
			tg.Set("env", "prod")
			tg.Set("team", "platform")

			assert.Equal(t, tt.want, tg.MatchesFilter(tt.filter))
		})
	}
}

func TestTags_Range(t *testing.T) {
	t.Parallel()

	tg := tags.New("test.range")
	tg.Set("a", "1")
	tg.Set("b", "2")
	tg.Set("c", "3")

	seen := make(map[string]string)
	tg.Range(func(k, v string) bool {
		seen[k] = v

		return true
	})

	assert.Equal(t, map[string]string{"a": "1", "b": "2", "c": "3"}, seen)
}

func TestTags_Range_EarlyStop(t *testing.T) {
	t.Parallel()

	tg := tags.New("test.range.stop")
	tg.Set("a", "1")
	tg.Set("b", "2")
	tg.Set("c", "3")

	count := 0
	tg.Range(func(_, _ string) bool {
		count++

		return false
	})

	assert.Equal(t, 1, count)
}

func TestTags_Range_Keys(t *testing.T) {
	t.Parallel()

	tg := tags.New("test.range.keys")
	tg.Set("x", "10")
	tg.Set("y", "20")

	var keys []string
	tg.Range(func(k, _ string) bool {
		keys = append(keys, k)

		return true
	})

	sort.Strings(keys)
	assert.Equal(t, []string{"x", "y"}, keys)
}

func TestTags_Concurrent(t *testing.T) {
	t.Parallel()

	tg := tags.New("test.concurrent")

	const goroutines = 100
	var wg sync.WaitGroup

	for range goroutines {
		wg.Go(func() {
			key := "key"
			tg.Set(key, "value")
			tg.HasTag(key)
			tg.Clone()
		})
	}

	wg.Wait()

	assert.True(t, tg.HasTag("key"))
}

func TestTags_MarshalJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(*tags.Tags)
		name  string
		want  string
	}{
		{name: "empty", want: "{}"},
		{
			name:  "single",
			setup: func(tg *tags.Tags) { tg.Set("env", "prod") },
			want:  `{"env":"prod"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tg := tags.New("test.marshal." + tt.name)
			if tt.setup != nil {
				tt.setup(tg)
			}

			data, err := json.Marshal(tg)
			require.NoError(t, err)
			assert.JSONEq(t, tt.want, string(data))
		})
	}
}

func TestTags_MarshalJSON_InStruct(t *testing.T) {
	t.Parallel()

	type resource struct {
		Tags *tags.Tags `json:"tags,omitempty"`
		Name string     `json:"name"`
	}

	tg := tags.New("test.marshal.struct")
	tg.Set("team", "platform")

	r := resource{Name: "bucket", Tags: tg}
	data, err := json.Marshal(r)
	require.NoError(t, err)
	assert.JSONEq(t, `{"name":"bucket","tags":{"team":"platform"}}`, string(data))
}

func TestTags_UnmarshalJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want    map[string]string
		name    string
		input   string
		wantErr bool
	}{
		{name: "empty_object", input: `{}`, want: map[string]string{}},
		{name: "single", input: `{"env":"prod"}`, want: map[string]string{"env": "prod"}},
		{name: "multi", input: `{"env":"prod","team":"platform"}`, want: map[string]string{
			"env": "prod", "team": "platform",
		}},
		{name: "invalid_json", input: `not-json`, wantErr: true},
		{name: "array_is_invalid", input: `[{"Key":"env","Value":"prod"}]`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tg := tags.New("test.unmarshal." + tt.name)
			err := json.Unmarshal([]byte(tt.input), tg)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, tg.Clone())
		})
	}
}

func TestTags_UnmarshalJSON_InStruct(t *testing.T) {
	t.Parallel()

	type resource struct {
		Tags *tags.Tags `json:"tags"`
		Name string     `json:"name"`
	}

	r := resource{Tags: tags.New("test.unmarshal.struct")}
	err := json.Unmarshal([]byte(`{"name":"bucket","tags":{"env":"staging"}}`), &r)
	require.NoError(t, err)
	assert.Equal(t, "bucket", r.Name)
	assert.Equal(t, map[string]string{"env": "staging"}, r.Tags.Clone())
}

func TestTags_RoundTrip(t *testing.T) {
	t.Parallel()

	tg := tags.New("test.roundtrip")
	tg.Set("env", "prod")
	tg.Set("team", "platform")

	data, err := json.Marshal(tg)
	require.NoError(t, err)

	tg2 := tags.New("test.roundtrip2")
	require.NoError(t, json.Unmarshal(data, tg2))
	assert.Equal(t, tg.Clone(), tg2.Clone())
}

func TestTags_UnmarshalJSON_ClearsStaleKeys(t *testing.T) {
	t.Parallel()

	tg := tags.New("test.unmarshal.stale")
	tg.Set("old-key", "old-value")
	require.True(t, tg.HasTag("old-key"))

	// Second unmarshal with a different payload must not leave "old-key" behind.
	require.NoError(t, json.Unmarshal([]byte(`{"new-key":"new-value"}`), tg))

	assert.False(t, tg.HasTag("old-key"), "stale key must be removed on unmarshal")
	v, ok := tg.Get("new-key")
	assert.True(t, ok)
	assert.Equal(t, "new-value", v)
	assert.Equal(t, 1, tg.Len())
}

func TestTags_Close(t *testing.T) {
	t.Parallel()

	tg := tags.New("test.close")
	tg.Set("k", "v")
	// Close must not panic.
	assert.NotPanics(t, func() { tg.Close() })
}

func TestTags_Close_Nil(t *testing.T) {
	t.Parallel()

	var tg *tags.Tags
	assert.NotPanics(t, func() { tg.Close() })
}
