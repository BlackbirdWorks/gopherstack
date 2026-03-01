package safemap_test

import (
	"sort"
	"sync"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/safemap"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMap_New(t *testing.T) {
	t.Parallel()

	m := safemap.New[string, int]("test.new")
	require.NotNil(t, m)
	assert.Equal(t, 0, m.Len())
}

func TestMap_SetGet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		key   string
		value int
	}{
		{name: "simple", key: "foo", value: 42},
		{name: "zero_value", key: "bar", value: 0},
		{name: "negative", key: "neg", value: -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := safemap.New[string, int]("test.setget." + tt.name)
			m.Set(tt.key, tt.value)

			got, ok := m.Get(tt.key)
			assert.True(t, ok)
			assert.Equal(t, tt.value, got)
		})
	}
}

func TestMap_Get_Missing(t *testing.T) {
	t.Parallel()

	m := safemap.New[string, int]("test.get.missing")

	got, ok := m.Get("nonexistent")
	assert.False(t, ok)
	assert.Zero(t, got)
}

func TestMap_Set_Overwrite(t *testing.T) {
	t.Parallel()

	m := safemap.New[string, string]("test.set.overwrite")
	m.Set("key", "first")
	m.Set("key", "second")

	got, ok := m.Get("key")
	assert.True(t, ok)
	assert.Equal(t, "second", got)
}

func TestMap_Delete(t *testing.T) {
	t.Parallel()

	m := safemap.New[string, int]("test.delete")
	m.Set("key", 1)

	m.Delete("key")

	_, ok := m.Get("key")
	assert.False(t, ok)
	assert.Equal(t, 0, m.Len())
}

func TestMap_Delete_Missing(t *testing.T) {
	t.Parallel()

	m := safemap.New[string, int]("test.delete.missing")

	// Deleting a nonexistent key should not panic.
	assert.NotPanics(t, func() { m.Delete("nonexistent") })
}

func TestMap_Len(t *testing.T) {
	t.Parallel()

	m := safemap.New[string, int]("test.len")
	assert.Equal(t, 0, m.Len())

	m.Set("a", 1)
	assert.Equal(t, 1, m.Len())

	m.Set("b", 2)
	assert.Equal(t, 2, m.Len())

	m.Delete("a")
	assert.Equal(t, 1, m.Len())
}

func TestMap_Keys(t *testing.T) {
	t.Parallel()

	m := safemap.New[string, int]("test.keys")
	m.Set("a", 1)
	m.Set("b", 2)
	m.Set("c", 3)

	keys := m.Keys()
	sort.Strings(keys)
	assert.Equal(t, []string{"a", "b", "c"}, keys)
}

func TestMap_Values(t *testing.T) {
	t.Parallel()

	m := safemap.New[string, int]("test.values")
	m.Set("a", 1)
	m.Set("b", 2)

	vals := m.Values()
	sort.Ints(vals)
	assert.Equal(t, []int{1, 2}, vals)
}

func TestMap_Clone(t *testing.T) {
	t.Parallel()

	m := safemap.New[string, string]("test.clone")
	m.Set("k1", "v1")
	m.Set("k2", "v2")

	cp := m.Clone()
	assert.Equal(t, map[string]string{"k1": "v1", "k2": "v2"}, cp)

	// Modifying the clone must not affect the original.
	cp["k1"] = "changed"

	got, _ := m.Get("k1")
	assert.Equal(t, "v1", got, "original must be unaffected by clone mutation")
}

func TestMap_Clone_Empty(t *testing.T) {
	t.Parallel()

	m := safemap.New[string, int]("test.clone.empty")
	cp := m.Clone()
	assert.NotNil(t, cp)
	assert.Empty(t, cp)
}

func TestMap_Range(t *testing.T) {
	t.Parallel()

	m := safemap.New[string, int]("test.range")
	m.Set("a", 1)
	m.Set("b", 2)
	m.Set("c", 3)

	seen := make(map[string]int)
	m.Range(func(k string, v int) bool {
		seen[k] = v

		return true
	})

	assert.Equal(t, map[string]int{"a": 1, "b": 2, "c": 3}, seen)
}

func TestMap_Range_EarlyStop(t *testing.T) {
	t.Parallel()

	m := safemap.New[string, int]("test.range.stop")
	m.Set("a", 1)
	m.Set("b", 2)
	m.Set("c", 3)

	count := 0
	m.Range(func(_ string, _ int) bool {
		count++

		return false // stop after first item
	})

	assert.Equal(t, 1, count)
}

func TestMap_ConcurrentSetGet(t *testing.T) {
	t.Parallel()

	m := safemap.New[int, int]("test.concurrent.setget")

	const goroutines = 100
	var wg sync.WaitGroup

	for i := range goroutines {
		wg.Go(func() {
			m.Set(i, i*2)
		})
	}

	wg.Wait()

	assert.Equal(t, goroutines, m.Len())
}

func TestMap_ConcurrentMutualExclusion(t *testing.T) {
	t.Parallel()

	m := safemap.New[string, int]("test.concurrent.exclusive")

	const goroutines = 50
	var wg sync.WaitGroup

	for i := range goroutines {
		wg.Go(func() {
			existing, _ := m.Get("counter")
			m.Set("counter", existing+i)
		})
	}

	wg.Wait()

	// The value should exist; exact value depends on ordering.
	_, ok := m.Get("counter")
	assert.True(t, ok)
}

func TestMap_IntKey(t *testing.T) {
	t.Parallel()

	m := safemap.New[int, string]("test.int.key")
	m.Set(1, "one")
	m.Set(2, "two")

	v, ok := m.Get(1)
	assert.True(t, ok)
	assert.Equal(t, "one", v)

	assert.Equal(t, 2, m.Len())
}
