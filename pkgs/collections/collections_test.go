package collections_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

func TestMap(t *testing.T) {
	t.Parallel()

	got := collections.Map([]int{1, 2, 3}, func(i int) string { return strconv.Itoa(i * 2) })
	assert.Equal(t, []string{"2", "4", "6"}, got)
	assert.Nil(t, collections.Map[int, string](nil, strconv.Itoa))
}

func TestFilter(t *testing.T) {
	t.Parallel()

	got := collections.Filter([]int{1, 2, 3, 4}, func(i int) bool { return i%2 == 0 })
	assert.Equal(t, []int{2, 4}, got)
	assert.Empty(t, collections.Filter([]int{1, 3}, func(i int) bool { return i%2 == 0 }))
	assert.Nil(t, collections.Filter[int](nil, func(int) bool { return true }))
}

func TestMapValues(t *testing.T) {
	t.Parallel()

	m := map[string]int{"a": 1, "b": 2}
	got := collections.MapValues(m, func(v int) int { return v * 10 })
	assert.ElementsMatch(t, []int{10, 20}, got)
	assert.Nil(t, collections.MapValues[string, int, int](nil, func(v int) int { return v }))
}

func TestValues(t *testing.T) {
	t.Parallel()

	m := map[string]int{"a": 1, "b": 2, "c": 3}
	assert.ElementsMatch(t, []int{1, 2, 3}, collections.Values(m))
	assert.Nil(t, collections.Values[string, int](nil))
}

func TestSortedKeys(t *testing.T) {
	t.Parallel()

	m := map[string]int{"c": 1, "a": 2, "b": 3}
	assert.Equal(t, []string{"a", "b", "c"}, collections.SortedKeys(m))
	assert.Nil(t, collections.SortedKeys[string, int](nil))
}
