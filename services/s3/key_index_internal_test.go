package s3

import (
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func keyIndexKeys(x *keyIndex) []string {
	var out []string
	for c := x.seek(func(string) bool { return true }); x.valid(c); c = x.next(c) {
		out = append(out, x.key(c))
	}

	return out
}

func TestKeyIndexMatchesSortedSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ops  int
		span int
	}{
		{name: "small", ops: 200, span: 50},
		{name: "splits", ops: 6000, span: 4000},
		{name: "churn", ops: 20000, span: 1500},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rng := rand.New(rand.NewPCG(1, 2))
			var idx keyIndex
			ref := map[string]struct{}{}

			for range tt.ops {
				k := "k" + strconv.Itoa(rng.IntN(tt.span))
				if rng.IntN(3) == 0 {
					idx.remove(k)
					delete(ref, k)
				} else {
					idx.insert(k)
					ref[k] = struct{}{}
				}
			}

			want := make([]string, 0, len(ref))
			for k := range ref {
				want = append(want, k)
			}

			slices.Sort(want)

			require.Equal(t, want, keyIndexKeys(&idx))
			assert.Equal(t, len(want), idx.len())

			for _, c := range idx.chunks {
				assert.NotEmpty(t, c)
				assert.LessOrEqual(t, len(c), keyChunkMax)
			}

			for _, probe := range []string{"", "k1", "k19", "k5", "k99999", "zzz"} {
				cur := idx.seek(func(k string) bool { return k >= probe })
				i := searchKey(want, probe)
				assert.Equal(t, len(want)-i, idx.remaining(cur), probe)

				if i < len(want) {
					assert.Equal(t, want[i], idx.key(cur))
				}

				pre := "k1"
				from := idx.seek(func(k string) bool { return strings.HasPrefix(k, pre) })
				if idx.valid(from) {
					skip := idx.seekFrom(from, func(k string) bool { return !strings.HasPrefix(k, pre) })
					j := searchFunc(want, func(k string) bool { return !strings.HasPrefix(k, pre) && k > pre })
					assert.Equal(t, len(want)-j, idx.remaining(skip))
				}
			}
		})
	}
}

func searchKey(s []string, k string) int {
	i, _ := slices.BinarySearch(s, k)

	return i
}

func searchFunc(s []string, pred func(string) bool) int {
	for i, k := range s {
		if pred(k) {
			return i
		}
	}

	return len(s)
}

func TestRebuildKeyIndex(t *testing.T) {
	t.Parallel()

	b := &StoredBucket{Objects: map[string]*StoredObject{}}
	for i := range 3000 {
		b.Objects["o"+strconv.Itoa(i)] = nil
	}

	rebuildKeyIndex(b)
	b.indexInsert("a")
	b.indexRemove("o5")

	keys := keyIndexKeys(&b.keyIndex)
	assert.True(t, slices.IsSorted(keys))
	assert.Len(t, keys, 3000)
	assert.Equal(t, 3000, b.keyIndex.len())
}
