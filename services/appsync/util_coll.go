package appsync

import (
	"crypto/rand"
	"encoding/binary"
	"maps"
	"math"
	"math/big"
	"sort"
	"strings"
)

func utilCollections() map[string]utilFn {
	out := map[string]utilFn{
		"str.toUpper": func(_ *utilEnv, a []any) (any, error) { return strings.ToUpper(argStr(a, 0)), nil },
		"str.toLower": func(_ *utilEnv, a []any) (any, error) { return strings.ToLower(argStr(a, 0)), nil },
		"str.toReplace": func(_ *utilEnv, a []any) (any, error) {
			return strings.ReplaceAll(argStr(a, 0), argStr(a, argSecond), argStr(a, argThird)), nil
		},
	}

	maps.Copy(out, utilMath())
	maps.Copy(out, utilListMap())

	return out
}

func numArg(a []any, i int) float64 {
	f, _, _ := toNum(argAt(a, i))

	return f
}

func utilMath() map[string]utilFn {
	return map[string]utilFn{
		"math.roundNum": func(_ *utilEnv, a []any) (any, error) { return int64(math.Round(numArg(a, 0))), nil },
		"math.minVal": func(_ *utilEnv, a []any) (any, error) {
			return numResult(math.Min(numArg(a, 0), numArg(a, argSecond)), a), nil
		},
		"math.maxVal": func(_ *utilEnv, a []any) (any, error) {
			return numResult(math.Max(numArg(a, 0), numArg(a, argSecond)), a), nil
		},
		"math.randomDouble": func(_ *utilEnv, _ []any) (any, error) { return randomFloat(), nil },
		"math.randomWithinRange": func(_ *utilEnv, a []any) (any, error) {
			lo, hi := int64(numArg(a, 0)), int64(numArg(a, argSecond))
			if hi <= lo {
				return lo, nil
			}

			n, err := rand.Int(rand.Reader, big.NewInt(hi-lo))
			if err != nil {
				return nil, err
			}

			return lo + n.Int64(), nil
		},
	}
}

func numResult(f float64, a []any) any {
	_, i1, _ := toNum(argAt(a, 0))
	_, i2, _ := toNum(argAt(a, 1))

	if i1 && i2 {
		return int64(f)
	}

	return f
}

func utilListMap() map[string]utilFn {
	return map[string]utilFn{
		"list.copyAndRetainAll": func(_ *utilEnv, a []any) (any, error) { return filterList(a, true), nil },
		"list.copyAndRemoveAll": func(_ *utilEnv, a []any) (any, error) { return filterList(a, false), nil },
		"list.sortList": func(_ *utilEnv, a []any) (any, error) {
			items := append([]any(nil), listOf(argAt(a, 0))...)
			desc := vtruthy(argAt(a, 1))
			prop := argStrIf(a, argThird)

			sort.SliceStable(items, func(i, j int) bool {
				x, y := sortKey(items[i], prop), sortKey(items[j], prop)
				if desc {
					return vcompare(">", x, y)
				}

				return vcompare("<", x, y)
			})

			return items, nil
		},
		"map.copyAndRetainAllKeys": func(_ *utilEnv, a []any) (any, error) { return filterMap(a, true), nil },
		"map.copyAndRemoveAllKeys": func(_ *utilEnv, a []any) (any, error) { return filterMap(a, false), nil },
	}
}

func sortKey(v any, prop string) any {
	if m, ok := v.(map[string]any); ok && prop != "" {
		return m[prop]
	}

	return v
}

func filterList(a []any, retain bool) []any {
	other := listOf(argAt(a, 1))
	out := []any{}

	for _, it := range listOf(argAt(a, 0)) {
		found := false

		for _, o := range other {
			if vequal(it, o) {
				found = true

				break
			}
		}

		if found == retain {
			out = append(out, it)
		}
	}

	return out
}

func filterMap(a []any, retain bool) map[string]any {
	keys := map[string]bool{}
	for _, k := range listOf(argAt(a, 1)) {
		keys[vrender(k)] = true
	}

	src, _ := argAt(a, 0).(map[string]any)
	out := map[string]any{}

	for k, v := range src {
		if keys[k] == retain {
			out[k] = v
		}
	}

	return out
}

const (
	floatBits  = 53
	floatShift = 64 - floatBits
)

func randomFloat() float64 {
	var b [8]byte

	_, _ = rand.Read(b[:])

	return float64(binary.BigEndian.Uint64(b[:])>>floatShift) / float64(uint64(1)<<floatBits)
}
