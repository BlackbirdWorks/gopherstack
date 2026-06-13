package sagemaker

import (
	"sort"
	"strconv"
)

// sagemakerListPaged paginates a store using index-based tokens.
// clone must return a deep copy of its argument.
// less defines the sort order.
func sagemakerListPaged[T any](
	store map[string]*T,
	nextToken string,
	clone func(*T) *T,
	less func(a, b *T) bool,
) ([]*T, string) {
	list := make([]*T, 0, len(store))
	for _, item := range store {
		list = append(list, clone(item))
	}

	sort.Slice(list, func(i, j int) bool { return less(list[i], list[j]) })

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(list) {
		return []*T{}, ""
	}

	end := startIdx + sagemakerDefaultPageSize

	var outToken string

	if end < len(list) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}

// sagemakerListKeyPaged paginates a store using name-key-based tokens.
// clone must return a deep copy of its argument.
func sagemakerListKeyPaged[T any](
	store map[string]*T,
	nextToken string,
	clone func(*T) *T,
) ([]*T, string) {
	keys := make([]string, 0, len(store))
	for k := range store {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	start := 0
	if nextToken != "" {
		for i, k := range keys {
			if k == nextToken {
				start = i

				break
			}
		}
	}

	end := min(start+sagemakerDefaultPageSize, len(keys))

	out := make([]*T, 0, end-start)
	for _, k := range keys[start:end] {
		out = append(out, clone(store[k]))
	}

	next := ""
	if end < len(keys) {
		next = keys[end]
	}

	return out, next
}
