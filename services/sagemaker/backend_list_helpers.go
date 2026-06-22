package sagemaker

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
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
	keys := collections.SortedKeys(store)

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

// sagemakerCreate handles the common create-resource-by-name pattern:
// acquire lock, check for duplicate, build ARN, build item, store, return clone.
func sagemakerCreate[T any](
	ctx context.Context,
	b *InMemoryBackend,
	opName, name, arnResource string,
	storeOf func(string) map[string]*T,
	dupErr func(string) error,
	build func(arnStr string, now time.Time) *T,
	clone func(*T) *T,
) (*T, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock(opName)
	defer b.mu.Unlock()

	store := storeOf(region)
	if _, ok := store[name]; ok {
		return nil, dupErr(name)
	}

	arnStr := arn.Build("sagemaker", region, b.accountID, arnResource+"/"+name)
	now := time.Now()

	item := build(arnStr, now)
	store[name] = item

	return clone(item), nil
}

// sagemakerDupErr returns a formatted "already exists" error wrapping ErrValidation.
func sagemakerDupErr(kind, name string) error {
	return fmt.Errorf("%w: %s %q already exists", ErrValidation, kind, name)
}
