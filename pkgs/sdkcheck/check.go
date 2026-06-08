// Package sdkcheck provides helpers for verifying that gopherstack service
// handlers cover all operations exposed by the corresponding AWS SDK v2 client.
package sdkcheck

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

// buildSDKMethodSet returns the set of exported method names on sdkClientPtr,
// excluding the "Options" configuration accessor which is present on every AWS
// SDK v2 Client but is not an API operation.
//
// sdkClientPtr must be a non-nil pointer to a struct.
func buildSDKMethodSet(sdkClientPtr any) map[string]bool {
	methods := make(map[string]bool)
	for m := range reflect.TypeOf(sdkClientPtr).Methods() {
		if m.Name != "Options" {
			methods[m.Name] = true
		}
	}

	return methods
}

// buildSet returns a deduplicated set from slice, also returning any duplicates found.
func buildSet(items []string) (map[string]bool, []string) {
	set := make(map[string]bool, len(items))
	var dups []string
	for _, item := range items {
		if set[item] {
			dups = append(dups, item)
		}
		set[item] = true
	}
	sort.Strings(dups)

	return set, dups
}

// findStale returns entries from the notImplemented set that are not in sdkMethods
// (i.e. they don't correspond to a real SDK operation).
func findStale(notImplSet, sdkMethods map[string]bool) []string {
	var stale []string
	for m := range notImplSet {
		if !sdkMethods[m] {
			stale = append(stale, m)
		}
	}
	sort.Strings(stale)

	return stale
}

// findOverlapping returns entries that appear in both sets.
func findOverlapping(a, b map[string]bool) []string {
	var overlap []string
	for k := range a {
		if b[k] {
			overlap = append(overlap, k)
		}
	}
	sort.Strings(overlap)

	return overlap
}

// findUnaccounted returns SDK method names not present in either supportedSet or notImplSet.
func findUnaccounted(sdkMethods, supportedSet, notImplSet map[string]bool) []string {
	var unaccounted []string
	for name := range sdkMethods {
		if !supportedSet[name] && !notImplSet[name] {
			unaccounted = append(unaccounted, name)
		}
	}
	sort.Strings(unaccounted)

	return unaccounted
}

// CheckCompleteness verifies that every exported method on sdkClientPtr is
// either listed in supportedOps (the handler's GetSupportedOperations slice) or
// explicitly listed in notImplemented. It also performs quality checks on the
// two lists themselves.
//
// sdkClientPtr must be a non-nil pointer to an AWS SDK v2 Client struct, e.g.
// &s3.Client{}.
//
// The test fails if:
//   - sdkClientPtr is nil or not a pointer type.
//   - An SDK method is not accounted for in either list (new upstream operation).
//   - notImplemented contains entries that are not real SDK methods (typos / SDK renames).
//   - notImplemented contains duplicate entries.
//   - supportedOps contains duplicate entries.
//   - supportedOps and notImplemented contain overlapping entries.
//
// The "Options" method, which exists on every AWS SDK v2 Client but is not an
// API operation, is always excluded from the check.
func CheckCompleteness(tb testing.TB, sdkClientPtr any, supportedOps []string, notImplemented []string) {
	tb.Helper()

	rt := reflect.TypeOf(sdkClientPtr)
	if rt == nil {
		assert.Fail(tb, "sdkClientPtr must not be nil — pass a pointer such as &s3.Client{}")

		return
	}

	if rt.Kind() != reflect.Pointer {
		assert.Fail(tb, fmt.Sprintf(
			"sdkClientPtr must be a pointer (e.g. &s3.Client{}), got %T", sdkClientPtr,
		))

		return
	}

	sdkMethods := buildSDKMethodSet(sdkClientPtr)
	supportedSet, dupSupported := buildSet(supportedOps)
	notImplSet, dupNotImpl := buildSet(notImplemented)

	assert.Empty(tb, dupSupported,
		"GetSupportedOperations() contains duplicate entries — remove the duplicates.")

	assert.Empty(tb, dupNotImpl,
		"notImplemented slice contains duplicate entries — remove the duplicates.")

	assert.Empty(tb, findOverlapping(supportedSet, notImplSet),
		"The same method appears in both GetSupportedOperations() and notImplemented — "+
			"remove it from notImplemented if it is implemented, or from GetSupportedOperations() if it is not.")

	assert.Empty(tb, findStale(notImplSet, sdkMethods),
		"notImplemented contains entries that are not exported methods on the SDK client.\n"+
			"These may be typos or methods that were renamed/removed in a newer SDK version.")

	assert.Empty(tb, findUnaccounted(sdkMethods, supportedSet, notImplSet),
		"SDK methods found that are neither in GetSupportedOperations() nor in the notImplemented list.\n"+
			"Either implement the missing operations and add them to GetSupportedOperations(), "+
			"or add them to the notImplemented slice in this test.")
}
