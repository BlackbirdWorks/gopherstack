// Package sdkcheck provides helpers for verifying that gopherstack service
// handlers cover all operations exposed by the corresponding AWS SDK v2 client.
package sdkcheck

import (
	"reflect"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

// CheckCompleteness verifies that every exported method on sdkClientPtr is
// either listed in supportedOps (the handler's GetSupportedOperations slice) or
// explicitly listed in notImplemented.
//
// sdkClientPtr must be a pointer to an AWS SDK v2 Client struct, e.g.
// &s3.Client{}.
//
// The test fails if a method exists on the SDK client that is not accounted for
// in either list, which normally happens when the upstream SDK adds a new
// operation that gopherstack has not yet been updated to handle.
func CheckCompleteness(t *testing.T, sdkClientPtr any, supportedOps []string, notImplemented []string) {
	t.Helper()

	notImplSet := make(map[string]bool, len(notImplemented))
	for _, m := range notImplemented {
		notImplSet[m] = true
	}

	supportedSet := make(map[string]bool, len(supportedOps))
	for _, op := range supportedOps {
		supportedSet[op] = true
	}

	sdkType := reflect.TypeOf(sdkClientPtr)

	var unaccounted []string

	for i := range sdkType.NumMethod() {
		name := sdkType.Method(i).Name
		if !supportedSet[name] && !notImplSet[name] {
			unaccounted = append(unaccounted, name)
		}
	}

	sort.Strings(unaccounted)

	assert.Empty(t, unaccounted,
		"SDK methods found that are neither in GetSupportedOperations() nor in the notImplemented list.\n"+
			"Either implement the missing operations and add them to GetSupportedOperations(), "+
			"or add them to the notImplemented slice in this test.\n"+
			"Unaccounted methods: %v", unaccounted)
}
