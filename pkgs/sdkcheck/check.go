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

// buildSDKMethodSet returns exported method names on sdkClientPtr (a non-nil
// pointer to a struct), excluding "Options" which is not an API operation.
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

// findStale returns candidateSet entries not in sdkMethods: stale
// notImplemented entries, or "phantom" supportedOps entries AWS doesn't have.
func findStale(candidateSet, sdkMethods map[string]bool) []string {
	var stale []string
	for m := range candidateSet {
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

// phantomAllowlist lists supportedOps entries that legitimately aren't a real
// SDK method, keyed by the client's concrete pointer type
// (fmt.Sprintf("%T", sdkClientPtr), e.g. "*s3.Client") to avoid adding a
// parameter to CheckCompleteness's ~160 call sites. Each value is a short
// justification; keep this list rare — the phantom check is a hard gate so
// bogus supportedOps entries get caught immediately.
//
//nolint:gochecknoglobals // static lookup table, same pattern as errCodeLookup elsewhere
var phantomAllowlist = map[string]map[string]string{
	"*s3.Client": {
		"PostObject":         "browser-form POST upload; real S3 REST op, no SDK client method",
		"PresignedGetObject": "presigned-URL helper for GET; client-side SDK helper, not a wire op",
		"PresignedPutObject": "presigned-URL helper for PUT; client-side SDK helper, not a wire op",
	},
	"*iotdataplane.Client": {
		"ListConnections":       "gopherstack admin-only extension; not a real iotdataplane op (List)",
		"ListThingsWithShadows": "gopherstack admin-only extension; not a real iotdataplane op (Shadows)",
		"RegisterConnection":    "gopherstack admin-only extension; not a real iotdataplane op (Register)",
	},
	"*bedrockagent.Client": {
		"GetAgentMemory":    "real op on the bedrock-agent-runtime client, which is not vendored here",
		"DeleteAgentMemory": "real op on the bedrock-agent-runtime client, which is not vendored here",
	},
	"*rds.Client": {
		"GetPerformanceInsightsMetrics": "kept; real op is pi client's GetResourceMetrics, see PARITY.md",
	},
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

// CheckCompleteness verifies every exported method on sdkClientPtr (a non-nil
// pointer, e.g. &s3.Client{}) is accounted for in supportedOps or
// notImplemented, with no duplicates or overlap between the two, no stale
// entries in notImplemented, and no "phantom" entries in supportedOps unless
// allowed by phantomAllowlist. "Options" is always excluded.
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

	clientType := fmt.Sprintf("%T", sdkClientPtr)
	allowed := phantomAllowlist[clientType]

	var unexpectedPhantoms []string
	for _, op := range findStale(supportedSet, sdkMethods) {
		if _, ok := allowed[op]; !ok {
			unexpectedPhantoms = append(unexpectedPhantoms, op)
		}
	}
	sort.Strings(unexpectedPhantoms)

	assert.Empty(tb, unexpectedPhantoms,
		"GetSupportedOperations() contains entries that are not exported methods on the SDK client "+
			"(a \"phantom\" operation) and are not in phantomAllowlist[%q] in check.go: %v.\n"+
			"These may be typos, methods that were renamed/removed in a newer SDK version, an "+
			"operation that belongs to a different (sibling/data-plane) SDK client, or an operation "+
			"that was never real — verify the true operation name/shape against the actual AWS SDK "+
			"before assuming a rename. If this is a deliberate, documented gopherstack extension or "+
			"pseudo-operation, add it to phantomAllowlist in pkgs/sdkcheck/check.go with a "+
			"justification comment instead of suppressing this failure another way.",
		clientType, unexpectedPhantoms)

	assert.Empty(tb, findUnaccounted(sdkMethods, supportedSet, notImplSet),
		"SDK methods found that are neither in GetSupportedOperations() nor in the notImplemented list.\n"+
			"Either implement the missing operations and add them to GetSupportedOperations(), "+
			"or add them to the notImplemented slice in this test.")
}
