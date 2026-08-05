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

// findStale returns entries from candidateSet that are not in sdkMethods (i.e.
// they don't correspond to a real SDK operation). It is used both to flag
// notImplemented entries that no longer exist on the SDK client, and to flag
// supportedOps entries that never existed on the SDK client at all — a
// "phantom" operation the handler claims to support but that AWS doesn't have.
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

// phantomAllowlist lists supportedOps entries that legitimately do not
// correspond to a real method on the AWS SDK v2 client, keyed by the client's
// concrete pointer type as reported by fmt.Sprintf("%T", sdkClientPtr) (e.g.
// "*s3.Client"). Keying off the client type rather than adding a new
// parameter to CheckCompleteness avoids touching its ~160 existing call
// sites — every call already passes sdkClientPtr, and reflect.TypeOf already
// derives the SDK method set from it, so deriving the allowlist key the same
// way fits the existing API instead of widening it.
//
// Each entry's value is a short justification for why the name is a
// deliberate, documented pseudo-operation or extension rather than a typo, a
// sibling-SDK mix-up, or a fabricated operation. Keep this list short: the
// reverse phantom check below is a hard failure specifically so bogus
// supportedOps entries get caught immediately. Only add an entry here for a
// name that will never exist on the real AWS SDK client by design.
//
//nolint:gochecknoglobals // static lookup table, same pattern as errCodeLookup elsewhere
var phantomAllowlist = map[string]map[string]string{
	"*s3.Client": {
		// Browser-style POST form-data upload (see services/s3/post_object.go).
		// S3's POST Object is a REST API operation with no corresponding SDK
		// client method — the SDK only ever issues PutObject.
		"PostObject": "browser-form POST upload; real S3 REST op, no SDK client method",
		// Presigned-URL pseudo-operations: presigning is a client-side SDK
		// helper (e.g. s3.PresignClient), not a wire operation, so it has no
		// corresponding method on the Client returned by findStale.
		"PresignedGetObject": "presigned-URL helper for GET; client-side SDK helper, not a wire op",
		"PresignedPutObject": "presigned-URL helper for PUT; client-side SDK helper, not a wire op",
	},
	"*iotdataplane.Client": {
		// gopherstack-only admin extensions served on /_admin/... paths (see
		// services/iotdataplane/handler_connections.go:37,59); not part of
		// the real AWS iotdataplane API.
		"ListConnections":       "gopherstack admin-only extension; not a real iotdataplane op (List)",
		"ListThingsWithShadows": "gopherstack admin-only extension; not a real iotdataplane op (Shadows)",
		"RegisterConnection":    "gopherstack admin-only extension; not a real iotdataplane op (Register)",
	},
	"*rds.Client": {
		// Deliberately kept: real Performance Insights functionality with no
		// wire-shape-accurate replacement. The real op is GetResourceMetrics on
		// a separate "pi" SDK client this repo doesn't depend on. See the
		// performance_insights family and gaps entry in services/rds/PARITY.md.
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

// CheckCompleteness verifies that every exported method on sdkClientPtr is
// either listed in supportedOps (the handler's GetSupportedOperations slice) or
// explicitly listed in notImplemented. It also performs quality checks on the
// two lists themselves, in both directions: every SDK method must be
// accounted for, and every entry in notImplemented must correspond to a real
// SDK method. A third check catches the reverse defect: supportedOps entries
// that don't correspond to a real SDK method at all ("phantom" operations),
// unless the exact (client type, name) pair is listed in phantomAllowlist.
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
//   - supportedOps contains an entry that is not a real SDK method and is not
//     in phantomAllowlist for this client type (a "phantom" operation — the
//     handler claims to support something AWS doesn't have, the check is
//     being run against the wrong sibling SDK client, or it's a typo/rename).
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
