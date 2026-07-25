// Package strs provides small, dependency-free case-folding string helpers.
// It exists so that "compare/store these two strings ignoring case" logic —
// a recurring need wherever a service backend emulates an AWS API whose
// resource identifiers are case-insensitive (e.g. RDS's DBInstanceIdentifier,
// DBClusterIdentifier, DBSnapshotIdentifier, DBParameterGroupName: real AWS
// lower-cases these internally, so creating "MyDB" then "mydb" collides with
// an AlreadyExists fault instead of producing two distinct resources) — has
// one shared, tested home instead of being reimplemented per service.
//
// A service backend that stores such a resource in a map keyed by its
// identifier (directly, or via github.com/blackbirdworks/gopherstack/pkgs/store's
// Table[V] keyFn) should normalize through [Fold] at every store boundary:
// the keyFn used by Put/Restore, and every raw string passed to Get/Has/Delete
// (which, unlike Put, do not invoke keyFn — they index the map directly). The
// ORIGINAL caller-supplied casing should be preserved in the stored value's
// own identifier field, so wire responses keep echoing back exactly what the
// caller sent — only the lookup key folds case, never the data.
package strs

import "strings"

// Fold normalizes a string to its canonical case-insensitive comparison/
// lookup-key form. AWS's own case-folding (for the identifiers this package
// exists to support) is a lowercase fold, not full Unicode case-folding, so
// this deliberately uses strings.ToLower rather than a more general notion
// of "same letter" — these identifiers are ASCII-range names (letters,
// digits, hyphens) in every service that documents this behavior.
func Fold(s string) string {
	return strings.ToLower(s)
}

// ContainsFold reports whether values contains target under the same
// case-insensitive comparison as [Fold]. Useful for matching a client-supplied
// filter value (or any other identifier-shaped string) against a stored,
// case-insensitive identifier without needing to fold the whole values slice
// first.
func ContainsFold(values []string, target string) bool {
	for _, v := range values {
		if Equal(v, target) {
			return true
		}
	}

	return false
}

// Equal reports whether a and b are the same string under case-insensitive
// comparison — the two-value counterpart to [ContainsFold]. A thin,
// self-documenting wrapper over strings.EqualFold so "are these two
// identifiers the same resource" comparisons in a backend read the same way
// and are grep-able as identifier logic rather than an incidental string
// comparison.
func Equal(a, b string) bool {
	return strings.EqualFold(a, b)
}
