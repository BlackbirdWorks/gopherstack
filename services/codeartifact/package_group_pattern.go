package codeartifact

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// This file implements AWS CodeArtifact's package group pattern matching
// algorithm, as documented in "Package group definition syntax and matching
// behavior":
// https://docs.aws.amazon.com/codeartifact/latest/ug/package-group-definition-syntax-matching-behavior.html
//
// A pattern mirrors a package path (/format/namespace/name) but only
// specifies a prefix of its components, terminated by a suffix that
// determines matching behavior for the last specified component:
//   - "$" matches the component's value exactly.
//   - "~" matches the component as a word-boundary prefix.
//   - "*" matches any value of the component (and leaves every deeper
//     component completely unconstrained).
//
// Components after the last specified one are always unconstrained. The
// "most specific" matching pattern among several candidates is the one
// whose match-space is the smallest (a proper subset of every less-specific
// candidate's match-space) -- see isProperSubsetPattern.
//
// Scope note: this implements the "strong match" (exact) half of AWS's
// matching algorithm only. The "weak match" half (case-folding,
// dash/dot/underscore-equivalence, confusable-character normalization used
// for dependency-confusion protection) is NOT implemented -- see
// PARITY.md's gaps list.

// groupPatternTarget identifies which package-path component (format,
// namespace, or name) a parsed pattern's suffix applies to.
type groupPatternTarget int

const (
	groupTargetFormat groupPatternTarget = iota
	groupTargetNamespace
	groupTargetName
)

// groupMatchType is the suffix character's matching behavior.
type groupMatchType int

const (
	groupMatchExact groupMatchType = iota
	groupMatchPrefix
	groupMatchWildcard
)

// groupPattern is a parsed package group pattern.
type groupPattern struct {
	value     string
	segments  []string
	target    groupPatternTarget
	matchType groupMatchType
}

// parseGroupPattern parses a package group pattern string (e.g. "/npm/*",
// "/maven/com.anycompany~", "/npm/space/react$") into its structured form.
// Returns an error wrapping ErrValidation if the pattern is malformed.
func parseGroupPattern(pattern string) (*groupPattern, error) {
	if len(pattern) < 2 || pattern[0] != '/' {
		return nil, fmt.Errorf(
			"%w: package group pattern %q must start with '/' and have length >= 2", ErrValidation, pattern,
		)
	}

	rest := pattern[1:]
	parts := strings.Split(rest, "/")

	if len(parts) < 1 || len(parts) > 3 {
		return nil, fmt.Errorf(
			"%w: package group pattern %q must specify 1 to 3 path components", ErrValidation, pattern,
		)
	}

	last := parts[len(parts)-1]

	var mt groupMatchType

	var value string

	switch {
	case last == "*":
		mt = groupMatchWildcard
		value = ""
	case strings.HasSuffix(last, "$"):
		mt = groupMatchExact
		value = strings.TrimSuffix(last, "$")
	case strings.HasSuffix(last, "~"):
		mt = groupMatchPrefix
		value = strings.TrimSuffix(last, "~")
	default:
		return nil, fmt.Errorf(
			"%w: package group pattern %q must end with '*', '$', or '~'", ErrValidation, pattern,
		)
	}

	return &groupPattern{
		segments:  parts[:len(parts)-1],
		target:    groupPatternTarget(len(parts) - 1),
		matchType: mt,
		value:     value,
	}, nil
}

// isWordRune reports whether r is a "word" character per AWS's definition:
// any letter, number, or mark character.
func isWordRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsNumber(r) || unicode.IsMark(r)
}

// hasPrefixWordBoundary reports whether s begins with prefix, followed
// either by nothing or by a non-word character (AWS's word-boundary prefix
// match rule for the "~" suffix).
func hasPrefixWordBoundary(s, prefix string) bool {
	if !strings.HasPrefix(s, prefix) {
		return false
	}

	if len(s) == len(prefix) {
		return true
	}

	r, _ := utf8.DecodeRuneInString(s[len(prefix):])

	return !isWordRune(r)
}

// components returns the package-path components in order (format,
// namespace, name).
func packageComponents(format, namespace, name string) [3]string {
	return [3]string{format, namespace, name}
}

// matches reports whether the given package coordinate falls within p's
// match-space.
func (p *groupPattern) matches(format, namespace, name string) bool {
	comps := packageComponents(format, namespace, name)

	for i, want := range p.segments {
		if comps[i] != want {
			return false
		}
	}

	got := comps[p.target]

	switch p.matchType {
	case groupMatchWildcard:
		return true
	case groupMatchExact:
		return got == p.value
	case groupMatchPrefix:
		return hasPrefixWordBoundary(got, p.value)
	default:
		return false
	}
}

// specificityRank returns a monotonic specificity score: higher means more
// specific (a smaller match-space). Used to pick the "most specific"
// matching pattern -- see isProperSubsetPattern for the exact ordering this
// approximates.
func (p *groupPattern) specificityRank() int {
	const (
		targetWeight = 1000
		typeWeight   = 100
		maxValueLen  = 99
	)

	var mtWeight int

	switch p.matchType {
	case groupMatchExact:
		mtWeight = 3
	case groupMatchPrefix:
		mtWeight = 2
	case groupMatchWildcard:
		mtWeight = 1
	}

	valLen := min(len(p.value), maxValueLen)

	return int(p.target)*targetWeight + mtWeight*typeWeight + valLen
}

// isProperSubsetPattern reports whether a's match-space is a strict subset
// of b's match-space -- i.e. every package that matches a also matches b,
// but not vice versa. This defines the package-group parent/child hierarchy:
// b is an ancestor of a iff this returns true.
func isProperSubsetPattern(a, b *groupPattern) bool {
	if b.target > a.target {
		return false
	}

	for i := range int(b.target) {
		if a.segments[i] != b.segments[i] {
			return false
		}
	}

	if b.target < a.target {
		// b's target position is one of a's literal (pre-target) segments.
		av := a.segments[b.target]

		return literalSatisfies(av, b)
	}

	// Same target position: compare a's own condition against b's.
	return conditionIsSubsetOf(a, b)
}

// literalSatisfies reports whether the literal value av (a fixed path
// component) falls within pattern p's condition at its own target position.
func literalSatisfies(av string, p *groupPattern) bool {
	switch p.matchType {
	case groupMatchWildcard:
		return true
	case groupMatchExact:
		return av == p.value
	case groupMatchPrefix:
		return hasPrefixWordBoundary(av, p.value)
	default:
		return false
	}
}

// conditionIsSubsetOf reports whether a's target-position condition is a
// (possibly proper) subset of b's, given a.target == b.target. It excludes
// the case where a and b are identical patterns (callers exclude self when
// searching for a parent, and CreatePackageGroup rejects duplicate
// patterns, so two distinct groups in the same domain never have an
// identical pattern).
func conditionIsSubsetOf(a, b *groupPattern) bool {
	switch b.matchType {
	case groupMatchWildcard:
		return true
	case groupMatchPrefix:
		switch a.matchType {
		case groupMatchExact:
			return hasPrefixWordBoundary(a.value, b.value)
		case groupMatchPrefix:
			return a.value != b.value && hasPrefixWordBoundary(a.value, b.value)
		default:
			return false
		}
	case groupMatchExact:
		// Nothing distinct can be a proper subset of an exact match (its
		// match-space is a single point).
		return false
	default:
		return false
	}
}
