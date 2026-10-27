// Package gcpevents holds the Eventarc pieces shared by GCP emulators: path patterns and CloudEvents in binary
// content mode.
package gcpevents

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// OperatorPathPattern is the EventFilter operator that selects path-pattern matching.
const OperatorPathPattern = "match-path-pattern"

// ErrPattern means a path pattern is malformed.
var ErrPattern = errors.New("invalid path pattern")

var captureRe = regexp.MustCompile(`^\{[A-Za-z_][A-Za-z0-9_]*(=\*\*)?\}$`)

// CompilePathPattern turns an Eventarc path pattern into an anchored expression: "*" matches within one
// segment, "**" spans segments, "{name}" captures one segment and "{name=**}" several.
func CompilePathPattern(p string) (*regexp.Regexp, error) {
	var sb strings.Builder

	sb.WriteString("^")

	for i, seg := range strings.Split(p, "/") {
		if i > 0 {
			sb.WriteString("/")
		}

		switch {
		case seg == "**":
			sb.WriteString(".*")
		case strings.HasPrefix(seg, "{"):
			if !captureRe.MatchString(seg) {
				return nil, fmt.Errorf("%w: bad capture %q", ErrPattern, seg)
			}

			if strings.HasSuffix(seg, "=**}") {
				sb.WriteString(".+")
			} else {
				sb.WriteString("[^/]+")
			}
		default:
			for _, part := range strings.SplitAfter(seg, "*") {
				sb.WriteString(regexp.QuoteMeta(strings.TrimSuffix(part, "*")))

				if strings.HasSuffix(part, "*") {
					sb.WriteString("[^/]*")
				}
			}
		}
	}

	sb.WriteString("$")

	re, err := regexp.Compile(sb.String())
	if err != nil {
		return nil, fmt.Errorf("%w: %q", ErrPattern, p)
	}

	return re, nil
}

// MatchFilter reports whether got satisfies an event filter: exact equality, or a path pattern when operator is
// OperatorPathPattern.
func MatchFilter(operator, want, got string) bool {
	if operator != OperatorPathPattern {
		return got == want
	}

	re, err := CompilePathPattern(want)

	return err == nil && re.MatchString(got)
}
