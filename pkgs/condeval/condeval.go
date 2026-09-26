// Package condeval holds IAM policy / STS trust-policy condition-operator
// logic shared between services/iam and services/sts: ARN segment-wise
// matching (ArnEquals/ArnLike/ArnNotEquals/ArnNotLike) and Date operand
// parsing/comparison (DateEquals/.../DateGreaterThanEquals). Each caller
// keeps its own wildcard matcher and its own operator-dispatch/IfExists/
// set-qualifier plumbing; only the AWS-documented matching rules that were
// starting to be copy-pasted verbatim between the two packages live here.
package condeval

import (
	"strconv"
	"strings"
	"time"
)

// ArnSegmentCount is the number of colon-delimited components in an ARN
// (arn:partition:service:region:account-id:resource).
const ArnSegmentCount = 6

// ArnMatch implements ArnEquals/ArnLike, which AWS documents as behaving
// identically: case-sensitive, with each of the six colon-delimited ARN
// components wildcard-matched separately via match, rather than one glob
// over the whole string (which would let a wildcard span a segment
// boundary).
// https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_condition_operators.html#Conditions_ARN
func ArnMatch(pattern, value string, match func(pattern, value string) bool) bool {
	pParts := strings.SplitN(pattern, ":", ArnSegmentCount)
	vParts := strings.SplitN(value, ":", ArnSegmentCount)

	if len(pParts) != len(vParts) {
		return false
	}

	for i, p := range pParts {
		if !match(p, vParts[i]) {
			return false
		}
	}

	return true
}

// AnyArnMatch reports whether value matches any ARN pattern in patterns.
func AnyArnMatch(patterns []string, value string, match func(pattern, value string) bool) bool {
	for _, p := range patterns {
		if ArnMatch(p, value, match) {
			return true
		}
	}

	return false
}

// isoDateLayouts are the W3C ISO 8601 profiles AWS documents for Date
// condition values, tried in order before falling back to epoch seconds.
// https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_condition_operators.html#Conditions_Date
var isoDateLayouts = []string{ //nolint:gochecknoglobals // read-only lookup table
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02T15:04:05",
	"2006-01-02",
}

// ParseDate parses a Date condition operand, accepting both ISO 8601 and
// epoch (UNIX) seconds, as AWS documents for aws:CurrentTime/aws:EpochTime.
func ParseDate(v string) (time.Time, bool) {
	for _, layout := range isoDateLayouts {
		if t, err := time.Parse(layout, v); err == nil {
			return t, true
		}
	}

	if secs, err := strconv.ParseFloat(v, 64); err == nil {
		whole := int64(secs)
		nanos := int64((secs - float64(whole)) * float64(time.Second))

		return time.Unix(whole, nanos).UTC(), true
	}

	return time.Time{}, false
}

// CompareDate evaluates one of the six Date condition operators (already
// lower-cased and IfExists-stripped) for a single actual/candidate pair.
// Unrecognized operators return false.
func CompareDate(op string, actual, candidate time.Time) bool {
	switch op {
	case "dateequals":
		return actual.Equal(candidate)
	case "datenotequals":
		return !actual.Equal(candidate)
	case "datelessthan":
		return actual.Before(candidate)
	case "datelessthanequals":
		return !actual.After(candidate)
	case "dategreaterthan":
		return actual.After(candidate)
	case "dategreaterthanequals":
		return !actual.Before(candidate)
	default:
		return false
	}
}
