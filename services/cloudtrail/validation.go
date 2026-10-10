package cloudtrail

import (
	"errors"
	"fmt"
	"net/netip"
	"regexp"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

const (
	minTrailNameLen = 3
	maxTrailNameLen = 128
	minEDSNameLen   = 3
	maxEDSNameLen   = 128
	minRetentionDay = 7
	maxRetentionDay = 3653
	maxLookupLimit  = 50
	maxEventSelects = 5
)

var (
	// ErrInvalidTrailName is returned for a trail name outside CloudTrail's naming rules.
	ErrInvalidTrailName = awserr.New("InvalidTrailNameException", awserr.ErrInvalidParameter)
	// ErrInvalidLookupAttributes is returned for an unknown LookupAttribute key.
	ErrInvalidLookupAttributes = awserr.New("InvalidLookupAttributesException", awserr.ErrInvalidParameter)
	// ErrInvalidMaxResults is returned when MaxResults is outside the operation's range.
	ErrInvalidMaxResults = awserr.New("InvalidMaxResultsException", awserr.ErrInvalidParameter)
	// ErrInvalidEventSelectors is returned for malformed basic event selectors.
	ErrInvalidEventSelectors = awserr.New("InvalidEventSelectorsException", awserr.ErrInvalidParameter)

	trailNameRe        = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?$`)
	adjacentSeparators = regexp.MustCompile(`[._-]{2}`)
	edsNameRe          = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)
)

// validateTrailName applies CreateTrail's documented naming rules (api_op_CreateTrail.go).
func validateTrailName(name string) error {
	if len(name) < minTrailNameLen || len(name) > maxTrailNameLen {
		return fmt.Errorf(
			"%w: trail name must be %d-%d characters",
			ErrInvalidTrailName,
			minTrailNameLen,
			maxTrailNameLen,
		)
	}

	if !trailNameRe.MatchString(name) {
		return fmt.Errorf(
			"%w: trail name may contain only letters, numbers, periods, underscores and dashes, "+
				"and must start and end with a letter or number", ErrInvalidTrailName,
		)
	}

	if adjacentSeparators.MatchString(name) {
		return fmt.Errorf(
			"%w: trail name must not contain adjacent periods, underscores or dashes",
			ErrInvalidTrailName,
		)
	}

	if _, err := netip.ParseAddr(name); err == nil {
		return fmt.Errorf("%w: trail name must not be formatted as an IP address", ErrInvalidTrailName)
	}

	return nil
}

func validateEventDataStoreName(name string) error {
	if len(name) < minEDSNameLen || len(name) > maxEDSNameLen || !edsNameRe.MatchString(name) {
		return fmt.Errorf(
			"%w: event data store name must be %d-%d characters of letters, numbers, periods, underscores and dashes",
			ErrValidation, minEDSNameLen, maxEDSNameLen,
		)
	}

	return nil
}

func validateRetentionPeriod(days int32) error {
	if days != 0 && (days < minRetentionDay || days > maxRetentionDay) {
		return fmt.Errorf(
			"%w: RetentionPeriod must be between %d and %d days",
			ErrValidation,
			minRetentionDay,
			maxRetentionDay,
		)
	}

	return nil
}

func validateLookupInput(attrs []LookupAttribute, maxResults int32) error {
	if maxResults < 0 || maxResults > maxLookupLimit {
		return fmt.Errorf("%w: MaxResults must be between 1 and %d", ErrInvalidMaxResults, maxLookupLimit)
	}

	for _, a := range attrs {
		if !slices.Contains(lookupAttributeKeys(), a.AttributeKey) {
			return fmt.Errorf("%w: invalid AttributeKey %q", ErrInvalidLookupAttributes, a.AttributeKey)
		}
	}

	return nil
}

func validateEventSelectors(selectors []EventSelector) error {
	if len(selectors) > maxEventSelects {
		return fmt.Errorf("%w: at most %d event selectors are allowed", ErrInvalidEventSelectors, maxEventSelects)
	}

	for _, s := range selectors {
		if !slices.Contains(readWriteTypes(), s.ReadWriteType) {
			return fmt.Errorf("%w: invalid ReadWriteType %q", ErrInvalidEventSelectors, s.ReadWriteType)
		}
	}

	return nil
}

// publicMessage drops the sentinel/code text that error wrapping prepends, so
// clients do not see "Code: Code: detail".
func publicMessage(err error) string {
	msg := err.Error()

	for e := errors.Unwrap(err); e != nil; e = errors.Unwrap(e) {
		msg = strings.TrimPrefix(msg, e.Error()+": ")
	}

	for _, m := range errorMappings {
		msg = strings.TrimPrefix(msg, m.code+": ")
	}

	return msg
}

func lookupAttributeKeys() []string {
	return strings.Fields("EventId EventName ReadOnly Username ResourceType ResourceName EventSource AccessKeyId")
}

func readWriteTypes() []string { return strings.Fields("All ReadOnly WriteOnly") }
