package resourcegroups

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
)

const (
	resourceQueryCFNStack = "CLOUDFORMATION_STACK_1_0"
	allSupportedType      = "AWS::AllSupported"
	queryMaxLen           = 4096
	tagFilterKeyMaxLen    = 128
)

var resourceTypeFilterRe = regexp.MustCompile(
	`^AWS::[A-Za-z0-9]+::[A-Za-z0-9]+$`,
)

// validateQueryShape checks the decoded JSON body of ResourceQuery.Query against the
// documented per-type shape. Unknown members are tolerated.
func validateQueryShape(queryType, query string) error {
	if len(query) > queryMaxLen {
		return fmt.Errorf("%w: ResourceQuery.Query must be at most %d characters", ErrValidation, queryMaxLen)
	}

	trimmed := bytes.TrimSpace([]byte(query))
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return fmt.Errorf("%w: ResourceQuery.Query must be a JSON object", ErrValidation)
	}

	switch queryType {
	case resourceQueryTagFilters:
		return validateTagFilterQuery(trimmed)
	case resourceQueryCFNStack:
		return validateStackQuery(trimmed)
	}

	return nil
}

func validateTagFilterQuery(raw []byte) error {
	var q tagFilterQuery
	if err := json.Unmarshal(raw, &q); err != nil {
		return fmt.Errorf("%w: ResourceQuery.Query is not a valid TAG_FILTERS_1_0 query: %w", ErrValidation, err)
	}

	if err := validateResourceTypeFilters(q.ResourceTypeFilters); err != nil {
		return err
	}

	for _, f := range q.TagFilters {
		if f.Key == "" || len(f.Key) > tagFilterKeyMaxLen {
			return fmt.Errorf(
				"%w: TagFilters Key must be between 1 and %d characters", ErrValidation, tagFilterKeyMaxLen)
		}
	}

	return nil
}

func validateStackQuery(raw []byte) error {
	var q cfnQuery
	if err := json.Unmarshal(raw, &q); err != nil {
		return fmt.Errorf(
			"%w: ResourceQuery.Query is not a valid CLOUDFORMATION_STACK_1_0 query: %w", ErrValidation, err)
	}

	if q.StackIdentifier == "" {
		return fmt.Errorf("%w: StackIdentifier is required for a CLOUDFORMATION_STACK_1_0 query", ErrValidation)
	}

	return validateResourceTypeFilters(q.ResourceTypeFilters)
}

func validateResourceTypeFilters(filters []string) error {
	for _, rt := range filters {
		if rt != allSupportedType && !resourceTypeFilterRe.MatchString(rt) {
			return fmt.Errorf("%w: ResourceTypeFilters value %q is not a valid resource type", ErrValidation, rt)
		}
	}

	return nil
}
