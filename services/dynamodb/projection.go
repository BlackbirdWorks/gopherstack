// Package dynamodb implements the AWS DynamoDB mock service.
// projection.go validates and resolves ProjectionExpression / AttributesToGet
// parameters shared by GetItem, BatchGetItem, Query, and Scan.
package dynamodb

import (
	"fmt"
	"maps"
	"strings"
)

// validateProjectionParams returns an error when both ProjectionExpression and
// AttributesToGet are supplied (AWS rejects this combination).
func validateProjectionParams(projectionExpr string, attributesToGet []string) error {
	if projectionExpr != "" && len(attributesToGet) > 0 {
		return NewValidationException(
			"Cannot specify both AttributesToGet and ProjectionExpression",
		)
	}

	return nil
}

// resolveProjection returns the effective projection expression string,
// falling back to AttributesToGet when ProjectionExpression is empty, plus
// any synthetic expression attribute name aliases it introduced (nil when
// projectionExpr was used directly, or when there's nothing to project).
//
// AttributesToGet is a plain list of attribute names, not an expression --
// AWS never subjects it to expression-only restrictions such as the
// reserved-word check, so each entry is aliased as a whole (never reused as a
// bare path segment) before being handed to the ProjectionExpression parser.
func resolveProjection(projectionExpr string, attributesToGet []string) (string, map[string]string) {
	if projectionExpr != "" {
		return projectionExpr, nil
	}

	if len(attributesToGet) == 0 {
		return "", nil
	}

	aliases := make(map[string]string, len(attributesToGet))
	parts := make([]string, len(attributesToGet))

	for i, name := range attributesToGet {
		alias := fmt.Sprintf("#atg%d", i)
		aliases[alias] = name
		parts[i] = alias
	}

	return strings.Join(parts, ","), aliases
}

// mergeAttrNames returns a map containing every entry of both base and extra.
// extra is applied second so its aliases win on key collision (extra is
// always the synthetic AttributesToGet aliases resolveProjection introduces,
// which are never valid #-prefixed request input, so no real collision can
// happen in practice). A nil result is returned only when both inputs are empty.
func mergeAttrNames(base, extra map[string]string) map[string]string {
	if len(base) == 0 {
		return extra
	}

	if len(extra) == 0 {
		return base
	}

	merged := make(map[string]string, len(base)+len(extra))
	maps.Copy(merged, base)
	maps.Copy(merged, extra)

	return merged
}
