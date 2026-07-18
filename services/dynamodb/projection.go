// Package dynamodb implements the AWS DynamoDB mock service.
// projection.go validates and resolves ProjectionExpression / AttributesToGet
// parameters shared by GetItem, BatchGetItem, Query, and Scan.
package dynamodb

import "strings"

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

// resolveProjection returns the effective projection string, falling back to
// a comma-separated AttributesToGet list when ProjectionExpression is empty.
func resolveProjection(projectionExpr string, attributesToGet []string) string {
	if projectionExpr != "" {
		return projectionExpr
	}

	if len(attributesToGet) == 0 {
		return ""
	}

	return strings.Join(attributesToGet, ",")
}
