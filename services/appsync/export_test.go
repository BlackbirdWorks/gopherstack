// Package appsync exposes unexported functions for use in external test packages.
// These exports exist solely for testing and must not be called from production code.
package appsync

// RenderVTL is the exported test hook for the VTL template renderer.
// It is only exposed for package-level tests and should not be used in production code.
func RenderVTL(tmpl string, args map[string]any, result any) (string, error) {
	return renderVTL(tmpl, args, result)
}

// ToDynamoDBJSON is the exported test hook for the DynamoDB JSON formatter.
// It is only exposed for package-level tests and should not be used in production code.
func ToDynamoDBJSON(val any) string {
	return toDynamoDBJSON(val)
}
