package glue

// GlueResourceNameForTest exposes glueResourceName for unit tests.
func GlueResourceNameForTest(resourceARN, resourceType string) string {
	return glueResourceName(resourceARN, resourceType)
}
