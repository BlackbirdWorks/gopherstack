package s3

// Exported wrappers for internal functions used in tests.

// DetailTypeFromEventName exposes detailTypeFromEventName for external tests.
func DetailTypeFromEventName(eventName string) string {
	return detailTypeFromEventName(eventName)
}

// ReasonFromEventName exposes reasonFromEventName for external tests.
func ReasonFromEventName(eventName string) string {
	return reasonFromEventName(eventName)
}
