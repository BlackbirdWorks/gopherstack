package cloudwatchlogs

// MaxEventsPerStream exposes the per-stream event cap for use in tests.
const MaxEventsPerStream = maxEventsPerStream

// FilterPatternMatches exposes the filter pattern matching function for use in tests.
func FilterPatternMatches(pattern, message string) bool {
	return filterPatternMatches(pattern, message)
}
