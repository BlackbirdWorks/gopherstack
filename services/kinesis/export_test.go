package kinesis

// ParseThrottlePercentageForTest exposes parseThrottlePercentage for unit tests.
func ParseThrottlePercentageForTest(s string) float64 {
	return parseThrottlePercentage(s)
}
