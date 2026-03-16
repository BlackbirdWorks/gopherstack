package cloudwatch

// EvaluateAlarmRuleForTest is a test-visible wrapper around evaluateAlarmRule.
func EvaluateAlarmRuleForTest(rule string, states map[string]string) string {
	return evaluateAlarmRule(rule, func(name string) string {
		if s, ok := states[name]; ok {
			return s
		}

		return alarmStateInsufficientData
	})
}

// CwMaxMetricNamesPerNamespace exposes the constant for tests.
const CwMaxMetricNamesPerNamespace = cwMaxMetricNamesPerNamespace

// CwMetricRetentionDays exposes the constant for tests.
const CwMetricRetentionDays = cwMetricRetentionDays
