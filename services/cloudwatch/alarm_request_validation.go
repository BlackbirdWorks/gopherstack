package cloudwatch

import (
	"regexp"
	"slices"
)

const cwMaxAlarmNameLen = 255

var percentileStat = regexp.MustCompile(`^p(100|\d{1,2}(\.\d{0,2})?)$`)

// validateMetricAlarmRequest checks request-shape rules the PutMetricAlarm API
// enforces before any state is touched; the backend stays lenient for internal callers.
func validateMetricAlarmRequest(a *MetricAlarm) error {
	if len(a.AlarmName) > cwMaxAlarmNameLen {
		return apiErrorf(ErrValidationFault,
			"1 validation error detected: Value '%s' at 'alarmName' failed to satisfy constraint: "+
				"Member must have length less than or equal to %d", a.AlarmName, cwMaxAlarmNameLen)
	}

	if err := checkEnum("comparisonOperator", a.ComparisonOperator,
		"GreaterThanOrEqualToThreshold", "GreaterThanThreshold", "LessThanThreshold",
		"LessThanOrEqualToThreshold", "LessThanLowerOrGreaterThanUpperThreshold",
		"LessThanLowerThreshold", "GreaterThanUpperThreshold"); err != nil {
		return err
	}

	if err := checkEnum("statistic", a.Statistic,
		"SampleCount", "Average", "Sum", "Minimum", "Maximum"); err != nil {
		return err
	}

	if err := checkEnum("treatMissingData", a.TreatMissingData,
		"breaching", "notBreaching", "ignore", "missing"); err != nil {
		return err
	}

	if a.ExtendedStatistic != "" && a.ExtendedStatistic[0] == 'p' && !percentileStat.MatchString(a.ExtendedStatistic) {
		return apiErrorf(ErrValidation, "The value %s for parameter ExtendedStatistic is invalid.", a.ExtendedStatistic)
	}

	if len(a.Metrics) == 0 && a.Period > 0 && a.Period%60 != 0 && !slices.Contains([]int32{10, 20, 30}, a.Period) {
		return apiErrorf(ErrValidation, "Period must be 10, 20, 30, or a multiple of 60 seconds, got %d", a.Period)
	}

	return nil
}
