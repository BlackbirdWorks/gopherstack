package cloudwatch

import (
	"slices"
	"time"
)

const cwMaxStatisticsDatapoints = 1440

var validPeriodsSubMinute = []int32{1, 5, 10, 20, 30} //nolint:gochecknoglobals // immutable lookup

func validateGetMetricStatisticsRequest(
	startTime, endTime time.Time,
	period int32,
	statistics, extendedStatistics []string,
) error {
	if !startTime.Before(endTime) {
		return apiErrorf(ErrValidation, "The parameter StartTime must be less than the parameter EndTime.")
	}

	if period <= 0 || (period%60 != 0 && !slices.Contains(validPeriodsSubMinute, period)) {
		return apiErrorf(ErrValidation, "Period must be 1, 5, 10, 20, 30, or a multiple of 60 seconds, got %d", period)
	}

	if len(statistics) == 0 && len(extendedStatistics) == 0 {
		return apiErrorf(ErrParamCombination,
			"At least one of the parameters Statistics, ExtendedStatistics must be specified.")
	}

	for _, s := range statistics {
		if err := checkEnum("statistics", s, "SampleCount", "Average", "Sum", "Minimum", "Maximum"); err != nil {
			return err
		}
	}

	if n := int64(endTime.Sub(startTime) / (time.Duration(period) * time.Second)); n > cwMaxStatisticsDatapoints {
		return apiErrorf(ErrParamCombination,
			"You have requested up to %d datapoints, which exceeds the limit of %d. "+
				"You may reduce the datapoints requested by increasing Period, or decreasing the time range.",
			n, cwMaxStatisticsDatapoints)
	}

	return nil
}

const (
	cwMaxDimensionsPerDatum = 30
	cwMaxNameLen            = 255
)

func validateDatumIdentity(d MetricDatum) error {
	if len(d.MetricName) > cwMaxNameLen {
		return apiErrorf(ErrValidation, "The value %s for parameter MetricName is invalid.", d.MetricName[:32]+"...")
	}

	if !validStandardUnit(d.Unit) {
		return apiErrorf(ErrValidation, "The parameter MetricData.member.Unit must be a valid unit, got %q.", d.Unit)
	}

	if len(d.Dimensions) > cwMaxDimensionsPerDatum {
		return apiErrorf(ErrValidation,
			"The collection Dimensions must not have a size greater than %d.", cwMaxDimensionsPerDatum)
	}

	for _, dim := range d.Dimensions {
		if len(dim.Name) > cwMaxNameLen || len(dim.Value) > cwMaxNameLen {
			return apiErrorf(ErrValidation, "Dimension %q name or value exceeds %d characters.", dim.Name, cwMaxNameLen)
		}
	}

	return nil
}
