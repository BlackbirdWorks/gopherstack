package cloudwatch

import (
	"errors"
)

const (
	errResourceNotFoundException = "ResourceNotFoundException"
)

// ErrAlarmNotFound is returned when a requested alarm does not exist.
var ErrAlarmNotFound = errors.New(errResourceNotFoundException)

// ErrAlarmNameRequired is returned when an alarm name is missing.
var ErrAlarmNameRequired = errors.New("AlarmName is required")

// ErrAlarmRuleRequired is returned when a composite alarm rule is missing.
var ErrAlarmRuleRequired = errors.New("AlarmRule is required")

// ErrDashboardNotFound is returned when a requested dashboard does not exist.
var ErrDashboardNotFound = errors.New(errResourceNotFoundException)

// ErrDashboardNameRequired is returned when a dashboard name is missing.
var ErrDashboardNameRequired = errors.New("DashboardName is required")

// ErrAlarmMuteRuleNotFound is returned when a requested alarm mute rule does not exist.
var ErrAlarmMuteRuleNotFound = errors.New(errResourceNotFoundException)

// ErrAnomalyDetectorNotFound is returned when a requested anomaly detector does not exist.
var ErrAnomalyDetectorNotFound = errors.New(errResourceNotFoundException)

// ErrMetricStreamNotFound is returned when a requested metric stream does not exist.
var ErrMetricStreamNotFound = errors.New(errResourceNotFoundException)

// ErrInsightRuleNotFound is returned when a requested insight rule does not exist.
var ErrInsightRuleNotFound = errors.New(errResourceNotFoundException)

// ErrValidation is returned when a caller provides an invalid or missing parameter.
var ErrValidation = errors.New("InvalidParameterValue")

// ErrInvalidNextToken is returned when a pagination NextToken is invalid or expired.
var ErrInvalidNextToken = errors.New("InvalidParameterValue: NextToken is invalid or expired")

// ErrValueAndStatisticSet is returned when more than one of Value, StatisticSet,
// and the Values/Counts array are set on a MetricDatum. AWS treats all three
// input shapes as mutually exclusive.
var ErrValueAndStatisticSet = errors.New(
	"InvalidParameterCombination: Value, StatisticValues, and Values/Counts are mutually exclusive",
)

// ErrValuesCountsLengthMismatch is returned when a MetricDatum's Counts array is
// present but does not have the same number of elements as its Values array.
var ErrValuesCountsLengthMismatch = errors.New(
	"InvalidParameterValue: Values and Counts arrays must be the same length",
)

// ErrTooManyValues is returned when a MetricDatum's Values array exceeds the
// 150-unique-value limit documented for PutMetricData.
var ErrTooManyValues = errors.New(
	"InvalidParameterValue: the maximum values array size is 150",
)

// ErrInvalidMetricValue is returned when a metric value (Value, Sum, Min, Max, or
// an entry of the Values array) is NaN, +/-Infinity, or outside the documented
// -2^360..2^360 range that CloudWatch accepts.
var ErrInvalidMetricValue = errors.New(
	"InvalidParameterValue: metric values must be finite and within -2^360 to 2^360",
)

// ErrMetricSeriesLimitExceeded is returned when storing a batch of MetricDatum
// entries would push the namespace or account past its distinct-time-series cap.
var ErrMetricSeriesLimitExceeded = errors.New("LimitExceeded: metric series limit reached")

// ErrMetricTimestampOutOfRange is returned when a MetricDatum's Timestamp falls
// outside CloudWatch's documented PutMetricData acceptance window: no more than
// cwMetricTimestampPastWindow in the past, and no more than
// cwMetricTimestampFutureWindow in the future, relative to now.
var ErrMetricTimestampOutOfRange = errors.New(
	"InvalidParameterValue: Timestamp must not be more than two weeks before or " +
		"two hours after the current time",
)
