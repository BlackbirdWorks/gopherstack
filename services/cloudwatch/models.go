package cloudwatch

import (
	"time"
)

// MetricDatum holds a single metric data point.
//
// A datum is built from exactly one of three mutually-exclusive input shapes:
// a single Value, a pre-aggregated StatisticValues set, or a Values/Counts
// array pair (up to 150 unique values, each with an occurrence count). The
// Has* flags record which shape was supplied on the wire so validation can
// enforce mutual exclusion by presence rather than by zero-value guessing.
type MetricDatum struct {
	Timestamp  time.Time   `json:"Timestamp"`
	MetricName string      `json:"MetricName"`
	Namespace  string      `json:"Namespace"`
	Unit       string      `json:"Unit,omitempty"`
	Dimensions []Dimension `json:"Dimensions,omitempty"`
	// Values and Counts implement the PutMetricData "array of values" input: each
	// Values[i] occurred Counts[i] times during the period. Populated only when
	// HasValuesArray is true; Counts is defaulted to all-1s by the parser when the
	// caller omits it.
	Values []float64 `json:"Values,omitempty"`
	Counts []float64 `json:"Counts,omitempty"`
	Count  float64   `json:"SampleCount"`
	Value  float64   `json:"Value"`
	Sum    float64   `json:"Sum"`
	Min    float64   `json:"Min"`
	Max    float64   `json:"Max"`
	// StorageResolution is 1 (high-resolution) or 60 (standard); 0 means "use default".
	StorageResolution int32 `json:"StorageResolution,omitempty"`
	// HasStatisticSet is true when the datum was built from a StatisticValues input.
	HasStatisticSet bool `json:"-"`
	// HasValuesArray is true when the datum was built from a Values/Counts input.
	HasValuesArray bool `json:"-"`
	// HasValue is true when the caller explicitly supplied the Value field
	// (independent of whether that value is the float zero value).
	HasValue bool `json:"-"`
}

// Metric represents a named metric (name+namespace+dimensions).
type Metric struct {
	Namespace  string      `json:"Namespace"`
	MetricName string      `json:"MetricName"`
	Dimensions []Dimension `json:"Dimensions,omitempty"`
}

// Dimension is a key-value pair for a metric.
type Dimension struct {
	Name  string `json:"Name"`
	Value string `json:"Value"`
}

// Datapoint holds aggregated stats for GetMetricStatistics.
type Datapoint struct {
	Average            *float64           `json:"Average,omitempty"`
	Sum                *float64           `json:"Sum,omitempty"`
	Minimum            *float64           `json:"Minimum,omitempty"`
	Maximum            *float64           `json:"Maximum,omitempty"`
	SampleCount        *float64           `json:"SampleCount,omitempty"`
	ExtendedStatistics map[string]float64 `json:"ExtendedStatistics,omitempty"`
	// BandLower and BandUpper are the anomaly detection band boundaries for this point.
	// Only populated when a matching AnomalyDetector exists for the metric.
	BandLower *float64  `json:"BandLower,omitempty"`
	BandUpper *float64  `json:"BandUpper,omitempty"`
	Timestamp time.Time `json:"Timestamp"`
	Unit      string    `json:"Unit,omitempty"`
}

// MetricAlarm represents a CloudWatch metric alarm.
type MetricAlarm struct {
	CreatedAt                          time.Time `json:"AlarmCreatedAt"`
	StateTransitionedTimestamp         time.Time `json:"StateTransitionedTimestamp"`
	AlarmConfigurationUpdatedTimestamp time.Time `json:"AlarmConfigurationUpdatedTimestamp"`
	StateValue                         string    `json:"StateValue"`
	Namespace                          string    `json:"Namespace"`
	MetricName                         string    `json:"MetricName"`
	ComparisonOperator                 string    `json:"ComparisonOperator"`
	Statistic                          string    `json:"Statistic"`
	ExtendedStatistic                  string    `json:"ExtendedStatistic,omitempty"`
	TreatMissingData                   string    `json:"TreatMissingData,omitempty"`
	AlarmName                          string    `json:"AlarmName"`
	StateReason                        string    `json:"StateReason,omitempty"`
	StateReasonData                    string    `json:"StateReasonData,omitempty"`
	AlarmDescription                   string    `json:"AlarmDescription,omitempty"`
	AlarmArn                           string    `json:"AlarmArn"`
	// ThresholdMetricID references the MetricDataQuery ID whose result is used as the
	// dynamic threshold (anomaly band) for GreaterThanUpperThreshold and
	// LessThanLowerOrGreaterThanUpperThreshold comparison operators.
	ThresholdMetricID       string      `json:"ThresholdMetricId,omitempty"`
	AlarmActions            []string    `json:"AlarmActions,omitempty"`
	OKActions               []string    `json:"OKActions,omitempty"`
	InsufficientDataActions []string    `json:"InsufficientDataActions,omitempty"`
	Dimensions              []Dimension `json:"Dimensions,omitempty"`
	// Metrics holds the MetricDataQuery list for multi-metric / metric-math alarms.
	// When set, MetricName/Namespace/Statistic/Period/Dimensions are ignored for evaluation.
	Metrics           []MetricDataQuery `json:"Metrics,omitempty"`
	Threshold         float64           `json:"Threshold"`
	EvaluationPeriods int32             `json:"EvaluationPeriods"`
	DatapointsToAlarm int32             `json:"DatapointsToAlarm,omitempty"`
	Period            int32             `json:"Period"`
	ActionsEnabled    bool              `json:"ActionsEnabled"`
}

// CompositeAlarm represents a CloudWatch composite alarm that combines child alarms.
type CompositeAlarm struct {
	CreatedAt                  time.Time `json:"AlarmCreatedAt"`
	StateTransitionedTimestamp time.Time `json:"StateTransitionedTimestamp"`
	StateValue                 string    `json:"StateValue"`
	AlarmName                  string    `json:"AlarmName"`
	AlarmRule                  string    `json:"AlarmRule"`
	AlarmDescription           string    `json:"AlarmDescription,omitempty"`
	AlarmArn                   string    `json:"AlarmArn"`
	StateReason                string    `json:"StateReason,omitempty"`
	AlarmActions               []string  `json:"AlarmActions,omitempty"`
	OKActions                  []string  `json:"OKActions,omitempty"`
	InsufficientDataActions    []string  `json:"InsufficientDataActions,omitempty"`
	ActionsEnabled             bool      `json:"ActionsEnabled"`
}

// AlarmHistoryItem represents a single history entry for an alarm.
type AlarmHistoryItem struct {
	Timestamp       time.Time `json:"Timestamp"`
	AlarmName       string    `json:"AlarmName"`
	AlarmType       string    `json:"AlarmType,omitempty"`
	HistoryItemType string    `json:"HistoryItemType"`
	HistorySummary  string    `json:"HistorySummary"`
	HistoryData     string    `json:"HistoryData,omitempty"`
}

// MetricStat specifies a metric and statistic for a MetricDataQuery.
type MetricStat struct {
	Namespace  string      `json:"Namespace"`
	MetricName string      `json:"MetricName"`
	Stat       string      `json:"Stat"`
	Dimensions []Dimension `json:"Dimensions,omitempty"`
	Period     int32       `json:"Period"`
}

// MetricDataQuery is a single query in a GetMetricData request.
// Expression supports metric math (e.g. "m1+m2"). When non-empty, MetricStat is ignored.
// AccountId, when set to an account other than the local account, causes the query to return
// empty data (cross-account metrics are not supported locally but must not error).
// ReturnData controls whether the query result is included in the response.
type MetricDataQuery struct {
	ID         string     `json:"Id"`
	Label      string     `json:"Label,omitempty"`
	Expression string     `json:"Expression,omitempty"`
	AccountID  string     `json:"AccountId,omitempty"`
	MetricStat MetricStat `json:"MetricStat"`
	Period     int32      `json:"Period,omitempty"`
	ReturnData bool       `json:"ReturnData"`
}

// MetricDataMessage is a diagnostic message returned by GetMetricData, either
// at the top level of the response or attached to an individual MetricDataResult.
// Code is a short machine-readable identifier (e.g. "ArithmeticError",
// "Forbidden", "MaxDatapointsExceeded"); Value is the human-readable text.
type MetricDataMessage struct {
	Code  string `json:"Code"`
	Value string `json:"Value"`
}

// MetricDataResult is a single result entry in a GetMetricData response.
// StatusCode is "Complete", "PartialData", "InternalError", or "Forbidden".
// Messages carries per-result diagnostics (e.g. arithmetic errors in metric math).
type MetricDataResult struct {
	Timestamps []time.Time         `json:"Timestamps"`
	ID         string              `json:"Id"`
	Label      string              `json:"Label,omitempty"`
	StatusCode string              `json:"StatusCode"`
	Values     []float64           `json:"Values"`
	Messages   []MetricDataMessage `json:"Messages,omitempty"`
}

// GetMetricDataPage is the paginated result of a GetMetricData request.
// NextToken is non-empty when the response was truncated to honour MaxDatapoints.
// Messages carries top-level diagnostics (e.g. a truncation notice).
type GetMetricDataPage struct {
	NextToken string
	Results   []MetricDataResult
	Messages  []MetricDataMessage
}

// DashboardEntry represents a single CloudWatch dashboard summary entry returned by ListDashboards.
type DashboardEntry struct {
	LastModified  time.Time `json:"LastModified"`
	DashboardArn  string    `json:"DashboardArn"`
	DashboardName string    `json:"DashboardName"`
	Size          int64     `json:"Size"`
}

// DashboardValidationMessage mirrors aws-sdk-go-v2's
// types.DashboardValidationMessage (DataPath + Message only on the wire).
// IsError is emulator-internal bookkeeping (never serialized): when any message
// in a PutDashboard validation pass has IsError set, the whole call fails and the
// dashboard body is not persisted (matches AWS's documented "if this result
// includes error messages, the input was not valid and the operation failed" /
// "if this result includes only warning messages, the dashboard was still
// created or modified" distinction, which the response shape itself does not
// carry directly — it's inferred from whether the API call succeeded at all).
type DashboardValidationMessage struct {
	DataPath string `json:"DataPath,omitempty"`
	Message  string `json:"Message"`
	IsError  bool   `json:"-"`
}

// AnomalyDetector represents a CloudWatch anomaly detector.
type AnomalyDetector struct {
	Namespace  string      `json:"Namespace"`
	MetricName string      `json:"MetricName"`
	Stat       string      `json:"Stat"`
	StateValue string      `json:"StateValue"`
	Dimensions []Dimension `json:"Dimensions,omitempty"`
	BandWidth  float64     `json:"BandWidth,omitempty"`
}

// InsightRule represents a CloudWatch Contributor Insights rule.
type InsightRule struct {
	CreatedAt   time.Time `json:"CreatedAt"`
	Name        string    `json:"Name"`
	State       string    `json:"State"`
	Schema      string    `json:"Schema"`
	Definition  string    `json:"Definition"`
	Arn         string    `json:"RuleArn"`
	ManagedRule bool      `json:"ManagedRule"`
}

// MetricStreamFilter specifies a namespace-level include/exclude filter for a metric stream.
type MetricStreamFilter struct {
	Namespace   string   `json:"Namespace"`
	MetricNames []string `json:"MetricNames,omitempty"`
}

// MetricStream represents a CloudWatch metric stream.
type MetricStream struct {
	CreationDate   time.Time            `json:"CreationDate"`
	LastUpdateDate time.Time            `json:"LastUpdateDate"`
	Name           string               `json:"Name"`
	FirehoseArn    string               `json:"FirehoseArn"`
	RoleArn        string               `json:"RoleArn"`
	OutputFormat   string               `json:"OutputFormat"`
	State          string               `json:"State"`
	Arn            string               `json:"Arn"`
	IncludeFilters []MetricStreamFilter `json:"IncludeFilters,omitempty"`
	ExcludeFilters []MetricStreamFilter `json:"ExcludeFilters,omitempty"`
}

// AlarmMuteRule represents a CloudWatch alarm mute rule.
type AlarmMuteRule struct {
	CreationTime  time.Time `json:"CreationTime"`
	MuteStartTime time.Time `json:"MuteStartTime"`
	MuteName      string    `json:"MuteName"`
	Description   string    `json:"Description,omitempty"`
	AlarmNames    []string  `json:"AlarmNames,omitempty"`
	MuteDuration  int32     `json:"MuteDuration"`
}

// AlarmContributor represents a single contributor returned by DescribeAlarmContributors.
type AlarmContributor struct {
	Keys []string `json:"Keys"`
	Sum  float64  `json:"Sum"`
}

// InsightRuleFailure represents a failed rule in batch insight rule operations.
type InsightRuleFailure struct {
	RuleName           string `json:"RuleName"`
	FailureCode        string `json:"FailureCode"`
	FailureDescription string `json:"FailureDescription,omitempty"`
}

// metricRecord holds time-series data for a single (MetricName, Dimensions) combination.
type metricRecord struct {
	MetricName string        `json:"MetricName"`
	Dimensions []Dimension   `json:"Dimensions,omitempty"`
	Points     []MetricDatum `json:"Points"`
}

// dashboardRecord holds dashboard body and metadata.
type dashboardRecord struct {
	LastModified time.Time `json:"LastModified"`
	Name         string    `json:"Name"`
	Body         string    `json:"Body"`
}
