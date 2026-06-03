package cloudwatch

import (
	"time"
)

// MetricDatum holds a single metric data point.
type MetricDatum struct {
	MetricName        string      `json:"MetricName"`
	Namespace         string      `json:"Namespace"`
	Unit              string      `json:"Unit,omitempty"`
	Timestamp         time.Time   `json:"Timestamp"`
	Dimensions        []Dimension `json:"Dimensions,omitempty"`
	Value             float64     `json:"Value"`
	Count             float64     `json:"SampleCount"`
	Sum               float64     `json:"Sum"`
	Min               float64     `json:"Min"`
	Max               float64     `json:"Max"`
	StorageResolution int32       `json:"StorageResolution,omitempty"`
	// HasStatisticSet is true when the datum was built from a StatisticValues input (not from Value).
	// Used to enforce mutual exclusion of Value + StatisticSet at the handler level.
	HasStatisticSet bool `json:"-"`
}

// UnprocessedMetricDatum describes a MetricDatum entry that could not be stored.
type UnprocessedMetricDatum struct {
	MetricName   string `json:"MetricName"`
	ErrorCode    string `json:"ErrorCode"`
	ErrorMessage string `json:"ErrorMessage"`
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
	CreatedAt                          time.Time   `json:"AlarmCreatedAt"`
	StateTransitionedTimestamp         time.Time   `json:"StateTransitionedTimestamp"`
	AlarmConfigurationUpdatedTimestamp time.Time   `json:"AlarmConfigurationUpdatedTimestamp"`
	StateValue                         string      `json:"StateValue"`
	Namespace                          string      `json:"Namespace"`
	MetricName                         string      `json:"MetricName"`
	ComparisonOperator                 string      `json:"ComparisonOperator"`
	Statistic                          string      `json:"Statistic"`
	ExtendedStatistic                  string      `json:"ExtendedStatistic,omitempty"`
	TreatMissingData                   string      `json:"TreatMissingData,omitempty"`
	AlarmName                          string      `json:"AlarmName"`
	StateReason                        string      `json:"StateReason,omitempty"`
	StateReasonData                    string      `json:"StateReasonData,omitempty"`
	AlarmDescription                   string      `json:"AlarmDescription,omitempty"`
	AlarmArn                           string      `json:"AlarmArn"`
	AlarmActions                       []string    `json:"AlarmActions,omitempty"`
	OKActions                          []string    `json:"OKActions,omitempty"`
	InsufficientDataActions            []string    `json:"InsufficientDataActions,omitempty"`
	Dimensions                         []Dimension `json:"Dimensions,omitempty"`
	Threshold                          float64     `json:"Threshold"`
	EvaluationPeriods                  int32       `json:"EvaluationPeriods"`
	DatapointsToAlarm                  int32       `json:"DatapointsToAlarm,omitempty"`
	Period                             int32       `json:"Period"`
	ActionsEnabled                     bool        `json:"ActionsEnabled"`
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

// MetricFilter represents a CloudWatch Logs metric filter.
type MetricFilter struct {
	CreationTime          time.Time              `json:"CreationTime"`
	FilterName            string                 `json:"FilterName"`
	LogGroupName          string                 `json:"LogGroupName"`
	FilterPattern         string                 `json:"FilterPattern"`
	MetricTransformations []MetricTransformation `json:"MetricTransformations,omitempty"`
}

// MetricTransformation describes how to map matched log events to a metric.
type MetricTransformation struct {
	MetricName      string  `json:"MetricName"`
	MetricNamespace string  `json:"MetricNamespace"`
	MetricValue     string  `json:"MetricValue"`
	Unit            string  `json:"Unit,omitempty"`
	DefaultValue    float64 `json:"DefaultValue,omitempty"`
}

// MetricDataResult is a single result entry in a GetMetricData response.
type MetricDataResult struct {
	Timestamps []time.Time `json:"Timestamps"`
	ID         string      `json:"Id"`
	Label      string      `json:"Label,omitempty"`
	StatusCode string      `json:"StatusCode"`
	Values     []float64   `json:"Values"`
}

// DashboardEntry represents a single CloudWatch dashboard summary entry returned by ListDashboards.
type DashboardEntry struct {
	LastModified  time.Time `json:"LastModified"`
	DashboardArn  string    `json:"DashboardArn"`
	DashboardName string    `json:"DashboardName"`
	Size          int64     `json:"Size"`
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
