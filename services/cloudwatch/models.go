package cloudwatch

import "time"

// MetricDatum holds a single metric data point.
type MetricDatum struct {
	Timestamp  time.Time `json:"Timestamp"`
	MetricName string    `json:"MetricName"`
	Namespace  string    `json:"Namespace"`
	Unit       string    `json:"Unit,omitempty"`
	Value      float64   `json:"Value"`
	Count      float64   `json:"SampleCount"`
	Sum        float64   `json:"Sum"`
	Min        float64   `json:"Min"`
	Max        float64   `json:"Max"`
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
	Timestamp   time.Time `json:"Timestamp"`
	Average     *float64  `json:"Average,omitempty"`
	Sum         *float64  `json:"Sum,omitempty"`
	Minimum     *float64  `json:"Minimum,omitempty"`
	Maximum     *float64  `json:"Maximum,omitempty"`
	SampleCount *float64  `json:"SampleCount,omitempty"`
	Unit        string    `json:"Unit,omitempty"`
}

// MetricAlarm represents a CloudWatch metric alarm.
type MetricAlarm struct {
	CreatedAt               time.Time `json:"AlarmCreatedAt"`
	StateValue              string    `json:"StateValue"`
	Namespace               string    `json:"Namespace"`
	MetricName              string    `json:"MetricName"`
	ComparisonOperator      string    `json:"ComparisonOperator"`
	Statistic               string    `json:"Statistic"`
	AlarmName               string    `json:"AlarmName"`
	StateReason             string    `json:"StateReason,omitempty"`
	AlarmDescription        string    `json:"AlarmDescription,omitempty"`
	AlarmArn                string    `json:"AlarmArn"`
	AlarmActions            []string  `json:"AlarmActions,omitempty"`
	OKActions               []string  `json:"OKActions,omitempty"`
	InsufficientDataActions []string  `json:"InsufficientDataActions,omitempty"`
	Threshold               float64   `json:"Threshold"`
	EvaluationPeriods       int32     `json:"EvaluationPeriods"`
	Period                  int32     `json:"Period"`
	ActionsEnabled          bool      `json:"ActionsEnabled"`
}

// CompositeAlarm represents a CloudWatch composite alarm that combines child alarms.
type CompositeAlarm struct {
	CreatedAt               time.Time `json:"AlarmCreatedAt"`
	StateValue              string    `json:"StateValue"`
	AlarmName               string    `json:"AlarmName"`
	AlarmRule               string    `json:"AlarmRule"`
	AlarmDescription        string    `json:"AlarmDescription,omitempty"`
	AlarmArn                string    `json:"AlarmArn"`
	StateReason             string    `json:"StateReason,omitempty"`
	AlarmActions            []string  `json:"AlarmActions,omitempty"`
	OKActions               []string  `json:"OKActions,omitempty"`
	InsufficientDataActions []string  `json:"InsufficientDataActions,omitempty"`
	ActionsEnabled          bool      `json:"ActionsEnabled"`
}

// AlarmHistoryItem represents a single history entry for an alarm.
type AlarmHistoryItem struct {
	Timestamp       time.Time `json:"Timestamp"`
	AlarmName       string    `json:"AlarmName"`
	HistoryItemType string    `json:"HistoryItemType"`
	HistorySummary  string    `json:"HistorySummary"`
	HistoryData     string    `json:"HistoryData,omitempty"`
}

// MetricStat specifies a metric and statistic for a MetricDataQuery.
type MetricStat struct {
	Namespace  string `json:"Namespace"`
	MetricName string `json:"MetricName"`
	Stat       string `json:"Stat"`
	Period     int32  `json:"Period"`
}

// MetricDataQuery is a single query in a GetMetricData request.
type MetricDataQuery struct {
	ID         string     `json:"Id"`
	Label      string     `json:"Label,omitempty"`
	MetricStat MetricStat `json:"MetricStat"`
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
