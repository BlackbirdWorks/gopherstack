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
	Unit        string    `json:"Unit,omitempty"`
	Average     *float64  `json:"Average,omitempty"`
	Sum         *float64  `json:"Sum,omitempty"`
	Minimum     *float64  `json:"Minimum,omitempty"`
	Maximum     *float64  `json:"Maximum,omitempty"`
	SampleCount *float64  `json:"SampleCount,omitempty"`
}

// MetricAlarm represents a CloudWatch alarm.
type MetricAlarm struct {
	AlarmName          string    `json:"AlarmName"`
	AlarmArn           string    `json:"AlarmArn"`
	Namespace          string    `json:"Namespace"`
	MetricName         string    `json:"MetricName"`
	ComparisonOperator string    `json:"ComparisonOperator"`
	EvaluationPeriods  int32     `json:"EvaluationPeriods"`
	Period             int32     `json:"Period"`
	Statistic          string    `json:"Statistic"`
	Threshold          float64   `json:"Threshold"`
	StateValue         string    `json:"StateValue"`
	StateReason        string    `json:"StateReason,omitempty"`
	AlarmDescription   string    `json:"AlarmDescription,omitempty"`
	CreatedAt          time.Time `json:"AlarmCreatedAt"`
}
