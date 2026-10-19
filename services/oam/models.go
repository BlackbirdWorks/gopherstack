package oam

// Sink is a monitoring-account sink.
type Sink struct {
	Tags      map[string]string `json:"tags,omitempty"`
	ARN       string            `json:"arn"`
	ID        string            `json:"id"`
	Name      string            `json:"name"`
	Account   string            `json:"account"`
	Region    string            `json:"region"`
	Policy    string            `json:"policy,omitempty"`
	Seq       int64             `json:"seq"`
	HasPolicy bool              `json:"hasPolicy,omitempty"`
}

// Link is a source-account link to a sink.
type Link struct {
	Tags           map[string]string `json:"tags,omitempty"`
	LogGroupFilter *string           `json:"logGroupFilter,omitempty"`
	MetricFilter   *string           `json:"metricFilter,omitempty"`
	ARN            string            `json:"arn"`
	ID             string            `json:"id"`
	Label          string            `json:"label"`
	LabelTemplate  string            `json:"labelTemplate"`
	SinkARN        string            `json:"sinkArn"`
	Account        string            `json:"account"`
	Region         string            `json:"region"`
	ResourceTypes  []string          `json:"resourceTypes"`
	Seq            int64             `json:"seq"`
}

// LinkConfig is the optional filter set of a link.
type LinkConfig struct {
	LogGroupFilter *string
	MetricFilter   *string
}
