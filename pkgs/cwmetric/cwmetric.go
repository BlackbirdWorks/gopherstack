// Package cwmetric is the shared shape for services that publish in-process CloudWatch metrics.
package cwmetric

// Dimension is a metric dimension name/value pair.
type Dimension struct {
	Name  string
	Value string
}

// Point is one metric data point published to the CloudWatch backend of Region.
type Point struct {
	Region     string
	Namespace  string
	Name       string
	Unit       string
	Dimensions []Dimension
	Value      float64
	// Samples, when positive, marks a statistic set: Samples observations with Sum, Min and Max.
	Samples float64
	Sum     float64
	Min     float64
	Max     float64
}

// Emitter publishes metric points to CloudWatch.
type Emitter interface {
	EmitMetric(p Point) error
}

// EmitterFunc adapts a function to Emitter.
type EmitterFunc func(p Point) error

// EmitMetric implements Emitter.
func (f EmitterFunc) EmitMetric(p Point) error { return f(p) }
