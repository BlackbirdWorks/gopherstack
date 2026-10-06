package firehose

import "github.com/blackbirdworks/gopherstack/pkgs/cwmetric"

// AWS/Firehose metrics per firehose/latest/dev/monitoring-with-cloudwatch-metrics (dimension DeliveryStreamName).
const (
	metricNamespace = "AWS/Firehose"
	metricUnitCount = "Count"
	metricUnitBytes = "Bytes"
)

// SetMetricEmitter sets the emitter that publishes AWS/Firehose metrics to CloudWatch.
func (b *InMemoryBackend) SetMetricEmitter(e cwmetric.Emitter) { b.metrics.Set(e) }

func (b *InMemoryBackend) putMetric(region, stream, name, unit string, v float64) {
	b.metrics.Put(region, metricNamespace, name, unit, v, cwmetric.Dimension{Name: "DeliveryStreamName", Value: stream})
}

func (b *InMemoryBackend) emitIncoming(region, stream string, records, bytes int) {
	if !b.metrics.Enabled() {
		return
	}

	b.putMetric(region, stream, "IncomingRecords", metricUnitCount, float64(records))
	b.putMetric(region, stream, "IncomingBytes", metricUnitBytes, float64(bytes))
}

// emitS3Put publishes one S3 put's outcome; records and bytes are only counted for a successful put.
func (b *InMemoryBackend) emitS3Put(region, stream string, records, bytes int, ok bool) {
	if !b.metrics.Enabled() {
		return
	}

	if !ok {
		b.putMetric(region, stream, "DeliveryToS3.Success", metricUnitCount, 0)

		return
	}

	b.putMetric(region, stream, "DeliveryToS3.Success", metricUnitCount, 1)
	b.putMetric(region, stream, "DeliveryToS3.Records", metricUnitCount, float64(records))
	b.putMetric(region, stream, "DeliveryToS3.Bytes", metricUnitBytes, float64(bytes))
}
