package firehose

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
)

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
func (b *InMemoryBackend) emitS3Put(region, stream string, records, bytes int, ok bool, oldest time.Time) {
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
	b.putFreshness(region, stream, "DeliveryToS3", oldest)
}

const (
	metricUnitSeconds = "Seconds"
	apiPutRecordBatch = "PutRecordBatch"
	millisPerMicro    = 1000.0
)

func (b *InMemoryBackend) putFreshness(region, stream, prefix string, oldest time.Time) {
	if !oldest.IsZero() {
		b.putMetric(region, stream, prefix+".DataFreshness", metricUnitSeconds, time.Since(oldest).Seconds())
	}
}

func sumBytes(recs [][]byte) int {
	n := 0
	for _, r := range recs {
		n += len(r)
	}

	return n
}

// emitPutRequest publishes the PutRecord / PutRecordBatch API metrics and IncomingPutRequests.
func (b *InMemoryBackend) emitPutRequest(region, stream, api string, records, bytes int, took time.Duration) {
	if !b.metrics.Enabled() {
		return
	}

	b.putMetric(region, stream, "IncomingPutRequests", metricUnitCount, 1)
	b.putMetric(region, stream, api+".Bytes", metricUnitBytes, float64(bytes))
	b.putMetric(region, stream, api+".Latency", "Milliseconds", float64(took.Microseconds())/millisPerMicro)
	b.putMetric(region, stream, api+".Requests", metricUnitCount, 1)

	if api == apiPutRecordBatch {
		b.putMetric(region, stream, api+".Records", metricUnitCount, float64(records))
	}
}

// emitDestinationDelivery publishes DeliveryTo<Destination>.* for one non-S3 delivery attempt.
func (b *InMemoryBackend) emitDestinationDelivery(snap *flushSnapshot, t nonS3Target, sent, undelivered [][]byte) {
	if t.metric == "" || !b.metrics.Enabled() {
		return
	}

	delivered := len(sent) - len(undelivered)
	success := float64(delivered)

	if t.perCall {
		success = 0
		if delivered > 0 {
			success = 1
		}
	}

	b.putMetric(snap.region, snap.streamName, t.metric+".Success", metricUnitCount, success)

	if delivered <= 0 {
		return
	}

	b.putMetric(snap.region, snap.streamName, t.metric+".Records", metricUnitCount, float64(delivered))
	b.putMetric(snap.region, snap.streamName, t.metric+".Bytes", metricUnitBytes,
		float64(sumBytes(sent)-sumBytes(undelivered)))
	b.putFreshness(snap.region, snap.streamName, t.metric, snap.oldest)
}

// emitBackup publishes BackupToS3.* for one S3 backup write.
func (b *InMemoryBackend) emitBackup(snap *flushSnapshot, stream string, ok bool) {
	if !b.metrics.Enabled() {
		return
	}

	if !ok {
		b.putMetric(snap.region, stream, "BackupToS3.Success", metricUnitCount, 0)

		return
	}

	b.putMetric(snap.region, stream, "BackupToS3.Success", metricUnitCount, 1)
	b.putMetric(snap.region, stream, "BackupToS3.Records", metricUnitCount, float64(len(snap.backupRecords)))
	b.putMetric(snap.region, stream, "BackupToS3.Bytes", metricUnitBytes, float64(sumBytes(snap.backupRecords)))
	b.putFreshness(snap.region, stream, "BackupToS3", snap.oldest)
}
