package kinesis

import (
	"errors"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
)

// Metric names, units and the StreamName dimension per the Kinesis Data Streams
// CloudWatch metrics docs (docs.aws.amazon.com/streams/latest/dev/monitoring-with-cloudwatch.html).
const (
	kinesisMetricNamespace    = "AWS/Kinesis"
	metricUnitCount           = "Count"
	metricUnitBytes           = "Bytes"
	metricUnitMillis          = "Milliseconds"
	errCodeThroughputExceeded = "ProvisionedThroughputExceededException"
)

// SetMetricEmitter sets the emitter that publishes AWS/Kinesis metrics to CloudWatch.
func (b *InMemoryBackend) SetMetricEmitter(e cwmetric.Emitter) { b.metrics.Set(e) }

func streamDim(name string) cwmetric.Dimension {
	return cwmetric.Dimension{Name: "StreamName", Value: name}
}

// metricStart returns the clock reading for latency, or zero when metrics are off.
func (b *InMemoryBackend) metricStart() time.Time {
	if !b.metrics.Enabled() {
		return time.Time{}
	}

	return time.Now()
}

func (b *InMemoryBackend) putMetric(region, stream, name, unit string, v float64) {
	b.metrics.Put(region, kinesisMetricNamespace, name, unit, v, streamDim(stream))
}

func recordBytes(partitionKey string, data []byte) float64 {
	return float64(len(partitionKey) + len(data))
}

func (b *InMemoryBackend) emitPutRecord(region string, in *PutRecordInput, start time.Time, err error) {
	if !b.metrics.Enabled() || errors.Is(err, ErrStreamNotFound) {
		return
	}

	name := in.StreamName
	b.putMetric(region, name, "PutRecord.Latency", metricUnitMillis, millisSince(start))

	if err != nil {
		b.putMetric(region, name, "PutRecord.Success", metricUnitCount, 0)

		if errors.Is(err, ErrProvisionedThroughputExceeded) {
			b.putMetric(region, name, "WriteProvisionedThroughputExceeded", metricUnitCount, 1)
		}

		return
	}

	n := recordBytes(in.PartitionKey, in.Data)
	b.putMetric(region, name, "PutRecord.Success", metricUnitCount, 1)
	b.putMetric(region, name, "PutRecord.Bytes", metricUnitBytes, n)
	b.putMetric(region, name, "IncomingRecords", metricUnitCount, 1)
	b.putMetric(region, name, "IncomingBytes", metricUnitBytes, n)
}

func (b *InMemoryBackend) emitPutRecords(
	region string, in *PutRecordsInput, results []PutRecordsResultEntry, start time.Time,
) {
	if !b.metrics.Enabled() {
		return
	}

	var okRecords, failed, throttled int

	var okBytes float64

	for i, r := range results {
		switch r.ErrorCode {
		case "":
			okRecords++
			okBytes += recordBytes(in.Records[i].PartitionKey, in.Records[i].Data)
		case errCodeThroughputExceeded:
			throttled++
			failed++
		default:
			failed++
		}
	}

	name := in.StreamName
	success := 0.0

	if okRecords > 0 {
		success = 1
	}

	b.putMetric(region, name, "PutRecords.Latency", metricUnitMillis, millisSince(start))
	b.putMetric(region, name, "PutRecords.Success", metricUnitCount, success)
	b.putMetric(region, name, "PutRecords.TotalRecords", metricUnitCount, float64(len(results)))
	b.putMetric(region, name, "PutRecords.SuccessfulRecords", metricUnitCount, float64(okRecords))
	b.putMetric(region, name, "PutRecords.FailedRecords", metricUnitCount, float64(failed))
	b.putMetric(region, name, "PutRecords.ThrottledRecords", metricUnitCount, float64(throttled))

	if throttled > 0 {
		b.putMetric(region, name, "WriteProvisionedThroughputExceeded", metricUnitCount, float64(throttled))
	}

	if okRecords == 0 {
		return
	}

	b.putMetric(region, name, "PutRecords.Records", metricUnitCount, float64(okRecords))
	b.putMetric(region, name, "PutRecords.Bytes", metricUnitBytes, okBytes)
	b.putMetric(region, name, "IncomingRecords", metricUnitCount, float64(okRecords))
	b.putMetric(region, name, "IncomingBytes", metricUnitBytes, okBytes)
}

func (b *InMemoryBackend) emitGetRecords(region, stream string, results []GetRecordResult, start time.Time) {
	if !b.metrics.Enabled() {
		return
	}

	var bytes, age float64

	for _, r := range results {
		bytes += float64(len(r.Data))
	}

	if n := len(results); n > 0 {
		age = float64(time.Since(results[n-1].ApproximateArrivalTimestamp).Milliseconds())
	}

	b.putMetric(region, stream, "GetRecords.Latency", metricUnitMillis, millisSince(start))
	b.putMetric(region, stream, "GetRecords.Success", metricUnitCount, 1)
	b.putMetric(region, stream, "GetRecords.Records", metricUnitCount, float64(len(results)))
	b.putMetric(region, stream, "GetRecords.Bytes", metricUnitBytes, bytes)
	b.putMetric(region, stream, "GetRecords.IteratorAgeMilliseconds", metricUnitMillis, age)
}

func millisSince(start time.Time) float64 {
	return float64(time.Since(start)) / float64(time.Millisecond)
}
