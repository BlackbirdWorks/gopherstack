package cloudwatch

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// metricStreamDeliveryTarget is a lock-free snapshot of the fields
// deliverMetricStreams needs, copied out of the backend's MetricStream table
// while the write lock is held so delivery itself can run lock-free.
type metricStreamDeliveryTarget struct {
	Name           string
	FirehoseArn    string
	OutputFormat   string
	IncludeFilters []MetricStreamFilter
	ExcludeFilters []MetricStreamFilter
}

// firehoseStreamNameFromARN extracts the delivery stream name from a Firehose
// ARN (arn:aws:firehose:<region>:<account>:deliverystream/<name>).
func firehoseStreamNameFromARN(firehoseArn string) string {
	const marker = "deliverystream/"
	if _, name, found := strings.Cut(firehoseArn, marker); found {
		return name
	}

	return ""
}

// metricStreamValue is the max/min/sum/count shape every CloudWatch Metric
// Streams JSON record carries per datapoint.
type metricStreamValue struct {
	Max   float64 `json:"max"`
	Min   float64 `json:"min"`
	Sum   float64 `json:"sum"`
	Count float64 `json:"count"`
}

// metricStreamRecord is the public CloudWatch Metric Streams JSON output
// format (one JSON object per line, per metric per timestamp). This is not a
// generated aws-sdk-go-v2 type -- it's the payload contract CloudWatch writes
// to the destination Firehose stream, documented at
// https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-Metric-Streams-formats-json.html,
// not a request/response shape the SDK models.
type metricStreamRecord struct {
	Dimensions       map[string]string `json:"dimensions,omitempty"`
	MetricStreamName string            `json:"metric_stream_name"`
	AccountID        string            `json:"account_id"`
	Region           string            `json:"region"`
	Namespace        string            `json:"namespace"`
	MetricName       string            `json:"metric_name"`
	Unit             string            `json:"unit,omitempty"`
	Value            metricStreamValue `json:"value"`
	Timestamp        int64             `json:"timestamp"`
}

// datumStreamValue reduces a MetricDatum to the max/min/sum/count shape metric
// streams deliver. Every PutMetricData input shape (Value, StatisticSet,
// Values/Counts) is already aggregated into Sum/Min/Max/Count at write time
// (see storeDatum/aggregateValuesCounts), so this mirrors that same
// canonical reduction rather than re-deriving it.
func datumStreamValue(d MetricDatum) metricStreamValue {
	if d.HasStatisticSet || d.HasValuesArray {
		return metricStreamValue{Max: d.Max, Min: d.Min, Sum: d.Sum, Count: d.Count}
	}

	return metricStreamValue{Max: d.Value, Min: d.Value, Sum: d.Value, Count: 1}
}

func dimensionsMap(dims []Dimension) map[string]string {
	if len(dims) == 0 {
		return nil
	}

	m := make(map[string]string, len(dims))
	for _, d := range dims {
		m[d.Name] = d.Value
	}

	return m
}

// buildMetricStreamRecords serializes data into the metric-stream JSON
// format, one record per datum. Marshal errors (never expected for this
// shape) drop just that record rather than failing the whole batch.
func buildMetricStreamRecords(
	streamName, accountID, region, namespace string,
	data []MetricDatum,
) [][]byte {
	records := make([][]byte, 0, len(data))

	for _, d := range data {
		rec := metricStreamRecord{
			MetricStreamName: streamName,
			AccountID:        accountID,
			Region:           region,
			Namespace:        namespace,
			MetricName:       d.MetricName,
			Dimensions:       dimensionsMap(d.Dimensions),
			Timestamp:        d.Timestamp.UnixMilli(),
			Value:            datumStreamValue(d),
			Unit:             d.Unit,
		}

		body, err := json.Marshal(rec)
		if err != nil {
			continue
		}

		records = append(records, body)
	}

	return records
}

// deliverMetricStreams delivers data to every target stream, filtering out
// metrics each stream's IncludeFilters/ExcludeFilters deny -- the same
// per-datum filterExcludesMetric/filterIncludesMetric logic PutMetricData
// used just to decide whether ANY delivery was owed. Only OutputFormat
// "json" is actually serialized: PutMetricStream's other two real values
// (opentelemetry0.7/opentelemetry1.0) are genuine OTLP protobuf wire
// formats this backend has no encoder for -- those streams' LastUpdateDate
// still advances (real state, set by the caller before this runs) but no
// record is delivered, a documented gap rather than a fabricated payload.
// A nil firehosePutter (SetFirehosePutter never wired, a cli.go change out
// of this pass's scope) is a documented no-op, not a silent failure.
func (b *InMemoryBackend) deliverMetricStreams(
	streams []metricStreamDeliveryTarget,
	namespace string,
	data []MetricDatum,
) {
	if b.firehosePutter == nil {
		return
	}

	for _, s := range streams {
		b.deliverOneMetricStream(s, namespace, data)
	}
}

// streamMatchedData filters data down to the datums s's IncludeFilters/
// ExcludeFilters allow.
func streamMatchedData(
	s metricStreamDeliveryTarget,
	namespace string,
	data []MetricDatum,
) []MetricDatum {
	matched := make([]MetricDatum, 0, len(data))

	for _, d := range data {
		if filterExcludesMetric(s.ExcludeFilters, namespace, d.MetricName) {
			continue
		}

		if len(s.IncludeFilters) > 0 &&
			!filterIncludesMetric(s.IncludeFilters, namespace, d.MetricName) {
			continue
		}

		matched = append(matched, d)
	}

	return matched
}

// deliverOneMetricStream delivers data's allowed subset to a single stream.
func (b *InMemoryBackend) deliverOneMetricStream(
	s metricStreamDeliveryTarget,
	namespace string,
	data []MetricDatum,
) {
	if s.OutputFormat != "json" {
		return
	}

	matched := streamMatchedData(s, namespace, data)
	if len(matched) == 0 {
		return
	}

	firehoseStreamName := firehoseStreamNameFromARN(s.FirehoseArn)
	if firehoseStreamName == "" {
		return
	}

	records := buildMetricStreamRecords(s.Name, b.accountID, b.region, namespace, matched)
	if len(records) == 0 {
		return
	}

	if _, err := b.firehosePutter.PutRecordBatch(context.Background(), firehoseStreamName, records); err != nil {
		ctx := context.Background()
		logger.Load(ctx).WarnContext(ctx, "cloudwatch: metric-stream delivery to Firehose failed",
			"stream", s.Name, "firehoseStream", firehoseStreamName, "error", err)
	}
}
