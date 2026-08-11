package cloudwatch_test

import (
	"context"
	"encoding/json"
	"io"
	"sync"
	"testing"
	"time"

	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
	"github.com/blackbirdworks/gopherstack/services/firehose"
)

// interopS3Storer is a minimal firehose.S3Storer used only to prove the
// cloudwatch<->firehose metric-stream delivery path lands a record; it does
// not aim to be a general-purpose test double.
type interopS3Storer struct {
	bodies [][]byte
	mu     sync.Mutex
}

func (m *interopS3Storer) PutObject(
	_ context.Context, input *sdk_s3.PutObjectInput,
) (*sdk_s3.PutObjectOutput, error) {
	body, _ := io.ReadAll(input.Body)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.bodies = append(m.bodies, body)

	return &sdk_s3.PutObjectOutput{}, nil
}

func (m *interopS3Storer) captured() [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([][]byte, len(m.bodies))
	copy(out, m.bodies)

	return out
}

// TestFirehosePutter_SatisfiedByRealFirehoseBackend locks the cross-service
// contract this pass's metric-stream delivery depends on: cloudwatch's
// FirehosePutter interface is structurally satisfied by firehose's own
// InMemoryBackend.PutRecordBatch, with no adapter needed. cli.go wiring
// (cwBk.SetFirehosePutter(fhBk)) itself is out of this pass's scope, but this
// proves the two services' interfaces genuinely line up rather than merely
// sharing a plausible-looking name.
func TestFirehosePutter_SatisfiedByRealFirehoseBackend(t *testing.T) {
	t.Parallel()

	var _ cloudwatch.FirehosePutter = (*firehose.InMemoryBackend)(nil)
}

// TestMetricStream_EndToEnd_RealFirehoseBackend wires a real firehose
// InMemoryBackend (not a mock) as cloudwatch's FirehosePutter and proves a
// PutMetricData call actually lands a record in a real Firehose delivery
// stream's S3 destination -- the full cross-service path bd gopherstack-lrmf
// asked to verify end to end, not just each side in isolation.
func TestMetricStream_EndToEnd_RealFirehoseBackend(t *testing.T) {
	t.Parallel()

	fhBackend := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	s3mock := &interopS3Storer{}
	fhBackend.SetS3Backend(s3mock)

	stream, err := fhBackend.CreateDeliveryStream(context.Background(), firehose.CreateDeliveryStreamInput{
		Name: "metric-stream-target",
		S3Destination: &firehose.S3DestinationDescription{
			BucketARN: "arn:aws:s3:::metric-stream-bucket",
			BufferingHints: &firehose.BufferingHints{
				SizeInMBs:         1,
				IntervalInSeconds: 0,
			},
		},
	})
	require.NoError(t, err)

	cwBackend := cloudwatch.NewInMemoryBackend()
	cwBackend.SetFirehosePutter(fhBackend)

	require.NoError(t, cwBackend.PutMetricStream(&cloudwatch.MetricStream{
		Name:         "cw-to-firehose",
		FirehoseArn:  "arn:aws:firehose:us-east-1:000000000000:deliverystream/" + stream.Name,
		RoleArn:      "arn:aws:iam::000000000000:role/metric-stream-role",
		OutputFormat: "json",
	}))

	require.NoError(t, cwBackend.PutMetricData("AWS/EC2", []cloudwatch.MetricDatum{{
		MetricName: "CPUUtilization",
		Timestamp:  time.Now(),
		Value:      77,
		HasValue:   true,
	}}))

	// Firehose only writes to S3 on flush (size/interval threshold or an
	// explicit FlushAll) -- PutRecordBatch alone just buffers, matching every
	// other firehose delivery test in this codebase.
	fhBackend.FlushAll(context.Background())

	require.Eventually(t, func() bool {
		return len(s3mock.captured()) > 0
	}, 2*time.Second, 20*time.Millisecond, "expected the metric-stream record to reach the Firehose S3 destination")

	body := s3mock.captured()[0]
	var rec map[string]any
	require.NoError(t, json.Unmarshal([]byte(firstLine(body)), &rec))
	assert.Equal(t, "CPUUtilization", rec["metric_name"])
	assert.Equal(t, "AWS/EC2", rec["namespace"])
}

// firstLine returns the first newline-delimited record from a Firehose S3
// object body (writeRecordsToBucket newline-separates records).
func firstLine(body []byte) string {
	for i, b := range body {
		if b == '\n' {
			return string(body[:i])
		}
	}

	return string(body)
}
