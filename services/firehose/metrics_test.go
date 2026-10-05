package firehose_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
	"github.com/blackbirdworks/gopherstack/services/firehose"
)

var errS3Down = errors.New("s3 down")

type failingS3 struct{}

func (failingS3) PutObject(context.Context, *s3sdk.PutObjectInput) (*s3sdk.PutObjectOutput, error) {
	return nil, errS3Down
}

type metricRecorder struct {
	sums map[string]float64
	unit map[string]string
	dims map[string]string
	mu   sync.Mutex
}

func newMetricRecorder() *metricRecorder {
	return &metricRecorder{sums: map[string]float64{}, unit: map[string]string{}, dims: map[string]string{}}
}

func (r *metricRecorder) EmitMetric(p cwmetric.Point) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.sums[p.Namespace+"/"+p.Name] += p.Value
	r.unit[p.Namespace+"/"+p.Name] = p.Unit
	r.dims[p.Namespace+"/"+p.Name] = p.Dimensions[0].Name + "=" + p.Dimensions[0].Value + "@" + p.Region

	return nil
}

func TestFirehoseMetrics_IncomingAndS3Delivery(t *testing.T) {
	t.Parallel()

	cases := []struct {
		s3       firehose.S3Storer
		wantSums map[string]float64
		wantUnit map[string]string
		name     string
		absent   string
		batch    [][]byte
		single   []byte
	}{
		{
			name:   "put record and batch then successful delivery",
			s3:     &mockS3Storer{},
			single: []byte("abcd"),
			batch:  [][]byte{[]byte("12"), []byte("345")},
			wantSums: map[string]float64{
				"AWS/Firehose/IncomingRecords":      3,
				"AWS/Firehose/IncomingBytes":        9,
				"AWS/Firehose/DeliveryToS3.Success": 1,
				"AWS/Firehose/DeliveryToS3.Records": 3,
				// each record is newline-terminated in the object body: 5 + 3 + 4
				"AWS/Firehose/DeliveryToS3.Bytes": 12,
			},
			wantUnit: map[string]string{
				"AWS/Firehose/IncomingRecords":      "Count",
				"AWS/Firehose/IncomingBytes":        "Bytes",
				"AWS/Firehose/DeliveryToS3.Success": "Count",
				"AWS/Firehose/DeliveryToS3.Records": "Count",
				"AWS/Firehose/DeliveryToS3.Bytes":   "Bytes",
			},
		},
		{
			name:   "failed delivery reports zero success and no records",
			absent: "AWS/Firehose/DeliveryToS3.Records",
			s3:     failingS3{},
			single: []byte("abcd"),
			wantSums: map[string]float64{
				"AWS/Firehose/IncomingRecords":      1,
				"AWS/Firehose/IncomingBytes":        4,
				"AWS/Firehose/DeliveryToS3.Success": 0,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := newMetricRecorder()
			b := firehose.NewInMemoryBackend("000000000000", flushRegion)
			b.SetS3Backend(tc.s3)
			b.SetMetricEmitter(rec)

			_, err := b.CreateDeliveryStream(t.Context(), firehose.CreateDeliveryStreamInput{
				Name:          "metrics-stream",
				S3Destination: &firehose.S3DestinationDescription{BucketARN: "arn:aws:s3:::metrics-bucket"},
			})
			require.NoError(t, err)

			require.NoError(t, b.PutRecord(t.Context(), "metrics-stream", tc.single))

			if tc.batch != nil {
				_, err = b.PutRecordBatch(t.Context(), "metrics-stream", tc.batch)
				require.NoError(t, err)
			}

			b.FlushAll(t.Context())

			rec.mu.Lock()
			defer rec.mu.Unlock()

			for name, want := range tc.wantSums {
				got, ok := rec.sums[name]
				assert.True(t, ok, "series %s missing", name)
				assert.InDelta(t, want, got, 0, name)
				assert.Equal(t, "DeliveryStreamName=metrics-stream@"+flushRegion, rec.dims[name], name)
			}

			for name, want := range tc.wantUnit {
				assert.Equal(t, want, rec.unit[name], name)
			}

			if tc.absent != "" {
				assert.NotContains(t, rec.sums, tc.absent)
			}
		})
	}
}

func TestStagingDestinations_RoleAuthorization(t *testing.T) {
	t.Parallel()

	stage := &firehose.S3DestinationDescription{
		BucketARN: "arn:aws:s3:::stage", RoleARN: "arn:aws:iam::000000000000:role/firehose",
	}
	hints := &firehose.BufferingHints{SizeInMBs: 1, IntervalInSeconds: 300}

	kinds := map[string]firehose.CreateDeliveryStreamInput{
		"iceberg": {IcebergDestination: &firehose.IcebergDestinationDescription{
			RoleARN: "arn:aws:iam::000000000000:role/firehose", BufferingHints: hints, S3Destination: stage,
		}},
		"snowflake": {SnowflakeDestination: &firehose.SnowflakeDestinationDescription{
			Database: "db", Schema: "s", Table: "t", S3Destination: stage,
			BufferingHints: &firehose.SnowflakeBufferingHints{SizeInMBs: 1, IntervalInSeconds: 300},
		}},
		"redshift": {RedshiftDestination: &firehose.RedshiftDestinationDescription{
			ClusterJDBCURL: "jdbc:redshift://c1.abc.us-east-1.redshift.amazonaws.com:5439/db",
			CopyCommand:    &firehose.RedshiftCopyCommand{DataTableName: "t"},
			Username:       "u", S3Destination: stage,
		}},
	}

	cases := []struct {
		deny       map[string]bool
		name       string
		wantPuts   int
		wantFailed int64
	}{
		{name: "allowed", deny: nil, wantPuts: 1, wantFailed: 0},
		{name: "denied", deny: map[string]bool{"s3:PutObject": true}, wantPuts: 0, wantFailed: 1},
	}

	for kind, in := range kinds {
		for _, tc := range cases {
			t.Run(kind+"/"+tc.name, func(t *testing.T) {
				t.Parallel()

				s3mock := &mockS3Storer{}
				b := firehose.NewInMemoryBackend("000000000000", flushRegion)
				b.SetS3Backend(s3mock)
				b.SetRoleAuthorizer(denyAuthorizer{deny: tc.deny})

				input := in
				input.Name = "stage-" + kind
				_, err := b.CreateDeliveryStream(t.Context(), input)
				require.NoError(t, err)

				require.NoError(t, b.PutRecord(t.Context(), input.Name, []byte("payload")))
				b.FlushAll(t.Context())

				assert.Len(t, s3mock.calls, tc.wantPuts)
				assert.Equal(t, tc.wantFailed, firehose.StreamFailedRecords(b, flushRegion, input.Name))
			})
		}
	}
}
