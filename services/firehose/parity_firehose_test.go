package firehose_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/firehose"
)

const parityRegion = "us-east-1"

// okLambdaResponse builds a Lambda transform response body that marks every supplied
// (recordID, data) pair as "Ok".
func okLambdaResponse(records ...struct{ id, data string }) []byte {
	type rec struct {
		RecordID string `json:"recordId"`
		Result   string `json:"result"`
		Data     string `json:"data"`
	}
	out := struct {
		Records []rec `json:"records"`
	}{}
	for _, r := range records {
		out.Records = append(out.Records, rec{
			RecordID: r.id,
			Result:   "Ok",
			Data:     base64.StdEncoding.EncodeToString([]byte(r.data)),
		})
	}
	b, _ := json.Marshal(out)

	return b
}

// TestParity_LambdaTransform_AllDestinations verifies the Lambda transform now runs for
// non-S3 destinations (previously hardwired to S3), so a configured ProcessingConfiguration
// transforms records before they reach the HTTP endpoint.
func TestParity_LambdaTransform_AllDestinations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		transform  string
		wantOnWire string
	}{
		{name: "transformed_payload_reaches_http", transform: "TRANSFORMED", wantOnWire: "TRANSFORMED"},
		{name: "different_payload", transform: "hello-world", wantOnWire: "hello-world"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := newCaptureServer(t, http.StatusOK)
			b := firehose.NewInMemoryBackend("000000000000", parityRegion)
			// Record IDs are the record's zero-based index (see buildLambdaTransformPayload).
			b.SetLambdaBackend(&mockLambdaInvoker{
				response: okLambdaResponse(struct{ id, data string }{"0", tt.transform}),
			})

			stream, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{
				Name: "http-transform",
				HTTPEndpointDestination: &firehose.HTTPEndpointDestinationDescription{
					EndpointConfiguration: &firehose.HTTPEndpointConfiguration{URL: srv.srv.URL},
					RetryOptions:          &firehose.RetryOptions{DurationInSeconds: 1},
					ProcessingConfiguration: &firehose.ProcessingConfiguration{
						Enabled: true,
						Processors: []firehose.Processor{{
							Type: "Lambda",
							Parameters: []firehose.ProcessorParameter{
								{ParameterName: "LambdaArn", ParameterValue: "fn"},
							},
						}},
					},
				},
			})
			require.NoError(t, err)

			require.NoError(t, b.PutRecord(context.TODO(), stream.Name, []byte("original")))
			b.FlushAll(context.Background())

			assert.Eventually(t, func() bool { return len(srv.captured()) > 0 },
				3*time.Second, 20*time.Millisecond)

			var payload struct {
				Records []struct {
					Data string `json:"data"`
				} `json:"records"`
			}
			require.NoError(t, json.Unmarshal(srv.captured()[0].body, &payload))
			require.Len(t, payload.Records, 1)
			decoded, decErr := base64.StdEncoding.DecodeString(payload.Records[0].Data)
			require.NoError(t, decErr)
			assert.Equal(t, tt.wantOnWire, string(decoded))
		})
	}
}

// TestParity_ProcessingFailed_RoutesToErrorOutput verifies that Lambda "ProcessingFailed"
// records are written to the S3 ErrorOutputPrefix and counted in FailedRecords, while "Ok"
// records are delivered to the main prefix.
func TestParity_ProcessingFailed_RoutesToErrorOutput(t *testing.T) {
	t.Parallel()

	s3mock := &mockS3Storer{}
	lambdaResponse := `{"records":[` +
		`{"recordId":"0","result":"Ok","data":"b2s="},` + // "ok"
		`{"recordId":"1","result":"ProcessingFailed","data":""}` +
		`]}`
	b := firehose.NewInMemoryBackend("000000000000", parityRegion)
	b.SetS3Backend(s3mock)
	b.SetLambdaBackend(&mockLambdaInvoker{response: []byte(lambdaResponse)})

	_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{
		Name: "failroute",
		S3Destination: &firehose.S3DestinationDescription{
			BucketARN:         "arn:aws:s3:::main-bucket",
			ErrorOutputPrefix: "errors/",
			ProcessingConfiguration: &firehose.ProcessingConfiguration{
				Enabled: true,
				Processors: []firehose.Processor{{
					Type:       "Lambda",
					Parameters: []firehose.ProcessorParameter{{ParameterName: "LambdaArn", ParameterValue: "fn"}},
				}},
			},
		},
	})
	require.NoError(t, err)

	_, err = b.PutRecordBatch(context.TODO(), "failroute", [][]byte{[]byte("first"), []byte("second")})
	require.NoError(t, err)
	b.FlushAll(context.Background())

	require.Len(t, s3mock.calls, 2)

	var main, errOut *mockS3PutCall
	for _, c := range s3mock.calls {
		if strings.Contains(c.key, "errors/") {
			errOut = c
		} else {
			main = c
		}
	}
	require.NotNil(t, main, "expected a main-prefix delivery")
	require.NotNil(t, errOut, "expected an error-output delivery")
	assert.Contains(t, string(main.body), "ok")
	assert.Contains(t, string(errOut.body), "second", "original failed record routed to error output")

	assert.Equal(t, int64(1), firehose.StreamFailedRecords(b, parityRegion, "failroute"))
}

// TestParity_S3BackupMode_DeliversBackupRecords verifies that with S3BackupMode=Enabled the
// accumulated backup copies are actually delivered to the backup bucket (previously they
// were buffered forever and never sent).
func TestParity_S3BackupMode_DeliversBackupRecords(t *testing.T) {
	t.Parallel()

	s3mock := &mockS3Storer{}
	b := firehose.NewInMemoryBackend("000000000000", parityRegion)
	b.SetS3Backend(s3mock)

	_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{
		Name: "backup-stream",
		S3Destination: &firehose.S3DestinationDescription{
			BucketARN:    "arn:aws:s3:::main-bucket",
			S3BackupMode: "Enabled",
			S3BackupDescription: &firehose.S3BackupDescription{
				BucketARN: "arn:aws:s3:::backup-bucket",
				Prefix:    "backup/",
			},
		},
	})
	require.NoError(t, err)

	require.NoError(t, b.PutRecord(context.TODO(), "backup-stream", []byte("payload")))
	b.FlushAll(context.Background())

	var mainSeen, backupSeen bool
	for _, c := range s3mock.calls {
		switch c.bucket {
		case "main-bucket":
			mainSeen = true
			assert.Contains(t, string(c.body), "payload")
		case "backup-bucket":
			backupSeen = true
			assert.Contains(t, c.key, "backup/")
			assert.Contains(t, string(c.body), "payload")
		}
	}
	assert.True(t, mainSeen, "main delivery expected")
	assert.True(t, backupSeen, "S3 backup delivery expected")
}

// TestParity_DataFormatConversion verifies that DataFormatConversionConfiguration is honored
// end-to-end: valid JSON records are converted to a canonical form and delivered, while
// records that cannot be parsed are routed to the error output.
func TestParity_DataFormatConversion(t *testing.T) {
	t.Parallel()

	s3mock := &mockS3Storer{}
	b := firehose.NewInMemoryBackend("000000000000", parityRegion)
	b.SetS3Backend(s3mock)

	_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{
		Name: "convert-stream",
		S3Destination: &firehose.S3DestinationDescription{
			BucketARN:         "arn:aws:s3:::convert-bucket",
			ErrorOutputPrefix: "conv-errors/",
			FileExtension:     ".parquet",
			DataFormatConversion: &firehose.DataFormatConversionConfig{
				Enabled:             true,
				SchemaConfiguration: &firehose.SchemaConfiguration{DatabaseName: "db", TableName: "t"},
				InputFormatConfiguration: &firehose.InputFormatConfiguration{
					Deserializer: &firehose.Deserializer{OpenXJSONSerDe: &firehose.OpenXJSONSerDe{}},
				},
				OutputFormatConfiguration: &firehose.OutputFormatConfiguration{
					Serializer: &firehose.Serializer{ParquetSerDe: &firehose.ParquetSerDe{}},
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = b.PutRecordBatch(context.TODO(), "convert-stream",
		[][]byte{[]byte(`{"b":2,"a":1}`), []byte("not-json")})
	require.NoError(t, err)
	b.FlushAll(context.Background())

	require.Len(t, s3mock.calls, 2)

	var converted, errOut *mockS3PutCall
	for _, c := range s3mock.calls {
		if strings.Contains(c.key, "conv-errors/") {
			errOut = c
		} else {
			converted = c
		}
	}
	require.NotNil(t, converted)
	require.NotNil(t, errOut)
	// Canonical, key-sorted JSON output.
	assert.Contains(t, string(converted.body), `{"a":1,"b":2}`)
	// FileExtension applied to the converted object key.
	assert.Contains(t, converted.key, ".parquet")
	// Unparseable record routed to the error output.
	assert.Contains(t, string(errOut.body), "not-json")
	assert.Equal(t, int64(1), firehose.StreamFailedRecords(b, parityRegion, "convert-stream"))
}

// TestParity_DataFormatConversion_ValidationRejected verifies the AWS-accurate validation:
// enabling format conversion without a schema/serializer is rejected at create time.
func TestParity_DataFormatConversion_ValidationRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cfg  map[string]any
		name string
	}{
		{name: "missing_schema", cfg: map[string]any{"Enabled": true}},
		{
			name: "missing_serializer",
			cfg: map[string]any{
				"Enabled":             true,
				"SchemaConfiguration": map[string]any{"DatabaseName": "db", "TableName": "t"},
				"InputFormatConfiguration": map[string]any{
					"Deserializer": map[string]any{"OpenXJsonSerDe": map[string]any{}},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestFirehoseHandler(t)
			rec := doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{
				"DeliveryStreamName": "bad-convert",
				"ExtendedS3DestinationConfiguration": map[string]any{
					"BucketARN":                         "arn:aws:s3:::b",
					"RoleARN":                           "arn:aws:iam::000000000000:role/r",
					"DataFormatConversionConfiguration": tt.cfg,
				},
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "InvalidArgumentException")
		})
	}
}

// TestParity_DynamicPartitioning verifies that dynamic partitioning splits records into
// per-partition objects using the !{partitionKeyFromQuery:...} prefix expression, and routes
// records that cannot be partitioned to the error output.
func TestParity_DynamicPartitioning(t *testing.T) {
	t.Parallel()

	s3mock := &mockS3Storer{}
	b := firehose.NewInMemoryBackend("000000000000", parityRegion)
	b.SetS3Backend(s3mock)

	_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{
		Name: "partition-stream",
		S3Destination: &firehose.S3DestinationDescription{
			BucketARN:                        "arn:aws:s3:::part-bucket",
			Prefix:                           "data/cust=!{partitionKeyFromQuery:.customer}/",
			ErrorOutputPrefix:                "part-errors/",
			DynamicPartitioningConfiguration: &firehose.DynamicPartitioningConfiguration{Enabled: true},
		},
	})
	require.NoError(t, err)

	_, err = b.PutRecordBatch(context.TODO(), "partition-stream", [][]byte{
		[]byte(`{"customer":"alice","v":1}`),
		[]byte(`{"customer":"bob","v":2}`),
		[]byte(`{"customer":"alice","v":3}`),
		[]byte(`{"no_customer":true}`),
	})
	require.NoError(t, err)
	b.FlushAll(context.Background())

	var aliceKey, bobKey, errKey string
	for _, c := range s3mock.calls {
		switch {
		case strings.Contains(c.key, "part-errors/"):
			errKey = c.key
		case strings.Contains(c.key, "cust=alice/"):
			aliceKey = c.key
		case strings.Contains(c.key, "cust=bob/"):
			bobKey = c.key
		}
	}
	assert.NotEmpty(t, aliceKey, "expected an alice partition object")
	assert.NotEmpty(t, bobKey, "expected a bob partition object")
	assert.NotEmpty(t, errKey, "record without partition key routed to error output")
	assert.Equal(t, int64(1), firehose.StreamFailedRecords(b, parityRegion, "partition-stream"))
}

// TestParity_UpdateDestination_SwitchType verifies UpdateDestination can switch a stream's
// destination type, clearing the previous destination.
func TestParity_UpdateDestination_SwitchType(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", parityRegion)
	_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{
		Name:          "switch-stream",
		S3Destination: &firehose.S3DestinationDescription{BucketARN: "arn:aws:s3:::orig"},
	})
	require.NoError(t, err)

	err = b.UpdateDestination(context.TODO(), "switch-stream", "1", firehose.UpdateDestinationInput{
		HTTPEndpointDestination: &firehose.HTTPEndpointDestinationDescription{
			EndpointConfiguration: &firehose.HTTPEndpointConfiguration{URL: "https://example.com"},
		},
	})
	require.NoError(t, err)

	s, err := b.DescribeDeliveryStream(context.TODO(), "switch-stream")
	require.NoError(t, err)
	assert.Nil(t, s.S3Destination, "S3 destination should be cleared after switching to HTTP")
	require.NotNil(t, s.HTTPEndpointDestination)
	assert.Equal(t, "https://example.com", s.HTTPEndpointDestination.EndpointConfiguration.URL)
	assert.Equal(t, "2", s.VersionID)
}

// TestParity_UpdateDestination_RequiresVersion verifies the CurrentDeliveryStreamVersionId
// is mandatory and multiple/zero destination updates are rejected.
func TestParity_UpdateDestination_RequiresVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   firehose.UpdateDestinationInput
		name    string
		version string
	}{
		{
			name:    "empty_version",
			version: "",
			input: firehose.UpdateDestinationInput{
				S3Destination: &firehose.S3DestinationDescription{BucketARN: "arn:aws:s3:::b"},
			},
		},
		{
			name:    "no_destination",
			version: "1",
			input:   firehose.UpdateDestinationInput{},
		},
		{
			name:    "two_destinations",
			version: "1",
			input: firehose.UpdateDestinationInput{
				S3Destination:           &firehose.S3DestinationDescription{BucketARN: "arn:aws:s3:::b"},
				HTTPEndpointDestination: &firehose.HTTPEndpointDestinationDescription{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := firehose.NewInMemoryBackend("000000000000", parityRegion)
			_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{
				Name:          "ver-req",
				S3Destination: &firehose.S3DestinationDescription{BucketARN: "arn:aws:s3:::orig"},
			})
			require.NoError(t, err)

			updErr := b.UpdateDestination(context.TODO(), "ver-req", tt.version, tt.input)
			require.Error(t, updErr)
			assert.ErrorIs(t, updErr, firehose.ErrValidation)
		})
	}
}

// TestParity_ListByType_Filter verifies the DeliveryStreamType filter on the backend list.
func TestParity_ListByType_Filter(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", parityRegion)
	_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{Name: "dp"})
	require.NoError(t, err)
	_, err = b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{
		Name:               "ks",
		DeliveryStreamType: "KinesisStreamAsSource",
		Source: &firehose.SourceDescription{
			KinesisStreamSourceDescription: &firehose.KinesisStreamSourceDescription{
				KinesisStreamARN: "arn:aws:kinesis:us-east-1:000000000000:stream/s",
			},
		},
	})
	require.NoError(t, err)

	assert.Equal(t, []string{"dp"}, b.ListDeliveryStreamsByType(context.TODO(), "DirectPut"))
	assert.Equal(t, []string{"ks"}, b.ListDeliveryStreamsByType(context.TODO(), "KinesisStreamAsSource"))
	assert.ElementsMatch(t, []string{"dp", "ks"}, b.ListDeliveryStreamsByType(context.TODO(), ""))
}

// TestParity_ListCache_ConsistentAfterMutations verifies the cached sorted-name list stays
// correct across create/delete (the cache is invalidated on mutation).
func TestParity_ListCache_ConsistentAfterMutations(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", parityRegion)
	for _, n := range []string{"c", "a", "b"} {
		_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{Name: n})
		require.NoError(t, err)
	}

	// First call populates the cache; second call must return the same sorted result.
	assert.Equal(t, []string{"a", "b", "c"}, b.ListDeliveryStreams(context.TODO()))
	assert.Equal(t, []string{"a", "b", "c"}, b.ListDeliveryStreams(context.TODO()))

	require.NoError(t, b.DeleteDeliveryStream(context.TODO(), "b"))
	assert.Equal(t, []string{"a", "c"}, b.ListDeliveryStreams(context.TODO()))

	// Mutating the returned slice must not corrupt the cache.
	got := b.ListDeliveryStreams(context.TODO())
	got[0] = "mutated"
	assert.Equal(t, []string{"a", "c"}, b.ListDeliveryStreams(context.TODO()))
}

// TestParity_PendingFlush_Efficiency verifies only streams holding buffered records are
// tracked for interval flushing, and the entry is cleared once the buffer is flushed.
func TestParity_PendingFlush_Efficiency(t *testing.T) {
	t.Parallel()

	s3mock := &mockS3Storer{}
	b := firehose.NewInMemoryBackend("000000000000", parityRegion)
	b.SetS3Backend(s3mock)

	_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{
		Name: "pending-stream",
		S3Destination: &firehose.S3DestinationDescription{
			BucketARN:      "arn:aws:s3:::b",
			BufferingHints: &firehose.BufferingHints{SizeInMBs: 100, IntervalInSeconds: 300},
		},
	})
	require.NoError(t, err)

	// No buffered records yet → nothing pending.
	assert.Equal(t, 0, firehose.PendingFlushCount(b))

	// Buffer a small record (well under the size threshold) → stream becomes pending.
	require.NoError(t, b.PutRecord(context.TODO(), "pending-stream", []byte("x")))
	assert.Equal(t, 1, firehose.PendingFlushCount(b))

	// Flushing empties the buffer → the pending entry is cleared.
	b.FlushAll(context.Background())
	assert.Equal(t, 0, firehose.PendingFlushCount(b))
}
