package iotanalytics_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotanalyticssdk "github.com/aws/aws-sdk-go-v2/service/iotanalytics" //nolint:staticcheck // AWS has deprecated this service; gopherstack still supports it
	iotanalyticstypes "github.com/aws/aws-sdk-go-v2/service/iotanalytics/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotanalytics"
)

// TestSampleChannelData_StartTimeExcludesEarlierMessages drives
// BatchPutMessage/SampleChannelData through the real SDK client. Before the
// fix, handleSampleChannelData never read the startTime/endTime query params
// at all -- both are real *time.Time members of SampleChannelDataInput
// (api_op_SampleChannelData.go:45,56), serialized via
// encoder.SetQuery("startTime").String(smithytime.FormatDateTime(...))
// (serializers.go:2192) -- so a real client's time window was always
// ignored and every stored message came back regardless of when it arrived.
//
// Message arrival time is recorded with whole-second resolution
// ([epochSeconds]), matching every other stored timestamp in this backend,
// so the two batches below are separated by a real sleep across a second
// boundary rather than a synctest fake clock: SampleChannelData is served
// over a real httptest.Server/SDK client round trip, which synctest's
// bubble can't durably block on (see gopherstack-tests skill).
//
//nolint:staticcheck // iotanalytics is AWS-deprecated; gopherstack still emulates it
func TestSampleChannelData_StartTimeExcludesEarlierMessages(t *testing.T) {
	t.Parallel()

	backend := iotanalytics.NewInMemoryBackend()
	client := newTestIoTAnalyticsClient(t, iotanalytics.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateChannel(ctx, &iotanalyticssdk.CreateChannelInput{
		ChannelName: aws.String("sample_window_channel"),
	})
	require.NoError(t, err)

	_, err = client.BatchPutMessage(ctx, &iotanalyticssdk.BatchPutMessageInput{
		ChannelName: aws.String("sample_window_channel"),
		Messages: []iotanalyticstypes.Message{
			{MessageId: aws.String("early"), Payload: []byte(`{"phase":"early"}`)},
		},
	})
	require.NoError(t, err)

	time.Sleep(1100 * time.Millisecond)
	cutoff := time.Now()
	time.Sleep(1100 * time.Millisecond)

	_, err = client.BatchPutMessage(ctx, &iotanalyticssdk.BatchPutMessageInput{
		ChannelName: aws.String("sample_window_channel"),
		Messages: []iotanalyticstypes.Message{
			{MessageId: aws.String("late"), Payload: []byte(`{"phase":"late"}`)},
		},
	})
	require.NoError(t, err)

	out, err := client.SampleChannelData(ctx, &iotanalyticssdk.SampleChannelDataInput{
		ChannelName: aws.String("sample_window_channel"),
		StartTime:   aws.Time(cutoff),
	})
	require.NoError(t, err)
	require.Len(t, out.Payloads, 1, "StartTime must exclude the message that arrived before it")
	assert.JSONEq(t, `{"phase":"late"}`, string(out.Payloads[0]))
}

// TestUpdateDatastore_PartitionsImmutable covers gopherstack-wksweep-iota-1:
// the real UpdateDatastoreInput (iotanalytics@v1.32.0
// api_op_UpdateDatastore.go) has no partitions member at all -- partitions
// are settable only at CreateDatastore and are immutable afterward. A typed
// SDK client can't even construct an UpdateDatastoreInput with a partitions
// field to prove a fail-before/pass-after delta (the field never existed on
// the real struct), so this proves immutability across a real client's
// update instead: partitions set at creation must survive an update that
// changes an unrelated field.
//
//nolint:staticcheck // iotanalytics is AWS-deprecated; gopherstack still emulates it
func TestUpdateDatastore_PartitionsImmutable(t *testing.T) {
	t.Parallel()

	h := iotanalytics.NewHandler(iotanalytics.NewInMemoryBackend())
	client := newTestIoTAnalyticsClient(t, h)
	ctx := t.Context()

	_, err := client.CreateDatastore(ctx, &iotanalyticssdk.CreateDatastoreInput{
		DatastoreName: aws.String("wire_fix_ds"),
		DatastorePartitions: &iotanalyticstypes.DatastorePartitions{
			Partitions: []iotanalyticstypes.DatastorePartition{
				{AttributePartition: &iotanalyticstypes.Partition{AttributeName: aws.String("deviceId")}},
			},
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateDatastore(ctx, &iotanalyticssdk.UpdateDatastoreInput{
		DatastoreName: aws.String("wire_fix_ds"),
		RetentionPeriod: &iotanalyticstypes.RetentionPeriod{
			NumberOfDays: aws.Int32(30),
		},
	})
	require.NoError(t, err)

	got, err := client.DescribeDatastore(ctx, &iotanalyticssdk.DescribeDatastoreInput{
		DatastoreName: aws.String("wire_fix_ds"),
	})
	require.NoError(t, err)
	require.NotNil(t, got.Datastore.DatastorePartitions)
	require.Len(t, got.Datastore.DatastorePartitions.Partitions, 1)
	assert.Equal(t, "deviceId",
		aws.ToString(got.Datastore.DatastorePartitions.Partitions[0].AttributePartition.AttributeName))
}

// TestUpdateDatastore_RawPartitionsFieldIgnored is the raw-body
// fail-before/pass-after proof for gopherstack-wksweep-iota-1 that
// TestUpdateDatastore_PartitionsImmutable above can't provide with a typed
// client: before the fix, gopherstack's updateDatastoreRequest read a
// "partitions" key that no real client can send, but a raw HTTP body could.
// Sending it directly must have no effect.
func TestUpdateDatastore_RawPartitionsFieldIgnored(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, http.MethodPost, "/datastores", map[string]any{
		"datastoreName": "raw_wire_fix_ds",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	updateRec := doRequest(t, h, http.MethodPut, "/datastores/raw_wire_fix_ds", map[string]any{
		"partitions": map[string]any{
			"partitions": []any{
				map[string]any{"attributePartition": map[string]any{"attributeName": "shouldNotApply"}},
			},
		},
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	descRec := doRequest(t, h, http.MethodGet, "/datastores/raw_wire_fix_ds", nil)
	require.Equal(t, http.StatusOK, descRec.Code)
	assert.NotContains(t, descRec.Body.String(), "shouldNotApply",
		"UpdateDatastore must not accept a partitions field; real UpdateDatastoreInput has none")
}
