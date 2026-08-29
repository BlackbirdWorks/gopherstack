package iotanalytics_test

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotanalyticssdk "github.com/aws/aws-sdk-go-v2/service/iotanalytics" //nolint:staticcheck // AWS has deprecated this service; gopherstack still supports it
	iotanalyticstypes "github.com/aws/aws-sdk-go-v2/service/iotanalytics/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotanalytics"
)

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
