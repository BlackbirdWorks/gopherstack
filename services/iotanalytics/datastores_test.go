package iotanalytics_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotanalytics"
)

func TestInMemoryBackend_Datastore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		datastoreName string
		errType       string
		wantErr       bool
	}{
		{
			name:          "create_and_describe",
			datastoreName: "my_datastore",
		},
		{
			name:          "describe_not_found",
			datastoreName: "nonexistent",
			wantErr:       true,
			errType:       "describe",
		},
		{
			name:          "delete_not_found",
			datastoreName: "nonexistent",
			wantErr:       true,
			errType:       "delete",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotanalytics.NewInMemoryBackend()

			switch tt.errType {
			case "describe":
				_, err := b.DescribeDatastore(tt.datastoreName)
				require.Error(t, err)
				assert.Equal(t, iotanalytics.ErrDatastoreNotFound, err)
			case "delete":
				err := b.DeleteDatastore(tt.datastoreName)
				require.Error(t, err)
				assert.Equal(t, iotanalytics.ErrDatastoreNotFound, err)
			default:
				ds, err := b.CreateDatastore(context.Background(), tt.datastoreName, nil, nil, nil, nil, nil)
				require.NoError(t, err)
				assert.Equal(t, tt.datastoreName, ds.Name)
				assert.Equal(t, "ACTIVE", ds.Status)

				got, err := b.DescribeDatastore(tt.datastoreName)
				require.NoError(t, err)
				assert.Equal(t, tt.datastoreName, got.Name)

				list := b.ListDatastores()
				assert.Len(t, list, 1)

				err = b.DeleteDatastore(tt.datastoreName)
				require.NoError(t, err)

				list = b.ListDatastores()
				assert.Empty(t, list)
			}
		})
	}
}

// TestInMemoryBackend_DatastorePartitionsValidation verifies CreateDatastore and
// UpdateDatastore validate the DatastorePartitions union shape: exactly one of
// attributePartition/timestampPartition must be set per entry, and the set variant must
// carry a non-empty attributeName -- mirroring the AWS SDK's client-side validators.
func TestInMemoryBackend_DatastorePartitionsValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		partitions *iotanalytics.DatastorePartitions
		name       string
		wantErr    bool
	}{
		{
			name: "valid_attribute_partition",
			partitions: &iotanalytics.DatastorePartitions{
				Partitions: []iotanalytics.DatastorePartitionEntry{
					{AttributePartition: &iotanalytics.AttributePartition{AttributeName: "deviceId"}},
				},
			},
		},
		{
			name: "valid_timestamp_partition",
			partitions: &iotanalytics.DatastorePartitions{
				Partitions: []iotanalytics.DatastorePartitionEntry{
					{TimestampPartition: &iotanalytics.TimestampPartition{AttributeName: "ts"}},
				},
			},
		},
		{
			name: "valid_multiple_mixed",
			partitions: &iotanalytics.DatastorePartitions{
				Partitions: []iotanalytics.DatastorePartitionEntry{
					{AttributePartition: &iotanalytics.AttributePartition{AttributeName: "deviceId"}},
					{TimestampPartition: &iotanalytics.TimestampPartition{AttributeName: "ts"}},
				},
			},
		},
		{
			name: "missing_attribute_name",
			partitions: &iotanalytics.DatastorePartitions{
				Partitions: []iotanalytics.DatastorePartitionEntry{
					{AttributePartition: &iotanalytics.AttributePartition{}},
				},
			},
			wantErr: true,
		},
		{
			name: "missing_timestamp_attribute_name",
			partitions: &iotanalytics.DatastorePartitions{
				Partitions: []iotanalytics.DatastorePartitionEntry{
					{TimestampPartition: &iotanalytics.TimestampPartition{}},
				},
			},
			wantErr: true,
		},
		{
			name: "both_variants_set",
			partitions: &iotanalytics.DatastorePartitions{
				Partitions: []iotanalytics.DatastorePartitionEntry{
					{
						AttributePartition: &iotanalytics.AttributePartition{AttributeName: "deviceId"},
						TimestampPartition: &iotanalytics.TimestampPartition{AttributeName: "ts"},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "neither_variant_set",
			partitions: &iotanalytics.DatastorePartitions{
				Partitions: []iotanalytics.DatastorePartitionEntry{{}},
			},
			wantErr: true,
		},
		{
			name:       "nil_partitions_ok",
			partitions: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotanalytics.NewInMemoryBackend()
			_, err := b.CreateDatastore(context.Background(), "ds_"+tt.name, nil, nil, nil, nil, tt.partitions)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, iotanalytics.ErrValidation)

				return
			}

			require.NoError(t, err)

			err = b.UpdateDatastore("ds_"+tt.name, nil, nil, nil, tt.partitions)
			require.NoError(t, err)
		})
	}
}

// TestInMemoryBackend_SortedListDatastores verifies ListDatastores returns datastores sorted by name.
func TestInMemoryBackend_SortedListDatastores(t *testing.T) {
	t.Parallel()

	b := iotanalytics.NewInMemoryBackend()
	b.AddDatastoreInternal("z_store")
	b.AddDatastoreInternal("a_store")
	b.AddDatastoreInternal("m_store")

	stores := b.ListDatastores()
	require.Len(t, stores, 3)
	assert.Equal(t, "a_store", stores[0].Name)
	assert.Equal(t, "m_store", stores[1].Name)
	assert.Equal(t, "z_store", stores[2].Name)
}
