package iotanalytics_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotanalytics"
)

func TestInMemoryBackend_Dataset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		datasetName string
		errType     string
	}{
		{
			name:        "create_and_describe",
			datasetName: "my_dataset",
		},
		{
			name:        "describe_not_found",
			datasetName: "nonexistent",
			errType:     "describe",
		},
		{
			name:        "delete_not_found",
			datasetName: "nonexistent",
			errType:     "delete",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotanalytics.NewInMemoryBackend()

			switch tt.errType {
			case "describe":
				_, err := b.DescribeDataset(tt.datasetName)
				require.Error(t, err)
				assert.Equal(t, iotanalytics.ErrDatasetNotFound, err)
			case "delete":
				err := b.DeleteDataset(tt.datasetName)
				require.Error(t, err)
				assert.Equal(t, iotanalytics.ErrDatasetNotFound, err)
			default:
				ds, err := b.CreateDataset(context.Background(), tt.datasetName, nil, nil, nil, nil, nil, nil, nil)
				require.NoError(t, err)
				assert.Equal(t, tt.datasetName, ds.Name)

				got, err := b.DescribeDataset(tt.datasetName)
				require.NoError(t, err)
				assert.Equal(t, tt.datasetName, got.Name)

				list := b.ListDatasets()
				assert.Len(t, list, 1)

				err = b.DeleteDataset(tt.datasetName)
				require.NoError(t, err)
			}
		})
	}
}

// TestInMemoryBackend_DatasetContentCap verifies that when maxDatasetContents is exceeded,
// the oldest content version is evicted and the newest is retained.
func TestInMemoryBackend_DatasetContentCap(t *testing.T) {
	t.Parallel()

	const maxContents = 100

	b := iotanalytics.NewInMemoryBackend()

	_, err := b.CreateDataset(context.Background(), "capped_ds", nil, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	// Fill to exactly the cap.
	var firstVersionID string
	for i := range maxContents {
		c, cerr := b.CreateDatasetContent("capped_ds", "")
		require.NoError(t, cerr)
		if i == 0 {
			firstVersionID = c.VersionID
		}
	}

	contents, err := b.ListDatasetContents("capped_ds")
	require.NoError(t, err)
	assert.Len(t, contents, maxContents, "should have exactly cap versions before exceeding")

	// Add one more — oldest should be evicted.
	newest, err := b.CreateDatasetContent("capped_ds", "")
	require.NoError(t, err)

	contents, err = b.ListDatasetContents("capped_ds")
	require.NoError(t, err)
	assert.Len(t, contents, maxContents, "count must not exceed cap after overflow")

	// The first version must be gone.
	for _, c := range contents {
		assert.NotEqual(t, firstVersionID, c.VersionID, "oldest version must be evicted")
	}

	// The newest version must be present.
	_, err = b.GetDatasetContent("capped_ds", newest.VersionID)
	assert.NoError(t, err, "newest version must be retained")
}

// TestInMemoryBackend_SortedListDatasets verifies ListDatasets returns datasets sorted by name.
func TestInMemoryBackend_SortedListDatasets(t *testing.T) {
	t.Parallel()

	b := iotanalytics.NewInMemoryBackend()
	b.AddDatasetInternal("z_set")
	b.AddDatasetInternal("a_set")
	b.AddDatasetInternal("m_set")

	sets := b.ListDatasets()
	require.Len(t, sets, 3)
	assert.Equal(t, "a_set", sets[0].Name)
	assert.Equal(t, "m_set", sets[1].Name)
	assert.Equal(t, "z_set", sets[2].Name)
}
