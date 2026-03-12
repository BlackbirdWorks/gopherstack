package iotanalytics_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotanalytics"
)

func TestInMemoryBackend_Channel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		channelName string
		errType     string
		wantErr     bool
	}{
		{
			name:        "create_and_describe",
			channelName: "my-channel",
		},
		{
			name:        "describe_not_found",
			channelName: "nonexistent",
			wantErr:     true,
			errType:     "describe",
		},
		{
			name:        "delete_not_found",
			channelName: "nonexistent",
			wantErr:     true,
			errType:     "delete",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotanalytics.NewInMemoryBackend()

			switch tt.errType {
			case "describe":
				_, err := b.DescribeChannel(tt.channelName)
				require.Error(t, err)
				assert.Equal(t, iotanalytics.ErrChannelNotFound, err)
			case "delete":
				err := b.DeleteChannel(tt.channelName)
				require.Error(t, err)
				assert.Equal(t, iotanalytics.ErrChannelNotFound, err)
			default:
				ch, err := b.CreateChannel(tt.channelName, map[string]string{"env": "test"})
				require.NoError(t, err)
				assert.Equal(t, tt.channelName, ch.Name)
				assert.Equal(t, "ACTIVE", ch.Status)
				assert.NotEmpty(t, ch.ARN)

				got, err := b.DescribeChannel(tt.channelName)
				require.NoError(t, err)
				assert.Equal(t, tt.channelName, got.Name)

				err = b.UpdateChannel(tt.channelName)
				require.NoError(t, err)

				list := b.ListChannels()
				assert.Len(t, list, 1)

				err = b.DeleteChannel(tt.channelName)
				require.NoError(t, err)

				list = b.ListChannels()
				assert.Empty(t, list)
			}
		})
	}
}

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
			datastoreName: "my-datastore",
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
				ds, err := b.CreateDatastore(tt.datastoreName, nil)
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

func TestInMemoryBackend_Dataset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		datasetName string
		errType     string
	}{
		{
			name:        "create_and_describe",
			datasetName: "my-dataset",
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
				ds, err := b.CreateDataset(tt.datasetName, nil)
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

func TestInMemoryBackend_Pipeline(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		pipelineName string
		errType      string
	}{
		{
			name:         "create_and_describe",
			pipelineName: "my-pipeline",
		},
		{
			name:         "describe_not_found",
			pipelineName: "nonexistent",
			errType:      "describe",
		},
		{
			name:         "delete_not_found",
			pipelineName: "nonexistent",
			errType:      "delete",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotanalytics.NewInMemoryBackend()

			switch tt.errType {
			case "describe":
				_, err := b.DescribePipeline(tt.pipelineName)
				require.Error(t, err)
				assert.Equal(t, iotanalytics.ErrPipelineNotFound, err)
			case "delete":
				err := b.DeletePipeline(tt.pipelineName)
				require.Error(t, err)
				assert.Equal(t, iotanalytics.ErrPipelineNotFound, err)
			default:
				p, err := b.CreatePipeline(tt.pipelineName, nil)
				require.NoError(t, err)
				assert.Equal(t, tt.pipelineName, p.Name)

				got, err := b.DescribePipeline(tt.pipelineName)
				require.NoError(t, err)
				assert.Equal(t, tt.pipelineName, got.Name)

				list := b.ListPipelines()
				assert.Len(t, list, 1)

				err = b.UpdatePipeline(tt.pipelineName)
				require.NoError(t, err)

				err = b.DeletePipeline(tt.pipelineName)
				require.NoError(t, err)
			}
		})
	}
}

func TestInMemoryBackend_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		channelName string
		tags        map[string]string
		removeTags  []string
		wantCount   int
	}{
		{
			name:        "tag_and_list",
			channelName: "tagged-channel",
			tags:        map[string]string{"env": "test", "team": "ops"},
			wantCount:   2,
		},
		{
			name:        "tag_and_untag",
			channelName: "untagged-channel",
			tags:        map[string]string{"env": "test", "team": "ops"},
			removeTags:  []string{"env"},
			wantCount:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotanalytics.NewInMemoryBackend()

			ch, err := b.CreateChannel(tt.channelName, nil)
			require.NoError(t, err)

			tagList := make([]iotanalytics.ExportedTagDTO, 0, len(tt.tags))
			for k, v := range tt.tags {
				tagList = append(tagList, iotanalytics.ExportedTagDTO{Key: k, Value: v})
			}

			err = b.TagResource(ch.ARN, tagList)
			require.NoError(t, err)

			if len(tt.removeTags) > 0 {
				err = b.UntagResource(ch.ARN, tt.removeTags)
				require.NoError(t, err)
			}

			got, err := b.ListTagsForResource(ch.ARN)
			require.NoError(t, err)
			assert.Len(t, got, tt.wantCount)
		})
	}
}
