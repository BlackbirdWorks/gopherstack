package iotanalytics_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotanalytics"
)

func TestInMemoryBackend_Pipeline(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		pipelineName string
		errType      string
	}{
		{
			name:         "create_and_describe",
			pipelineName: "my_pipeline",
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
				p, err := b.CreatePipeline(context.Background(), tt.pipelineName, nil, nil)
				require.NoError(t, err)
				assert.Equal(t, tt.pipelineName, p.Name)

				got, err := b.DescribePipeline(tt.pipelineName)
				require.NoError(t, err)
				assert.Equal(t, tt.pipelineName, got.Name)

				list := b.ListPipelines()
				assert.Len(t, list, 1)

				err = b.UpdatePipeline(tt.pipelineName, nil)
				require.NoError(t, err)

				err = b.DeletePipeline(tt.pipelineName)
				require.NoError(t, err)
			}
		})
	}
}

// TestInMemoryBackend_SortedListPipelines verifies ListPipelines returns pipelines sorted by name.
func TestInMemoryBackend_SortedListPipelines(t *testing.T) {
	t.Parallel()

	b := iotanalytics.NewInMemoryBackend()
	b.AddPipelineInternal("z_pipe")
	b.AddPipelineInternal("a_pipe")
	b.AddPipelineInternal("m_pipe")

	pipes := b.ListPipelines()
	require.Len(t, pipes, 3)
	assert.Equal(t, "a_pipe", pipes[0].Name)
	assert.Equal(t, "m_pipe", pipes[1].Name)
	assert.Equal(t, "z_pipe", pipes[2].Name)
}

// TestInMemoryBackend_DeepCopy_Pipeline verifies DescribePipeline returns an independent copy.
func TestInMemoryBackend_DeepCopy_Pipeline(t *testing.T) {
	t.Parallel()

	b := iotanalytics.NewInMemoryBackend()
	_, err := b.CreatePipeline(context.Background(), "immutable_pipe", map[string]string{"env": "prod"}, nil)
	require.NoError(t, err)

	p, err := b.DescribePipeline("immutable_pipe")
	require.NoError(t, err)

	// Mutate the returned copy.
	p.Tags["env"] = "dev"
	p.Name = "mutated"

	p2, err := b.DescribePipeline("immutable_pipe")
	require.NoError(t, err)
	assert.Equal(t, "immutable_pipe", p2.Name)
	assert.Equal(t, "prod", p2.Tags["env"])
}

// TestInMemoryBackend_NonNilReprocessingSummaries verifies a freshly created pipeline has a
// non-nil (empty) Reprocessings map, so callers can range over it without a nil check.
func TestInMemoryBackend_NonNilReprocessingSummaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		pipelineName string
	}{
		{name: "new_pipeline_has_empty_map", pipelineName: "fresh_pipe"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotanalytics.NewInMemoryBackend()
			p, err := b.CreatePipeline(context.Background(), tt.pipelineName, nil, nil)
			require.NoError(t, err)
			require.NotNil(t, p.Reprocessings)
			assert.Empty(t, p.Reprocessings)
		})
	}
}
