package gcpbigquery

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamingBufferReported(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := NewInMemoryBackend()
		_, err := b.CreateDataset("p", map[string]any{"datasetReference": map[string]any{"datasetId": "d"}})
		require.NoError(t, err)

		_, err = b.CreateTable("p", "d", map[string]any{
			"tableReference": map[string]any{"projectId": "p", "datasetId": "d", "tableId": "t"},
			"schema":         map[string]any{"fields": []any{map[string]any{"name": "v", "type": "INTEGER"}}},
		})
		require.NoError(t, err)

		before, err := b.GetTable("p", "d", "t")
		require.NoError(t, err)
		assert.NotContains(t, before, "streamingBuffer")

		_, err = b.InsertAll("p", "d", "t", InsertRequest{Rows: []InsertRow{
			{JSON: map[string]any{"v": 1}}, {JSON: map[string]any{"v": 2}},
		}})
		require.NoError(t, err)

		got, err := b.GetTable("p", "d", "t")
		require.NoError(t, err)

		sb, ok := got["streamingBuffer"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "2", sb["estimatedRows"])
		assert.Equal(t, "16", sb["estimatedBytes"])

		time.Sleep(streamFlushWindow + time.Minute)

		later, err := b.GetTable("p", "d", "t")
		require.NoError(t, err)
		assert.NotContains(t, later, "streamingBuffer")
	})
}
