package rds_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRDSBackend_CancelExportTask_RemovesFromMap(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.StartExportTask("my-task", "arn:aws:rds:us-east-1:000000000000:snapshot:s1", "my-bucket")
	require.NoError(t, err)

	task, err := b.CancelExportTask("my-task")
	require.NoError(t, err)
	assert.Equal(t, "canceled", task.Status)

	// Task should no longer be in the map.
	_, err = b.DescribeExportTasks("my-task")
	require.Error(t, err)
}
