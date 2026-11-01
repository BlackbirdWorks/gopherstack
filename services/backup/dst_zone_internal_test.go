package backup

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCalculateLifecycle_DSTZone(t *testing.T) {
	t.Parallel()

	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	from := time.Date(2026, time.November, 1, 0, 0, 0, 0, ny)
	cl := calculateLifecycle(&Lifecycle{MoveToColdStorageAfterDays: 1, DeleteAfterDays: 2}, from)

	require.NotNil(t, cl.MoveToColdStorageAt)
	require.NotNil(t, cl.DeleteAt)
	assert.Equal(t, 24*time.Hour, cl.MoveToColdStorageAt.Sub(from))
	assert.Equal(t, 48*time.Hour, cl.DeleteAt.Sub(from))
}
