package cloudwatchlogs

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetentionTargets_DSTZone(t *testing.T) {
	t.Parallel()

	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	b := NewInMemoryBackend()
	_, err = b.CreateLogGroup(context.Background(), "g", "", "")
	require.NoError(t, err)

	now := time.Date(2026, time.November, 9, 12, 0, 0, 0, ny)
	targets := NewJanitor(b, 0).retentionTargets(now)

	require.Len(t, targets, 1)
	assert.Equal(t, now.Add(-defaultMaxRetentionDays*24*time.Hour).UnixMilli(), targets[0].cutoffMs)
}
