package cloudhsmv2

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackupExpiresAt_DSTZone(t *testing.T) {
	t.Parallel()

	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	created := time.Date(2026, time.November, 1, 0, 0, 0, 0, ny)
	b := &Backup{Created: created, RetentionDays: 1}

	assert.Equal(t, 24*time.Hour, b.expiresAt().Sub(created))
}
