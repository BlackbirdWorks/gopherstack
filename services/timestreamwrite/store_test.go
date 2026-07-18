package timestreamwrite_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/timestreamwrite"
)

// TestInMemoryBackend_Reset verifies that Reset clears all backend state.
func TestInMemoryBackend_Reset(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	_, err := b.CreateDatabase("reset-db", "", nil)
	require.NoError(t, err)

	b.Reset()
	assert.Equal(t, 0, timestreamwrite.DatabaseCount(b))
}

// TestInMemoryBackend_AccountID verifies AccountID returns a non-empty value.
func TestInMemoryBackend_AccountID(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	assert.NotEmpty(t, b.AccountID())
}

// TestInMemoryBackend_Region verifies Region returns a non-empty value.
func TestInMemoryBackend_Region(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	assert.NotEmpty(t, b.Region())
}

// TestInMemoryBackend_ScheduledQueryARNPassesValidation verifies that
// isKnownARNLocked accepts any ARN containing "scheduled-query/", since the
// TimestreamWrite service acts as the unified tag store for Timestream Query's
// scheduled-query resources too.
func TestInMemoryBackend_ScheduledQueryARNPassesValidation(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	// No databases or tables created; yet scheduled-query ARNs should be accepted.
	err := b.TagResource(
		"arn:aws:timestream:eu-west-1:123456789012:scheduled-query/daily-report",
		map[string]string{"managed": "true"},
	)
	require.NoError(t, err)

	tags := b.ListTagsForResource("arn:aws:timestream:eu-west-1:123456789012:scheduled-query/daily-report")
	assert.Equal(t, "true", tags["managed"])
}
