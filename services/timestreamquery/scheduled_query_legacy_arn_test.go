package timestreamquery_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/timestreamquery"
)

// A snapshot taken before ARNs carried a random suffix still restores and keeps names unique.
func TestScheduledQueryRestoresLegacyBareNameArn(t *testing.T) {
	t.Parallel()

	src := timestreamquery.NewInMemoryBackend("123456789012", "us-east-1")
	sq, err := src.CreateScheduledQuery(
		t.Context(), "daily", "SELECT 1", "rate(5 minutes)", "arn", "", "", "", "", "", "", "", "", nil, nil,
	)
	require.NoError(t, err)

	legacy := sq.Arn[:strings.LastIndex(sq.Arn, "-")]
	snap := strings.ReplaceAll(string(src.Snapshot(t.Context())), sq.Arn, legacy)
	require.NotContains(t, snap, sq.Arn)

	dst := timestreamquery.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, dst.Restore(t.Context(), []byte(snap)))

	got, err := dst.DescribeScheduledQuery(t.Context(), legacy)
	require.NoError(t, err)
	assert.Equal(t, "daily", got.Name)

	_, err = dst.CreateScheduledQuery(
		t.Context(), "daily", "SELECT 1", "rate(5 minutes)", "arn", "", "", "", "", "", "", "", "", nil, nil,
	)
	require.ErrorIs(t, err, timestreamquery.ErrAlreadyExists)
	require.NoError(t, dst.DeleteScheduledQuery(t.Context(), legacy))
}
