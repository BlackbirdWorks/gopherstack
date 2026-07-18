package mediatailor_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediatailor"
)

// TestGetChannelSchedule_Paginates verifies GetChannelSchedule actually
// respects maxResults/nextToken instead of always returning every program
// with an empty NextToken.
func TestGetChannelSchedule_Paginates(t *testing.T) {
	t.Parallel()

	b := mediatailor.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateChannel("ch1", "LOOP", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateSourceLocation("sl1", "https://example.com", nil)
	require.NoError(t, err)

	_, err = b.CreateVodSource("sl1", "vs1", nil, nil)
	require.NoError(t, err)

	for _, name := range []string{"prog-a", "prog-b", "prog-c"} {
		_, progErr := b.CreateProgram("ch1", name, "sl1", "vs1", "", nil)
		require.NoError(t, progErr)
	}

	page1, next1, err := b.GetChannelSchedule("ch1", 1, "")
	require.NoError(t, err)
	require.Len(t, page1, 1)
	require.NotEmpty(t, next1, "a NextToken must be returned when more pages remain")

	page2, _, err := b.GetChannelSchedule("ch1", 1, next1)
	require.NoError(t, err)
	require.Len(t, page2, 1)
	assert.NotEqual(t, page1[0].ProgramName, page2[0].ProgramName, "pages must not repeat items")
}
