package mediatailor_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediatailor"
)

// TestBackend_PutPlaybackConfiguration_OnlyNameRequired verifies the backend
// only requires Name. A previous pass required AdDecisionServerUrl and
// VideoContentSourceUrl too, which rejects requests a real SDK client is
// allowed to send — confirmed against aws-sdk-go-v2/service/mediatailor's
// validators.go and botocore's service-2.json, whose only required member
// for PutPlaybackConfigurationInput is Name.
func TestBackend_PutPlaybackConfiguration_OnlyNameRequired(t *testing.T) {
	t.Parallel()

	b := mediatailor.NewInMemoryBackend("000000000000", "us-east-1")

	cfg, err := b.PutPlaybackConfiguration("cfg", "", "", nil)
	require.NoError(t, err, "AdDecisionServerUrl and VideoContentSourceUrl must both be optional")
	assert.Equal(t, "cfg", cfg.Name)

	_, err = b.PutPlaybackConfiguration("", "", "", nil)
	assert.Error(t, err, "Name must still be required")
}

// TestBackend_ListFunctions_Paginates verifies ListFunctions actually
// respects maxResults/nextToken instead of always returning every function
// with an empty NextToken.
func TestBackend_ListFunctions_Paginates(t *testing.T) {
	t.Parallel()

	b := mediatailor.NewInMemoryBackend("000000000000", "us-east-1")

	for _, id := range []string{"fn-a", "fn-b", "fn-c"} {
		_, err := b.PutFunction(id, "AWS_LAMBDA", "", nil)
		require.NoError(t, err)
	}

	page1, next1, err := b.ListFunctions(1, "")
	require.NoError(t, err)
	require.Len(t, page1, 1)
	require.NotEmpty(t, next1, "a NextToken must be returned when more pages remain")

	page2, next2, err := b.ListFunctions(1, next1)
	require.NoError(t, err)
	require.Len(t, page2, 1)
	assert.NotEqual(t, page1[0].FunctionID, page2[0].FunctionID, "pages must not repeat items")

	page3, next3, err := b.ListFunctions(10, next2)
	require.NoError(t, err)
	assert.Len(t, page3, 1, "the last page must contain the remaining item")
	assert.Empty(t, next3, "the final page must not carry a NextToken")
}

// TestBackend_GetChannelSchedule_Paginates verifies GetChannelSchedule
// actually respects maxResults/nextToken instead of always returning every
// program with an empty NextToken.
func TestBackend_GetChannelSchedule_Paginates(t *testing.T) {
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
