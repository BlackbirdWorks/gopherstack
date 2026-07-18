package mediatailor_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediatailor"
)

// TestListFunctions_Paginates verifies ListFunctions actually respects
// maxResults/nextToken instead of always returning every function with an
// empty NextToken.
func TestListFunctions_Paginates(t *testing.T) {
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
