package iotwireless_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotwireless"
)

// TestInMemoryBackend_SortedListFuotaTasks verifies deterministic sort order for FUOTA tasks.
func TestInMemoryBackend_SortedListFuotaTasks(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	for _, name := range []string{"ft-z", "ft-a", "ft-m"} {
		_, err := b.CreateFuotaTask(testAccountID, testRegion, name, "", "", "", nil)
		require.NoError(t, err)
	}

	tasks := b.ListFuotaTasks(testAccountID, testRegion)
	require.Len(t, tasks, 3)
	assert.Equal(t, "ft-a", tasks[0].Name)
	assert.Equal(t, "ft-m", tasks[1].Name)
	assert.Equal(t, "ft-z", tasks[2].Name)
}
