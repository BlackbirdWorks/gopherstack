package glue_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

func Test_CreateSchema_ReturnedCopyNotAliasedByLaterUpdate(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend(testAccountID, testRegion)
	defer b.Close()

	_, err := b.CreateRegistry("reg1", "", nil)
	require.NoError(t, err)

	created, err := b.CreateSchema("reg1", "sch1", "AVRO", "NONE", "", nil)
	require.NoError(t, err)
	require.Equal(t, "NONE", created.Compatibility)

	// UpdateSchema must not retroactively change a struct already handed back to a
	// caller from CreateSchema.
	require.NoError(t, b.UpdateSchema("reg1", "sch1", "BACKWARD", ""))
	assert.Equal(t, "NONE", created.Compatibility, "CreateSchema's returned copy must not alias backend state")

	fresh, err := b.DescribeSchema("reg1", "sch1")
	require.NoError(t, err)
	assert.Equal(t, "BACKWARD", fresh.Compatibility)
}
