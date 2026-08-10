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

func Test_CreateSchema_RejectsInvalidCompatibility(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		compatibility string
		wantErr       bool
	}{
		{name: "empty_allowed", compatibility: ""},
		{name: "none", compatibility: "NONE"},
		{name: "disabled", compatibility: "DISABLED"},
		{name: "backward", compatibility: "BACKWARD"},
		{name: "backward_all", compatibility: "BACKWARD_ALL"},
		{name: "forward", compatibility: "FORWARD"},
		{name: "forward_all", compatibility: "FORWARD_ALL"},
		{name: "full", compatibility: "FULL"},
		{name: "full_all", compatibility: "FULL_ALL"},
		{name: "garbage_rejected", compatibility: "SOMETIMES", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glue.NewInMemoryBackend(testAccountID, testRegion)
			defer b.Close()

			_, err := b.CreateRegistry("reg", "", nil)
			require.NoError(t, err)

			_, err = b.CreateSchema("reg", "sch", "AVRO", tt.compatibility, "", nil)
			if tt.wantErr {
				require.ErrorIs(t, err, glue.ErrValidation)

				return
			}

			require.NoError(t, err)
		})
	}
}

func Test_UpdateSchema_RejectsInvalidCompatibility(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend(testAccountID, testRegion)
	defer b.Close()

	_, err := b.CreateRegistry("reg", "", nil)
	require.NoError(t, err)

	_, err = b.CreateSchema("reg", "sch", "AVRO", "NONE", "", nil)
	require.NoError(t, err)

	err = b.UpdateSchema("reg", "sch", "GARBAGE", "")
	require.ErrorIs(t, err, glue.ErrValidation)

	fresh, err := b.DescribeSchema("reg", "sch")
	require.NoError(t, err)
	assert.Equal(t, "NONE", fresh.Compatibility, "rejected update must not partially apply")
}

func Test_RegisterSchemaVersion_DisabledCompatibility(t *testing.T) {
	t.Parallel()

	const (
		def1 = `{"type":"record","name":"A","fields":[{"name":"id","type":"int"}]}`
		def2 = `{"type":"record","name":"A","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}`
	)

	tests := []struct {
		name          string
		compatibility string
		wantSecondErr bool
	}{
		{name: "disabled_blocks_second_version", compatibility: "DISABLED", wantSecondErr: true},
		{name: "none_allows_second_version", compatibility: "NONE"},
		{name: "backward_allows_second_version_unchecked", compatibility: "BACKWARD"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glue.NewInMemoryBackend(testAccountID, testRegion)
			defer b.Close()

			_, err := b.CreateRegistry("reg", "", nil)
			require.NoError(t, err)

			_, err = b.CreateSchema("reg", "sch", "AVRO", tt.compatibility, "", nil)
			require.NoError(t, err)

			_, err = b.RegisterSchemaVersion("reg", "sch", def1)
			require.NoError(t, err, "the first version must always be accepted regardless of compatibility mode")

			_, err = b.RegisterSchemaVersion("reg", "sch", def2)
			if tt.wantSecondErr {
				require.ErrorIs(t, err, glue.ErrValidation)

				return
			}

			require.NoError(t, err)
		})
	}
}
