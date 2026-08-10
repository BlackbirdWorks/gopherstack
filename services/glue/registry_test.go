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

	created, _, err := b.CreateSchema("reg1", "sch1", "AVRO", "NONE", "", "", nil)
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

			_, _, err = b.CreateSchema("reg", "sch", "AVRO", tt.compatibility, "", "", nil)
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

	_, _, err = b.CreateSchema("reg", "sch", "AVRO", "NONE", "", "", nil)
	require.NoError(t, err)

	err = b.UpdateSchema("reg", "sch", "GARBAGE", "")
	require.ErrorIs(t, err, glue.ErrValidation)

	fresh, err := b.DescribeSchema("reg", "sch")
	require.NoError(t, err)
	assert.Equal(t, "NONE", fresh.Compatibility, "rejected update must not partially apply")
}

func Test_CreateSchema_WithDefinition(t *testing.T) {
	t.Parallel()

	const def = `{"type":"record","name":"A","fields":[{"name":"id","type":"int"}]}`

	tests := []struct {
		name        string
		definition  string
		wantVersion bool
		wantLatest  int64
		wantNext    int64
	}{
		{name: "with_definition_creates_version_one", definition: def, wantVersion: true, wantLatest: 1, wantNext: 2},
		{name: "without_definition_creates_no_version", definition: "", wantVersion: false, wantLatest: 0, wantNext: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glue.NewInMemoryBackend(testAccountID, testRegion)
			defer b.Close()

			_, err := b.CreateRegistry("reg", "", nil)
			require.NoError(t, err)

			s, sv, err := b.CreateSchema("reg", "sch", "AVRO", "NONE", "", tt.definition, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.wantLatest, s.LatestSchemaVersion)
			assert.Equal(t, tt.wantNext, s.NextSchemaVersion)

			if !tt.wantVersion {
				assert.Nil(t, sv)
				assert.Empty(t, b.ListSchemaVersions("reg", "sch"))

				return
			}

			require.NotNil(t, sv)
			assert.Equal(t, int64(1), sv.VersionNumber)
			assert.JSONEq(t, def, sv.SchemaDefinition)
			assert.NotEmpty(t, sv.SchemaVersionID)

			versions := b.ListSchemaVersions("reg", "sch")
			require.Len(t, versions, 1)
			assert.Equal(t, sv.SchemaVersionID, versions[0].SchemaVersionID)
		})
	}
}

func Test_CreateSchema_RejectsInvalidDefinition(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend(testAccountID, testRegion)
	defer b.Close()

	_, err := b.CreateRegistry("reg", "", nil)
	require.NoError(t, err)

	_, _, err = b.CreateSchema("reg", "sch", "AVRO", "NONE", "", "not json", nil)
	require.ErrorIs(t, err, glue.ErrValidation)

	_, err = b.DescribeSchema("reg", "sch")
	assert.ErrorIs(t, err, glue.ErrNotFound, "a rejected definition must not leave a half-created schema")
}

// Test_CreateSchema_DisabledCompatibility_VersionSlotInteraction covers the
// interaction flagged in gopherstack-i60f: DISABLED permits exactly one
// version, so where that first version comes from determines whether a
// later RegisterSchemaVersion is legal.
func Test_CreateSchema_DisabledCompatibility_VersionSlotInteraction(t *testing.T) {
	t.Parallel()

	const (
		def1 = `{"type":"record","name":"A","fields":[{"name":"id","type":"int"}]}`
		def2 = `{"type":"record","name":"A","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}`
	)

	tests := []struct {
		name             string
		createDefinition string
		wantRegisterErr  bool
	}{
		{
			name:             "created_with_definition_leaves_no_room_for_a_second_version",
			createDefinition: def1,
			wantRegisterErr:  true,
		},
		{
			name:             "created_without_definition_still_allows_the_first_registration",
			createDefinition: "",
			wantRegisterErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glue.NewInMemoryBackend(testAccountID, testRegion)
			defer b.Close()

			_, err := b.CreateRegistry("reg", "", nil)
			require.NoError(t, err)

			s, sv, err := b.CreateSchema("reg", "sch", "AVRO", "DISABLED", "", tt.createDefinition, nil)
			require.NoError(t, err)

			if tt.createDefinition != "" {
				require.NotNil(t, sv)
				assert.Equal(t, int64(1), s.LatestSchemaVersion)
			} else {
				assert.Nil(t, sv)
				assert.Equal(t, int64(0), s.LatestSchemaVersion)
			}

			_, err = b.RegisterSchemaVersion("reg", "sch", def2)
			if tt.wantRegisterErr {
				require.ErrorIs(t, err, glue.ErrValidation)

				return
			}

			require.NoError(t, err, "the first version must always be accepted regardless of compatibility mode")
		})
	}
}

func Test_CreateSchema_WithDefinition_RoundTripsThroughPersistence(t *testing.T) {
	t.Parallel()

	const def = `{"type":"record","name":"A","fields":[{"name":"id","type":"int"}]}`

	orig := glue.NewInMemoryBackend(testAccountID, testRegion)
	defer orig.Close()

	_, err := orig.CreateRegistry("reg", "", nil)
	require.NoError(t, err)

	_, sv, err := orig.CreateSchema("reg", "sch", "AVRO", "NONE", "", def, nil)
	require.NoError(t, err)
	require.NotNil(t, sv)

	snap := orig.Snapshot(t.Context())
	require.NotNil(t, snap)

	restored := glue.NewInMemoryBackend(testAccountID, testRegion)
	defer restored.Close()
	require.NoError(t, restored.Restore(t.Context(), snap))

	s, err := restored.DescribeSchema("reg", "sch")
	require.NoError(t, err)
	assert.Equal(t, int64(1), s.LatestSchemaVersion)

	versions := restored.ListSchemaVersions("reg", "sch")
	require.Len(t, versions, 1)
	assert.Equal(t, sv.SchemaVersionID, versions[0].SchemaVersionID)
	assert.JSONEq(t, def, versions[0].SchemaDefinition)
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

			_, _, err = b.CreateSchema("reg", "sch", "AVRO", tt.compatibility, "", "", nil)
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
